use std::collections::BTreeSet;

use common::{
    CatalogObjectId, ColumnDefault, ConstraintId, ConstraintKind, ConstraintSchema, DbError,
    DependencyEdge, DependencyType, RelationKind, Result, StoredExpression, TableId,
};

use crate::CatalogSnapshot;

pub(crate) fn reconcile_constraints_and_dependencies(snapshot: &mut CatalogSnapshot) -> Result<()> {
    snapshot.dependencies = build_dependencies(snapshot)?;
    Ok(())
}

pub(crate) fn build_dependencies(snapshot: &CatalogSnapshot) -> Result<BTreeSet<DependencyEdge>> {
    let mut edges = BTreeSet::new();
    for table in snapshot.tables_by_id.values() {
        edge(
            &mut edges,
            CatalogObjectId::Table(table.id),
            CatalogObjectId::Schema(table.schema_id),
            DependencyType::Normal,
        );
        for column in &table.columns {
            let column_id = CatalogObjectId::Column {
                relation: table.id,
                column: column.object_id,
            };
            edge(
                &mut edges,
                column_id,
                CatalogObjectId::Table(table.id),
                DependencyType::Internal,
            );
            if let Some(default) = &column.default {
                let default_id = CatalogObjectId::ColumnDefault {
                    relation: table.id,
                    column: column.object_id,
                };
                edge(&mut edges, default_id, column_id, DependencyType::Internal);
                add_default_dependencies(&mut edges, default_id, table.id, default);
                if matches!(default, ColumnDefault::Nextval(sequence) if snapshot.sequences_by_id.get(sequence).is_some_and(|schema| schema.owned))
                {
                    let ColumnDefault::Nextval(sequence) = default else {
                        continue;
                    };
                    edge(
                        &mut edges,
                        CatalogObjectId::Sequence(*sequence),
                        column_id,
                        DependencyType::Auto,
                    );
                }
            }
        }
        if let RelationKind::Toast { base_table } = table.relation_kind {
            edge(
                &mut edges,
                CatalogObjectId::Table(table.id),
                CatalogObjectId::Table(base_table),
                DependencyType::Internal,
            );
        }
    }
    for view in snapshot.views_by_id.values() {
        edge(
            &mut edges,
            CatalogObjectId::View(view.id),
            CatalogObjectId::Schema(view.schema_id),
            DependencyType::Normal,
        );
        for column in &view.columns {
            edge(
                &mut edges,
                CatalogObjectId::Column {
                    relation: view.id,
                    column: column.object_id,
                },
                CatalogObjectId::View(view.id),
                DependencyType::Internal,
            );
        }
        for referenced in view.query.referenced_catalog_objects()? {
            edge(
                &mut edges,
                CatalogObjectId::View(view.id),
                referenced,
                DependencyType::Normal,
            );
        }
    }
    for index in snapshot.indexes_by_id.values() {
        edge(
            &mut edges,
            CatalogObjectId::Index(index.id),
            CatalogObjectId::Table(index.table),
            DependencyType::Auto,
        );
        for column in stable_columns(snapshot, index.table, &index.columns)? {
            edge(
                &mut edges,
                CatalogObjectId::Index(index.id),
                CatalogObjectId::Column {
                    relation: index.table,
                    column,
                },
                DependencyType::Normal,
            );
        }
    }
    for sequence in snapshot.sequences_by_id.values() {
        edge(
            &mut edges,
            CatalogObjectId::Sequence(sequence.id),
            CatalogObjectId::Schema(sequence.schema_id),
            DependencyType::Normal,
        );
    }
    for constraint in snapshot.constraints_by_id.values() {
        add_constraint_dependencies(snapshot, &mut edges, constraint)?;
    }
    for table in snapshot.statistics.keys() {
        edge(
            &mut edges,
            CatalogObjectId::Statistics(*table),
            CatalogObjectId::Table(*table),
            DependencyType::Auto,
        );
    }
    Ok(edges)
}

pub(crate) fn validate_constraints_and_dependencies(snapshot: &CatalogSnapshot) -> Result<()> {
    let mut names = BTreeSet::new();
    let mut greatest_id = None;
    for (id, constraint) in &snapshot.constraints_by_id {
        crate::system::constraint_oid(*id)?;
        if constraint.id != *id {
            return Err(DbError::internal(format!(
                "catalog constraint key {id} does not match object id {}",
                constraint.id
            )));
        }
        if constraint.name.is_empty() || !names.insert((constraint.table, constraint.name.clone()))
        {
            return Err(DbError::internal(format!(
                "catalog has duplicate or empty constraint name {} on table {}",
                constraint.name, constraint.table
            )));
        }
        if constraint.deferrable || constraint.initially_deferred || !constraint.validated {
            return Err(DbError::internal(format!(
                "constraint {} has unsupported deferred or unvalidated state",
                constraint.name
            )));
        }
        let table = snapshot
            .tables_by_id
            .get(&constraint.table)
            .ok_or_else(|| {
                DbError::internal(format!(
                    "constraint {} references missing table {}",
                    constraint.name, constraint.table
                ))
            })?;
        if table.relation_kind != RelationKind::User {
            return Err(DbError::internal(format!(
                "constraint {} belongs to a hidden relation",
                constraint.name
            )));
        }
        match &constraint.kind {
            ConstraintKind::Check { expression } => {
                common::validate_stored_expression_shape(expression)?;
                validate_expression_references(snapshot, table.id, expression)?;
            }
            ConstraintKind::PrimaryKey { columns, index } => {
                validate_key_constraint(snapshot, constraint, columns, *index, true)?;
            }
            ConstraintKind::Unique { columns, index } => {
                validate_key_constraint(snapshot, constraint, columns, *index, false)?;
            }
            ConstraintKind::ForeignKey {
                columns,
                referenced_table,
                referenced_constraint,
                referenced_columns,
                supporting_index,
                ..
            } => {
                validate_stable_columns(table, columns, &constraint.name)?;
                let parent = snapshot.tables_by_id.get(referenced_table).ok_or_else(|| {
                    DbError::internal(format!(
                        "foreign key {} references missing table {referenced_table}",
                        constraint.name
                    ))
                })?;
                validate_stable_columns(parent, referenced_columns, &constraint.name)?;
                if columns.is_empty() || columns.len() != referenced_columns.len() {
                    return Err(DbError::internal(format!(
                        "foreign key {} has invalid column lists",
                        constraint.name
                    )));
                }
                let referenced = snapshot
                    .constraints_by_id
                    .get(referenced_constraint)
                    .ok_or_else(|| {
                        DbError::internal(format!(
                            "foreign key {} references missing constraint {referenced_constraint}",
                            constraint.name
                        ))
                    })?;
                let referenced_key_columns = match &referenced.kind {
                    ConstraintKind::PrimaryKey { columns, .. }
                    | ConstraintKind::Unique { columns, .. } => columns,
                    _ => {
                        return Err(DbError::internal(format!(
                            "foreign key {} references a non-key constraint",
                            constraint.name
                        )));
                    }
                };
                if referenced.table != *referenced_table
                    || referenced_key_columns != referenced_columns
                {
                    return Err(DbError::internal(format!(
                        "foreign key {} does not match its referenced constraint",
                        constraint.name
                    )));
                }
                for (source, target) in columns.iter().zip(referenced_columns) {
                    let source = table.column_by_object_id(*source).ok_or_else(|| {
                        DbError::internal("validated foreign-key source column disappeared")
                    })?;
                    let target = parent.column_by_object_id(*target).ok_or_else(|| {
                        DbError::internal("validated foreign-key target column disappeared")
                    })?;
                    if source.data_type != target.data_type
                        || source.wire_type() != target.wire_type()
                        || source.max_length != target.max_length
                    {
                        return Err(DbError::internal(format!(
                            "foreign key {} has incompatible column types",
                            constraint.name
                        )));
                    }
                }
                if supporting_index
                    .is_some_and(|index| !snapshot.indexes_by_id.contains_key(&index))
                {
                    return Err(DbError::internal(format!(
                        "foreign key {} references missing supporting index",
                        constraint.name
                    )));
                }
            }
        }
        greatest_id = Some(greatest_id.map_or(*id, |greatest: ConstraintId| greatest.max(*id)));
    }
    if greatest_id.is_some_and(|id| snapshot.next_constraint_id <= id) {
        return Err(DbError::internal(
            "catalog constraint allocator does not exceed every live constraint id",
        ));
    }
    for index in snapshot.indexes_by_id.values() {
        let Some(constraint_id) = index.constraint else {
            continue;
        };
        let constraint = snapshot
            .constraints_by_id
            .get(&constraint_id)
            .ok_or_else(|| {
                DbError::internal(format!(
                    "index {} references missing constraint {constraint_id}",
                    index.name
                ))
            })?;
        let backing_index = match constraint.kind {
            ConstraintKind::PrimaryKey { index, .. } | ConstraintKind::Unique { index, .. } => {
                index
            }
            _ => {
                return Err(DbError::internal(format!(
                    "index {} is owned by a non-key constraint",
                    index.name
                )));
            }
        };
        if backing_index != index.id
            || constraint.table != index.table
            || constraint.name != index.name
        {
            return Err(DbError::internal(format!(
                "index {} does not match its owning constraint",
                index.name
            )));
        }
    }
    let expected = build_dependencies(snapshot)?;
    if snapshot.dependencies != expected {
        return Err(DbError::internal(
            "serialized catalog dependency graph does not match catalog objects",
        ));
    }
    validate_dependency_targets(snapshot, &expected)?;
    validate_internal_cycles(&expected)
}

pub(crate) fn dependency_drop_closure(
    snapshot: &CatalogSnapshot,
    roots: impl IntoIterator<Item = CatalogObjectId>,
) -> Result<BTreeSet<CatalogObjectId>> {
    let roots: BTreeSet<_> = roots.into_iter().collect();
    for root in &roots {
        if snapshot.dependencies.iter().any(|edge| {
            edge.dependent == *root
                && edge.dependency_type == DependencyType::Internal
                && !roots.contains(&edge.referenced)
        }) {
            return Err(DbError::plan(
                common::SqlState::DependentObjectsStillExist,
                format!("cannot drop internally owned catalog object {root:?} directly"),
            ));
        }
    }

    let mut closure = roots;
    loop {
        let mut changed = false;
        for edge in &snapshot.dependencies {
            if closure.contains(&edge.referenced)
                && matches!(
                    edge.dependency_type,
                    DependencyType::Auto | DependencyType::Internal
                )
                && closure.insert(edge.dependent)
            {
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    if let Some(edge) = snapshot.dependencies.iter().find(|edge| {
        closure.contains(&edge.referenced)
            && edge.dependency_type == DependencyType::Normal
            && !closure.contains(&edge.dependent)
    }) {
        return Err(DbError::plan(
            common::SqlState::DependentObjectsStillExist,
            format!(
                "cannot drop {:?} because {:?} depends on it",
                edge.referenced, edge.dependent
            ),
        ));
    }
    Ok(closure)
}

fn validate_key_constraint(
    snapshot: &CatalogSnapshot,
    constraint: &ConstraintSchema,
    columns: &[common::ColumnObjectId],
    index: common::IndexId,
    primary: bool,
) -> Result<()> {
    let table = snapshot
        .tables_by_id
        .get(&constraint.table)
        .ok_or_else(|| DbError::internal("validated constraint table disappeared"))?;
    validate_stable_columns(table, columns, &constraint.name)?;
    let index_schema = snapshot.indexes_by_id.get(&index).ok_or_else(|| {
        DbError::internal(format!(
            "constraint {} references missing backing index {index}",
            constraint.name
        ))
    })?;
    let dense: Vec<_> = columns
        .iter()
        .map(|column| {
            table
                .dense_column_id(*column)
                .ok_or_else(|| DbError::internal("validated key column disappeared"))
        })
        .collect::<Result<_>>()?;
    if index_schema.table != table.id
        || !index_schema.unique
        || index_schema.constraint != Some(constraint.id)
        || index_schema.columns != dense
        || (primary && table.primary_key != dense)
    {
        return Err(DbError::internal(format!(
            "constraint {} does not match its backing index or table projection",
            constraint.name
        )));
    }
    Ok(())
}

fn validate_stable_columns(
    table: &common::TableSchema,
    columns: &[common::ColumnObjectId],
    constraint: &str,
) -> Result<()> {
    let mut seen = BTreeSet::new();
    if columns.is_empty() {
        return Err(DbError::internal(format!(
            "constraint {constraint} has no columns"
        )));
    }
    for column in columns {
        if table.column_by_object_id(*column).is_none() || !seen.insert(*column) {
            return Err(DbError::internal(format!(
                "constraint {constraint} has a missing or duplicate stable column"
            )));
        }
    }
    Ok(())
}

fn validate_expression_references(
    snapshot: &CatalogSnapshot,
    relation: TableId,
    expression: &StoredExpression,
) -> Result<()> {
    let table = snapshot
        .tables_by_id
        .get(&relation)
        .ok_or_else(|| DbError::internal("stored expression relation is missing"))?;
    let mut error = None;
    expression.root.for_each_column_reference(&mut |column| {
        if error.is_none() && table.column_by_object_id(column).is_none() {
            error = Some(DbError::internal(format!(
                "stored expression references unknown stable column id {column}"
            )));
        }
    });
    expression
        .root
        .for_each_sequence_reference(&mut |sequence| {
            if error.is_none() && !snapshot.sequences_by_id.contains_key(&sequence) {
                error = Some(DbError::internal(format!(
                    "stored expression references unknown sequence id {sequence}"
                )));
            }
        });
    if let Some(error) = error {
        return Err(error);
    }
    Ok(())
}

fn validate_dependency_targets(
    snapshot: &CatalogSnapshot,
    dependencies: &BTreeSet<DependencyEdge>,
) -> Result<()> {
    for dependency in dependencies {
        if !object_exists(snapshot, dependency.dependent)
            || !object_exists(snapshot, dependency.referenced)
        {
            return Err(DbError::internal(format!(
                "catalog dependency contains dangling object: {dependency:?}"
            )));
        }
    }
    Ok(())
}

fn object_exists(snapshot: &CatalogSnapshot, object: CatalogObjectId) -> bool {
    match object {
        CatalogObjectId::Schema(id) => snapshot.schemas_by_id.contains_key(&id),
        CatalogObjectId::Table(id) => snapshot.tables_by_id.contains_key(&id),
        CatalogObjectId::View(id) => snapshot.views_by_id.contains_key(&id),
        CatalogObjectId::Index(id) => snapshot.indexes_by_id.contains_key(&id),
        CatalogObjectId::Sequence(id) => snapshot.sequences_by_id.contains_key(&id),
        CatalogObjectId::Constraint(id) => snapshot.constraints_by_id.contains_key(&id),
        CatalogObjectId::Function(id) => common::stored_query_function_exists(id),
        CatalogObjectId::SystemRelation(oid) => crate::SystemView::from_relation_oid(oid).is_some(),
        CatalogObjectId::Statistics(id) => snapshot.statistics.contains_key(&id),
        CatalogObjectId::Column { relation, column } => {
            snapshot
                .tables_by_id
                .get(&relation)
                .is_some_and(|table| table.column_by_object_id(column).is_some())
                || snapshot
                    .views_by_id
                    .get(&relation)
                    .is_some_and(|view| view.columns.iter().any(|item| item.object_id == column))
        }
        CatalogObjectId::ColumnDefault { relation, column } => snapshot
            .tables_by_id
            .get(&relation)
            .and_then(|table| table.column_by_object_id(column))
            .is_some_and(|column| column.default.is_some()),
    }
}

fn validate_internal_cycles(dependencies: &BTreeSet<DependencyEdge>) -> Result<()> {
    for start in dependencies
        .iter()
        .filter(|edge| edge.dependency_type == DependencyType::Internal)
        .map(|edge| edge.dependent)
    {
        let mut current = start;
        let mut seen = BTreeSet::new();
        while seen.insert(current) {
            let Some(next) = dependencies
                .iter()
                .find(|edge| {
                    edge.dependent == current && edge.dependency_type == DependencyType::Internal
                })
                .map(|edge| edge.referenced)
            else {
                break;
            };
            current = next;
        }
        if current == start {
            return Err(DbError::internal(
                "catalog dependency graph contains an internal ownership cycle",
            ));
        }
    }
    Ok(())
}

fn add_constraint_dependencies(
    snapshot: &CatalogSnapshot,
    edges: &mut BTreeSet<DependencyEdge>,
    constraint: &ConstraintSchema,
) -> Result<()> {
    let object = CatalogObjectId::Constraint(constraint.id);
    edge(
        edges,
        object,
        CatalogObjectId::Table(constraint.table),
        DependencyType::Auto,
    );
    match &constraint.kind {
        ConstraintKind::Check { expression } => {
            add_expression_dependencies(edges, object, constraint.table, expression);
        }
        ConstraintKind::PrimaryKey { columns, index }
        | ConstraintKind::Unique { columns, index } => {
            for column in columns {
                edge(
                    edges,
                    object,
                    CatalogObjectId::Column {
                        relation: constraint.table,
                        column: *column,
                    },
                    DependencyType::Normal,
                );
            }
            edge(
                edges,
                CatalogObjectId::Index(*index),
                object,
                DependencyType::Internal,
            );
        }
        ConstraintKind::ForeignKey {
            columns,
            referenced_table,
            referenced_constraint,
            referenced_columns,
            supporting_index,
            ..
        } => {
            for column in columns {
                edge(
                    edges,
                    object,
                    CatalogObjectId::Column {
                        relation: constraint.table,
                        column: *column,
                    },
                    DependencyType::Normal,
                );
            }
            edge(
                edges,
                object,
                CatalogObjectId::Table(*referenced_table),
                DependencyType::Normal,
            );
            edge(
                edges,
                object,
                CatalogObjectId::Constraint(*referenced_constraint),
                DependencyType::Normal,
            );
            for column in referenced_columns {
                edge(
                    edges,
                    object,
                    CatalogObjectId::Column {
                        relation: *referenced_table,
                        column: *column,
                    },
                    DependencyType::Normal,
                );
            }
            if let Some(index) = supporting_index {
                if !snapshot.indexes_by_id.contains_key(index) {
                    return Err(DbError::internal(format!(
                        "foreign key {} references missing supporting index {index}",
                        constraint.name
                    )));
                }
                edge(
                    edges,
                    object,
                    CatalogObjectId::Index(*index),
                    DependencyType::Normal,
                );
            }
        }
    }
    Ok(())
}

fn add_default_dependencies(
    edges: &mut BTreeSet<DependencyEdge>,
    dependent: CatalogObjectId,
    relation: TableId,
    default: &ColumnDefault,
) {
    match default {
        ColumnDefault::Nextval(sequence) => edge(
            edges,
            dependent,
            CatalogObjectId::Sequence(*sequence),
            DependencyType::Normal,
        ),
        ColumnDefault::Expr(expression) => {
            add_expression_dependencies(edges, dependent, relation, expression);
        }
        ColumnDefault::Const(_) => {}
    }
}

fn add_expression_dependencies(
    edges: &mut BTreeSet<DependencyEdge>,
    dependent: CatalogObjectId,
    relation: TableId,
    expression: &StoredExpression,
) {
    expression.root.for_each_column_reference(&mut |column| {
        edge(
            edges,
            dependent,
            CatalogObjectId::Column { relation, column },
            DependencyType::Normal,
        );
    });
    expression
        .root
        .for_each_function_reference(&mut |function| {
            edge(
                edges,
                dependent,
                CatalogObjectId::Function(function),
                DependencyType::Normal,
            );
        });
    expression
        .root
        .for_each_sequence_reference(&mut |sequence| {
            edge(
                edges,
                dependent,
                CatalogObjectId::Sequence(sequence),
                DependencyType::Normal,
            );
        });
}

fn stable_columns(
    snapshot: &CatalogSnapshot,
    table: TableId,
    columns: &[common::ColumnId],
) -> Result<Vec<common::ColumnObjectId>> {
    let schema = snapshot
        .tables_by_id
        .get(&table)
        .ok_or_else(|| DbError::internal(format!("catalog references missing table id {table}")))?;
    columns
        .iter()
        .map(|column| {
            schema.stable_column_id(*column).ok_or_else(|| {
                DbError::internal(format!(
                    "catalog references missing column {column} on table {}",
                    schema.name
                ))
            })
        })
        .collect()
}

fn edge(
    edges: &mut BTreeSet<DependencyEdge>,
    dependent: CatalogObjectId,
    referenced: CatalogObjectId,
    dependency_type: DependencyType,
) {
    edges.insert(DependencyEdge {
        dependent,
        referenced,
        dependency_type,
    });
}
