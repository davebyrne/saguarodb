use common::{DataType, DbError, ParsedColumnDef, Result, SqlState, Value};
use sqlparser::ast as sql;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::{
    Assignment, BinOp, Expr, FromItem, FunctionArg, InsertSource, JoinType, OrderByItem,
    SelectItem, SelectStatement, Statement, UnaryOp,
};

pub fn parse_statement(sql: &str) -> Result<Statement> {
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)
        .map_err(|err| parse_error(format!("failed to parse SQL: {err}")))?;

    if statements.len() != 1 {
        return Err(parse_error("expected exactly one SQL statement"));
    }

    convert_statement(statements.remove(0))
}

fn convert_statement(statement: sql::Statement) -> Result<Statement> {
    match statement {
        sql::Statement::CreateTable(table) => convert_create_table(table),
        sql::Statement::Drop {
            object_type,
            if_exists,
            mut names,
            cascade,
            restrict,
            purge,
            temporary,
        } => {
            if object_type != sql::ObjectType::Table
                || if_exists
                || names.len() != 1
                || cascade
                || restrict
                || purge
                || temporary
            {
                return unsupported("unsupported DROP TABLE form");
            }

            Ok(Statement::DropTable {
                name: object_name(&names.remove(0))?,
            })
        }
        sql::Statement::Insert(insert) => convert_insert(insert),
        sql::Statement::Query(query) => Ok(Statement::Select(convert_query_to_select(*query)?)),
        sql::Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
            or,
        } => {
            if from.is_some() || returning.is_some() || or.is_some() {
                return unsupported("unsupported UPDATE form");
            }

            let table = table_name_from_table_with_joins(&table)?;
            let assignments = assignments
                .into_iter()
                .map(convert_assignment)
                .collect::<Result<Vec<_>>>()?;
            let filter = selection.map(|expr| convert_expr(&expr)).transpose()?;

            Ok(Statement::Update {
                table,
                assignments,
                filter,
            })
        }
        sql::Statement::Delete(delete) => convert_delete(delete),
        sql::Statement::Explain {
            describe_alias,
            analyze,
            verbose,
            query_plan,
            estimate,
            statement,
            format,
            options,
        } => {
            if describe_alias != sql::DescribeAlias::Explain
                || analyze
                || verbose
                || query_plan
                || estimate
                || format.is_some()
                || options.is_some()
            {
                return unsupported("unsupported EXPLAIN form");
            }
            match convert_statement(*statement)? {
                Statement::Select(select) => {
                    Ok(Statement::Explain(Box::new(Statement::Select(select))))
                }
                _ => unsupported("EXPLAIN supports SELECT only in v1"),
            }
        }
        _ => unsupported("unsupported SQL statement"),
    }
}

fn convert_create_table(table: sql::CreateTable) -> Result<Statement> {
    let sql::CreateTable {
        name,
        columns,
        constraints,
        hive_distribution,
        hive_formats,
        table_properties,
        with_options,
        file_format,
        location,
        or_replace,
        temporary,
        external,
        global,
        if_not_exists,
        transient,
        volatile,
        iceberg,
        query,
        without_rowid,
        like,
        clone,
        engine,
        comment,
        auto_increment_offset,
        default_charset,
        collation,
        on_commit,
        on_cluster,
        primary_key: clickhouse_primary_key,
        order_by,
        partition_by,
        cluster_by,
        clustered_by,
        options,
        inherits,
        strict,
        copy_grants,
        enable_schema_evolution,
        change_tracking,
        data_retention_time_in_days,
        max_data_extension_time_in_days,
        default_ddl_collation,
        with_aggregation_policy,
        with_row_access_policy,
        with_tags,
        external_volume,
        base_location,
        catalog,
        catalog_sync,
        storage_serialization_policy,
        ..
    } = table;

    if or_replace
        || temporary
        || external
        || global.is_some()
        || if_not_exists
        || transient
        || volatile
        || iceberg
        || !matches!(hive_distribution, sql::HiveDistributionStyle::NONE)
        || hive_formats.as_ref().is_some_and(hive_format_has_options)
        || !table_properties.is_empty()
        || !with_options.is_empty()
        || file_format.is_some()
        || location.is_some()
        || query.is_some()
        || without_rowid
        || like.is_some()
        || clone.is_some()
        || engine.is_some()
        || comment.is_some()
        || auto_increment_offset.is_some()
        || default_charset.is_some()
        || collation.is_some()
        || on_commit.is_some()
        || on_cluster.is_some()
        || clickhouse_primary_key.is_some()
        || order_by.is_some()
        || partition_by.is_some()
        || cluster_by.is_some()
        || clustered_by.is_some()
        || options.as_ref().is_some_and(|options| !options.is_empty())
        || inherits.is_some()
        || strict
        || copy_grants
        || enable_schema_evolution.is_some()
        || change_tracking.is_some()
        || data_retention_time_in_days.is_some()
        || max_data_extension_time_in_days.is_some()
        || default_ddl_collation.is_some()
        || with_aggregation_policy.is_some()
        || with_row_access_policy.is_some()
        || with_tags.is_some()
        || external_volume.is_some()
        || base_location.is_some()
        || catalog.is_some()
        || catalog_sync.is_some()
        || storage_serialization_policy.is_some()
    {
        return unsupported("unsupported CREATE TABLE form");
    }

    let mut primary_key = Vec::new();
    let columns = columns
        .into_iter()
        .map(|column| convert_column_def(column, &mut primary_key))
        .collect::<Result<Vec<_>>>()?;

    for constraint in constraints {
        match constraint {
            sql::TableConstraint::PrimaryKey {
                name,
                index_name,
                index_type,
                columns,
                index_options,
                characteristics,
            } => {
                if name.is_some()
                    || index_name.is_some()
                    || index_type.is_some()
                    || !index_options.is_empty()
                    || characteristics.is_some()
                {
                    return unsupported("unsupported PRIMARY KEY constraint form");
                }
                set_primary_key(
                    &mut primary_key,
                    columns.iter().map(ident_name).collect::<Result<Vec<_>>>()?,
                )?;
            }
            _ => return unsupported("unsupported table constraint"),
        }
    }

    Ok(Statement::CreateTable {
        name: object_name(&name)?,
        columns,
        primary_key,
    })
}

fn convert_column_def(
    column: sql::ColumnDef,
    primary_key: &mut Vec<String>,
) -> Result<ParsedColumnDef> {
    let mut nullable = true;

    for option in &column.options {
        if option.name.is_some() {
            return unsupported("unsupported named column constraint");
        }

        match &option.option {
            sql::ColumnOption::Null => nullable = true,
            sql::ColumnOption::NotNull => nullable = false,
            sql::ColumnOption::Unique {
                is_primary,
                characteristics,
            } => {
                if !is_primary {
                    return unsupported("unsupported column option");
                }
                if characteristics.is_some() {
                    return unsupported("unsupported PRIMARY KEY constraint form");
                }
                let column_name = ident_name(&column.name)?;
                set_primary_key(primary_key, vec![column_name])?;
                nullable = false;
            }
            _ => return unsupported("unsupported column option"),
        }
    }

    Ok(ParsedColumnDef {
        name: ident_name(&column.name)?,
        data_type: convert_data_type(&column.data_type)?,
        nullable,
    })
}

fn set_primary_key(primary_key: &mut Vec<String>, columns: Vec<String>) -> Result<()> {
    if !primary_key.is_empty() {
        return unsupported("multiple PRIMARY KEY constraints");
    }
    *primary_key = columns;
    Ok(())
}

fn hive_format_has_options(format: &sql::HiveFormat) -> bool {
    format.row_format.is_some()
        || format
            .serde_properties
            .as_ref()
            .is_some_and(|properties| !properties.is_empty())
        || format.storage.is_some()
        || format.location.is_some()
}

fn convert_insert(insert: sql::Insert) -> Result<Statement> {
    let sql::Insert {
        table,
        table_alias,
        columns,
        source,
        or,
        ignore,
        overwrite,
        assignments,
        partitioned,
        after_columns,
        has_table_keyword,
        on,
        returning,
        replace_into,
        priority,
        insert_alias,
        settings,
        format_clause,
        ..
    } = insert;

    if table_alias.is_some()
        || or.is_some()
        || ignore
        || overwrite
        || !assignments.is_empty()
        || partitioned.is_some()
        || !after_columns.is_empty()
        || has_table_keyword
        || on.is_some()
        || returning.is_some()
        || replace_into
        || priority.is_some()
        || insert_alias.is_some()
        || settings.is_some()
        || format_clause.is_some()
    {
        return unsupported("unsupported INSERT form");
    }

    let sql::TableObject::TableName(table) = table else {
        return unsupported("unsupported INSERT target");
    };
    let source = source.ok_or_else(|| parse_error("INSERT requires a source"))?;
    let source = if let sql::SetExpr::Values(values) = source.body.as_ref() {
        if query_has_modifiers(&source) {
            return unsupported("unsupported INSERT VALUES source modifiers");
        }
        InsertSource::Values(
            values
                .rows
                .iter()
                .map(|row| row.iter().map(convert_expr).collect::<Result<Vec<_>>>())
                .collect::<Result<Vec<_>>>()?,
        )
    } else if matches!(source.body.as_ref(), sql::SetExpr::Select(_)) {
        InsertSource::Query(Box::new(convert_query_to_select(*source)?))
    } else {
        return unsupported("unsupported INSERT source");
    };

    Ok(Statement::Insert {
        table: object_name(&table)?,
        columns: columns.iter().map(ident_name).collect::<Result<Vec<_>>>()?,
        source,
    })
}

fn query_has_modifiers(query: &sql::Query) -> bool {
    query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
}

fn convert_delete(delete: sql::Delete) -> Result<Statement> {
    if !delete.tables.is_empty()
        || delete.using.is_some()
        || delete.returning.is_some()
        || !delete.order_by.is_empty()
        || delete.limit.is_some()
    {
        return unsupported("unsupported DELETE form");
    }

    let tables = match &delete.from {
        sql::FromTable::WithFromKeyword(tables) => tables,
        sql::FromTable::WithoutKeyword(tables) => tables,
    };
    if tables.len() != 1 {
        return unsupported("DELETE requires exactly one table");
    }

    Ok(Statement::Delete {
        table: table_name_from_table_with_joins(&tables[0])?,
        filter: delete
            .selection
            .map(|expr| convert_expr(&expr))
            .transpose()?,
    })
}

fn convert_query_to_select(query: sql::Query) -> Result<SelectStatement> {
    if query.with.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
    {
        return unsupported("unsupported SELECT query form");
    }

    let (limit, offset) = convert_limit_clause(query.limit_clause)?;
    let order_by = query
        .order_by
        .map(convert_order_by)
        .transpose()?
        .unwrap_or_default();

    let sql::SetExpr::Select(select) = *query.body else {
        return unsupported("unsupported SELECT body");
    };
    convert_select(*select, order_by, limit, offset)
}

fn convert_select(
    select: sql::Select,
    order_by: Vec<OrderByItem>,
    limit: Option<u64>,
    offset: Option<u64>,
) -> Result<SelectStatement> {
    if select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
    {
        return unsupported("unsupported SELECT form");
    }

    let group_by = match select.group_by {
        sql::GroupByExpr::Expressions(exprs, modifiers) if modifiers.is_empty() => {
            exprs.iter().map(convert_expr).collect::<Result<Vec<_>>>()?
        }
        sql::GroupByExpr::Expressions(_, _) | sql::GroupByExpr::All(_) => {
            return unsupported("unsupported GROUP BY form");
        }
    };

    Ok(SelectStatement {
        columns: select
            .projection
            .iter()
            .map(convert_select_item)
            .collect::<Result<Vec<_>>>()?,
        from: select
            .from
            .iter()
            .map(convert_table_with_joins)
            .collect::<Result<Vec<_>>>()?,
        filter: select.selection.as_ref().map(convert_expr).transpose()?,
        group_by,
        having: select.having.as_ref().map(convert_expr).transpose()?,
        order_by,
        limit,
        offset,
    })
}

fn convert_select_item(item: &sql::SelectItem) -> Result<SelectItem> {
    match item {
        sql::SelectItem::Wildcard(options) => {
            reject_wildcard_options(options)?;
            Ok(SelectItem::Wildcard)
        }
        sql::SelectItem::QualifiedWildcard(kind, options) => {
            reject_wildcard_options(options)?;
            let sql::SelectItemQualifiedWildcardKind::ObjectName(name) = kind else {
                return unsupported("unsupported qualified wildcard");
            };
            Ok(SelectItem::QualifiedWildcard(object_name(name)?))
        }
        sql::SelectItem::UnnamedExpr(expr) => Ok(SelectItem::Expression {
            expr: convert_expr(expr)?,
            alias: None,
        }),
        sql::SelectItem::ExprWithAlias { expr, alias } => Ok(SelectItem::Expression {
            expr: convert_expr(expr)?,
            alias: Some(ident_name(alias)?),
        }),
    }
}

fn convert_table_with_joins(table: &sql::TableWithJoins) -> Result<FromItem> {
    let mut item = convert_table_factor(&table.relation)?;
    for join in &table.joins {
        item = convert_join(item, join)?;
    }
    Ok(item)
}

fn convert_join(left: FromItem, join: &sql::Join) -> Result<FromItem> {
    let right = convert_table_factor(&join.relation)?;
    let (join_type, condition) = match &join.join_operator {
        sql::JoinOperator::Inner(constraint) | sql::JoinOperator::Join(constraint) => {
            (JoinType::Inner, required_on_constraint(constraint)?)
        }
        sql::JoinOperator::LeftOuter(constraint) | sql::JoinOperator::Left(constraint) => {
            (JoinType::Left, required_on_constraint(constraint)?)
        }
        sql::JoinOperator::RightOuter(constraint) | sql::JoinOperator::Right(constraint) => {
            (JoinType::Right, required_on_constraint(constraint)?)
        }
        sql::JoinOperator::FullOuter(constraint) => {
            (JoinType::Full, required_on_constraint(constraint)?)
        }
        sql::JoinOperator::CrossJoin => (JoinType::Cross, None),
        sql::JoinOperator::CrossApply
        | sql::JoinOperator::OuterApply
        | sql::JoinOperator::AsOf { .. }
        | sql::JoinOperator::Semi(_)
        | sql::JoinOperator::LeftSemi(_)
        | sql::JoinOperator::RightSemi(_)
        | sql::JoinOperator::Anti(_)
        | sql::JoinOperator::LeftAnti(_)
        | sql::JoinOperator::RightAnti(_)
        | sql::JoinOperator::StraightJoin(_) => return unsupported("unsupported JOIN form"),
    };

    Ok(FromItem::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type,
        condition,
    })
}

fn required_on_constraint(constraint: &sql::JoinConstraint) -> Result<Option<Expr>> {
    match constraint {
        sql::JoinConstraint::On(expr) => Ok(Some(convert_expr(expr)?)),
        sql::JoinConstraint::Using(_) => unsupported("USING joins are not supported"),
        sql::JoinConstraint::Natural => unsupported("NATURAL joins are not supported"),
        sql::JoinConstraint::None => unsupported("non-CROSS joins require an ON predicate"),
    }
}

fn convert_table_factor(table: &sql::TableFactor) -> Result<FromItem> {
    match table {
        sql::TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } => {
            if args.is_some()
                || !with_hints.is_empty()
                || version.is_some()
                || *with_ordinality
                || !partitions.is_empty()
                || json_path.is_some()
                || sample.is_some()
                || !index_hints.is_empty()
            {
                return unsupported("unsupported table factor");
            }
            let alias = alias.as_ref().map(table_alias_name).transpose()?;
            Ok(FromItem::Table {
                name: object_name(name)?,
                alias,
            })
        }
        _ => unsupported("unsupported table factor"),
    }
}

fn table_name_from_table_with_joins(table: &sql::TableWithJoins) -> Result<String> {
    if !table.joins.is_empty() {
        return unsupported("joins are not supported here");
    }
    let FromItem::Table { name, alias: None } = convert_table_factor(&table.relation)? else {
        return unsupported("expected table name");
    };
    Ok(name)
}

fn convert_order_by(order_by: sql::OrderBy) -> Result<Vec<OrderByItem>> {
    if order_by.interpolate.is_some() {
        return unsupported("unsupported ORDER BY form");
    }

    let sql::OrderByKind::Expressions(expressions) = order_by.kind else {
        return unsupported("unsupported ORDER BY form");
    };

    expressions
        .iter()
        .map(|item| {
            if item.with_fill.is_some() {
                return unsupported("unsupported ORDER BY form");
            }
            Ok(OrderByItem {
                expr: convert_expr(&item.expr)?,
                ascending: item.options.asc.unwrap_or(true),
                nulls_first: item.options.nulls_first,
            })
        })
        .collect()
}

fn convert_limit_clause(
    limit_clause: Option<sql::LimitClause>,
) -> Result<(Option<u64>, Option<u64>)> {
    match limit_clause {
        None => Ok((None, None)),
        Some(sql::LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) => {
            if !limit_by.is_empty() {
                return unsupported("unsupported LIMIT form");
            }
            Ok((
                limit.as_ref().map(convert_u64_expr).transpose()?,
                offset
                    .as_ref()
                    .map(|offset| convert_u64_expr(&offset.value))
                    .transpose()?,
            ))
        }
        Some(sql::LimitClause::OffsetCommaLimit { .. }) => unsupported("unsupported LIMIT form"),
    }
}

fn convert_assignment(assignment: sql::Assignment) -> Result<Assignment> {
    let sql::AssignmentTarget::ColumnName(column) = assignment.target else {
        return unsupported("unsupported assignment target");
    };

    Ok(Assignment {
        column: object_name(&column)?,
        value: convert_expr(&assignment.value)?,
    })
}

fn convert_expr(expr: &sql::Expr) -> Result<Expr> {
    match expr {
        sql::Expr::Identifier(ident) => Ok(Expr::ColumnRef {
            table: None,
            column: ident_name(ident)?,
        }),
        sql::Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [table, column] => Ok(Expr::ColumnRef {
                table: Some(ident_name(table)?),
                column: ident_name(column)?,
            }),
            _ => unsupported("unsupported qualified identifier"),
        },
        sql::Expr::Value(value) => convert_value(&value.value),
        sql::Expr::Nested(expr) => convert_expr(expr),
        sql::Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(convert_expr(left)?),
            op: convert_bin_op(op)?,
            right: Box::new(convert_expr(right)?),
        }),
        sql::Expr::UnaryOp { op, expr } => match op {
            sql::UnaryOperator::Minus => Ok(Expr::UnaryOp {
                op: UnaryOp::Neg,
                expr: Box::new(convert_expr(expr)?),
            }),
            sql::UnaryOperator::Not => Ok(Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(convert_expr(expr)?),
            }),
            sql::UnaryOperator::Plus => convert_expr(expr),
            _ => unsupported("unsupported unary operator"),
        },
        sql::Expr::IsNull(expr) => Ok(Expr::IsNull(Box::new(convert_expr(expr)?))),
        sql::Expr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(convert_expr(expr)?))),
        sql::Expr::InList {
            expr,
            list,
            negated,
        } => Ok(Expr::InList {
            expr: Box::new(convert_expr(expr)?),
            list: list.iter().map(convert_expr).collect::<Result<Vec<_>>>()?,
            negated: *negated,
        }),
        sql::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(Expr::Between {
            expr: Box::new(convert_expr(expr)?),
            low: Box::new(convert_expr(low)?),
            high: Box::new(convert_expr(high)?),
            negated: *negated,
        }),
        sql::Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => {
            if *any || escape_char.is_some() {
                return unsupported("unsupported LIKE form");
            }
            Ok(Expr::Like {
                expr: Box::new(convert_expr(expr)?),
                pattern: Box::new(convert_expr(pattern)?),
                negated: *negated,
            })
        }
        sql::Expr::Case {
            operand,
            conditions,
            else_result,
        } => Ok(Expr::Case {
            operand: operand
                .as_ref()
                .map(|expr| convert_expr(expr).map(Box::new))
                .transpose()?,
            when_clauses: conditions
                .iter()
                .map(|when| Ok((convert_expr(&when.condition)?, convert_expr(&when.result)?)))
                .collect::<Result<Vec<_>>>()?,
            else_clause: else_result
                .as_ref()
                .map(|expr| convert_expr(expr).map(Box::new))
                .transpose()?,
        }),
        sql::Expr::Cast {
            kind,
            expr,
            data_type,
            format,
        } => {
            if *kind != sql::CastKind::Cast || format.is_some() {
                return unsupported("unsupported CAST form");
            }
            Ok(Expr::Cast {
                expr: Box::new(convert_expr(expr)?),
                data_type: convert_data_type(data_type)?,
            })
        }
        sql::Expr::Function(function) => convert_function(function),
        sql::Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => convert_substring(expr, substring_from.as_deref(), substring_for.as_deref()),
        sql::Expr::Trim {
            expr,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            if trim_where.is_some() || trim_what.is_some() || trim_characters.is_some() {
                return unsupported("only TRIM(expr) is supported in v1");
            }
            Ok(Expr::Function {
                name: "trim".to_string(),
                args: vec![FunctionArg::Expr(convert_expr(expr)?)],
                distinct: false,
            })
        }
        _ => unsupported("unsupported expression"),
    }
}

/// Normalizes `SUBSTRING(expr [FROM start] [FOR len])` and the comma form
/// `SUBSTRING(expr, start[, len])` into a `substring` function call. A start
/// position is required in v1.
fn convert_substring(
    expr: &sql::Expr,
    substring_from: Option<&sql::Expr>,
    substring_for: Option<&sql::Expr>,
) -> Result<Expr> {
    let Some(from) = substring_from else {
        return unsupported("SUBSTRING requires a start position in v1");
    };
    let mut args = vec![
        FunctionArg::Expr(convert_expr(expr)?),
        FunctionArg::Expr(convert_expr(from)?),
    ];
    if let Some(for_expr) = substring_for {
        args.push(FunctionArg::Expr(convert_expr(for_expr)?));
    }
    Ok(Expr::Function {
        name: "substring".to_string(),
        args,
        distinct: false,
    })
}

fn convert_value(value: &sql::Value) -> Result<Expr> {
    match value {
        sql::Value::Null => Ok(Expr::Literal(Value::Null)),
        sql::Value::Boolean(value) => Ok(Expr::Literal(Value::Boolean(*value))),
        sql::Value::Number(value, _) => {
            let value = value
                .parse::<i64>()
                .map_err(|_| parse_error("invalid integer literal"))?;
            Ok(Expr::Literal(Value::Integer(value)))
        }
        sql::Value::SingleQuotedString(value) => Ok(Expr::Literal(Value::Text(value.clone()))),
        sql::Value::Placeholder(name) => convert_placeholder(name),
        _ => unsupported("unsupported literal"),
    }
}

fn convert_placeholder(name: &str) -> Result<Expr> {
    let digits = name
        .strip_prefix('$')
        .ok_or_else(|| parse_error(format!("unsupported placeholder {name}")))?;
    let index = digits
        .parse::<u32>()
        .map_err(|_| parse_error(format!("invalid placeholder {name}")))?;
    if index == 0 {
        return Err(parse_error("placeholder index must be >= 1"));
    }
    Ok(Expr::Placeholder(index))
}

fn convert_function(function: &sql::Function) -> Result<Expr> {
    if function.uses_odbc_syntax
        || !matches!(function.parameters, sql::FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return unsupported("unsupported function call");
    }

    let (args, distinct) = match &function.args {
        sql::FunctionArguments::List(args) => {
            if !args.clauses.is_empty() {
                return unsupported("unsupported function argument clause");
            }
            let distinct = matches!(
                args.duplicate_treatment,
                Some(sql::DuplicateTreatment::Distinct)
            );
            let converted = args
                .args
                .iter()
                .map(convert_function_arg)
                .collect::<Result<Vec<_>>>()?;
            (converted, distinct)
        }
        sql::FunctionArguments::None => (Vec::new(), false),
        sql::FunctionArguments::Subquery(_) => return unsupported("unsupported function argument"),
    };

    Ok(Expr::Function {
        name: object_name(&function.name)?,
        args,
        distinct,
    })
}

fn convert_function_arg(arg: &sql::FunctionArg) -> Result<FunctionArg> {
    let sql::FunctionArg::Unnamed(arg) = arg else {
        return unsupported("named function arguments are not supported");
    };
    match arg {
        sql::FunctionArgExpr::Expr(expr) => Ok(FunctionArg::Expr(convert_expr(expr)?)),
        sql::FunctionArgExpr::Wildcard => Ok(FunctionArg::Wildcard),
        sql::FunctionArgExpr::QualifiedWildcard(_) => {
            unsupported("qualified function wildcards are not supported")
        }
    }
}

fn convert_bin_op(op: &sql::BinaryOperator) -> Result<BinOp> {
    match op {
        sql::BinaryOperator::Plus => Ok(BinOp::Add),
        sql::BinaryOperator::Minus => Ok(BinOp::Sub),
        sql::BinaryOperator::Multiply => Ok(BinOp::Mul),
        sql::BinaryOperator::Divide => Ok(BinOp::Div),
        sql::BinaryOperator::Modulo => Ok(BinOp::Mod),
        sql::BinaryOperator::Eq => Ok(BinOp::Eq),
        sql::BinaryOperator::NotEq => Ok(BinOp::Neq),
        sql::BinaryOperator::Lt => Ok(BinOp::Lt),
        sql::BinaryOperator::LtEq => Ok(BinOp::LtEq),
        sql::BinaryOperator::Gt => Ok(BinOp::Gt),
        sql::BinaryOperator::GtEq => Ok(BinOp::GtEq),
        sql::BinaryOperator::And => Ok(BinOp::And),
        sql::BinaryOperator::Or => Ok(BinOp::Or),
        sql::BinaryOperator::StringConcat => Ok(BinOp::Concat),
        _ => unsupported("unsupported binary operator"),
    }
}

fn convert_data_type(data_type: &sql::DataType) -> Result<DataType> {
    match data_type {
        sql::DataType::Integer(None) | sql::DataType::Int(None) => Ok(DataType::Integer),
        sql::DataType::Text
        | sql::DataType::Varchar(None)
        | sql::DataType::Char(None)
        | sql::DataType::Character(None) => Ok(DataType::Text),
        sql::DataType::Boolean | sql::DataType::Bool => Ok(DataType::Boolean),
        _ => unsupported("unsupported data type"),
    }
}

fn convert_u64_expr(expr: &sql::Expr) -> Result<u64> {
    let sql::Expr::Value(value) = expr else {
        return unsupported("LIMIT/OFFSET must be integer literals");
    };
    let sql::Value::Number(value, _) = &value.value else {
        return unsupported("LIMIT/OFFSET must be integer literals");
    };
    value
        .parse::<u64>()
        .map_err(|_| parse_error("LIMIT/OFFSET must be non-negative integer literals"))
}

fn table_alias_name(alias: &sql::TableAlias) -> Result<String> {
    if !alias.columns.is_empty() {
        return unsupported("table column aliases are not supported");
    }
    ident_name(&alias.name)
}

fn object_name(name: &sql::ObjectName) -> Result<String> {
    name.0
        .iter()
        .map(|part| {
            let ident = part
                .as_ident()
                .ok_or_else(|| parse_error("unsupported object name part"))?;
            ident_name(ident)
        })
        .collect::<Result<Vec<_>>>()
        .map(|parts| parts.join("."))
}

fn ident_name(ident: &sql::Ident) -> Result<String> {
    if ident.quote_style.is_some() {
        return Err(parse_error("quoted identifiers are not supported"));
    }
    Ok(ident.value.to_ascii_lowercase())
}

fn reject_wildcard_options(options: &sql::WildcardAdditionalOptions) -> Result<()> {
    if options.opt_ilike.is_some()
        || options.opt_exclude.is_some()
        || options.opt_except.is_some()
        || options.opt_replace.is_some()
        || options.opt_rename.is_some()
    {
        return unsupported("unsupported wildcard options");
    }
    Ok(())
}

fn parse_error(message: impl Into<String>) -> DbError {
    DbError::parse(SqlState::SyntaxError, message)
}

fn unsupported<T>(message: impl Into<String>) -> Result<T> {
    Err(parse_error(message))
}
