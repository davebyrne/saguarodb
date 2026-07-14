use catalog::CatalogManager;
use common::{
    ColumnId, DbError, ForeignKeyConstraint, IndexConstraintKind, IndexId, Key,
    PRIMARY_KEY_INDEX_ID, Result, RowIdentity, SqlState, TableId, TableSchema, Value,
};
use storage::DependentRowProbe;

use crate::copy::value_text;
use crate::query::ExecutionContext;

struct OutgoingForeignKey {
    constraint: ForeignKeyConstraint,
    parent: TableSchema,
    access_index: IndexId,
}

struct IncomingForeignKey {
    constraint: ForeignKeyConstraint,
    child: TableSchema,
    supporting_index: Option<IndexId>,
}

struct PendingOutgoing<'a> {
    foreign_key: &'a OutgoingForeignKey,
    key: Key,
    invalidated_self_reference: bool,
}

/// Statement-scoped, catalog-resolved referential-integrity checks shared by
/// every executor write path targeting one table.
pub(crate) struct ReferentialIntegrity<'a> {
    ctx: &'a ExecutionContext<'a>,
    target: TableSchema,
    outgoing: Vec<OutgoingForeignKey>,
    incoming: Vec<IncomingForeignKey>,
}

impl<'a> ReferentialIntegrity<'a> {
    pub(crate) fn new(ctx: &'a ExecutionContext<'a>, target: TableSchema) -> Result<Self> {
        let mut outgoing = Vec::with_capacity(target.foreign_keys.len());
        for constraint in &target.foreign_keys {
            let parent = require_table(ctx.catalog.as_ref(), constraint.referenced_table)?;
            let referenced_index = ctx
                .catalog
                .get_index(constraint.referenced_index)?
                .ok_or_else(|| {
                    DbError::internal(format!(
                        "foreign key {} references a missing constraint index",
                        constraint.name
                    ))
                })?;
            let access_index = match referenced_index.constraint {
                IndexConstraintKind::PrimaryKey => PRIMARY_KEY_INDEX_ID,
                IndexConstraintKind::Unique => referenced_index.id,
                IndexConstraintKind::None => {
                    return Err(DbError::internal(format!(
                        "foreign key {} references a non-constraint index",
                        constraint.name
                    )));
                }
            };
            outgoing.push(OutgoingForeignKey {
                constraint: constraint.clone(),
                parent,
                access_index,
            });
        }

        let mut incoming = Vec::new();
        for (child, constraint) in ctx.catalog.list_incoming_foreign_keys(target.id)? {
            let supporting_index = ctx
                .catalog
                .find_foreign_key_supporting_index(child.id, &constraint.columns)?;
            incoming.push(IncomingForeignKey {
                constraint,
                child,
                supporting_index,
            });
        }
        incoming.sort_by_key(|foreign_key| (foreign_key.child.id, foreign_key.constraint.id));

        Ok(Self {
            ctx,
            target,
            outgoing,
            incoming,
        })
    }

    pub(crate) fn target(&self) -> &TableSchema {
        &self.target
    }

    pub(crate) fn requires_update_lock(&self, assignments: &[ColumnId]) -> bool {
        self.incoming.iter().any(|foreign_key| {
            assignments
                .iter()
                .any(|column| foreign_key.constraint.referenced_columns.contains(column))
        })
    }

    pub(crate) fn validate_outgoing(&self, values: &[Value]) -> Result<()> {
        self.validate_outgoing_after_change(None, values)
    }

    pub(crate) fn validate_outgoing_update(
        &self,
        old_values: &[Value],
        new_values: &[Value],
    ) -> Result<()> {
        self.validate_outgoing_after_change(Some(old_values), new_values)
    }

    fn validate_outgoing_after_change(
        &self,
        old_values: Option<&[Value]>,
        values: &[Value],
    ) -> Result<()> {
        let mut pending = Vec::new();
        for foreign_key in &self.outgoing {
            let key = key_for_columns(&self.target, &foreign_key.constraint.columns, values)?;
            let mut old_key = None;
            if let Some(old_values) = old_values {
                let previous =
                    key_for_columns(&self.target, &foreign_key.constraint.columns, old_values)?;
                old_key = Some(previous);
            }
            let self_parent_keys = if foreign_key.constraint.referenced_table == self.target.id {
                let new_parent = key_for_columns(
                    &self.target,
                    &foreign_key.constraint.referenced_columns,
                    values,
                )?;
                let old_parent = old_values
                    .map(|old_values| {
                        key_for_columns(
                            &self.target,
                            &foreign_key.constraint.referenced_columns,
                            old_values,
                        )
                    })
                    .transpose()?;
                Some((old_parent, new_parent))
            } else {
                None
            };
            let self_parent_changed =
                self_parent_keys
                    .as_ref()
                    .is_some_and(|(old_parent, new_parent)| {
                        old_parent
                            .as_ref()
                            .is_some_and(|old_parent| old_parent != new_parent)
                    });
            if old_key.as_ref().is_some_and(|old_key| *old_key == key) && !self_parent_changed {
                continue;
            }
            if key.0.iter().any(|value| matches!(value, Value::Null)) {
                continue;
            }
            let mut invalidated_self_reference = false;
            if let Some((old_parent, new_parent)) = &self_parent_keys {
                if key == *new_parent {
                    continue;
                }
                invalidated_self_reference = old_parent
                    .as_ref()
                    .is_some_and(|old_parent| key == *old_parent && old_parent != new_parent);
            }
            pending.push(PendingOutgoing {
                foreign_key,
                key,
                invalidated_self_reference,
            });
        }
        pending.sort_by(|left, right| {
            (
                left.foreign_key.parent.id,
                &left.key,
                left.foreign_key.constraint.id,
            )
                .cmp(&(
                    right.foreign_key.parent.id,
                    &right.key,
                    right.foreign_key.constraint.id,
                ))
        });

        for pending in pending {
            let foreign_key = pending.foreign_key;
            if pending.invalidated_self_reference {
                return Err(child_violation(
                    &self.target,
                    &foreign_key.parent,
                    &foreign_key.constraint,
                    &pending.key,
                ));
            }
            if foreign_key.access_index == common::PRIMARY_KEY_INDEX_ID {
                self.ctx.statement.ssi_tracker.record_tuple_read(
                    self.ctx.statement.txn_id,
                    foreign_key.parent.id,
                    &pending.key,
                );
            } else {
                self.ctx
                    .statement
                    .ssi_tracker
                    .record_relation_read(self.ctx.statement.txn_id, foreign_key.parent.id);
            }
            if !self.ctx.storage.referenced_key_exists(
                &self.ctx.statement,
                self.ctx.relations.as_ref(),
                foreign_key.parent.id,
                foreign_key.access_index,
                &pending.key,
            )? {
                return Err(child_violation(
                    &self.target,
                    &foreign_key.parent,
                    &foreign_key.constraint,
                    &pending.key,
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn validate_parent_update(
        &self,
        old_values: &[Value],
        new_values: &[Value],
        excluded: Option<&RowIdentity>,
    ) -> Result<()> {
        for foreign_key in &self.incoming {
            let old_key = key_for_columns(
                &self.target,
                &foreign_key.constraint.referenced_columns,
                old_values,
            )?;
            let new_key = key_for_columns(
                &self.target,
                &foreign_key.constraint.referenced_columns,
                new_values,
            )?;
            if old_key == new_key || old_key.0.iter().any(|value| matches!(value, Value::Null)) {
                continue;
            }
            self.validate_no_dependents(foreign_key, &old_key, excluded)?;
        }
        Ok(())
    }

    pub(crate) fn validate_parent_delete(
        &self,
        old_values: &[Value],
        excluded: Option<&RowIdentity>,
    ) -> Result<()> {
        for foreign_key in &self.incoming {
            let old_key = key_for_columns(
                &self.target,
                &foreign_key.constraint.referenced_columns,
                old_values,
            )?;
            if old_key.0.iter().any(|value| matches!(value, Value::Null)) {
                continue;
            }
            self.validate_no_dependents(foreign_key, &old_key, excluded)?;
        }
        Ok(())
    }

    fn validate_no_dependents(
        &self,
        foreign_key: &IncomingForeignKey,
        key: &Key,
        excluded: Option<&RowIdentity>,
    ) -> Result<()> {
        self.ctx
            .statement
            .ssi_tracker
            .record_relation_read(self.ctx.statement.txn_id, foreign_key.child.id);
        let excluded = if foreign_key.child.id == self.target.id {
            excluded
        } else {
            None
        };
        if self.ctx.storage.dependent_row_exists(
            &self.ctx.statement,
            self.ctx.relations.as_ref(),
            DependentRowProbe {
                table: foreign_key.child.id,
                columns: &foreign_key.constraint.columns,
                key,
                supporting_index: foreign_key.supporting_index,
                excluded,
            },
        )? {
            return Err(parent_violation(
                &self.target,
                &foreign_key.child,
                &foreign_key.constraint,
                key,
            ));
        }
        Ok(())
    }
}

fn require_table(catalog: &dyn CatalogManager, table: TableId) -> Result<TableSchema> {
    catalog.get_table(table)?.ok_or_else(|| {
        DbError::internal(format!("foreign key references missing table id {table}"))
    })
}

fn key_for_columns(schema: &TableSchema, columns: &[ColumnId], values: &[Value]) -> Result<Key> {
    let mut key = Vec::with_capacity(columns.len());
    for column in columns {
        let slot = schema
            .columns
            .iter()
            .position(|candidate| candidate.id == *column)
            .ok_or_else(|| {
                DbError::internal(format!(
                    "foreign key references missing column id {column} on table {}",
                    schema.name
                ))
            })?;
        let value = values.get(slot).ok_or_else(|| {
            DbError::internal(format!(
                "foreign-key row shape does not match table {}",
                schema.name
            ))
        })?;
        key.push(value.clone());
    }
    Ok(Key(key))
}

fn child_violation(
    child: &TableSchema,
    parent: &TableSchema,
    constraint: &ForeignKeyConstraint,
    key: &Key,
) -> DbError {
    let mut error = DbError::execute(
        SqlState::ForeignKeyViolation,
        format!(
            "insert or update on table \"{}\" violates foreign key constraint \"{}\"",
            child.name, constraint.name
        ),
    );
    error.detail = Some(format!(
        "Key ({})=({}) is not present in table \"{}\".",
        column_names(child, &constraint.columns),
        key_values(key),
        parent.name
    ));
    error
}

fn parent_violation(
    parent: &TableSchema,
    child: &TableSchema,
    constraint: &ForeignKeyConstraint,
    key: &Key,
) -> DbError {
    let mut error = DbError::execute(
        SqlState::ForeignKeyViolation,
        format!(
            "update or delete on table \"{}\" violates foreign key constraint \"{}\" on table \"{}\"",
            parent.name, constraint.name, child.name
        ),
    );
    error.detail = Some(format!(
        "Key ({})=({}) is still referenced from table \"{}\".",
        column_names(parent, &constraint.referenced_columns),
        key_values(key),
        child.name
    ));
    error
}

fn column_names(schema: &TableSchema, columns: &[ColumnId]) -> String {
    columns
        .iter()
        .map(|column| {
            schema
                .columns
                .iter()
                .find(|candidate| candidate.id == *column)
                .map_or_else(
                    || format!("<missing {column}>"),
                    |column| column.name.clone(),
                )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn key_values(key: &Key) -> String {
    key.0
        .iter()
        .map(|value| value_text(value).unwrap_or_else(|| "null".to_string()))
        .collect::<Vec<_>>()
        .join(", ")
}
