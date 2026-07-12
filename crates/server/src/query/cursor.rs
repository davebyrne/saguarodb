use std::collections::BTreeSet;
use std::sync::Arc;

use common::{ColumnInfo, DbError, IsolationLevel, Result, SqlState};
use executor::FetchStatus;
use parser::{Query, Statement};
use planner::{logical_plan, physical_plan};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use super::{
    ExecutionContextInput, PreparedStatement, QueryService, QuerySessionContext, STREAM_BATCH_ROWS,
    StatementClass, StreamMessage, Transaction, TransactionSnapshots, classify_bound,
    prepared_schema_versions, validate_prepared_schema_versions_in_catalog,
};

pub(crate) enum CursorFetchStatus {
    Exhausted { count: u64 },
    Suspended { count: u64 },
}

pub(crate) struct StartedCursor {
    pub(crate) handle: QueryCursorHandle,
    pub(crate) columns: Vec<ColumnInfo>,
    pub(crate) relations: BTreeSet<common::TableId>,
}

pub(crate) struct QueryCursorHandle {
    tx: mpsc::Sender<CursorCommand>,
    _task: JoinHandle<()>,
}

impl QueryCursorHandle {
    pub(crate) async fn start_fetch(
        &self,
        max_rows: Option<u64>,
        row_tx: mpsc::Sender<StreamMessage>,
    ) -> Result<oneshot::Receiver<Result<CursorFetchStatus>>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(CursorCommand::Fetch {
                max_rows,
                row_tx,
                reply_tx,
            })
            .await
            .map_err(|_| DbError::internal("cursor worker is no longer running"))?;
        Ok(reply_rx)
    }
}

enum CursorCommand {
    Fetch {
        max_rows: Option<u64>,
        row_tx: mpsc::Sender<StreamMessage>,
        reply_tx: oneshot::Sender<Result<CursorFetchStatus>>,
    },
}

struct CursorSetup {
    txn: Option<Transaction>,
    default_isolation: IsolationLevel,
    result: Result<StartedCursorSetup>,
}

struct StartedCursorSetup {
    columns: Vec<ColumnInfo>,
    relations: BTreeSet<common::TableId>,
}

struct CursorWorkerInput {
    service: Arc<QueryService>,
    prepared: Arc<PreparedStatement>,
    params: Vec<common::Value>,
    txn: Option<Transaction>,
    default_isolation: IsolationLevel,
    session: QuerySessionContext,
    setup_tx: oneshot::Sender<CursorSetup>,
    cmd_rx: mpsc::Receiver<CursorCommand>,
}

struct SqlCursorWorkerInput {
    service: Arc<QueryService>,
    query: Query,
    txn: Transaction,
    default_isolation: IsolationLevel,
    session: QuerySessionContext,
    setup_tx: oneshot::Sender<CursorSetup>,
    cmd_rx: mpsc::Receiver<CursorCommand>,
}

impl QueryService {
    pub(crate) async fn start_prepared_cursor(
        service: Arc<Self>,
        prepared: Arc<PreparedStatement>,
        params: Vec<common::Value>,
        txn: Option<Transaction>,
        default_isolation: IsolationLevel,
        session: QuerySessionContext,
    ) -> (Option<Transaction>, IsolationLevel, Result<StartedCursor>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        let (setup_tx, setup_rx) = oneshot::channel();
        let task = tokio::task::spawn_blocking(move || {
            run_cursor_worker(CursorWorkerInput {
                service,
                prepared,
                params,
                txn,
                default_isolation,
                session,
                setup_tx,
                cmd_rx,
            });
        });

        let setup = match setup_rx.await {
            Ok(setup) => setup,
            Err(_) => {
                return (
                    None,
                    default_isolation,
                    Err(DbError::internal("cursor worker stopped before setup")),
                );
            }
        };
        let CursorSetup {
            txn,
            default_isolation,
            result,
        } = setup;
        match result {
            Ok(setup) => (
                txn,
                default_isolation,
                Ok(StartedCursor {
                    handle: QueryCursorHandle {
                        tx: cmd_tx,
                        _task: task,
                    },
                    columns: setup.columns,
                    relations: setup.relations,
                }),
            ),
            Err(err) => (txn, default_isolation, Err(err)),
        }
    }

    pub(crate) async fn start_sql_cursor(
        service: Arc<Self>,
        query: Query,
        txn: Transaction,
        default_isolation: IsolationLevel,
        session: QuerySessionContext,
    ) -> (Option<Transaction>, IsolationLevel, Result<StartedCursor>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        let (setup_tx, setup_rx) = oneshot::channel();
        let task = tokio::task::spawn_blocking(move || {
            run_sql_cursor_worker(SqlCursorWorkerInput {
                service,
                query,
                txn,
                default_isolation,
                session,
                setup_tx,
                cmd_rx,
            });
        });

        let setup = match setup_rx.await {
            Ok(setup) => setup,
            Err(_) => {
                return (
                    None,
                    default_isolation,
                    Err(DbError::internal("cursor worker stopped before setup")),
                );
            }
        };
        let CursorSetup {
            txn,
            default_isolation,
            result,
        } = setup;
        match result {
            Ok(setup) => (
                txn,
                default_isolation,
                Ok(StartedCursor {
                    handle: QueryCursorHandle {
                        tx: cmd_tx,
                        _task: task,
                    },
                    columns: setup.columns,
                    relations: setup.relations,
                }),
            ),
            Err(err) => (txn, default_isolation, Err(err)),
        }
    }
}

fn run_cursor_worker(input: CursorWorkerInput) {
    let CursorWorkerInput {
        service,
        prepared,
        params,
        txn,
        default_isolation,
        session,
        setup_tx,
        cmd_rx,
    } = input;
    let session = service.with_catalog_introspection(session);
    let input = CursorWorkerInput {
        service,
        prepared,
        params,
        txn: None,
        default_isolation,
        session,
        setup_tx,
        cmd_rx,
    };
    match txn {
        Some(txn) => run_transaction_cursor_worker(input, txn),
        None => run_autocommit_cursor_worker(input),
    }
}

fn run_autocommit_cursor_worker(input: CursorWorkerInput) {
    let CursorWorkerInput {
        service,
        prepared,
        params,
        txn: _,
        default_isolation,
        session,
        setup_tx,
        mut cmd_rx,
    } = input;
    let bound = match service.substitute_prepared_params(&prepared, &params) {
        Ok(bound) => bound,
        Err(err) => return send_setup_error(setup_tx, None, default_isolation, err),
    };
    if !matches!(classify_bound(prepared.class, &bound), StatementClass::Read) {
        return send_setup_error(setup_tx, None, default_isolation, read_only_cursor_error());
    }
    let _object_guard = match service.lock_autocommit_prepared_read(
        &bound,
        &prepared.schema_versions,
        session.cancel().as_ref(),
    ) {
        Ok(guard) => guard,
        Err(err) => return send_setup_error(setup_tx, None, default_isolation, err),
    };
    let captured = match service.capture_consistent_snapshots_cancelable(0, session.cancel()) {
        Ok(captured) => captured,
        Err(err) => return send_setup_error(setup_tx, None, default_isolation, err),
    };
    let schema_versions =
        match prepared_schema_versions(&bound, service.components.catalog.as_ref()) {
            Ok(schema_versions) => schema_versions,
            Err(err) => return send_setup_error(setup_tx, None, default_isolation, err),
        };
    if let Err(err) = service.validate_relation_snapshot_schema_versions(
        captured.relations.as_ref(),
        &schema_versions,
        true,
    ) {
        return send_setup_error(setup_tx, None, default_isolation, err);
    }
    let runtime = session.statement_runtime(
        default_isolation,
        default_isolation,
        session.statement_timeout_ms(),
    );
    let ctx = match service.execution_context_for_bound(
        ExecutionContextInput {
            txn_id: 0,
            snapshot: captured.snapshot,
            relations: captured.relations,
            isolation: IsolationLevel::default(),
            gc_horizon: 0,
            live_txns: Arc::from([0]),
            runtime,
        },
        &bound,
    ) {
        Ok(ctx) => ctx,
        Err(err) => return send_setup_error(setup_tx, None, default_isolation, err),
    };
    let logical = match logical_plan(&bound) {
        Ok(logical) => logical,
        Err(err) => return send_setup_error(setup_tx, None, default_isolation, err),
    };
    let physical = match physical_plan(&logical, service.components.catalog.as_ref()) {
        Ok(physical) => physical,
        Err(err) => return send_setup_error(setup_tx, None, default_isolation, err),
    };
    let mut query = match service.engine.open_query(&ctx, &physical) {
        Ok(query) => query,
        Err(err) => return send_setup_error(setup_tx, None, default_isolation, err),
    };
    let columns = query.output_schema().to_vec();
    let relations = match super::bound_relation_ids(&bound) {
        Ok(relations) => relations,
        Err(err) => return send_setup_error(setup_tx, None, default_isolation, err),
    };
    let _ = setup_tx.send(CursorSetup {
        txn: None,
        default_isolation,
        result: Ok(StartedCursorSetup { columns, relations }),
    });
    drive_cursor_commands(&mut query, &mut cmd_rx, session.cancel().clone());
}

fn run_transaction_cursor_worker(input: CursorWorkerInput, mut txn: Transaction) {
    let CursorWorkerInput {
        service,
        prepared,
        params,
        txn: _,
        default_isolation,
        session,
        setup_tx,
        mut cmd_rx,
    } = input;
    if txn.failed {
        return send_failed_txn_setup_error(
            setup_tx,
            txn,
            default_isolation,
            DbError::execute(
                SqlState::InFailedSqlTransaction,
                "current transaction is aborted, commands ignored until end of transaction block",
            ),
        );
    }
    let bound = match service.substitute_prepared_params(&prepared, &params) {
        Ok(bound) => bound,
        Err(err) => return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err),
    };
    if !matches!(classify_bound(prepared.class, &bound), StatementClass::Read) {
        return send_failed_txn_setup_error(
            setup_tx,
            txn,
            default_isolation,
            read_only_cursor_error(),
        );
    }
    let updates = txn.truncate_updates.clone();
    let requests = service.object_requests_for_bound(&bound);
    let lock_result = match requests {
        Ok(requests) if requests.is_empty() => {
            service.transaction_catalog(&txn).and_then(|catalog| {
                validate_prepared_schema_versions_in_catalog(
                    &prepared.schema_versions,
                    catalog.as_ref(),
                )
                .map(|()| catalog)
            })
        }
        Ok(_) => match service.ensure_transaction_lock_owner(&mut txn, session.cancel()) {
            Ok(owner) => service.lock_prepared_bound_in_transaction(
                &bound,
                &prepared.schema_versions,
                &updates,
                owner,
                session.cancel().as_ref(),
            ),
            Err(err) => Err(err),
        },
        Err(err) => Err(err),
    };
    let validated_catalog = match lock_result {
        Ok(catalog) => catalog,
        Err(err) => {
            if err.code == SqlState::DeadlockDetected {
                service.abort_deadlock_victim(&mut txn);
            }
            return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err);
        }
    };
    let (catalog, catalog_is_snapshot) =
        match service.transaction_statement_catalog_from_validated(&txn, &bound, validated_catalog)
        {
            Ok(catalog) => catalog,
            Err(err) => return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err),
        };
    let snapshots = match service.snapshots_for_transaction(&mut txn, session.cancel()) {
        Ok(snapshots) => snapshots,
        Err(err) => return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err),
    };
    txn.first_statement_ran = true;
    if let Err(err) = service.validate_relation_snapshot_schema_versions(
        snapshots.relations.as_ref(),
        &prepared.schema_versions,
        true,
    ) {
        return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err);
    }
    let TransactionSnapshots {
        snapshot,
        relations,
        advertised,
    } = snapshots;
    let _cursor_advertised =
        advertised.or_else(|| Some(service.components.active_txns.advertise_xmin(snapshot.xmin)));
    let runtime = session.statement_runtime(
        txn.current_default_isolation(default_isolation),
        txn.isolation,
        txn.current_statement_timeout_ms(session.statement_timeout_ms()),
    );
    let ctx = match service.execution_context_with_selected_catalog(
        ExecutionContextInput {
            txn_id: txn.writing_xid(),
            snapshot,
            relations,
            isolation: txn.isolation,
            gc_horizon: service.components.gc_horizon(),
            live_txns: txn.live_txns(),
            runtime,
        },
        catalog.clone(),
        catalog_is_snapshot,
    ) {
        Ok(ctx) => ctx,
        Err(err) => return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err),
    };
    let logical = match logical_plan(&bound) {
        Ok(logical) => logical,
        Err(err) => return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err),
    };
    let physical = match physical_plan(&logical, catalog.as_ref()) {
        Ok(physical) => physical,
        Err(err) => return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err),
    };
    let mut query = match service.engine.open_query(&ctx, &physical) {
        Ok(query) => query,
        Err(err) => return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err),
    };
    let columns = query.output_schema().to_vec();
    let relations = match super::bound_relation_ids(&bound) {
        Ok(relations) => relations,
        Err(err) => return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err),
    };
    let _ = setup_tx.send(CursorSetup {
        txn: Some(txn),
        default_isolation,
        result: Ok(StartedCursorSetup { columns, relations }),
    });
    drive_cursor_commands(&mut query, &mut cmd_rx, session.cancel().clone());
}

fn run_sql_cursor_worker(input: SqlCursorWorkerInput) {
    let SqlCursorWorkerInput {
        service,
        query,
        mut txn,
        default_isolation,
        session,
        setup_tx,
        mut cmd_rx,
    } = input;
    let session = service.with_catalog_introspection(session);
    if txn.failed {
        return send_failed_txn_setup_error(
            setup_tx,
            txn,
            default_isolation,
            DbError::execute(
                SqlState::InFailedSqlTransaction,
                "current transaction is aborted, commands ignored until end of transaction block",
            ),
        );
    }

    let statement = Statement::Query(query);
    let initial = service.bind_with_object_requests(&statement);
    let locked = match initial {
        Ok((bound, requests)) if requests.is_empty() => {
            service.transaction_catalog(&txn).and_then(|catalog| {
                prepared_schema_versions(&bound, catalog.as_ref())
                    .map(|versions| (bound, versions, catalog))
            })
        }
        Ok(_) => {
            let updates = txn.truncate_updates.clone();
            match service.ensure_transaction_lock_owner(&mut txn, session.cancel()) {
                Ok(owner) => service.bind_and_lock_unprepared_in_transaction(
                    &statement,
                    &updates,
                    owner,
                    session.cancel().as_ref(),
                ),
                Err(err) => Err(err),
            }
        }
        Err(err) => Err(err),
    };
    let (bound, schema_versions, validated_catalog) = match locked {
        Ok(locked) => locked,
        Err(err) => {
            if err.code == SqlState::DeadlockDetected {
                service.abort_deadlock_victim(&mut txn);
            }
            return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err);
        }
    };
    if !matches!(
        classify_bound(StatementClass::Read, &bound),
        StatementClass::Read
    ) {
        return send_failed_txn_setup_error(
            setup_tx,
            txn,
            default_isolation,
            read_only_cursor_error(),
        );
    }

    let snapshots = match service.snapshots_for_transaction(&mut txn, session.cancel()) {
        Ok(snapshots) => snapshots,
        Err(err) => return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err),
    };
    txn.first_statement_ran = true;
    let (catalog, catalog_is_snapshot) =
        match service.transaction_statement_catalog_from_validated(&txn, &bound, validated_catalog)
        {
            Ok(catalog) => catalog,
            Err(err) => return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err),
        };
    if let Err(err) = service.validate_relation_snapshot_schema_versions(
        snapshots.relations.as_ref(),
        &schema_versions,
        true,
    ) {
        return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err);
    }

    let TransactionSnapshots {
        snapshot,
        relations,
        advertised,
    } = snapshots;
    let _cursor_advertised =
        advertised.or_else(|| Some(service.components.active_txns.advertise_xmin(snapshot.xmin)));
    let runtime = session.statement_runtime(
        txn.current_default_isolation(default_isolation),
        txn.isolation,
        txn.current_statement_timeout_ms(session.statement_timeout_ms()),
    );
    let ctx = match service.execution_context_with_selected_catalog(
        ExecutionContextInput {
            txn_id: txn.writing_xid(),
            snapshot,
            relations,
            isolation: txn.isolation,
            gc_horizon: service.components.gc_horizon(),
            live_txns: txn.live_txns(),
            runtime,
        },
        catalog.clone(),
        catalog_is_snapshot,
    ) {
        Ok(ctx) => ctx,
        Err(err) => return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err),
    };
    let logical = match logical_plan(&bound) {
        Ok(logical) => logical,
        Err(err) => return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err),
    };
    let physical = match physical_plan(&logical, catalog.as_ref()) {
        Ok(physical) => physical,
        Err(err) => return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err),
    };
    let mut query = match service.engine.open_query(&ctx, &physical) {
        Ok(query) => query,
        Err(err) => return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err),
    };
    let columns = query.output_schema().to_vec();
    let relations = match super::bound_relation_ids(&bound) {
        Ok(relations) => relations,
        Err(err) => return send_failed_txn_setup_error(setup_tx, txn, default_isolation, err),
    };
    let _ = setup_tx.send(CursorSetup {
        txn: Some(txn),
        default_isolation,
        result: Ok(StartedCursorSetup { columns, relations }),
    });
    drive_cursor_commands(&mut query, &mut cmd_rx, session.cancel().clone());
}

fn drive_cursor_commands(
    query: &mut executor::OpenQuery<'_>,
    cmd_rx: &mut mpsc::Receiver<CursorCommand>,
    cancel: Arc<common::QueryCancel>,
) {
    while let Some(command) = cmd_rx.blocking_recv() {
        match command {
            CursorCommand::Fetch {
                max_rows,
                row_tx,
                reply_tx,
            } => {
                let mut sink = super::ChannelRowSink::new(row_tx, cancel.clone());
                let result = query.fetch(max_rows, &mut sink, STREAM_BATCH_ROWS);
                let terminal = !matches!(result, Ok(FetchStatus::Suspended { .. }));
                let reply = result.map(|status| match status {
                    FetchStatus::Exhausted { count } => CursorFetchStatus::Exhausted { count },
                    FetchStatus::Suspended { count } => CursorFetchStatus::Suspended { count },
                });
                let _ = reply_tx.send(reply);
                if terminal {
                    break;
                }
            }
        }
    }
}

fn read_only_cursor_error() -> DbError {
    DbError::plan(
        SqlState::FeatureNotSupported,
        "portal suspension is supported only for read-only SELECT",
    )
}

fn send_setup_error(
    setup_tx: oneshot::Sender<CursorSetup>,
    txn: Option<Transaction>,
    default_isolation: IsolationLevel,
    err: DbError,
) {
    let _ = setup_tx.send(CursorSetup {
        txn,
        default_isolation,
        result: Err(err),
    });
}

fn send_failed_txn_setup_error(
    setup_tx: oneshot::Sender<CursorSetup>,
    mut txn: Transaction,
    default_isolation: IsolationLevel,
    err: DbError,
) {
    txn.failed = true;
    send_setup_error(setup_tx, Some(txn), default_isolation, err);
}
