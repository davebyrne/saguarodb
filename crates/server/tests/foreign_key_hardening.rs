mod support;

use std::time::Duration;

use common::{IsolationLevel, QueryCancel};
use support::{Connection, QueryOutcome, TestServer};

async fn wait_for_new_lock_waiter(server: &TestServer, previous: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if server.app().components.lock_manager.waiting_owner_count() > previous {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("blocked query must enter the lock-manager wait graph");
}

async fn assert_one_deadlock_victim(
    mut first: tokio::task::JoinHandle<(Connection, common::Result<QueryOutcome>)>,
    mut second: tokio::task::JoinHandle<(Connection, common::Result<QueryOutcome>)>,
) {
    let (mut completed, completed_outcome, first_completed) =
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                result = &mut first => {
                    let (connection, outcome) = result.unwrap();
                    (connection, outcome.unwrap(), true)
                }
                result = &mut second => {
                    let (connection, outcome) = result.unwrap();
                    (connection, outcome.unwrap(), false)
                }
            }
        })
        .await
        .expect("deadlock detector must resolve the cycle");
    if let Some(error) = completed_outcome.result.err() {
        assert!(error.message.contains("C=40P01"), "{error}");
        completed.ok("rollback").await;
        let (mut survivor, outcome) = tokio::time::timeout(Duration::from_secs(3), async {
            if first_completed {
                second.await.unwrap()
            } else {
                first.await.unwrap()
            }
        })
        .await
        .expect("deadlock survivor must finish");
        outcome.unwrap().result.unwrap();
        survivor.ok("commit").await;
    } else {
        let (mut victim, outcome) = tokio::time::timeout(Duration::from_secs(3), async {
            if first_completed {
                second.await.unwrap()
            } else {
                first.await.unwrap()
            }
        })
        .await
        .expect("deadlock victim must finish");
        let error = outcome
            .unwrap()
            .result
            .err()
            .expect("one deadlock participant must be victim");
        assert!(error.message.contains("C=40P01"), "{error}");
        victim.ok("rollback").await;
        completed.ok("commit").await;
    }
}

async fn setup_basic() -> (TestServer, Connection) {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table fk_parent (id integer primary key)")
        .await;
    setup
        .ok("create table fk_child (id integer primary key, parent_id integer references fk_parent)")
        .await;
    setup
        .ok("insert into fk_parent values (1), (2), (3), (4)")
        .await;
    (server, setup)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn child_insert_and_parent_delete_or_key_update_honor_both_winner_orders() {
    let (server, _setup) = setup_basic().await;

    let mut parent_first = Connection::connect(&server).await.unwrap();
    parent_first.ok("begin").await;
    parent_first.ok("delete from fk_parent where id = 1").await;
    let mut child_loser = Connection::connect(&server).await.unwrap();
    let child_task = tokio::spawn(async move {
        child_loser
            .query("insert into fk_child values (1, 1)")
            .await
    });
    wait_for_new_lock_waiter(&server, 0).await;
    assert!(!child_task.is_finished());
    parent_first.ok("commit").await;
    let error = child_task
        .await
        .unwrap()
        .unwrap()
        .result
        .err()
        .expect("child insert must lose");
    assert!(error.message.contains("C=23503"), "{error}");

    let mut child_first = Connection::connect(&server).await.unwrap();
    child_first.ok("begin").await;
    child_first.ok("insert into fk_child values (2, 2)").await;
    let mut parent_loser = Connection::connect(&server).await.unwrap();
    let delete_task = tokio::spawn(async move {
        parent_loser
            .query("delete from fk_parent where id = 2")
            .await
    });
    wait_for_new_lock_waiter(&server, 0).await;
    assert!(!delete_task.is_finished());
    child_first.ok("commit").await;
    let error = delete_task
        .await
        .unwrap()
        .unwrap()
        .result
        .err()
        .expect("parent delete must lose");
    assert!(error.message.contains("C=23503"), "{error}");

    let mut update_first = Connection::connect(&server).await.unwrap();
    update_first.ok("begin").await;
    update_first
        .ok("update fk_parent set id = 30 where id = 3")
        .await;
    let mut child_after_update = Connection::connect(&server).await.unwrap();
    let insert_task = tokio::spawn(async move {
        child_after_update
            .query("insert into fk_child values (3, 3)")
            .await
    });
    wait_for_new_lock_waiter(&server, 0).await;
    assert!(!insert_task.is_finished());
    update_first.ok("commit").await;
    let error = insert_task
        .await
        .unwrap()
        .unwrap()
        .result
        .err()
        .expect("child insert must lose");
    assert!(error.message.contains("C=23503"), "{error}");

    let mut child_before_update = Connection::connect(&server).await.unwrap();
    child_before_update.ok("begin").await;
    child_before_update
        .ok("insert into fk_child values (4, 4)")
        .await;
    let mut update_loser = Connection::connect(&server).await.unwrap();
    let update_task = tokio::spawn(async move {
        update_loser
            .query("update fk_parent set id = 40 where id = 4")
            .await
    });
    wait_for_new_lock_waiter(&server, 0).await;
    assert!(!update_task.is_finished());
    child_before_update.ok("commit").await;
    let error = update_task
        .await
        .unwrap()
        .unwrap()
        .result
        .err()
        .expect("parent update must lose");
    assert!(error.message.contains("C=23503"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unique_parent_races_and_dependent_child_changes_restart_correctly() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table uq_parent (id integer primary key, code integer unique)")
        .await;
    setup
        .ok("create table uq_child (id integer primary key, code integer references uq_parent(code))")
        .await;
    setup
        .ok("insert into uq_parent values (1, 10), (2, 20)")
        .await;

    let mut updater = Connection::connect(&server).await.unwrap();
    updater.ok("begin").await;
    updater
        .ok("update uq_parent set code = 11 where id = 1")
        .await;
    let mut child = Connection::connect(&server).await.unwrap();
    let child_task =
        tokio::spawn(async move { child.query("insert into uq_child values (1, 10)").await });
    wait_for_new_lock_waiter(&server, 0).await;
    assert!(!child_task.is_finished());
    updater.ok("commit").await;
    assert!(
        child_task
            .await
            .unwrap()
            .unwrap()
            .result
            .err()
            .expect("child insert must lose")
            .message
            .contains("C=23503")
    );

    let mut child_first = Connection::connect(&server).await.unwrap();
    child_first.ok("begin").await;
    child_first.ok("insert into uq_child values (9, 20)").await;
    let mut parent_loser = Connection::connect(&server).await.unwrap();
    let parent_update = tokio::spawn(async move {
        parent_loser
            .query("update uq_parent set code = 21 where id = 2")
            .await
    });
    wait_for_new_lock_waiter(&server, 0).await;
    assert!(!parent_update.is_finished());
    child_first.ok("commit").await;
    let error = parent_update
        .await
        .unwrap()
        .unwrap()
        .result
        .err()
        .expect("parent UNIQUE-key update must lose");
    assert!(error.message.contains("C=23503"), "{error}");
    setup.ok("delete from uq_child where id = 9").await;

    setup.ok("insert into uq_child values (2, 20)").await;
    let mut child_changer = Connection::connect(&server).await.unwrap();
    child_changer.ok("begin").await;
    child_changer
        .ok("update uq_child set code = null where id = 2")
        .await;
    let mut parent_delete = Connection::connect(&server).await.unwrap();
    let delete_task = tokio::spawn(async move {
        parent_delete
            .query("delete from uq_parent where id = 2")
            .await
    });
    wait_for_new_lock_waiter(&server, 0).await;
    assert!(!delete_task.is_finished());
    child_changer.ok("commit").await;
    delete_task.await.unwrap().unwrap().result.unwrap();

    setup.ok("insert into uq_parent values (3, 30)").await;
    setup.ok("insert into uq_child values (3, 30)").await;
    let mut child_deleter = Connection::connect(&server).await.unwrap();
    child_deleter.ok("begin").await;
    child_deleter.ok("delete from uq_child where id = 3").await;
    let mut blocked_delete = Connection::connect(&server).await.unwrap();
    let blocked_task = tokio::spawn(async move {
        blocked_delete
            .query("delete from uq_parent where id = 3")
            .await
    });
    wait_for_new_lock_waiter(&server, 0).await;
    assert!(!blocked_task.is_finished());
    child_deleter.ok("rollback").await;
    let error = blocked_task
        .await
        .unwrap()
        .unwrap()
        .result
        .err()
        .expect("parent delete must remain restricted");
    assert!(error.message.contains("C=23503"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unrelated_keys_progress_and_fk_tuple_waits_are_cancelable() {
    let (server, _setup) = setup_basic().await;
    let mut holder = Connection::connect(&server).await.unwrap();
    holder.ok("begin").await;
    holder.ok("insert into fk_child values (1, 1)").await;

    let mut blocked = Connection::connect(&server).await.unwrap();
    let blocked_task =
        tokio::spawn(async move { blocked.query("delete from fk_parent where id = 1").await });
    wait_for_new_lock_waiter(&server, 0).await;
    assert!(!blocked_task.is_finished());
    let unrelated = tokio::time::timeout(
        Duration::from_secs(2),
        server.simple_query("delete from fk_parent where id = 2"),
    )
    .await
    .expect("unrelated key must progress");
    unrelated.unwrap();
    holder.ok("rollback").await;
    blocked_task.await.unwrap().unwrap().result.unwrap();

    let mut creator = Connection::connect(&server).await.unwrap();
    creator.ok("begin").await;
    creator.ok("insert into fk_parent values (99)").await;
    let mut waiter = Connection::connect(&server).await.unwrap();
    let (pid, secret) = waiter.backend_key();
    let wait_task =
        tokio::spawn(async move { waiter.query("insert into fk_child values (99, 99)").await });
    wait_for_new_lock_waiter(&server, 0).await;
    assert!(!wait_task.is_finished());
    server.send_cancel(pid, secret).await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), wait_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .result
        .err()
        .expect("wait must be canceled");
    assert!(error.message.contains("C=57014"), "{error}");
    creator.ok("rollback").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_wait_visibility_follows_transaction_isolation() {
    for level in ["repeatable read", "serializable"] {
        let (server, _setup) = setup_basic().await;
        let mut child = Connection::connect(&server).await.unwrap();
        child.ok(&format!("begin isolation level {level}")).await;
        child.ok("select 1").await;
        let mut parent = Connection::connect(&server).await.unwrap();
        parent.ok("begin").await;
        parent.ok("insert into fk_parent values (99)").await;
        let task =
            tokio::spawn(async move { child.query("insert into fk_child values (99, 99)").await });
        wait_for_new_lock_waiter(&server, 0).await;
        assert!(!task.is_finished());
        parent.ok("commit").await;
        let error = task
            .await
            .unwrap()
            .unwrap()
            .result
            .err()
            .expect("retained snapshot must reject current parent");
        assert!(error.message.contains("C=40001"), "{level}: {error}");
    }

    let (server, _setup) = setup_basic().await;
    let mut child = Connection::connect(&server).await.unwrap();
    child.ok("begin isolation level read committed").await;
    let mut parent = Connection::connect(&server).await.unwrap();
    parent.ok("begin").await;
    parent.ok("insert into fk_parent values (99)").await;
    let task = tokio::spawn(async move {
        let outcome = child.query("insert into fk_child values (99, 99)").await;
        (child, outcome)
    });
    wait_for_new_lock_waiter(&server, 0).await;
    parent.ok("commit").await;
    let (mut child, outcome) = task.await.unwrap();
    outcome.unwrap().result.unwrap();
    child.ok("commit").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dependent_scan_post_wait_visibility_follows_transaction_isolation() {
    for level in ["read committed", "repeatable read", "serializable"] {
        let (server, mut setup) = setup_basic().await;
        setup.ok("insert into fk_child values (1, 1)").await;

        let mut parent = Connection::connect(&server).await.unwrap();
        parent.ok(&format!("begin isolation level {level}")).await;
        parent.ok("select 1").await;

        let mut child = Connection::connect(&server).await.unwrap();
        child.ok("begin").await;
        child
            .ok("update fk_child set parent_id = null where id = 1")
            .await;
        let task = tokio::spawn(async move {
            let outcome = parent.query("delete from fk_parent where id = 1").await;
            (parent, outcome)
        });
        wait_for_new_lock_waiter(&server, 0).await;
        child.ok("commit").await;

        let (mut parent, outcome) = task.await.unwrap();
        let outcome = outcome.unwrap();
        if level == "read committed" {
            outcome.result.unwrap();
            parent.ok("commit").await;
        } else {
            let error = outcome
                .result
                .err()
                .unwrap_or_else(|| panic!("{level} must reject the settled child successor"));
            assert!(error.message.contains("C=40001"), "{level}: {error}");
            parent.ok("rollback").await;
        }
    }
}

#[tokio::test]
async fn self_reference_and_savepoint_recovery_preserve_valid_work() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table nodes (id integer primary key, parent_id integer references nodes)")
        .await;
    conn.ok("insert into nodes values (1, 1)").await;
    conn.ok("update nodes set id = 2, parent_id = 2 where id = 1")
        .await;
    conn.ok("delete from nodes where id = 2").await;

    conn.ok("create table tx_parent (id integer primary key)")
        .await;
    conn.ok("create table tx_child (id integer primary key, p integer references tx_parent)")
        .await;
    conn.ok("insert into tx_parent values (1)").await;
    conn.ok("begin").await;
    conn.ok("insert into tx_child values (1, 1)").await;
    conn.ok("savepoint before_bad").await;
    let error = conn
        .query("insert into tx_child values (2, 99)")
        .await
        .unwrap()
        .result
        .err()
        .expect("foreign key violation expected");
    assert!(error.message.contains("C=23503"), "{error}");
    conn.ok("rollback to savepoint before_bad").await;
    conn.ok("commit").await;
    assert_eq!(
        server
            .simple_query("select id from tx_child")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("1".to_string())]]
    );
}

#[tokio::test]
async fn foreign_key_error_aborts_transaction_until_full_rollback() {
    let server = TestServer::start().await.unwrap();
    let mut conn = Connection::connect(&server).await.unwrap();
    conn.ok("create table tx_parent (id integer primary key)")
        .await;
    conn.ok("create table tx_child (id integer primary key, p integer references tx_parent)")
        .await;
    conn.ok("insert into tx_parent values (1)").await;

    conn.ok("begin").await;
    conn.ok("insert into tx_child values (1, 1)").await;
    let violation = conn
        .query("insert into tx_child values (2, 99)")
        .await
        .unwrap();
    assert_eq!(violation.status, b'E');
    let error = violation
        .result
        .err()
        .expect("foreign key violation expected");
    assert!(error.message.contains("C=23503"), "{error}");

    let aborted = conn.query("select * from tx_child").await.unwrap();
    assert_eq!(aborted.status, b'E');
    let error = aborted
        .result
        .err()
        .expect("aborted transaction must reject commands");
    assert!(error.message.contains("C=25P02"), "{error}");
    conn.ok("rollback").await;

    assert!(
        server
            .simple_query("select * from tx_child")
            .await
            .unwrap()
            .unwrap_rows()
            .is_empty()
    );
    server
        .simple_query("delete from tx_parent where id = 1")
        .await
        .unwrap();
    conn.ok("insert into tx_parent values (1)").await;
    conn.ok("insert into tx_child values (3, 1)").await;
}

#[tokio::test]
async fn prepared_child_dml_is_invalidated_across_fk_add_and_drop() {
    let server = TestServer::start().await.unwrap();
    let mut prepared = Connection::connect(&server).await.unwrap();
    prepared
        .ok("create table prep_parent (id integer primary key)")
        .await;
    prepared
        .ok("create table prep_child (id integer primary key, p integer)")
        .await;
    prepared.ok("insert into prep_parent values (1)").await;
    prepared
        .prepare("before_add", "insert into prep_child values (1, 99)")
        .await
        .unwrap();
    prepared
        .prepare("parent_before_add", "delete from prep_parent where id = 1")
        .await
        .unwrap();
    server
        .simple_query(
            "alter table prep_child add constraint prep_fk foreign key (p) references prep_parent",
        )
        .await
        .unwrap();

    let error = prepared
        .execute_prepared("before_add")
        .await
        .unwrap()
        .result
        .err()
        .expect("FK attachment must invalidate cached child DML");
    assert!(
        error.message.contains("C=0A000")
            && error.message.contains("cached plan must be reprepared"),
        "{error}"
    );
    prepared
        .prepare("while_present", "insert into prep_child values (2, 99)")
        .await
        .unwrap();
    let error = prepared
        .execute_prepared("while_present")
        .await
        .unwrap()
        .result
        .err()
        .expect("reprepared DML must enforce the attached FK");
    assert!(error.message.contains("C=23503"), "{error}");
    server
        .simple_query("insert into prep_child values (10, 1)")
        .await
        .unwrap();
    let error = prepared
        .execute_prepared("parent_before_add")
        .await
        .unwrap()
        .result
        .err()
        .expect("prepared parent DML must discover the newly attached FK");
    assert!(error.message.contains("C=23503"), "{error}");

    prepared
        .prepare("before_drop", "insert into prep_child values (3, 99)")
        .await
        .unwrap();
    prepared
        .prepare("parent_before_drop", "delete from prep_parent where id = 1")
        .await
        .unwrap();
    server
        .simple_query("alter table prep_child drop constraint prep_fk")
        .await
        .unwrap();
    let error = prepared
        .execute_prepared("before_drop")
        .await
        .unwrap()
        .result
        .err()
        .expect("FK removal must invalidate cached child DML");
    assert!(
        error.message.contains("C=0A000")
            && error.message.contains("cached plan must be reprepared"),
        "{error}"
    );
    let error = prepared
        .execute_prepared("parent_before_drop")
        .await
        .unwrap()
        .result
        .err()
        .expect("FK removal must invalidate cached parent DML identities");
    assert!(
        error.message.contains("C=0A000")
            && error.message.contains("cached plan must be reprepared"),
        "{error}"
    );
    prepared
        .prepare("after_drop", "insert into prep_child values (4, 99)")
        .await
        .unwrap();
    prepared
        .execute_prepared("after_drop")
        .await
        .unwrap()
        .unwrap();
    prepared
        .prepare("parent_after_drop", "delete from prep_parent where id = 1")
        .await
        .unwrap();
    prepared
        .execute_prepared("parent_after_drop")
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn checkpoint_restart_preserves_foreign_keys_and_allocator_high_water() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let server = TestServer::start_with_data_dir(&path).await.unwrap();
        server
            .simple_query("create table cp_parent (id integer primary key)")
            .await
            .unwrap();
        server
            .simple_query("create table cp_child (id integer primary key, p integer)")
            .await
            .unwrap();
        server
            .simple_query(
                "alter table cp_child add constraint first_fk foreign key (p) references cp_parent",
            )
            .await
            .unwrap();
        server
            .simple_query("alter table cp_child drop constraint first_fk")
            .await
            .unwrap();
        server
            .simple_query("alter table cp_child add constraint second_fk foreign key (p) references cp_parent")
            .await
            .unwrap();
        server.force_checkpoint().await.unwrap();
    }
    let server = TestServer::start_with_data_dir(&path).await.unwrap();
    let child = server
        .app()
        .components
        .catalog
        .get_table_by_name("cp_child")
        .unwrap()
        .unwrap();
    assert_eq!(child.foreign_keys[0].id, 1);
    assert_eq!(child.next_foreign_key_id, 2);
    let error = server
        .simple_query("insert into cp_child values (1, 99)")
        .await
        .err()
        .expect("foreign key violation expected");
    assert!(error.message.contains("23503"), "{error}");
}

#[tokio::test]
async fn crash_discards_uncommitted_transactional_fk_create() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let app = saguarodb_server::recovery::open_app(saguarodb_server::config::Config {
            data_dir: path.clone(),
            ..saguarodb_server::config::Config::default()
        })
        .unwrap();
        let cancel = std::sync::Arc::new(QueryCancel::new());
        let isolation = IsolationLevel::default();
        let (slot, _, result) = app
            .query_service
            .execute_simple("begin", None, isolation, &cancel);
        result.unwrap();
        let (slot, _, result) = app.query_service.execute_simple(
            "create table crash_parent (id integer primary key)",
            slot,
            isolation,
            &cancel,
        );
        result.unwrap();
        let (slot, _, result) = app.query_service.execute_simple(
            "create table crash_child (id integer primary key, p integer references crash_parent)",
            slot,
            isolation,
            &cancel,
        );
        result.unwrap();
        assert!(slot.is_some(), "transaction must remain in flight");
        app.components
            .wal
            .flush()
            .expect("uncommitted FK catalog WAL must be durable before crash");
        std::mem::forget(slot);
    }
    let server = TestServer::start_with_data_dir(&path).await.unwrap();
    for table in ["crash_parent", "crash_child"] {
        let error = server
            .simple_query(&format!("select * from {table}"))
            .await
            .err()
            .expect("in-flight table must be absent after recovery");
        assert!(error.message.contains("42P01"), "{table}: {error}");
    }
    server
        .simple_query("create table after_crash (id integer primary key)")
        .await
        .unwrap();
    let after = server
        .app()
        .components
        .catalog
        .get_table_by_name("after_crash")
        .unwrap()
        .unwrap();
    assert!(after.id > 2, "recovery must preserve burned relation IDs");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadlock_cycle_spanning_fk_wait_and_sequence_lock_has_one_victim() {
    let (server, mut setup) = setup_basic().await;
    setup.ok("create sequence fk_deadlock_seq").await;

    let mut first = Connection::connect(&server).await.unwrap();
    first.ok("begin").await;
    first.ok("select nextval('fk_deadlock_seq')").await;
    first.ok("insert into fk_child values (10, 1)").await;

    let mut second = Connection::connect(&server).await.unwrap();
    second.ok("begin").await;
    second.ok("update fk_parent set id = 22 where id = 2").await;
    let sequence_wait = tokio::spawn(async move {
        let outcome = second.query("drop sequence fk_deadlock_seq").await;
        (second, outcome)
    });
    wait_for_new_lock_waiter(&server, 0).await;
    assert!(!sequence_wait.is_finished());

    let fk_wait = tokio::spawn(async move {
        let outcome = first.query("insert into fk_child values (20, 2)").await;
        (first, outcome)
    });
    wait_for_new_lock_waiter(&server, 1).await;

    assert_one_deadlock_victim(fk_wait, sequence_wait).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadlock_cycle_spanning_fk_wait_and_relation_lock_has_one_victim() {
    let (server, _setup) = setup_basic().await;

    let mut first = Connection::connect(&server).await.unwrap();
    first.ok("begin").await;
    first.ok("insert into fk_child values (10, 1)").await;

    let mut second = Connection::connect(&server).await.unwrap();
    second.ok("begin").await;
    second.ok("update fk_parent set id = 22 where id = 2").await;
    let relation_wait = tokio::spawn(async move {
        let outcome = second.query("truncate fk_child").await;
        (second, outcome)
    });
    wait_for_new_lock_waiter(&server, 0).await;
    assert!(!relation_wait.is_finished());

    let fk_wait = tokio::spawn(async move {
        let outcome = first.query("insert into fk_child values (20, 2)").await;
        (first, outcome)
    });
    wait_for_new_lock_waiter(&server, 1).await;
    assert_one_deadlock_victim(fk_wait, relation_wait).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unique_parent_and_fk_dependent_scans_form_ssi_dependencies() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table ssi_parent (id integer primary key, code integer unique, payload integer)")
        .await;
    setup
        .ok("create table ssi_child (id integer primary key, code integer references ssi_parent(code))")
        .await;
    setup
        .ok("insert into ssi_parent values (1, 10, 0), (2, 20, 0)")
        .await;

    let mut child_writer = Connection::connect(&server).await.unwrap();
    let mut parent_writer = Connection::connect(&server).await.unwrap();
    child_writer.ok("begin isolation level serializable").await;
    parent_writer.ok("begin isolation level serializable").await;
    parent_writer.ok("select * from ssi_child").await;
    child_writer
        .ok("insert into ssi_child values (1, 10)")
        .await;
    parent_writer
        .ok("update ssi_parent set payload = 1 where id = 1")
        .await;
    let first = child_writer.query("commit").await.unwrap().result;
    let second = parent_writer.query("commit").await.unwrap().result;
    assert_eq!(
        [&first, &second]
            .into_iter()
            .filter(|result| result.is_err())
            .count(),
        1
    );
    assert!(
        first
            .err()
            .or(second.err())
            .unwrap()
            .message
            .contains("C=40001")
    );

    let mut parent_delete = Connection::connect(&server).await.unwrap();
    let mut child_insert = Connection::connect(&server).await.unwrap();
    parent_delete.ok("begin isolation level serializable").await;
    child_insert.ok("begin isolation level serializable").await;
    child_insert
        .ok("select * from ssi_parent where id = 2")
        .await;
    parent_delete
        .ok("delete from ssi_parent where id = 2")
        .await;
    child_insert
        .ok("insert into ssi_child values (2, 10)")
        .await;
    let first = parent_delete.query("commit").await.unwrap().result;
    let second = child_insert.query("commit").await.unwrap().result;
    assert_eq!(
        [&first, &second]
            .into_iter()
            .filter(|result| result.is_err())
            .count(),
        1
    );
    assert!(
        first
            .err()
            .or(second.err())
            .unwrap()
            .message
            .contains("C=40001")
    );
}
