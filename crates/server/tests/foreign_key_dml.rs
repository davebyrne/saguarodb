mod support;

use std::time::Duration;

use support::{Connection, TestServer};

async fn setup_foreign_key_server() -> (TestServer, Connection) {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table parents (id integer primary key)")
        .await;
    setup
        .ok("create table children (id integer primary key, parent_id integer)")
        .await;
    setup.ok("insert into parents values (1)").await;
    server
        .attach_foreign_key(
            "children_parent_id_fkey",
            "children",
            &["parent_id"],
            "parents",
            &["id"],
        )
        .unwrap();
    (server, setup)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_waits_for_an_earlier_creator_before_fk_validation() {
    let (server, _setup) = setup_foreign_key_server().await;
    let mut holder = Connection::connect(&server).await.unwrap();
    holder.ok("begin").await;
    holder.ok("insert into children values (1, 1)").await;

    let mut contender = Connection::connect(&server).await.unwrap();
    let task = tokio::spawn(async move {
        contender
            .query("insert into children values (1, 999) on conflict (id) do nothing returning id")
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !task.is_finished(),
        "the contender must wait for the earlier creator"
    );

    holder.ok("commit").await;
    let outcome = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("contender completed after holder commit")
        .expect("contender task")
        .expect("contender transport");
    assert!(
        outcome.result.unwrap().unwrap_rows().is_empty(),
        "DO NOTHING skips without validating the rejected parent 999"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_conflict_upsert_reserves_its_key_across_a_blocked_fk_probe() {
    let (server, _setup) = setup_foreign_key_server().await;
    let mut parent_holder = Connection::connect(&server).await.unwrap();
    parent_holder.ok("begin").await;
    parent_holder.ok("insert into parents values (99)").await;

    let mut first = Connection::connect(&server).await.unwrap();
    let first_task = tokio::spawn(async move {
        first
            .query(
                "insert into children values (2, 99) on conflict (id) do nothing returning parent_id",
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !first_task.is_finished(),
        "the first upsert must wait for its parent creator"
    );

    let mut second = Connection::connect(&server).await.unwrap();
    let second_task = tokio::spawn(async move {
        second
            .query(
                "insert into children values (2, 1) on conflict (id) do nothing returning parent_id",
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !second_task.is_finished(),
        "the later contender must wait for the reserved child key"
    );

    parent_holder.ok("commit").await;
    let first = tokio::time::timeout(Duration::from_secs(2), first_task)
        .await
        .expect("first upsert completed")
        .expect("first task")
        .expect("first transport")
        .result
        .unwrap()
        .unwrap_rows();
    let second = tokio::time::timeout(Duration::from_secs(2), second_task)
        .await
        .expect("second upsert completed")
        .expect("second task")
        .expect("second transport")
        .result
        .unwrap()
        .unwrap_rows();
    assert_eq!(first, vec![vec![Some("99".to_string())]]);
    assert!(
        second.is_empty(),
        "the late contender re-arbitrates and skips"
    );

    let rows = server
        .simple_query("select id, parent_id from children where id = 2")
        .await
        .unwrap()
        .unwrap_rows();
    assert_eq!(
        rows,
        vec![vec![Some("2".to_string()), Some("99".to_string())]]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pk_changing_upsert_reserves_the_effective_key_before_fk_validation() {
    let (server, mut setup) = setup_foreign_key_server().await;
    setup.ok("insert into children values (10, 1)").await;
    let mut parent_holder = Connection::connect(&server).await.unwrap();
    parent_holder.ok("begin").await;
    parent_holder.ok("insert into parents values (99)").await;

    let mut updater = Connection::connect(&server).await.unwrap();
    let updater_task = tokio::spawn(async move {
        updater
            .query(
                "insert into children values (10, 1) on conflict (id) do update \
                 set id = 11, parent_id = 99 returning id, parent_id",
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!updater_task.is_finished(), "the FK probe must wait");

    let mut contender = Connection::connect(&server).await.unwrap();
    let contender_task =
        tokio::spawn(async move { contender.query("insert into children values (11, 1)").await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !contender_task.is_finished(),
        "the contender must wait for the effective replacement key"
    );

    parent_holder.ok("commit").await;
    assert_eq!(
        updater_task
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("11".to_string()), Some("99".to_string())]]
    );
    let contender = contender_task
        .await
        .unwrap()
        .unwrap()
        .result
        .err()
        .expect("the late insert must lose the effective-key race");
    assert!(contender.message.contains("C=23505"), "{contender}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_rechecks_after_early_abort_and_releases_late_reservation_on_fk_abort() {
    let (server, _setup) = setup_foreign_key_server().await;
    let mut holder = Connection::connect(&server).await.unwrap();
    holder.ok("begin").await;
    holder.ok("insert into children values (20, 1)").await;
    let mut early = Connection::connect(&server).await.unwrap();
    let early_task = tokio::spawn(async move {
        early
            .query("insert into children values (20, 999) on conflict do nothing")
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!early_task.is_finished(), "the upsert must wait");
    holder.ok("rollback").await;
    let early = early_task
        .await
        .unwrap()
        .unwrap()
        .result
        .err()
        .expect("the proposed row must be validated after the creator aborts");
    assert!(early.message.contains("C=23503"), "{early}");

    let mut parent_holder = Connection::connect(&server).await.unwrap();
    parent_holder.ok("begin").await;
    parent_holder.ok("insert into parents values (199)").await;
    let mut first = Connection::connect(&server).await.unwrap();
    let first_task = tokio::spawn(async move {
        first
            .query("insert into children values (23, 199) on conflict do nothing")
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut second = Connection::connect(&server).await.unwrap();
    let second_task = tokio::spawn(async move {
        second
            .query("insert into children values (23, 1) on conflict do nothing returning parent_id")
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!first_task.is_finished() && !second_task.is_finished());
    parent_holder.ok("rollback").await;
    let first = first_task
        .await
        .unwrap()
        .unwrap()
        .result
        .err()
        .expect("the missing parent must abort the first upsert");
    assert!(first.message.contains("C=23503"), "{first}");
    assert_eq!(
        second_task
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("1".to_string())]],
        "the aborted upsert must release its reserved key"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_insert_reserves_its_key_across_a_blocked_fk_probe() {
    let (server, _setup) = setup_foreign_key_server().await;
    let mut parent_holder = Connection::connect(&server).await.unwrap();
    parent_holder.ok("begin").await;
    parent_holder.ok("insert into parents values (99)").await;

    let mut first = Connection::connect(&server).await.unwrap();
    let first_task = tokio::spawn(async move {
        first
            .query("insert into children values (3, 99) returning parent_id")
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!first_task.is_finished(), "the FK probe must wait");

    let mut second = Connection::connect(&server).await.unwrap();
    let second_task = tokio::spawn(async move {
        second
            .query("insert into children values (3, 1) returning parent_id")
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !second_task.is_finished(),
        "the later insert must wait for the reserved child key"
    );

    parent_holder.ok("commit").await;
    let first = first_task
        .await
        .unwrap()
        .unwrap()
        .result
        .unwrap()
        .unwrap_rows();
    assert_eq!(first, vec![vec![Some("99".to_string())]]);
    let second = second_task
        .await
        .unwrap()
        .unwrap()
        .result
        .err()
        .expect("the duplicate insert must fail");
    assert!(second.message.contains("C=23505"), "{second}");
    assert_eq!(
        server
            .simple_query("select parent_id from children where id = 3")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("99".to_string())]]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_insert_waits_for_an_earlier_creator_then_rechecks_after_abort() {
    let (server, _setup) = setup_foreign_key_server().await;
    let mut holder = Connection::connect(&server).await.unwrap();
    holder.ok("begin").await;
    holder.ok("insert into children values (5, 1)").await;

    let mut contender = Connection::connect(&server).await.unwrap();
    let contender_task = tokio::spawn(async move {
        contender
            .query("insert into children values (5, 1) returning parent_id")
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !contender_task.is_finished(),
        "the contender must wait for the earlier creator"
    );

    holder.ok("rollback").await;
    assert_eq!(
        contender_task
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("1".to_string())]],
        "the insert must recheck and proceed after the creator aborts"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_insert_rechecks_after_early_commit_and_releases_on_fk_abort() {
    let (server, _setup) = setup_foreign_key_server().await;
    let mut holder = Connection::connect(&server).await.unwrap();
    holder.ok("begin").await;
    holder.ok("insert into children values (21, 1)").await;
    let mut early = Connection::connect(&server).await.unwrap();
    let early_task =
        tokio::spawn(async move { early.query("insert into children values (21, 1)").await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!early_task.is_finished(), "the insert must wait");
    holder.ok("commit").await;
    let early = early_task
        .await
        .unwrap()
        .unwrap()
        .result
        .err()
        .expect("the insert must recheck the committed winner");
    assert!(early.message.contains("C=23505"), "{early}");

    let mut parent_holder = Connection::connect(&server).await.unwrap();
    parent_holder.ok("begin").await;
    parent_holder.ok("insert into parents values (199)").await;
    let mut first = Connection::connect(&server).await.unwrap();
    let first_task =
        tokio::spawn(async move { first.query("insert into children values (24, 199)").await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut second = Connection::connect(&server).await.unwrap();
    let second_task = tokio::spawn(async move {
        second
            .query("insert into children values (24, 1) returning parent_id")
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!first_task.is_finished() && !second_task.is_finished());
    parent_holder.ok("rollback").await;
    let first = first_task
        .await
        .unwrap()
        .unwrap()
        .result
        .err()
        .expect("the missing parent must abort the first insert");
    assert!(first.message.contains("C=23503"), "{first}");
    assert_eq!(
        second_task
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("1".to_string())]],
        "the aborted insert must release its reserved key"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_from_reserves_its_key_across_a_blocked_fk_probe() {
    let (server, _setup) = setup_foreign_key_server().await;
    let mut parent_holder = Connection::connect(&server).await.unwrap();
    parent_holder.ok("begin").await;
    parent_holder.ok("insert into parents values (99)").await;

    let mut copy = Connection::connect(&server).await.unwrap();
    copy.begin_copy_from("copy children from stdin")
        .await
        .unwrap();
    let copy_task = tokio::spawn(async move { copy.finish_copy_from(&[b"4\t99\n"]).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!copy_task.is_finished(), "the COPY FK probe must wait");

    let mut contender = Connection::connect(&server).await.unwrap();
    let contender_task =
        tokio::spawn(async move { contender.query("insert into children values (4, 1)").await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !contender_task.is_finished(),
        "the insert must wait for COPY's reserved child key"
    );

    parent_holder.ok("commit").await;
    let completion = copy_task.await.unwrap().unwrap();
    assert_eq!(completion.command_tag.as_deref(), Some("COPY 1"));
    let contender = contender_task
        .await
        .unwrap()
        .unwrap()
        .result
        .err()
        .expect("the duplicate insert must fail");
    assert!(contender.message.contains("C=23505"), "{contender}");
    assert_eq!(
        server
            .simple_query("select parent_id from children where id = 4")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("99".to_string())]]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_from_waits_for_an_earlier_creator_then_rechecks_after_commit() {
    let (server, _setup) = setup_foreign_key_server().await;
    let mut holder = Connection::connect(&server).await.unwrap();
    holder.ok("begin").await;
    holder.ok("insert into children values (6, 1)").await;

    let mut copy = Connection::connect(&server).await.unwrap();
    copy.begin_copy_from("copy children from stdin")
        .await
        .unwrap();
    let copy_task = tokio::spawn(async move { copy.finish_copy_from(&[b"6\t1\n"]).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !copy_task.is_finished(),
        "COPY must wait for the earlier creator"
    );

    holder.ok("commit").await;
    let completion = copy_task.await.unwrap().unwrap();
    assert_eq!(completion.error_code.as_deref(), Some("23505"));
    assert_eq!(
        server
            .simple_query("select parent_id from children where id = 6")
            .await
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("1".to_string())]],
        "COPY must recheck and preserve the committed winner"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_rechecks_after_early_abort_and_releases_late_reservation_on_fk_abort() {
    let (server, _setup) = setup_foreign_key_server().await;
    let mut holder = Connection::connect(&server).await.unwrap();
    holder.ok("begin").await;
    holder.ok("insert into children values (22, 1)").await;
    let mut early = Connection::connect(&server).await.unwrap();
    early
        .begin_copy_from("copy children from stdin")
        .await
        .unwrap();
    let early_task = tokio::spawn(async move { early.finish_copy_from(&[b"22\t1\n"]).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!early_task.is_finished(), "COPY must wait");
    holder.ok("rollback").await;
    assert_eq!(
        early_task.await.unwrap().unwrap().command_tag.as_deref(),
        Some("COPY 1"),
        "COPY must recheck and proceed after the creator aborts"
    );

    let mut parent_holder = Connection::connect(&server).await.unwrap();
    parent_holder.ok("begin").await;
    parent_holder.ok("insert into parents values (199)").await;
    let mut first = Connection::connect(&server).await.unwrap();
    first
        .begin_copy_from("copy children from stdin")
        .await
        .unwrap();
    let first_task = tokio::spawn(async move { first.finish_copy_from(&[b"25\t199\n"]).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut second = Connection::connect(&server).await.unwrap();
    let second_task = tokio::spawn(async move {
        second
            .query("insert into children values (25, 1) returning parent_id")
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!first_task.is_finished() && !second_task.is_finished());
    parent_holder.ok("rollback").await;
    assert_eq!(
        first_task.await.unwrap().unwrap().error_code.as_deref(),
        Some("23503")
    );
    assert_eq!(
        second_task
            .await
            .unwrap()
            .unwrap()
            .result
            .unwrap()
            .unwrap_rows(),
        vec![vec![Some("1".to_string())]],
        "the failed COPY must release its reserved key"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn page_backed_dml_enforces_pk_unique_copy_returning_and_no_mutation() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table parent_dml (id integer primary key, code integer unique, payload integer)")
        .await;
    setup
        .ok("create table child_dml (id integer primary key, parent_id integer, parent_code integer)")
        .await;
    setup.ok("insert into parent_dml values (1, 10, 0)").await;
    server
        .attach_foreign_key(
            "child_dml_parent_id_fkey",
            "child_dml",
            &["parent_id"],
            "parent_dml",
            &["id"],
        )
        .unwrap();
    server
        .attach_foreign_key(
            "child_dml_parent_code_fkey",
            "child_dml",
            &["parent_code"],
            "parent_dml",
            &["code"],
        )
        .unwrap();

    assert_eq!(
        setup
            .ok("insert into child_dml values (1, 1, 10) returning parent_id, parent_code")
            .await
            .rows(),
        vec![vec![Some("1".to_string()), Some("10".to_string())]]
    );

    let missing_pk = setup
        .query("insert into child_dml values (2, 999, 10) returning id")
        .await
        .unwrap()
        .result
        .err()
        .expect("the missing primary key must violate the FK");
    assert!(missing_pk.message.contains("C=23503"), "{missing_pk}");
    assert!(
        missing_pk.message.contains("child_dml_parent_id_fkey"),
        "{missing_pk}"
    );
    let missing_unique = setup
        .query("insert into child_dml values (2, 1, 999)")
        .await
        .unwrap()
        .result
        .err()
        .expect("the missing unique key must violate the FK");
    assert!(
        missing_unique.message.contains("C=23503"),
        "{missing_unique}"
    );
    assert!(
        missing_unique
            .message
            .contains("child_dml_parent_code_fkey"),
        "{missing_unique}"
    );
    assert!(
        setup
            .ok("select id from child_dml where id = 2")
            .await
            .rows()
            .is_empty()
    );

    let update = setup
        .query("update child_dml set parent_id = 999 where id = 1 returning parent_id")
        .await
        .unwrap()
        .result
        .err()
        .expect("the child update must violate the FK");
    assert!(update.message.contains("C=23503"), "{update}");
    assert_eq!(
        setup
            .ok("select parent_id from child_dml where id = 1")
            .await
            .rows(),
        vec![vec![Some("1".to_string())]]
    );

    for sql in [
        "update parent_dml set id = 2 where id = 1 returning id",
        "update parent_dml set code = 20 where id = 1 returning code",
        "delete from parent_dml where id = 1 returning id",
    ] {
        let error = setup
            .query(sql)
            .await
            .unwrap()
            .result
            .err()
            .expect("the referenced parent mutation must fail");
        assert!(error.message.contains("C=23503"), "{error}");
    }
    assert_eq!(
        setup.ok("select id, code from parent_dml").await.rows(),
        vec![vec![Some("1".to_string()), Some("10".to_string())]]
    );

    let copy = setup
        .copy_from("copy child_dml from stdin", &[b"2\t1\t10\n3\t999\t10\n"])
        .await
        .unwrap();
    assert_eq!(copy.error_code.as_deref(), Some("23503"));
    assert_eq!(
        setup
            .ok("select id from child_dml order by id")
            .await
            .rows(),
        vec![vec![Some("1".to_string())]],
        "the failed COPY rolls back its earlier valid row"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreign_key_parent_probe_forms_a_real_serializable_cycle() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table parent_ssi (id integer primary key, payload integer)")
        .await;
    setup
        .ok("create table child_ssi (id integer primary key, parent_id integer)")
        .await;
    setup.ok("insert into parent_ssi values (1, 0)").await;
    server
        .attach_foreign_key(
            "child_ssi_parent_id_fkey",
            "child_ssi",
            &["parent_id"],
            "parent_ssi",
            &["id"],
        )
        .unwrap();

    let mut child_writer = Connection::connect(&server).await.unwrap();
    let mut parent_writer = Connection::connect(&server).await.unwrap();
    child_writer.ok("begin isolation level serializable").await;
    parent_writer.ok("begin isolation level serializable").await;
    parent_writer.ok("select * from child_ssi").await;
    child_writer.ok("insert into child_ssi values (1, 1)").await;
    parent_writer
        .ok("update parent_ssi set payload = 1 where id = 1")
        .await;

    let first = child_writer.query("commit").await.unwrap().result;
    let second = parent_writer.query("commit").await.unwrap().result;
    assert_eq!(
        [&first, &second]
            .into_iter()
            .filter(|result| result.is_err())
            .count(),
        1,
        "the FK parent tuple read and child relation read must close an SSI cycle"
    );
    assert!(
        first
            .err()
            .or(second.err())
            .unwrap()
            .message
            .contains("C=40001")
    );
    assert_eq!(server.active_txn_count(), 0);
}
