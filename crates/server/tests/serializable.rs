mod support;

use support::{Connection, TestServer};

/// Classic write skew: two `SERIALIZABLE` transactions each read the whole table and
/// then update a different row based on what they read. Snapshot isolation (Repeatable
/// Read) lets both commit — a serializability anomaly — but SSI detects the rw-cycle
/// and aborts exactly one with `40001` (`docs/specs/ssi.md`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_skew_aborts_one_serializable_transaction() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup.ok("insert into t (id, v) values (1, 10)").await;
    setup.ok("insert into t (id, v) values (2, 20)").await;

    let mut t1 = Connection::connect(&server).await.unwrap();
    let mut t2 = Connection::connect(&server).await.unwrap();
    t1.ok("begin isolation level serializable").await;
    t2.ok("begin isolation level serializable").await;
    // Each reads the whole table (a relation SIREAD lock), then writes a different row.
    t1.ok("select v from t").await;
    t2.ok("select v from t").await;
    t1.ok("update t set v = 100 where id = 1").await;
    t2.ok("update t set v = 200 where id = 2").await;

    // Both are pivots; committing closes the rw-cycle, so exactly one aborts with 40001.
    let r1 = t1.query("commit").await.unwrap().result;
    let r2 = t2.query("commit").await.unwrap().result;
    let failures = [&r1, &r2].into_iter().filter(|r| r.is_err()).count();
    assert_eq!(
        failures,
        1,
        "exactly one serializable txn must abort (t1_err={}, t2_err={})",
        r1.is_err(),
        r2.is_err()
    );
    let err = r1.err().or(r2.err()).unwrap();
    assert!(
        err.message.contains("C=40001"),
        "victim gets 40001: {}",
        err.message
    );
    assert_eq!(server.active_txn_count(), 0);
}

/// Write-skew where one transaction WRITES before the other READS the affected row
/// (the read-side "conflict-out" ordering). T1 reads + writes before T2 even reads, so
/// the `t2 →rw t1` edge can only be formed at T2's READ (the writer's SIREAD scan ran
/// before T2's lock existed). Must still abort exactly one (`docs/specs/ssi.md` §6).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_skew_with_write_before_read_is_detected() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup.ok("insert into t (id, v) values (1, 10)").await;
    setup.ok("insert into t (id, v) values (2, 20)").await;

    let mut t1 = Connection::connect(&server).await.unwrap();
    let mut t2 = Connection::connect(&server).await.unwrap();
    t1.ok("begin isolation level serializable").await;
    t2.ok("begin isolation level serializable").await;
    // T1 fully reads then writes BEFORE T2 reads.
    t1.ok("select v from t").await;
    t1.ok("update t set v = 100 where id = 1").await;
    // T2 now reads the table — it sees the old row 1 (T1 is concurrent/invisible). This
    // read must form t2 →rw t1.
    t2.ok("select v from t").await;
    t2.ok("update t set v = 200 where id = 2").await;
    let r1 = t1.query("commit").await.unwrap().result;
    let r2 = t2.query("commit").await.unwrap().result;
    let failures = [&r1, &r2].into_iter().filter(|r| r.is_err()).count();
    assert_eq!(
        failures,
        1,
        "exactly one aborts (t1_err={}, t2_err={})",
        r1.is_err(),
        r2.is_err()
    );
    assert!(r1.err().or(r2.err()).unwrap().message.contains("C=40001"));
    assert_eq!(server.active_txn_count(), 0);
}

/// The `INSERT ... ON CONFLICT` primary-key arbiter probe is a read and must record a
/// SIREAD lock (`docs/specs/ssi.md` §5.1). Here each transaction probes one key with
/// `ON CONFLICT DO NOTHING` (the row exists → it inserts nothing) and updates the OTHER
/// row, forming a write-skew cycle that only closes if the probe reads are tracked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_conflict_probe_read_is_tracked_for_serializable() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup.ok("insert into t (id, v) values (1, 10)").await;
    setup.ok("insert into t (id, v) values (2, 20)").await;

    let mut t1 = Connection::connect(&server).await.unwrap();
    let mut t2 = Connection::connect(&server).await.unwrap();
    t1.ok("begin isolation level serializable").await;
    t2.ok("begin isolation level serializable").await;
    // Each probes one key (exists ⇒ DO NOTHING inserts nothing — a pure read of that
    // key) and then updates the other row.
    t1.ok("insert into t (id, v) values (1, 0) on conflict do nothing")
        .await;
    t2.ok("insert into t (id, v) values (2, 0) on conflict do nothing")
        .await;
    t1.ok("update t set v = 100 where id = 2").await;
    t2.ok("update t set v = 200 where id = 1").await;
    let r1 = t1.query("commit").await.unwrap().result;
    let r2 = t2.query("commit").await.unwrap().result;
    let failures = [&r1, &r2].into_iter().filter(|r| r.is_err()).count();
    assert_eq!(
        failures,
        1,
        "exactly one aborts (t1_err={}, t2_err={})",
        r1.is_err(),
        r2.is_err()
    );
    assert!(r1.err().or(r2.err()).unwrap().message.contains("C=40001"));
    assert_eq!(server.active_txn_count(), 0);
}

/// The SAME workload under REPEATABLE READ commits BOTH transactions: snapshot
/// isolation permits write skew. This proves SERIALIZABLE is strictly stronger — the
/// SSI machinery (not the shared snapshot) is what aborts the cycle above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_skew_is_allowed_under_repeatable_read() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup.ok("insert into t (id, v) values (1, 10)").await;
    setup.ok("insert into t (id, v) values (2, 20)").await;

    let mut t1 = Connection::connect(&server).await.unwrap();
    let mut t2 = Connection::connect(&server).await.unwrap();
    t1.ok("begin isolation level repeatable read").await;
    t2.ok("begin isolation level repeatable read").await;
    t1.ok("select v from t").await;
    t2.ok("select v from t").await;
    t1.ok("update t set v = 100 where id = 1").await;
    t2.ok("update t set v = 200 where id = 2").await;
    // Disjoint rows ⇒ no write-write conflict; snapshot isolation commits both.
    t1.ok("commit").await;
    t2.ok("commit").await;
    assert_eq!(server.active_txn_count(), 0);
    assert_eq!(
        setup.ok("select v from t order by id").await.rows(),
        vec![vec![Some("100".to_string())], vec![Some("200".to_string())]],
        "both write-skew updates committed under Repeatable Read"
    );
}

/// Phantom protection: a SERIALIZABLE transaction scans a table while a concurrent
/// SERIALIZABLE transaction INSERTs a new row into it (and each writes based on its
/// scan). The insert forms an rw-edge with the scan's relation SIREAD lock, closing a
/// cycle, so exactly one aborts with `40001` (`docs/specs/ssi.md` §6).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phantom_insert_into_scanned_table_aborts_one() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup.ok("insert into t (id, v) values (1, 10)").await;

    let mut t1 = Connection::connect(&server).await.unwrap();
    let mut t2 = Connection::connect(&server).await.unwrap();
    t1.ok("begin isolation level serializable").await;
    t2.ok("begin isolation level serializable").await;
    t1.ok("select v from t").await; // relation SIREAD lock on t
    t2.ok("select v from t").await;
    t1.ok("update t set v = 11 where id = 1").await; // edge t2 →rw t1
    t2.ok("insert into t (id, v) values (2, 20)").await; // phantom: edge t1 →rw t2
    let r1 = t1.query("commit").await.unwrap().result;
    let r2 = t2.query("commit").await.unwrap().result;
    let failures = [&r1, &r2].into_iter().filter(|r| r.is_err()).count();
    assert_eq!(
        failures,
        1,
        "exactly one aborts (t1_err={}, t2_err={})",
        r1.is_err(),
        r2.is_err()
    );
    assert!(
        r1.err().or(r2.err()).unwrap().message.contains("C=40001"),
        "the victim gets 40001"
    );
    assert_eq!(server.active_txn_count(), 0);
}

/// A composite primary-key prefix scan can use the PK index but is not a single
/// tuple read. It must therefore take a relation SIREAD lock; otherwise an insert
/// of another `(tenant, id)` value under that prefix would miss the phantom edge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composite_pk_prefix_index_scan_tracks_relation_siread() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (tenant integer, id integer, v integer, primary key (tenant, id))")
        .await;
    setup
        .ok("insert into t (tenant, id, v) values (1, 1, 10)")
        .await;

    let explain = setup
        .ok("explain select v from t where tenant = 1")
        .await
        .rows();
    assert!(
        explain[0][0].as_ref().unwrap().contains("IndexScan"),
        "composite PK prefix lookup should use an IndexScan, got: {explain:?}"
    );

    let mut t1 = Connection::connect(&server).await.unwrap();
    let mut t2 = Connection::connect(&server).await.unwrap();
    t1.ok("begin isolation level serializable").await;
    t2.ok("begin isolation level serializable").await;
    t1.ok("select v from t where tenant = 1").await;
    t2.ok("select v from t where tenant = 1").await;
    t1.ok("update t set v = 11 where tenant = 1 and id = 1")
        .await;
    t2.ok("insert into t (tenant, id, v) values (1, 2, 20)")
        .await;

    let r1 = t1.query("commit").await.unwrap().result;
    let r2 = t2.query("commit").await.unwrap().result;
    let failures = [&r1, &r2].into_iter().filter(|r| r.is_err()).count();
    assert_eq!(
        failures,
        1,
        "exactly one aborts (t1_err={}, t2_err={})",
        r1.is_err(),
        r2.is_err()
    );
    assert!(r1.err().or(r2.err()).unwrap().message.contains("C=40001"));
    assert_eq!(server.active_txn_count(), 0);
}

/// A read-only SERIALIZABLE transaction is never a pivot (no writes ⇒ no incoming
/// rw-edge), so it commits cleanly even when a concurrent SERIALIZABLE writer modifies
/// a row it read. A single rw-edge is not a cycle — no false abort.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_only_serializable_does_not_falsely_abort() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table t (id integer primary key, v integer)")
        .await;
    setup.ok("insert into t (id, v) values (1, 10)").await;

    let mut reader = Connection::connect(&server).await.unwrap();
    let mut writer = Connection::connect(&server).await.unwrap();
    reader.ok("begin isolation level serializable").await;
    writer.ok("begin isolation level serializable").await;
    reader.ok("select v from t").await; // reads the row the writer will change
    writer.ok("update t set v = 99 where id = 1").await; // edge reader →rw writer
    writer.ok("commit").await; // the writer is not a pivot ⇒ commits
    // The read-only reader has only an outgoing edge (no cycle) ⇒ commits cleanly.
    reader.ok("select v from t").await;
    reader.ok("commit").await;
    assert_eq!(server.active_txn_count(), 0);
}

/// A transaction that captured its snapshot while a concurrent TRUNCATE was in
/// progress can read the replacement only after the truncator releases
/// `AccessExclusive`. The retained relation-write record must still form the
/// reader's outbound rw-edge; a later inbound edge then makes that reader a doomed
/// pivot and aborts it with `40001`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transactional_truncate_relation_write_participates_in_ssi() {
    let server = TestServer::start().await.unwrap();
    let mut setup = Connection::connect(&server).await.unwrap();
    setup
        .ok("create table truncated (id integer primary key)")
        .await;
    setup
        .ok("create table anchor (id integer primary key)")
        .await;
    setup
        .ok("create table written (id integer primary key, v integer)")
        .await;
    setup.ok("insert into truncated values (1)").await;
    setup.ok("insert into anchor values (1)").await;
    setup.ok("insert into written values (1, 10)").await;

    let mut truncator = Connection::connect(&server).await.unwrap();
    let mut pivot = Connection::connect(&server).await.unwrap();
    let mut predecessor = Connection::connect(&server).await.unwrap();

    truncator.ok("begin isolation level serializable").await;
    truncator.ok("truncate truncated").await;

    pivot.ok("begin isolation level serializable").await;
    pivot.ok("select * from anchor").await;
    predecessor.ok("begin isolation level serializable").await;
    predecessor.ok("select * from written").await;

    truncator.ok("commit").await;
    assert!(
        pivot.ok("select * from truncated").await.rows().is_empty(),
        "the transaction follows the committed replacement generation"
    );

    let err = pivot
        .query("update written set v = 20 where id = 1")
        .await
        .unwrap()
        .result
        .err()
        .expect("the pivot must abort when it gains an inbound edge");
    assert!(err.message.contains("C=40001"), "victim gets 40001: {err}");
    pivot.ok("rollback").await;
    predecessor.ok("commit").await;
    assert_eq!(server.active_txn_count(), 0);
}
