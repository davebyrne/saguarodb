# Transaction-Owned Table Locks

**Status:** implementation contract

## 1. Purpose

SaguaroDB coordinates row conflicts through the server `LockManager`, while DDL
and maintenance historically used the database-wide writer/checkpoint guard.
Table locks add relation-scoped coordination so operations on unrelated tables
can proceed independently and so a transaction can safely retain a TRUNCATE
generation until COMMIT or ROLLBACK.

Table locks do not replace MVCC, page/index structural latches, row-conflict
waiting, the checkpoint guard, or schema-generation validation. They coordinate
which operations may use or replace a logical table and participate in the same
deadlock graph as row waits.

## 2. Lock modes

The first implementation has four modes, ordered from weakest to strongest:

```rust
pub enum RelationLockMode {
    AccessShare,
    RowExclusive,
    Share,
    AccessExclusive,
}
```

The symmetric compatibility matrix is:

| Held/requested | AccessShare | RowExclusive | Share | AccessExclusive |
|---|---:|---:|---:|---:|
| AccessShare | yes | yes | yes | no |
| RowExclusive | yes | yes | no | no |
| Share | yes | no | no | no |
| AccessExclusive | no | no | no | no |

Mappings:

- `AccessShare`: `SELECT`, `COPY TO`, and every user table read through a view,
  subquery, CTE, join, cursor, or suspended portal. A view reference also locks
  the view's logical relation id so replace/drop cannot change its definition.
- `RowExclusive`: `INSERT`, `UPDATE`, `DELETE`, and `COPY FROM`; a modifying
  statement takes this mode on its target and `AccessShare` on read-only source
  tables.
- `Share`: `CREATE INDEX` and the initial safe `VACUUM` integration. VACUUM may
  move to a weaker PostgreSQL-style mode only after storage is proven safe with
  concurrent writers.
- `AccessExclusive`: `TRUNCATE`, `DROP TABLE`/`DROP VIEW`, CREATE OR REPLACE VIEW
  on an existing view, and storage/schema-changing `ALTER TABLE` operations.

Locks are keyed by logical `TableId`, not physical storage id. Hidden TOAST work
is protected by the base user table's lock; callers do not separately expose or
lock hidden relation names.

The same manager also provides the two-mode sequence lifetime lock needed once
sequence DDL no longer uses the global exclusive guard:

- `SequenceAccess` is shared by `nextval`/`setval` and by DML/default expressions
  that may invoke them, including defaults evaluated while `ALTER TABLE ADD
  COLUMN` rewrites existing rows. `currval` also takes it for
  lifetime/revalidation even though it does not mutate state or require a shared
  writer guard.
- `SequenceExclusive` is used by DROP SEQUENCE and DROP TABLE's owned-sequence
  cascade; it conflicts with both modes. CREATE SEQUENCE is namespace-serialized
  by the catalog publication gate and publishes the new id before it can be referenced.

Sequence locks use the same owner, queue, cancellation, and deadlock graph as
table locks. Resource ordering is all table resources by ascending `TableId`, then
all sequence resources by ascending `SequenceId`. Binding collects referenced
sequence ids as well as table ids before acquisition. This prevents a bound
sequence call from racing removal and lets table/sequence cycles be detected.

## 3. Owners and lifetime

There are two owner kinds:

- A top-level transaction id owns every lock acquired by an explicit
  transaction. Locks survive statement completion, errors, `RELEASE SAVEPOINT`,
  and `ROLLBACK TO SAVEPOINT`, and release only at top-level COMMIT/ROLLBACK or
  disconnect cleanup.
- A generated statement-owner id owns locks for an autocommit or read-only
  statement that has no registered transaction id. Its RAII guard releases all
  locks when the statement/stream/portal operation finishes.

Autocommit DML, COPY FROM, standalone TRUNCATE, and relation-touching DDL allocate
and register their transaction id before table-lock acquisition; that top-level
xid is the table-lock owner and later row-wait owner. A single statement is never
split across statement and transaction graph nodes. If lock acquisition fails or
is canceled, normal pre-commit abort cleanup settles and deregisters the xid.
Generated statement owners are limited to operations that cannot later own row
locks, principally reads/COPY TO/EXPLAIN. VACUUM preallocates one maintenance xid
because its TOAST cleanup may perform row writes.

VACUUM is the one xid-keyed statement-lifetime owner: after its optional TOAST
deletes commit and the xid is deregistered, its owner token retains the already
granted `Share` locks through physical pruning. It performs no further row waits or
transactional writes and releases the grants at statement end. This preserves one
graph identity without exposing target tables between durable cleanup and pruning.

Subxids never own table locks independently. A savepoint statement uses the
top-level transaction owner so deadlock edges and lifetime remain transaction
scoped.

Before an explicit transaction acquires its first table or sequence lock—even
for a read—it acquires and retains the shared checkpoint-participant guard through
top-level completion. This preserves the universal guard-before-object order if a
later statement writes. Autocommit reads remain guard-free because their
statement owner cannot later upgrade into a writer. Consequently, checkpoint may
wait for an explicit transaction that has accessed a catalog object, including an
idle-in-transaction reader; this is an intentional correctness tradeoff until the
coarse checkpoint controller itself becomes deadlock-aware.

Acquisition is reentrant. Requesting a stronger mode upgrades the owner's grant;
the owner never conflicts with itself. An upgrade may block and participate in
deadlock detection.

## 4. Multi-table order, snapshots, and revalidation

A caller normalizes a request to one strongest mode per resource, sorts tables
then sequences as specified above, and acquires in that order. Stable ordering
avoids avoidable cycles; upgrades and mixed row/object waits still require
deadlock detection.

Binding/name lookup runs under the shared catalog publication gate and releases it
before waiting for locks. After acquiring all discovered object locks, an
unprepared statement reacquires the shared gate and rebinds; prepared execution
revalidates object identity, schema version, and current storage-generation ids.
It then captures relation generations before releasing the shared gate. A mismatch
returns the cached-plan-reprepare error. This full rebind/generation check is
required because TRUNCATE changes storage ids without changing schema version.

For an unprepared statement, rebound requests must converge before execution.
Normalize and compare rebound table/sequence ids and modes with the granted set.
If not covered, release the shared catalog gate, restore every grant changed by
this acquisition attempt to its prior mode (preserving locks retained from earlier
explicit-transaction statements), and retry discovery/acquisition in global order.
Prepared execution returns the reprepare error for changed identity-bound
objects. Execution-time name-resolved targets use the final catalog-mutation
coverage loop below.

Catalog-mutating autocommit execution performs one final coverage check after it
takes the exclusive catalog publication gate. This closes the window in which a
previously absent, name-resolved `IF EXISTS`/`OR REPLACE` target can be published
after shared-gate convergence. If the current request set is not covered, the
server releases the catalog gate, restores the attempt's baseline grants,
acquires the complete current request set in global order, and retries the
exclusive-gate check. It never waits for an object lock while holding the catalog
gate.

Relation-generation snapshots are statement-scoped, including under Repeatable
Read and Serializable; only the MVCC snapshot is retained by those isolation
levels. After binding/revalidation, a statement acquires all relation locks and
only then captures the current relation generations used by execution. An
explicit transaction retains the locks for every relation it has actually
referenced, so recapturing generations cannot change those relations behind the
transaction. This separation also means a transaction that previously referenced
only an unrelated table does not pin stale generations for every table.

Transactional TRUNCATE installs replacement generations only after acquiring
`AccessExclusive`. Its later statements recapture the replacements. Other
sessions cannot capture those generations while the lock is held: they acquire
their conflicting relation lock first, then capture either the committed
replacement or the restored original after the truncating transaction ends.

## 5. Unified waiting and deadlocks

Relation locks extend the existing server `LockManager`; they do not introduce a
second wait-for graph. The graph stores the complete set of dependencies for each
waiting owner. A relation request depends on every incompatible holder and every
earlier incompatible waiter that it is not allowed to bypass. Row waits remain a
singleton dependency set. Cycle detection walks all outgoing edges rather than
assuming one edge per owner.

Each resource has a FIFO request queue. A request may be granted only when it is
compatible with all current holders and does not bypass an earlier incompatible
waiter. Compatible requests may be granted together. Reentrant requests retain
their original queue position; an upgrade queues at the point the stronger mode
is requested while the existing weaker grant remains held. These rules prevent a
continuous stream of readers from starving a relation-changing operation.

The unified graph must detect relation-only and mixed cycles, for example:

```text
T1 holds AccessExclusive(A), waits for T2's row lock
T2 holds a row lock, waits for AccessShare(A) held by T1
```

Autocommit statement owners can hold relation locks and wait, but never own row
locks or multiple statement lifetimes that can form a cycle. They are blockers
in the relation table but do not require durable transaction registration.

After `deadlock_timeout`, a detecting transaction in a cycle removes its edge and
returns `SqlState::DeadlockDetected` (`40P01`). Cancellation while waiting returns
`SqlState::QueryCanceled` (`57014`). Release, commit, rollback, disconnect, and
partial subxid abort notify all waiters; every waiter always rechecks compatibility.

## 6. Lock acquisition boundaries

- Simple and extended statements acquire all table locks before physical
  execution.
- A streaming SELECT/COPY/portal retains its statement locks until the producer
  and consumer are finished or disconnected.
- An explicit transaction retains acquired locks in its `Transaction` state.
- SQL cursor table locks remain owned by the explicit transaction.
- Prepared execution revalidates schema versions after lock acquisition.
- Actual checkpoint still uses the database-wide exclusive checkpoint guard.

Lock ordering is:

1. A shared writer guard, when the operation writes data, catalog, or WAL.
2. Table locks, then sequence locks, in their stable resource order.
3. The catalog publication gate, when catalog mutation/undo requires it.
4. Existing per-file structural latch.
5. Buffer frame latch.
6. WAL mutex.

No storage/page latch may be held while waiting for a relation lock.

## 7. DDL and maintenance migration

Migration is operation-by-operation, but no committed rollout state may combine a
blocking relation-lock request with the exclusive checkpoint guard. Relation locks
are first installed on every table access path while relation-touching DDL and
maintenance still use only the legacy guard. They switch atomically to table locks
only after coverage is complete; at that point they use the shared writer guard so
checkpoint still drains their WAL/page work, plus the exclusive catalog
publication gate across provisional catalog mutation. Actual checkpoint remains the
only data-path operation that waits for the exclusive checkpoint guard.

- Standalone multi-table TRUNCATE takes `AccessExclusive` on every target.
- ALTER/DROP take `AccessExclusive` on the affected table.
- CREATE INDEX takes `Share`, permitting readers but blocking target writers.
- VACUUM initially takes `Share` and retains writer-draining safety. A future
  storage-concurrency project may weaken this.
- CREATE and public relation-name conflicts remain serialized by the catalog/DDL
  boundary until name-keyed namespace locks or operation-scoped catalog undo land.
- DROP SEQUENCE and owned-sequence cascades take `SequenceExclusive`; sequence
  calls/defaults take `SequenceAccess` for their statement/transaction lifetime.

The server catalog publication gate is an RW lock separate from the catalog's
internal data latch. Binding/name lookup/system-catalog capture takes its shared
side and releases it before waiting for any object lock. After object locks are
held, execution reacquires the shared side for revalidation. DDL takes the
exclusive side after object locks and holds it across public catalog/storage
mutation, WAL Commit flush, and either commit publication or rollback restore.
Thus no catalog reader observes provisional CREATE/DROP/ALTER state and
whole-catalog restore cannot overlap another catalog change. The universal
mutation order is shared writer guard, all table/sequence locks, exclusive catalog
gate, then storage latches. No path waits for an object lock while holding either
side of the gate.

Standalone TRUNCATE uses the exclusive catalog gate around catalog mutation and
durable publication. Transactional TRUNCATE does not mutate the public catalog:
it stores replacement schemas in a transaction-local catalog overlay used by the
owner's later binds. At top-level commit it takes the exclusive gate, publishes the
overlay atomically, then discards it; rollback discards it without public catalog
restore. Storage replacement/restoration still occurs while all target locks are
held. This prevents bind-before-lock from observing uncommitted storage ids.

## 8. Transactional TRUNCATE

`TRUNCATE [TABLE] <name> [, ...]` is allowed in a healthy explicit transaction
when no savepoint is currently open. It takes `AccessExclusive` on all targets and
retains those locks through transaction end. The restriction applies only to
TRUNCATE; VACUUM and other maintenance commands retain their documented
transaction-block restrictions.

Before swapping storage, reject `TRUNCATE` with `SqlState::ObjectInUse` (`55006`)
if the same session has a SQL cursor, suspended extended-protocol portal, or other
parked `OpenQuery` whose referenced table set intersects the targets. The server
does not invalidate a worker that still owns an old relation snapshot. Unrelated
parked queries are allowed.

Transactional TRUNCATE is a physical generation swap:

1. Resolve, sort, lock, and revalidate every target before allocating storage ids.
2. Prepare all replacement heap/identity-index/secondary-index/TOAST files and
   append the existing per-table logical `TruncateTable` WAL records under the
   transaction's current writing xid.
3. Record storage before-images and add replacement schemas to the transaction's
   catalog overlay without changing public catalog maps.
4. Install the storage replacement generations without appending/flushing Commit.
5. Return `TRUNCATE TABLE`; later statements owned by the same transaction use the
   replacements.

For a Serializable transaction, TRUNCATE records a relation-level SSI write for
every target before installation. It conflicts with all prior relation and tuple
SIREAD holders for that table, is visible to later relation or tuple SIREAD
registration, and participates in the ordinary commit-time dangerous-structure
check (`docs/specs/ssi.md`).

Repeated TRUNCATE of the same table in one top-level transaction is supported.
Catalog/storage undo records the original generation only on the first truncate.
Every replacement generation is tracked: rollback restores the original and
removes all replacements; commit retains only the final replacement and retires
the original plus any intermediate replacements. Inserts between truncates follow
ordinary MVCC cleanup, and no replacement file can become reachable after it has
been superseded.

Because every target remains `AccessExclusive` locked, another session cannot
acquire `AccessShare`/`RowExclusive` and observe the uncommitted generation. The
owning transaction may read the empty table and may populate it through INSERT or
COPY. Unrelated tables remain available.

COMMIT flushes one transaction Commit covering truncate and later writes, takes
the exclusive catalog gate to publish the overlay, performs storage commit cleanup
(retiring old generations), and only then releases locks. ROLLBACK, disconnect,
SSI failure, or commit-flush failure discards the overlay, restores old storage
generations, retires/removes replacements, and releases locks only afterward.

Replacement storage ids remain burned after rollback, matching existing recovery
rules. Recovery applies committed `TruncateTable` records and later physical COPY/
DML redo in WAL order. In-flight/aborted truncate records are skipped for catalog
publication while their replacement storage ids remain reserved.

`ROLLBACK TO SAVEPOINT` support for a TRUNCATE performed inside a savepoint is out
of scope for this milestone because catalog/generation undo is initially top-level.
Attempting it with an open savepoint returns `FeatureNotSupported` and poisons the
block like other statement errors.

## 9. Acceptance criteria

- Lock compatibility, reentrancy, upgrades, cancellation, release, and stable
  multi-table ordering have unit coverage.
- Relation-only and mixed row/relation deadlocks return exactly one `40P01` victim.
- A queued `AccessExclusive` request makes bounded progress under a stream of new
  readers; queue dependencies participate in deadlock detection.
- Every user-table read/write path acquires the documented modes, including COPY,
  views, prepared statements, cursors, and suspended portals.
- Concurrent sequence calls cannot outlive DROP SEQUENCE or an owned-sequence
  cascade; table/sequence mixed cycles are deadlock-detected.
- After migration, target-table conflicts block while unrelated-table work
  proceeds; relation-touching operations no longer take the coarse exclusive guard.
- Standalone TRUNCATE, ALTER, DROP, CREATE INDEX, and VACUUM retain correctness and
  recovery behavior under their relation modes.
- Transactional multi-table TRUNCATE commits and rolls back heap, secondary-index,
  and TOAST generations atomically; the owner sees its replacement generation and
  other sessions cannot see it before commit.
- Repeatable Read read → TRUNCATE → read sees the owner's replacement, while a
  second session starting a read during the truncate captures no uncommitted
  generation and sees the committed replacement or restored original afterward.
- Repeated same-table TRUNCATE with intervening inserts commits only the final
  generation or restores the original on rollback, without leaking a reachable
  intermediate generation or reusing a TOAST id in a surviving generation.
- Serializable whole-relation TRUNCATE forms rw-antidependencies with both prior
  and later tuple/relation reads and participates in dangerous-structure aborts.
- Crash-before-commit preserves original generations; crash-after-commit recovers
  the replacements and rows copied after TRUNCATE.
- PostgreSQL pgbench 18 initialization `dtgvp` and simple/extended/prepared smoke
  transactions complete against SaguaroDB.
