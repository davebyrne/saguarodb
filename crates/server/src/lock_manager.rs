//! Row-conflict and catalog-object lock coordination.
//!
//! Row waits and table/sequence lock waits intentionally share one wait-for graph
//! so mixed cycles cannot escape deadlock detection. See `docs/specs/deadlock.md`
//! and `docs/specs/table-locks.md`.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use common::{
    ConflictWaiter, DbError, Key, QueryCancel, Result, SchemaId, SequenceId, SqlState, TableId,
    TupleLockAcquire, TupleLockGrantChange, TupleLockManager, TupleLockMode, TupleLockTag,
    TupleLockWaitPolicy, TxnId,
};

use crate::registry::ActiveTxnRegistry;

const POLL_INTERVAL: Duration = Duration::from_millis(100);
static NEXT_MANAGER_ID: AtomicU64 = AtomicU64::new(1);

/// A logical lock owner. Subtransactions are canonicalized to their top-level xid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LockOwner {
    Transaction(TxnId),
    Statement(u64),
}

/// A lockable catalog object. Variant order is the global acquisition order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NormalizedCatalogName(String);

impl NormalizedCatalogName {
    pub fn new(name: impl AsRef<str>) -> Self {
        Self(name.as_ref().to_ascii_lowercase())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LockResource {
    Schema(SchemaId),
    CatalogName {
        schema: SchemaId,
        name: NormalizedCatalogName,
    },
    Table(TableId),
    Tuple {
        table: TableId,
        key: Key,
    },
    Sequence(SequenceId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RelationLockMode {
    AccessShare,
    RowShare,
    RowExclusive,
    Share,
    AccessExclusive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SequenceLockMode {
    Access,
    Exclusive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CatalogLockMode {
    Access,
    Exclusive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectLockMode {
    Catalog(CatalogLockMode),
    Relation(RelationLockMode),
    Sequence(SequenceLockMode),
    Tuple(TupleLockMode),
}

impl ObjectLockMode {
    fn compatible(self, other: Self) -> bool {
        match (self, other) {
            (Self::Catalog(CatalogLockMode::Access), Self::Catalog(CatalogLockMode::Access)) => {
                true
            }
            (Self::Catalog(_), Self::Catalog(_)) => false,
            (Self::Relation(left), Self::Relation(right)) => relation_compatible(left, right),
            (Self::Tuple(left), Self::Tuple(right)) => tuple_compatible(left, right),
            (
                Self::Sequence(SequenceLockMode::Access),
                Self::Sequence(SequenceLockMode::Access),
            ) => true,
            (Self::Sequence(_), Self::Sequence(_)) => false,
            // A resource is permanently typed, so this indicates an internal caller bug.
            _ => false,
        }
    }

    fn covers(self, requested: Self) -> bool {
        match (self, requested) {
            (Self::Catalog(held), Self::Catalog(requested)) => held >= requested,
            (Self::Relation(held), Self::Relation(requested)) => held >= requested,
            (Self::Sequence(held), Self::Sequence(requested)) => held >= requested,
            (Self::Tuple(held), Self::Tuple(requested)) => held >= requested,
            _ => false,
        }
    }

    fn strongest(self, other: Self) -> Result<Self> {
        match (self, other) {
            (Self::Catalog(left), Self::Catalog(right)) => Ok(Self::Catalog(left.max(right))),
            (Self::Relation(left), Self::Relation(right)) => Ok(Self::Relation(left.max(right))),
            (Self::Sequence(left), Self::Sequence(right)) => Ok(Self::Sequence(left.max(right))),
            (Self::Tuple(left), Self::Tuple(right)) => Ok(Self::Tuple(left.max(right))),
            _ => Err(DbError::internal(
                "object lock modes have different resource types",
            )),
        }
    }
}

fn relation_compatible(left: RelationLockMode, right: RelationLockMode) -> bool {
    use RelationLockMode::{AccessExclusive, AccessShare, RowExclusive, RowShare, Share};
    match left {
        AccessShare => right != AccessExclusive,
        RowShare => right != AccessExclusive,
        RowExclusive => matches!(right, AccessShare | RowShare | RowExclusive),
        Share => matches!(right, AccessShare | RowShare),
        AccessExclusive => false,
    }
}

fn tuple_compatible(left: TupleLockMode, right: TupleLockMode) -> bool {
    use TupleLockMode::{KeyShare, NoKeyUpdate, Share, Update};
    match left {
        KeyShare => right != Update,
        Share => matches!(right, KeyShare | Share),
        NoKeyUpdate => right == KeyShare,
        Update => false,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectLockRequest {
    pub resource: LockResource,
    pub mode: ObjectLockMode,
}

impl ObjectLockRequest {
    pub fn schema(schema: SchemaId, mode: CatalogLockMode) -> Self {
        Self {
            resource: LockResource::Schema(schema),
            mode: ObjectLockMode::Catalog(mode),
        }
    }

    pub fn catalog_name(schema: SchemaId, name: impl Into<String>) -> Self {
        Self {
            resource: LockResource::CatalogName {
                schema,
                name: NormalizedCatalogName::new(name.into()),
            },
            mode: ObjectLockMode::Catalog(CatalogLockMode::Exclusive),
        }
    }

    pub fn table(table_id: TableId, mode: RelationLockMode) -> Self {
        Self {
            resource: LockResource::Table(table_id),
            mode: ObjectLockMode::Relation(mode),
        }
    }

    pub fn sequence(sequence_id: SequenceId, mode: SequenceLockMode) -> Self {
        Self {
            resource: LockResource::Sequence(sequence_id),
            mode: ObjectLockMode::Sequence(mode),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OwnerGrantSnapshot {
    manager_id: u64,
    guard_id: u64,
    owner: LockOwner,
    grants: BTreeMap<LockResource, ObjectLockMode>,
}

#[derive(Debug, Clone, Copy)]
struct QueuedRequest {
    id: u64,
    owner: LockOwner,
    mode: ObjectLockMode,
}

enum AcquireOne {
    Acquired {
        previous: Option<ObjectLockMode>,
        previous_generation: Option<u64>,
        granted: ObjectLockMode,
        granted_generation: Option<u64>,
    },
    Skipped,
}

#[derive(Debug)]
struct TupleGrantReceipt {
    manager_id: u64,
    owner: TxnId,
    tag: TupleLockTag,
    previous: Option<TupleLockMode>,
    previous_generation: Option<u64>,
    granted: TupleLockMode,
    granted_generation: u64,
}

#[derive(Debug, Default)]
struct LockState {
    active_owners: BTreeSet<LockOwner>,
    waits_for: HashMap<LockOwner, BTreeSet<LockOwner>>,
    grants: BTreeMap<LockResource, BTreeMap<LockOwner, ObjectLockMode>>,
    queues: BTreeMap<LockResource, VecDeque<QueuedRequest>>,
    tuple_grant_generations: BTreeMap<(LockResource, LockOwner), u64>,
    next_tuple_grant_generation: u64,
}

#[derive(Debug)]
pub struct LockManager {
    id: u64,
    state: Mutex<LockState>,
    cond: Condvar,
    registry: ActiveTxnRegistry,
    deadlock_timeout: Duration,
    next_guard_id: AtomicU64,
    next_statement_owner: AtomicU64,
    next_request_id: AtomicU64,
}

impl LockManager {
    pub fn new(registry: ActiveTxnRegistry, deadlock_timeout: Duration) -> Result<Self> {
        let id = next_id(&NEXT_MANAGER_ID, "lock manager")?;
        Ok(Self {
            id,
            state: Mutex::new(LockState::default()),
            cond: Condvar::new(),
            registry,
            deadlock_timeout,
            next_guard_id: AtomicU64::new(1),
            next_statement_owner: AtomicU64::new(1),
            next_request_id: AtomicU64::new(1),
        })
    }

    pub fn statement_owner(self: &Arc<Self>) -> Result<ObjectLockGuard> {
        let id = next_id(&self.next_statement_owner, "statement lock owner")?;
        let owner = LockOwner::Statement(id);
        self.create_guard(owner)
    }

    pub fn transaction_owner(self: &Arc<Self>, xid: TxnId) -> Result<ObjectLockGuard> {
        let owner = LockOwner::Transaction(self.registry.top_of(xid));
        self.create_guard(owner)
    }

    fn create_guard(self: &Arc<Self>, owner: LockOwner) -> Result<ObjectLockGuard> {
        self.register_owner(owner)?;
        let guard_id = next_id(&self.next_guard_id, "object lock guard")?;
        Ok(ObjectLockGuard::new(Arc::clone(self), owner, guard_id))
    }

    pub fn on_txn_finished(&self) {
        let _guard = self.lock();
        self.cond.notify_all();
    }

    fn lock(&self) -> MutexGuard<'_, LockState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn register_owner(&self, owner: LockOwner) -> Result<()> {
        if self.lock().active_owners.insert(owner) {
            Ok(())
        } else {
            Err(DbError::internal(format!(
                "lock owner {owner:?} already has a lifetime guard"
            )))
        }
    }

    fn acquire_many(
        &self,
        owner: LockOwner,
        requests: &[ObjectLockRequest],
        cancel: &QueryCancel,
    ) -> Result<()> {
        for request in normalize_requests(requests)? {
            let _ = self.acquire_one(owner, request, TupleLockWaitPolicy::Block, cancel)?;
        }
        Ok(())
    }

    fn acquire_one(
        &self,
        owner: LockOwner,
        request: ObjectLockRequest,
        wait_policy: TupleLockWaitPolicy,
        cancel: &QueryCancel,
    ) -> Result<AcquireOne> {
        validate_request(&request)?;
        let mut state = self.lock();
        if !state.active_owners.contains(&owner) {
            return Err(DbError::internal(format!(
                "lock owner {owner:?} has no lifetime guard"
            )));
        }
        let previous = state
            .grants
            .get(&request.resource)
            .and_then(|grants| grants.get(&owner))
            .copied();
        let previous_generation = state
            .tuple_grant_generations
            .get(&(request.resource.clone(), owner))
            .copied();
        if let Some(held) = previous
            && held.covers(request.mode)
        {
            return Ok(AcquireOne::Acquired {
                previous: Some(held),
                previous_generation,
                granted: held,
                granted_generation: previous_generation,
            });
        }

        let request_id = next_id(&self.next_request_id, "object lock request")?;
        state
            .queues
            .entry(request.resource.clone())
            .or_default()
            .push_back(QueuedRequest {
                id: request_id,
                owner,
                mode: request.mode,
            });
        self.cond.notify_all();
        let mut last_detection = Instant::now();

        loop {
            if let Err(err) = cancel.check() {
                remove_request(&mut state, request.resource.clone(), request_id);
                state.waits_for.remove(&owner);
                self.cond.notify_all();
                return Err(err);
            }

            let blockers = request_blockers(
                &state,
                request.resource.clone(),
                request_id,
                owner,
                request.mode,
            );
            if blockers.is_empty() {
                remove_request(&mut state, request.resource.clone(), request_id);
                let granted = match previous {
                    Some(held) => match held.strongest(request.mode) {
                        Ok(mode) => mode,
                        Err(err) => {
                            state.waits_for.remove(&owner);
                            self.cond.notify_all();
                            return Err(err);
                        }
                    },
                    None => request.mode,
                };
                let granted_generation = if matches!(request.mode, ObjectLockMode::Tuple(_)) {
                    let next_generation = state
                        .next_tuple_grant_generation
                        .checked_add(1)
                        .ok_or_else(|| DbError::internal("tuple grant generation exhausted"));
                    let generation = match next_generation {
                        Ok(generation) => generation,
                        Err(err) => {
                            state.waits_for.remove(&owner);
                            self.cond.notify_all();
                            return Err(err);
                        }
                    };
                    state.next_tuple_grant_generation = generation;
                    state
                        .tuple_grant_generations
                        .insert((request.resource.clone(), owner), generation);
                    Some(generation)
                } else {
                    None
                };
                state
                    .grants
                    .entry(request.resource.clone())
                    .or_default()
                    .insert(owner, granted);
                state.waits_for.remove(&owner);
                self.cond.notify_all();
                return Ok(AcquireOne::Acquired {
                    previous,
                    previous_generation,
                    granted,
                    granted_generation,
                });
            }
            match wait_policy {
                TupleLockWaitPolicy::NoWait => {
                    remove_request(&mut state, request.resource.clone(), request_id);
                    state.waits_for.remove(&owner);
                    self.cond.notify_all();
                    return Err(DbError::execute(
                        SqlState::LockNotAvailable,
                        "could not obtain tuple lock on row",
                    ));
                }
                TupleLockWaitPolicy::SkipLocked => {
                    remove_request(&mut state, request.resource.clone(), request_id);
                    state.waits_for.remove(&owner);
                    self.cond.notify_all();
                    return Ok(AcquireOne::Skipped);
                }
                TupleLockWaitPolicy::Block => {}
            }
            state.waits_for.insert(owner, blockers);

            let (next_state, _woken) = match self.cond.wait_timeout(state, POLL_INTERVAL) {
                Ok(waited) => waited,
                Err(poisoned) => poisoned.into_inner(),
            };
            state = next_state;
            if last_detection.elapsed() >= self.deadlock_timeout {
                last_detection = Instant::now();
                if on_cycle(&state.waits_for, owner) {
                    remove_request(&mut state, request.resource.clone(), request_id);
                    state.waits_for.remove(&owner);
                    self.cond.notify_all();
                    return Err(DbError::execute(
                        SqlState::DeadlockDetected,
                        "deadlock detected",
                    ));
                }
            }
        }
    }

    fn restore_tuple_changes(
        &self,
        owner: LockOwner,
        changes: Vec<TupleLockGrantChange>,
    ) -> Result<()> {
        let mut state = self.lock();
        if !state.active_owners.contains(&owner) {
            return Err(DbError::internal(format!(
                "lock owner {owner:?} has no lifetime guard"
            )));
        }
        let owner_xid = match owner {
            LockOwner::Transaction(xid) => xid,
            LockOwner::Statement(_) => {
                return Err(DbError::internal(
                    "statement owners cannot restore tuple lock grants",
                ));
            }
        };
        let mut simulated = BTreeMap::<LockResource, (Option<ObjectLockMode>, Option<u64>)>::new();
        for change in changes.iter().rev() {
            let receipt = change
                .manager_payload::<TupleGrantReceipt>()
                .ok_or_else(|| DbError::internal("invalid tuple lock receipt payload"))?;
            if receipt.manager_id != self.id || receipt.owner != owner_xid {
                return Err(DbError::internal(
                    "tuple lock receipt belongs to a different manager or owner",
                ));
            }
            let resource = LockResource::Tuple {
                table: receipt.tag.table,
                key: receipt.tag.key.clone(),
            };
            let current = simulated.entry(resource).or_insert_with_key(|resource| {
                (
                    state
                        .grants
                        .get(resource)
                        .and_then(|owners| owners.get(&owner))
                        .copied(),
                    state
                        .tuple_grant_generations
                        .get(&(resource.clone(), owner))
                        .copied(),
                )
            });
            if *current
                != (
                    Some(ObjectLockMode::Tuple(receipt.granted)),
                    Some(receipt.granted_generation),
                )
            {
                return Err(DbError::internal(
                    "tuple lock grant changed before acquisition rollback",
                ));
            }
            *current = (
                receipt.previous.map(ObjectLockMode::Tuple),
                receipt.previous_generation,
            );
        }
        for (resource, (restored, restored_generation)) in simulated {
            match restored {
                Some(mode) => {
                    state
                        .grants
                        .entry(resource.clone())
                        .or_default()
                        .insert(owner, mode);
                    state.tuple_grant_generations.insert(
                        (resource, owner),
                        restored_generation.ok_or_else(|| {
                            DbError::internal("restored tuple grant has no generation")
                        })?,
                    );
                }
                None => {
                    if let Some(owners) = state.grants.get_mut(&resource) {
                        owners.remove(&owner);
                        if owners.is_empty() {
                            state.grants.remove(&resource);
                        }
                    }
                    state.tuple_grant_generations.remove(&(resource, owner));
                }
            }
        }
        state.waits_for.remove(&owner);
        self.cond.notify_all();
        Ok(())
    }

    fn owner_snapshot(&self, owner: LockOwner, guard_id: u64) -> OwnerGrantSnapshot {
        let state = self.lock();
        let grants = state
            .grants
            .iter()
            .filter_map(|(resource, owners)| {
                owners.get(&owner).map(|mode| (resource.clone(), *mode))
            })
            .collect();
        OwnerGrantSnapshot {
            manager_id: self.id,
            guard_id,
            owner,
            grants,
        }
    }

    fn restore_owner(
        &self,
        owner: LockOwner,
        guard_id: u64,
        snapshot: &OwnerGrantSnapshot,
    ) -> Result<()> {
        if snapshot.manager_id != self.id
            || snapshot.guard_id != guard_id
            || snapshot.owner != owner
        {
            return Err(DbError::internal(
                "object lock snapshot belongs to a different lock owner",
            ));
        }
        let mut state = self.lock();
        let can_restore = snapshot.grants.iter().all(|(resource, mode)| {
            state
                .grants
                .get(resource)
                .and_then(|owners| owners.get(&owner))
                .is_some_and(|held| held.covers(*mode))
        });
        if !can_restore {
            return Err(DbError::internal(
                "object lock snapshot is stale and cannot restore released grants",
            ));
        }
        for owners in state.grants.values_mut() {
            owners.remove(&owner);
        }
        state.grants.retain(|_, owners| !owners.is_empty());
        state
            .tuple_grant_generations
            .retain(|(_, grant_owner), _| *grant_owner != owner);
        for (resource, mode) in &snapshot.grants {
            state
                .grants
                .entry(resource.clone())
                .or_default()
                .insert(owner, *mode);
            if matches!(mode, ObjectLockMode::Tuple(_)) {
                state.next_tuple_grant_generation = state
                    .next_tuple_grant_generation
                    .checked_add(1)
                    .ok_or_else(|| DbError::internal("tuple grant generation exhausted"))?;
                let generation = state.next_tuple_grant_generation;
                state
                    .tuple_grant_generations
                    .insert((resource.clone(), owner), generation);
            }
        }
        state.waits_for.remove(&owner);
        self.cond.notify_all();
        Ok(())
    }

    fn release_owner(&self, owner: LockOwner) {
        let mut state = self.lock();
        state.active_owners.remove(&owner);
        for owners in state.grants.values_mut() {
            owners.remove(&owner);
        }
        state.grants.retain(|_, owners| !owners.is_empty());
        state
            .tuple_grant_generations
            .retain(|(_, grant_owner), _| *grant_owner != owner);
        for queue in state.queues.values_mut() {
            queue.retain(|request| request.owner != owner);
        }
        state.queues.retain(|_, queue| !queue.is_empty());
        state.waits_for.remove(&owner);
        for blockers in state.waits_for.values_mut() {
            blockers.remove(&owner);
        }
        state.waits_for.retain(|_, blockers| !blockers.is_empty());
        self.cond.notify_all();
    }
}

/// RAII lifetime for all object locks owned by one statement or top-level xid.
#[derive(Debug)]
pub struct ObjectLockGuard {
    manager: Arc<LockManager>,
    owner: LockOwner,
    guard_id: u64,
    released: bool,
}

impl ObjectLockGuard {
    fn new(manager: Arc<LockManager>, owner: LockOwner, guard_id: u64) -> Self {
        Self {
            manager,
            owner,
            guard_id,
            released: false,
        }
    }

    pub fn owner(&self) -> LockOwner {
        self.owner
    }

    pub fn acquire_many(
        &mut self,
        requests: &[ObjectLockRequest],
        cancel: &QueryCancel,
    ) -> Result<()> {
        self.manager.acquire_many(self.owner, requests, cancel)
    }

    pub fn snapshot(&self) -> OwnerGrantSnapshot {
        self.manager.owner_snapshot(self.owner, self.guard_id)
    }

    pub fn covers(&self, requests: &[ObjectLockRequest]) -> Result<bool> {
        let grants = self
            .manager
            .owner_snapshot(self.owner, self.guard_id)
            .grants;
        Ok(normalize_requests(requests)?.iter().all(|request| {
            grants
                .get(&request.resource)
                .is_some_and(|held| held.covers(request.mode))
        }))
    }

    pub fn restore(&mut self, snapshot: &OwnerGrantSnapshot) -> Result<()> {
        self.manager
            .restore_owner(self.owner, self.guard_id, snapshot)
    }

    pub fn release(mut self) {
        self.manager.release_owner(self.owner);
        self.released = true;
    }
}

impl Drop for ObjectLockGuard {
    fn drop(&mut self) {
        if !self.released {
            self.manager.release_owner(self.owner);
        }
    }
}

impl ConflictWaiter for LockManager {
    fn wait_for(
        &self,
        waiter_subxid: u64,
        blocker_subxid: u64,
        cancel: &QueryCancel,
    ) -> Result<()> {
        let waiter = LockOwner::Transaction(self.registry.top_of(waiter_subxid));
        let blocker = LockOwner::Transaction(self.registry.top_of(blocker_subxid));
        let mut state = self.lock();
        state.waits_for.insert(waiter, BTreeSet::from([blocker]));
        let mut last_detection = Instant::now();

        let result = loop {
            if !self.registry.is_active(blocker_subxid) {
                break Ok(());
            }
            if let Err(err) = cancel.check() {
                break Err(err);
            }
            let (next_state, _woken) = match self.cond.wait_timeout(state, POLL_INTERVAL) {
                Ok(waited) => waited,
                Err(poisoned) => poisoned.into_inner(),
            };
            state = next_state;
            if last_detection.elapsed() >= self.deadlock_timeout {
                last_detection = Instant::now();
                if on_cycle(&state.waits_for, waiter) {
                    state.waits_for.remove(&waiter);
                    return Err(DbError::execute(
                        SqlState::DeadlockDetected,
                        "deadlock detected",
                    ));
                }
            }
        };
        state.waits_for.remove(&waiter);
        result
    }
}

impl TupleLockManager for LockManager {
    fn acquire_tuple(
        &self,
        xid: TxnId,
        tag: &TupleLockTag,
        mode: TupleLockMode,
        wait_policy: TupleLockWaitPolicy,
        cancel: &QueryCancel,
    ) -> Result<TupleLockAcquire> {
        if !self.registry.is_active(xid) {
            return Err(DbError::internal(format!(
                "tuple lock xid {xid} is not active"
            )));
        }
        let owner = LockOwner::Transaction(self.registry.top_of(xid));
        let resource = LockResource::Tuple {
            table: tag.table,
            key: tag.key.clone(),
        };
        match self.acquire_one(
            owner,
            ObjectLockRequest {
                resource,
                mode: ObjectLockMode::Tuple(mode),
            },
            wait_policy,
            cancel,
        )? {
            AcquireOne::Acquired {
                previous,
                previous_generation,
                granted,
                granted_generation,
            } => {
                let previous = match previous {
                    Some(ObjectLockMode::Tuple(mode)) => Some(mode),
                    Some(_) => {
                        return Err(DbError::internal(
                            "tuple resource carried a non-tuple previous mode",
                        ));
                    }
                    None => None,
                };
                let granted = match granted {
                    ObjectLockMode::Tuple(mode) => mode,
                    _ => {
                        return Err(DbError::internal(
                            "tuple resource carried a non-tuple granted mode",
                        ));
                    }
                };
                let granted_generation = granted_generation
                    .ok_or_else(|| DbError::internal("tuple grant has no generation"))?;
                Ok(TupleLockAcquire::Acquired(
                    TupleLockGrantChange::manager_receipt(TupleGrantReceipt {
                        manager_id: self.id,
                        owner: self.registry.top_of(xid),
                        tag: tag.clone(),
                        previous,
                        previous_generation,
                        granted,
                        granted_generation,
                    }),
                ))
            }
            AcquireOne::Skipped => Ok(TupleLockAcquire::Skipped),
        }
    }

    fn restore_tuple_grants(&self, xid: TxnId, changes: Vec<TupleLockGrantChange>) -> Result<()> {
        let owner = LockOwner::Transaction(self.registry.top_of(xid));
        self.restore_tuple_changes(owner, changes)
    }
}

fn next_id(counter: &AtomicU64, kind: &str) -> Result<u64> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .map_err(|_| DbError::internal(format!("{kind} id space exhausted")))
}

fn validate_request(request: &ObjectLockRequest) -> Result<()> {
    let valid = matches!(
        (&request.resource, request.mode),
        (LockResource::Schema(_), ObjectLockMode::Catalog(_))
            | (LockResource::CatalogName { .. }, ObjectLockMode::Catalog(_))
            | (LockResource::Table(_), ObjectLockMode::Relation(_))
            | (LockResource::Tuple { .. }, ObjectLockMode::Tuple(_))
            | (LockResource::Sequence(_), ObjectLockMode::Sequence(_))
    );
    if valid {
        Ok(())
    } else {
        Err(DbError::internal(
            "object lock mode does not match resource",
        ))
    }
}

fn normalize_requests(requests: &[ObjectLockRequest]) -> Result<Vec<ObjectLockRequest>> {
    let mut normalized = BTreeMap::<LockResource, ObjectLockMode>::new();
    for request in requests {
        validate_request(request)?;
        match normalized.entry(request.resource.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(request.mode);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let mode = entry.get().strongest(request.mode)?;
                entry.insert(mode);
            }
        }
    }
    Ok(normalized
        .into_iter()
        .map(|(resource, mode)| ObjectLockRequest { resource, mode })
        .collect())
}

fn remove_request(state: &mut LockState, resource: LockResource, request_id: u64) {
    if let Some(queue) = state.queues.get_mut(&resource) {
        queue.retain(|request| request.id != request_id);
        if queue.is_empty() {
            state.queues.remove(&resource);
        }
    }
}

fn request_blockers(
    state: &LockState,
    resource: LockResource,
    request_id: u64,
    owner: LockOwner,
    mode: ObjectLockMode,
) -> BTreeSet<LockOwner> {
    let mut blockers = BTreeSet::new();
    if let Some(grants) = state.grants.get(&resource) {
        for (holder, held) in grants {
            if *holder != owner && !mode.compatible(*held) {
                blockers.insert(*holder);
            }
        }
    }
    if let Some(queue) = state.queues.get(&resource) {
        for earlier in queue {
            if earlier.id == request_id {
                break;
            }
            if earlier.owner != owner && !mode.compatible(earlier.mode) {
                blockers.insert(earlier.owner);
            }
        }
    }
    blockers
}

fn on_cycle(graph: &HashMap<LockOwner, BTreeSet<LockOwner>>, start: LockOwner) -> bool {
    fn visit(
        graph: &HashMap<LockOwner, BTreeSet<LockOwner>>,
        current: LockOwner,
        start: LockOwner,
        visited: &mut BTreeSet<LockOwner>,
    ) -> bool {
        let Some(next) = graph.get(&current) else {
            return false;
        };
        for owner in next {
            if *owner == start {
                return true;
            }
            if visited.insert(*owner) && visit(graph, *owner, start, visited) {
                return true;
            }
        }
        false
    }
    visit(graph, start, start, &mut BTreeSet::from([start]))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;
    use std::thread;

    use super::*;

    fn manager(timeout_ms: u64) -> (Arc<LockManager>, ActiveTxnRegistry) {
        let registry = ActiveTxnRegistry::new();
        (
            Arc::new(
                LockManager::new(registry.clone(), Duration::from_millis(timeout_ms)).unwrap(),
            ),
            registry,
        )
    }

    fn wait_until_queued(manager: &LockManager, owner: LockOwner, resource: LockResource) {
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut state = manager.lock();
        loop {
            if state
                .queues
                .get(&resource)
                .is_some_and(|queue| queue.iter().any(|request| request.owner == owner))
            {
                return;
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .expect("lock request was not queued within one second");
            let (next_state, timeout) = manager
                .cond
                .wait_timeout(state, remaining)
                .expect("lock manager mutex poisoned");
            state = next_state;
            assert!(!timeout.timed_out(), "lock request was not queued");
        }
    }

    #[test]
    fn relation_compatibility_matrix_matches_contract() {
        use RelationLockMode::{AccessExclusive, AccessShare, RowExclusive, RowShare, Share};
        let modes = [AccessShare, RowShare, RowExclusive, Share, AccessExclusive];
        let expected = [
            [true, true, true, true, false],
            [true, true, true, true, false],
            [true, true, true, false, false],
            [true, true, false, false, false],
            [false, false, false, false, false],
        ];
        for (left_index, left) in modes.into_iter().enumerate() {
            for (right_index, right) in modes.into_iter().enumerate() {
                assert_eq!(
                    relation_compatible(left, right),
                    expected[left_index][right_index]
                );
            }
        }
    }

    #[test]
    fn tuple_compatibility_matrix_matches_contract() {
        use TupleLockMode::{KeyShare, NoKeyUpdate, Share, Update};
        let modes = [KeyShare, Share, NoKeyUpdate, Update];
        let expected = [
            [true, true, true, false],
            [true, true, false, false],
            [true, false, false, false],
            [false, false, false, false],
        ];
        for (left_index, left) in modes.into_iter().enumerate() {
            for (right_index, right) in modes.into_iter().enumerate() {
                assert_eq!(
                    tuple_compatible(left, right),
                    expected[left_index][right_index]
                );
            }
        }
    }

    fn tuple_tag(value: i64) -> TupleLockTag {
        TupleLockTag {
            table: 1,
            key: Key(vec![common::Value::Integer(value)]),
        }
    }

    #[test]
    fn tuple_nowait_and_skip_locked_do_not_queue() {
        let (manager, registry) = manager(20);
        registry.register(1);
        registry.register(2);
        let _first = manager.transaction_owner(1).unwrap();
        let _second = manager.transaction_owner(2).unwrap();
        let cancel = QueryCancel::new();
        manager
            .acquire_tuple(
                1,
                &tuple_tag(7),
                TupleLockMode::Update,
                TupleLockWaitPolicy::Block,
                &cancel,
            )
            .unwrap();

        let err = manager
            .acquire_tuple(
                2,
                &tuple_tag(7),
                TupleLockMode::KeyShare,
                TupleLockWaitPolicy::NoWait,
                &cancel,
            )
            .unwrap_err();
        assert_eq!(err.code, SqlState::LockNotAvailable);
        assert!(matches!(
            manager
                .acquire_tuple(
                    2,
                    &tuple_tag(7),
                    TupleLockMode::KeyShare,
                    TupleLockWaitPolicy::SkipLocked,
                    &cancel,
                )
                .unwrap(),
            TupleLockAcquire::Skipped
        ));
        assert!(manager.lock().queues.is_empty());
    }

    #[test]
    fn tuple_grant_receipts_restore_upgrades_and_new_grants() {
        let (manager, registry) = manager(20);
        registry.register(1);
        let guard = manager.transaction_owner(1).unwrap();
        let cancel = QueryCancel::new();
        let first = manager
            .acquire_tuple(
                1,
                &tuple_tag(7),
                TupleLockMode::KeyShare,
                TupleLockWaitPolicy::Block,
                &cancel,
            )
            .unwrap();
        let upgrade = manager
            .acquire_tuple(
                1,
                &tuple_tag(7),
                TupleLockMode::Update,
                TupleLockWaitPolicy::Block,
                &cancel,
            )
            .unwrap();
        let first = match first {
            TupleLockAcquire::Acquired(change) => change,
            TupleLockAcquire::Skipped => panic!("blocking acquisition skipped"),
        };
        let upgrade = match upgrade {
            TupleLockAcquire::Acquired(change) => change,
            TupleLockAcquire::Skipped => panic!("blocking acquisition skipped"),
        };

        manager.restore_tuple_grants(1, vec![upgrade]).unwrap();
        assert_eq!(
            guard.snapshot().grants.get(&LockResource::Tuple {
                table: 1,
                key: tuple_tag(7).key,
            }),
            Some(&ObjectLockMode::Tuple(TupleLockMode::KeyShare))
        );
        manager.restore_tuple_grants(1, vec![first]).unwrap();
        assert!(guard.snapshot().grants.is_empty());
    }

    #[test]
    fn tuple_receipts_cannot_be_restored_by_another_owner() {
        let (manager, registry) = manager(20);
        registry.register(1);
        registry.register(2);
        let _first = manager.transaction_owner(1).unwrap();
        let second = manager.transaction_owner(2).unwrap();
        let cancel = QueryCancel::new();
        let receipt = match manager
            .acquire_tuple(
                1,
                &tuple_tag(7),
                TupleLockMode::KeyShare,
                TupleLockWaitPolicy::Block,
                &cancel,
            )
            .unwrap()
        {
            TupleLockAcquire::Acquired(change) => change,
            TupleLockAcquire::Skipped => panic!("blocking acquisition skipped"),
        };

        assert!(manager.restore_tuple_grants(2, vec![receipt]).is_err());
        assert!(second.snapshot().grants.is_empty());
    }

    #[test]
    fn stale_receipt_batch_does_not_partially_restore_grants() {
        let (manager, registry) = manager(20);
        registry.register(1);
        let guard = manager.transaction_owner(1).unwrap();
        let cancel = QueryCancel::new();
        let empty = guard.snapshot();
        let stale = match manager
            .acquire_tuple(
                1,
                &tuple_tag(7),
                TupleLockMode::Update,
                TupleLockWaitPolicy::Block,
                &cancel,
            )
            .unwrap()
        {
            TupleLockAcquire::Acquired(change) => change,
            TupleLockAcquire::Skipped => panic!("blocking acquisition skipped"),
        };
        let mut guard = guard;
        guard.restore(&empty).unwrap();
        manager
            .acquire_tuple(
                1,
                &tuple_tag(7),
                TupleLockMode::Update,
                TupleLockWaitPolicy::Block,
                &cancel,
            )
            .unwrap();
        let valid = match manager
            .acquire_tuple(
                1,
                &tuple_tag(8),
                TupleLockMode::Update,
                TupleLockWaitPolicy::Block,
                &cancel,
            )
            .unwrap()
        {
            TupleLockAcquire::Acquired(change) => change,
            TupleLockAcquire::Skipped => panic!("blocking acquisition skipped"),
        };
        let before = guard.snapshot().grants;

        assert!(manager.restore_tuple_grants(1, vec![stale, valid]).is_err());
        assert_eq!(guard.snapshot().grants, before);
    }

    #[test]
    fn requests_are_deduplicated_strongest_and_resource_sorted() {
        let requests = normalize_requests(&[
            ObjectLockRequest::catalog_name(2, "zeta"),
            ObjectLockRequest::schema(2, CatalogLockMode::Access),
            ObjectLockRequest::sequence(2, SequenceLockMode::Access),
            ObjectLockRequest::table(9, RelationLockMode::AccessShare),
            ObjectLockRequest::table(2, RelationLockMode::RowExclusive),
            ObjectLockRequest::table(9, RelationLockMode::AccessExclusive),
            ObjectLockRequest::sequence(2, SequenceLockMode::Exclusive),
            ObjectLockRequest::catalog_name(2, "alpha"),
            ObjectLockRequest::catalog_name(2, "ALPHA"),
            ObjectLockRequest::schema(2, CatalogLockMode::Exclusive),
        ])
        .unwrap();
        assert_eq!(
            requests,
            vec![
                ObjectLockRequest::schema(2, CatalogLockMode::Exclusive),
                ObjectLockRequest::catalog_name(2, "alpha"),
                ObjectLockRequest::catalog_name(2, "zeta"),
                ObjectLockRequest::table(2, RelationLockMode::RowExclusive),
                ObjectLockRequest::table(9, RelationLockMode::AccessExclusive),
                ObjectLockRequest::sequence(2, SequenceLockMode::Exclusive),
            ]
        );
    }

    #[test]
    fn mismatched_lock_mode_families_return_internal_errors() {
        assert!(matches!(
            ObjectLockMode::Catalog(CatalogLockMode::Access)
                .strongest(ObjectLockMode::Relation(RelationLockMode::AccessShare)),
            Err(err) if err.code == SqlState::InternalError
        ));

        let malformed = ObjectLockRequest {
            resource: LockResource::Table(1),
            mode: ObjectLockMode::Sequence(SequenceLockMode::Access),
        };
        assert!(matches!(
            normalize_requests(&[malformed]),
            Err(err) if err.code == SqlState::InternalError
        ));
    }

    #[test]
    fn snapshot_restore_preserves_preexisting_grants() {
        let (manager, _) = manager(20);
        let cancel = QueryCancel::new();
        let mut guard = manager.statement_owner().unwrap();
        guard
            .acquire_many(
                &[ObjectLockRequest::table(1, RelationLockMode::AccessShare)],
                &cancel,
            )
            .unwrap();
        let snapshot = guard.snapshot();
        guard
            .acquire_many(
                &[
                    ObjectLockRequest::table(1, RelationLockMode::AccessExclusive),
                    ObjectLockRequest::table(2, RelationLockMode::AccessShare),
                ],
                &cancel,
            )
            .unwrap();
        guard.restore(&snapshot).unwrap();
        assert_eq!(guard.snapshot().grants, snapshot.grants);
    }

    #[test]
    fn transaction_owner_has_one_lifetime_guard_across_subxids() {
        let (manager, registry) = manager(20);
        registry.register(1);
        let subxid = registry.register_subxid_allocated(1, || 2);
        let guard = manager.transaction_owner(1).unwrap();

        assert!(manager.transaction_owner(subxid).is_err());

        drop(guard);
        assert!(manager.transaction_owner(subxid).is_ok());
    }

    #[test]
    fn snapshot_cannot_be_restored_by_another_owner() {
        let (manager, _) = manager(20);
        let cancel = QueryCancel::new();
        let mut first = manager.statement_owner().unwrap();
        first
            .acquire_many(
                &[ObjectLockRequest::table(
                    1,
                    RelationLockMode::AccessExclusive,
                )],
                &cancel,
            )
            .unwrap();
        let snapshot = first.snapshot();
        let mut second = manager.statement_owner().unwrap();

        assert!(second.restore(&snapshot).is_err());
        assert!(second.snapshot().grants.is_empty());
    }

    #[test]
    fn stale_snapshot_cannot_recreate_a_released_grant() {
        let (manager, _) = manager(20);
        let cancel = QueryCancel::new();
        let mut guard = manager.statement_owner().unwrap();
        let empty = guard.snapshot();
        guard
            .acquire_many(
                &[ObjectLockRequest::table(
                    1,
                    RelationLockMode::AccessExclusive,
                )],
                &cancel,
            )
            .unwrap();
        let locked = guard.snapshot();
        guard.restore(&empty).unwrap();

        let mut reader = manager.statement_owner().unwrap();
        reader
            .acquire_many(
                &[ObjectLockRequest::table(1, RelationLockMode::AccessShare)],
                &cancel,
            )
            .unwrap();

        assert!(guard.restore(&locked).is_err());
        assert!(guard.snapshot().grants.is_empty());
    }

    #[test]
    fn queued_exclusive_prevents_reader_bypass() {
        let (manager, _) = manager(20);
        let cancel = Arc::new(QueryCancel::new());
        let mut reader = manager.statement_owner().unwrap();
        reader
            .acquire_many(
                &[ObjectLockRequest::table(1, RelationLockMode::AccessShare)],
                &cancel,
            )
            .unwrap();

        let (events_tx, events_rx) = mpsc::channel();
        let mut exclusive = manager.statement_owner().unwrap();
        let exclusive_owner = exclusive.owner();
        let exclusive_cancel = Arc::clone(&cancel);
        let exclusive_tx = events_tx.clone();
        let exclusive_thread = thread::spawn(move || {
            exclusive
                .acquire_many(
                    &[ObjectLockRequest::table(
                        1,
                        RelationLockMode::AccessExclusive,
                    )],
                    &exclusive_cancel,
                )
                .unwrap();
            exclusive_tx.send("exclusive").unwrap();
        });
        wait_until_queued(&manager, exclusive_owner, LockResource::Table(1));

        let mut late_reader = manager.statement_owner().unwrap();
        let late_reader_owner = late_reader.owner();
        let late_cancel = Arc::clone(&cancel);
        let late_tx = events_tx;
        let late_thread = thread::spawn(move || {
            late_reader
                .acquire_many(
                    &[ObjectLockRequest::table(1, RelationLockMode::AccessShare)],
                    &late_cancel,
                )
                .unwrap();
            late_tx.send("reader").unwrap();
        });
        wait_until_queued(&manager, late_reader_owner, LockResource::Table(1));
        drop(reader);

        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "exclusive"
        );
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "reader"
        );
        exclusive_thread.join().unwrap();
        late_thread.join().unwrap();
    }

    #[test]
    fn sequence_access_is_shared_and_exclusive_waits_for_every_holder() {
        let (manager, _) = manager(20);
        let cancel = Arc::new(QueryCancel::new());
        let mut first = manager.statement_owner().unwrap();
        first
            .acquire_many(
                &[ObjectLockRequest::sequence(1, SequenceLockMode::Access)],
                &cancel,
            )
            .unwrap();
        let mut second = manager.statement_owner().unwrap();
        second
            .acquire_many(
                &[ObjectLockRequest::sequence(1, SequenceLockMode::Access)],
                &cancel,
            )
            .unwrap();

        let mut exclusive = manager.statement_owner().unwrap();
        let exclusive_owner = exclusive.owner();
        let exclusive_cancel = Arc::clone(&cancel);
        let (tx, rx) = mpsc::channel();
        let thread = thread::spawn(move || {
            exclusive
                .acquire_many(
                    &[ObjectLockRequest::sequence(1, SequenceLockMode::Exclusive)],
                    &exclusive_cancel,
                )
                .unwrap();
            tx.send(()).unwrap();
        });
        wait_until_queued(&manager, exclusive_owner, LockResource::Sequence(1));

        drop(first);
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
        drop(second);
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        thread.join().unwrap();
    }

    #[test]
    fn relation_deadlock_returns_one_victim_and_survivor_proceeds() {
        let (manager, registry) = manager(30);
        registry.register(1);
        registry.register(2);
        let cancel = Arc::new(QueryCancel::new());
        let mut first = manager.transaction_owner(1).unwrap();
        let mut second = manager.transaction_owner(2).unwrap();
        first
            .acquire_many(
                &[ObjectLockRequest::table(
                    1,
                    RelationLockMode::AccessExclusive,
                )],
                &cancel,
            )
            .unwrap();
        second
            .acquire_many(
                &[ObjectLockRequest::table(
                    2,
                    RelationLockMode::AccessExclusive,
                )],
                &cancel,
            )
            .unwrap();

        let (tx, rx) = mpsc::channel();
        let release_victim = Arc::new(AtomicBool::new(false));
        let cancel_first = Arc::clone(&cancel);
        let release_first = Arc::clone(&release_victim);
        let tx_first = tx.clone();
        let first_thread = thread::spawn(move || {
            let result = first.acquire_many(
                &[ObjectLockRequest::table(
                    2,
                    RelationLockMode::AccessExclusive,
                )],
                &cancel_first,
            );
            tx_first
                .send((1, result.as_ref().err().map(|error| error.code)))
                .unwrap();
            if result
                .as_ref()
                .is_err_and(|error| error.code == SqlState::DeadlockDetected)
            {
                while !release_first.load(Ordering::Relaxed) {
                    thread::yield_now();
                }
            }
        });
        let cancel_second = Arc::clone(&cancel);
        let release_second = Arc::clone(&release_victim);
        let second_thread = thread::spawn(move || {
            let result = second.acquire_many(
                &[ObjectLockRequest::table(
                    1,
                    RelationLockMode::AccessExclusive,
                )],
                &cancel_second,
            );
            tx.send((2, result.as_ref().err().map(|error| error.code)))
                .unwrap();
            if result
                .as_ref()
                .is_err_and(|error| error.code == SqlState::DeadlockDetected)
            {
                while !release_second.load(Ordering::Relaxed) {
                    thread::yield_now();
                }
            }
        });

        let first_result = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(first_result.1, Some(SqlState::DeadlockDetected));
        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
        release_victim.store(true, Ordering::Relaxed);
        let second_result = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let results = [first_result, second_result];
        assert_eq!(
            results
                .iter()
                .filter(|(_, state)| *state == Some(SqlState::DeadlockDetected))
                .count(),
            1
        );
        assert_eq!(
            results.iter().filter(|(_, state)| state.is_none()).count(),
            1
        );
        first_thread.join().unwrap();
        second_thread.join().unwrap();
    }

    #[test]
    fn mixed_row_and_relation_deadlock_uses_one_graph() {
        let (manager, registry) = manager(30);
        registry.register(1);
        registry.register(2);
        let cancel = Arc::new(QueryCancel::new());
        let mut table_holder = manager.transaction_owner(1).unwrap();
        table_holder
            .acquire_many(
                &[ObjectLockRequest::table(
                    1,
                    RelationLockMode::AccessExclusive,
                )],
                &cancel,
            )
            .unwrap();
        let mut table_waiter = manager.transaction_owner(2).unwrap();
        let (tx, rx) = mpsc::channel();

        let row_manager = Arc::clone(&manager);
        let row_registry = registry.clone();
        let row_cancel = Arc::clone(&cancel);
        let row_tx = tx.clone();
        let row_thread = thread::spawn(move || {
            let result = row_manager.wait_for(1, 2, &row_cancel);
            if result
                .as_ref()
                .is_err_and(|error| error.code == SqlState::DeadlockDetected)
            {
                row_registry.deregister(1);
                row_manager.on_txn_finished();
            }
            drop(table_holder);
            row_tx
                .send(("row", result.as_ref().err().map(|error| error.code)))
                .unwrap();
        });

        let object_manager = Arc::clone(&manager);
        let object_registry = registry.clone();
        let object_cancel = Arc::clone(&cancel);
        let object_thread = thread::spawn(move || {
            let result = table_waiter.acquire_many(
                &[ObjectLockRequest::table(1, RelationLockMode::AccessShare)],
                &object_cancel,
            );
            if result
                .as_ref()
                .is_err_and(|error| error.code == SqlState::DeadlockDetected)
            {
                object_registry.deregister(2);
                object_manager.on_txn_finished();
            }
            object_manager.on_txn_finished();
            tx.send(("object", result.as_ref().err().map(|error| error.code)))
                .unwrap();
        });

        let results = [
            rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        ];
        assert_eq!(
            results
                .iter()
                .filter(|(_, state)| *state == Some(SqlState::DeadlockDetected))
                .count(),
            1
        );
        assert_eq!(
            results.iter().filter(|(_, state)| state.is_none()).count(),
            1
        );
        row_thread.join().unwrap();
        object_thread.join().unwrap();
    }

    #[test]
    fn cancellation_removes_queued_request() {
        let (manager, _) = manager(500);
        let holder_cancel = QueryCancel::new();
        let mut holder = manager.statement_owner().unwrap();
        holder
            .acquire_many(
                &[ObjectLockRequest::table(
                    1,
                    RelationLockMode::AccessExclusive,
                )],
                &holder_cancel,
            )
            .unwrap();
        let cancel = Arc::new(QueryCancel::new());
        let mut waiter = manager.statement_owner().unwrap();
        let waiter_cancel = Arc::clone(&cancel);
        let thread = thread::spawn(move || {
            waiter.acquire_many(
                &[ObjectLockRequest::table(1, RelationLockMode::AccessShare)],
                &waiter_cancel,
            )
        });
        thread::sleep(Duration::from_millis(20));
        cancel.request(common::CancelReason::UserRequest);
        let error = thread.join().unwrap().unwrap_err();
        assert_eq!(error.code, SqlState::QueryCanceled);
    }
}
