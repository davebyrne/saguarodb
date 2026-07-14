use super::*;

enum CurrentVersion {
    Dead,
    Live,
    Wait(u64),
}

enum DependentCandidateOutcome {
    Continue,
    Found,
    Restart,
    Wait(u64),
}

#[derive(Clone, Copy)]
struct DependentProbeContext<'a> {
    relations: &'a PageBackedRelationSnapshot,
    schema: &'a TableSchema,
    columns: &'a [ColumnId],
    key: &'a Key,
    excluded: Option<&'a common::RowIdentity>,
    indexed: bool,
}

type TaggedTupleChanges = Vec<(TupleLockTag, TupleLockGrantChange)>;

fn restore_tagged_changes(ctx: &StatementContext, changes: TaggedTupleChanges) -> Result<()> {
    restore_tuple_changes(ctx, changes.into_iter().map(|(_, change)| change).collect())
}

fn restore_tagged_changes_after_error<T>(
    ctx: &StatementContext,
    changes: TaggedTupleChanges,
    original: DbError,
) -> Result<T> {
    match restore_tagged_changes(ctx, changes) {
        Ok(()) => Err(original),
        Err(restore) => Err(DbError::internal(format!(
            "tuple-lock acquisition failed ({original}); restoring its grants also failed ({restore})"
        ))),
    }
}

impl PageBackedStorageEngine {
    pub(super) fn lock_row_with_changes(
        &self,
        ctx: &StatementContext,
        relations: &PageBackedRelationSnapshot,
        table: TableId,
        identity: &common::RowIdentity,
        mode: TupleLockMode,
        wait_policy: TupleLockWaitPolicy,
    ) -> Result<(LockRowResult, TaggedTupleChanges)> {
        let table_handle = self.table_handle(relations, table)?;
        let schema = table_handle.schema.clone();
        let mut next_tag = TupleLockTag {
            table,
            key: identity.key.clone(),
        };
        let mut held_tags = BTreeSet::new();
        let mut changes = Vec::new();

        loop {
            if held_tags.insert(next_tag.clone()) {
                match ctx.tuple_locks.acquire_tuple(
                    ctx.txn_id,
                    &next_tag,
                    mode,
                    wait_policy,
                    ctx.cancel.as_ref(),
                ) {
                    Ok(TupleLockAcquire::Acquired(change)) => {
                        changes.push((next_tag.clone(), change));
                    }
                    Ok(TupleLockAcquire::Skipped) => {
                        restore_tagged_changes(ctx, changes)?;
                        return Ok((LockRowResult::Skipped, Vec::new()));
                    }
                    Err(err) => return restore_tagged_changes_after_error(ctx, changes, err),
                }
            }

            let latest = loop {
                match self.resolve_latest_row_version(ctx, relations, &schema, identity) {
                    Ok(LatestRowVersion::WouldBlock(blocker)) => match wait_policy {
                        TupleLockWaitPolicy::Block => {
                            if let Err(err) = self.wait_for_conflict(ctx, blocker) {
                                return restore_tagged_changes_after_error(ctx, changes, err);
                            }
                        }
                        TupleLockWaitPolicy::NoWait => {
                            return restore_tagged_changes_after_error(
                                ctx,
                                changes,
                                DbError::execute(
                                    SqlState::LockNotAvailable,
                                    "could not obtain tuple lock on row",
                                ),
                            );
                        }
                        TupleLockWaitPolicy::SkipLocked => {
                            restore_tagged_changes(ctx, changes)?;
                            return Ok((LockRowResult::Skipped, Vec::new()));
                        }
                    },
                    Ok(other) => break other,
                    Err(err) => return restore_tagged_changes_after_error(ctx, changes, err),
                }
            };
            match latest {
                LatestRowVersion::Live { row, .. } => {
                    let current_tag = TupleLockTag {
                        table,
                        key: row.identity().key.clone(),
                    };
                    if held_tags.contains(&current_tag) {
                        return Ok((
                            LockRowResult::Locked(LockedRow::from_lock_grant(
                                table,
                                ctx.txn_id,
                                row.identity().clone(),
                                row.row().clone(),
                                mode,
                            )),
                            changes,
                        ));
                    }
                    next_tag = current_tag;
                }
                LatestRowVersion::Deleted => {
                    restore_tagged_changes(ctx, changes)?;
                    return Ok((LockRowResult::Deleted, Vec::new()));
                }
                LatestRowVersion::WouldBlock(_) => {
                    return restore_tagged_changes_after_error(
                        ctx,
                        changes,
                        storage_internal(
                            "in-progress row version escaped the tuple-lock wait loop",
                        ),
                    );
                }
            }
        }
    }

    pub(super) fn update_visible_with_lock_mode(
        &self,
        ctx: &StatementContext,
        relations: &PageBackedRelationSnapshot,
        table: TableId,
        key: &Key,
        row: Row,
        mode: TupleLockMode,
    ) -> Result<bool> {
        self.ensure_current_generation_for_write(relations, table)?;
        let table_handle = self.table_handle(relations, table)?;
        let schema = table_handle.schema.clone();
        let btree = self.btree(table_handle.primary_index_file_id);
        let visible = {
            let rewrite_latch = self.identity_rewrite_latch(table);
            let _rewrite_guard = rewrite_latch.read();
            self.locate_visible_version(&btree, key, &ctx.snapshot, &ctx.live_txns)?
        };
        let Some((previous_location, _infomask)) = visible else {
            return Ok(false);
        };
        let (resolved, xmin, _previous_row) = self
            .read_visible_row(ctx, relations, &schema, previous_location)?
            .ok_or_else(|| storage_internal("visible row disappeared during update"))?;
        let identity = common::RowIdentity {
            row_id: RowId {
                page_num: resolved.page_num,
                slot_num: resolved.slot_num,
            },
            xmin,
            key: key.clone(),
        };
        match <Self as StorageEngine>::lock_row(
            self,
            ctx,
            relations,
            table,
            &identity,
            mode,
            TupleLockWaitPolicy::Block,
        )? {
            LockRowResult::Locked(target) if *target.identity() == identity => {
                <Self as StorageEngine>::update_locked(self, ctx, relations, table, &target, row)
            }
            LockRowResult::Locked(_) | LockRowResult::Deleted => Err(concurrent_update_error()),
            LockRowResult::Skipped => Err(storage_internal(
                "blocking update tuple-lock acquisition skipped a row",
            )),
        }
    }

    pub(super) fn probe_referenced_key(
        &self,
        ctx: &StatementContext,
        relations: &PageBackedRelationSnapshot,
        table: TableId,
        access_index: IndexId,
        key: &Key,
    ) -> Result<bool> {
        let table_handle = self.table_handle(relations, table)?;
        let schema = table_handle.schema.clone();
        let (columns, secondary_index) = if access_index == common::PRIMARY_KEY_INDEX_ID {
            if schema.primary_key.is_empty() {
                return Err(storage_internal(
                    "foreign-key referenced probe selected a missing primary key",
                ));
            }
            (schema.primary_key.clone(), None)
        } else {
            let index = self.index_handle(relations, table, access_index)?.schema;
            if index.constraint != common::IndexConstraintKind::Unique {
                return Err(storage_internal(format!(
                    "foreign-key referenced probe selected non-constraint index {}",
                    index.id
                )));
            }
            (index.columns.clone(), Some(index))
        };
        if columns.len() != key.0.len() {
            return Err(storage_internal(
                "foreign-key referenced probe key width does not match its access index",
            ));
        }

        loop {
            let roots: Vec<RowLocation> = if let Some(index) = secondary_index.as_ref() {
                self.secondary_btree(index)
                    .range_cancelable(&KeyRange::Exact(key.clone()), Some(ctx.cancel.as_ref()))?
                    .into_iter()
                    .map(|(_, location)| location)
                    .collect()
            } else {
                self.btree(table_handle.primary_index_file_id)
                    .range_cancelable(&KeyRange::Exact(key.clone()), Some(ctx.cancel.as_ref()))?
                    .into_iter()
                    .map(|(_, location)| location)
                    .collect()
            };
            let mut candidates = Vec::new();
            let mut seen = HashSet::new();
            for root in roots {
                self.validate_referential_index_root(&schema, root)?;
                for (location, physical) in
                    self.collect_referential_chain_versions(&schema, root)?
                {
                    if seen.insert(location) {
                        candidates.push((location, physical, root));
                    }
                }
            }
            let mut restart = None;
            let mut rescan = false;
            for (location, physical, identity_root) in candidates {
                ctx.cancel.check()?;
                match self.current_version(ctx, &physical.header) {
                    CurrentVersion::Dead => continue,
                    CurrentVersion::Wait(blocker) => {
                        restart = Some(blocker);
                        break;
                    }
                    CurrentVersion::Live => {}
                }
                let xmin = physical.header.xmin;
                let row = self.materialize_current_row(ctx, relations, &schema, physical)?;
                let (candidate_key, _) = row_key_for_columns(&schema, &columns, &row)?;
                if candidate_key != *key {
                    return Err(storage_internal(
                        "foreign-key access index points to a live row with a different key",
                    ));
                }
                let identity = common::RowIdentity {
                    row_id: RowId {
                        page_num: location.page_num,
                        slot_num: location.slot_num,
                    },
                    xmin,
                    key: storage_identity_key_for_row(&schema, &row, identity_root)?,
                };
                let (lock_result, changes) = self.lock_row_with_changes(
                    ctx,
                    relations,
                    table,
                    &identity,
                    TupleLockMode::KeyShare,
                    TupleLockWaitPolicy::Block,
                )?;
                match lock_result {
                    LockRowResult::Locked(locked) => {
                        let (current_key, _) =
                            match row_key_for_columns(&schema, &columns, locked.row()) {
                                Ok(key) => key,
                                Err(err) => {
                                    return restore_tagged_changes_after_error(ctx, changes, err);
                                }
                            };
                        if current_key == *key {
                            if let Err(err) = self.ensure_current_visible_to_retained_snapshot(
                                ctx,
                                &schema,
                                locked.identity(),
                            ) {
                                return restore_tagged_changes_after_error(ctx, changes, err);
                            }
                            let current_tag = TupleLockTag {
                                table,
                                key: locked.identity().key.clone(),
                            };
                            let mut stale = Vec::new();
                            for (tag, change) in changes {
                                if tag != current_tag {
                                    stale.push((tag, change));
                                }
                            }
                            restore_tagged_changes(ctx, stale)?;
                            return Ok(true);
                        }
                        restore_tagged_changes(ctx, changes)?;
                        rescan = true;
                    }
                    LockRowResult::Deleted => rescan = true,
                    LockRowResult::Skipped => {
                        return Err(storage_internal(
                            "blocking foreign-key tuple lock unexpectedly skipped a row",
                        ));
                    }
                }
            }
            if let Some(blocker) = restart {
                self.wait_for_conflict(ctx, blocker)?;
                continue;
            }
            if rescan {
                continue;
            }
            return Ok(false);
        }
    }

    pub(super) fn probe_dependent_row(
        &self,
        ctx: &StatementContext,
        relations: &PageBackedRelationSnapshot,
        probe: DependentRowProbe<'_>,
    ) -> Result<bool> {
        let DependentRowProbe {
            table,
            columns,
            key,
            supporting_index,
            excluded,
        } = probe;
        let table_handle = self.table_handle(relations, table)?;
        let schema = table_handle.schema.clone();
        if columns.is_empty() || columns.len() != key.0.len() {
            return Err(storage_internal(
                "foreign-key dependent probe has an invalid column/key width",
            ));
        }
        let probe_ctx = DependentProbeContext {
            relations,
            schema: &schema,
            columns,
            key,
            excluded,
            indexed: supporting_index.is_some(),
        };

        loop {
            let indexed_roots = match supporting_index {
                Some(common::PRIMARY_KEY_INDEX_ID) => {
                    if schema.primary_key != columns {
                        return Err(storage_internal(
                            "foreign-key child primary index does not match probe columns",
                        ));
                    }
                    Some(
                        self.btree(table_handle.primary_index_file_id)
                            .range_cancelable(
                                &KeyRange::Exact(key.clone()),
                                Some(ctx.cancel.as_ref()),
                            )?
                            .into_iter()
                            .map(|(_, location)| location)
                            .collect(),
                    )
                }
                Some(index_id) => {
                    let index = self.index_handle(relations, schema.id, index_id)?.schema;
                    if index.columns != columns {
                        return Err(storage_internal(format!(
                            "foreign-key child index {} does not match probe columns",
                            index.id
                        )));
                    }
                    Some(
                        self.secondary_btree(&index)
                            .range_cancelable(
                                &KeyRange::Exact(key.clone()),
                                Some(ctx.cancel.as_ref()),
                            )?
                            .into_iter()
                            .map(|(_, location)| location)
                            .collect(),
                    )
                }
                None => None,
            };

            let outcome = if let Some(roots) = indexed_roots {
                self.probe_dependent_roots(ctx, probe_ctx, roots)?
            } else {
                self.probe_dependent_heap(ctx, probe_ctx)?
            };
            match outcome {
                DependentCandidateOutcome::Found => return Ok(true),
                DependentCandidateOutcome::Continue => return Ok(false),
                DependentCandidateOutcome::Restart => continue,
                DependentCandidateOutcome::Wait(blocker) => {
                    self.wait_for_conflict(ctx, blocker)?;
                }
            }
        }
    }

    fn probe_dependent_heap(
        &self,
        ctx: &StatementContext,
        probe: DependentProbeContext<'_>,
    ) -> Result<DependentCandidateOutcome> {
        let file_id = heap_file_id(probe.schema.storage_id);
        let page_count = self.buffer_pool.page_count(file_id)?;
        for page_num in 0..page_count {
            ctx.cancel.check()?;
            if self.buffer_pool.is_page_abandoned(file_id, page_num) {
                continue;
            }
            let roots = self.dependent_heap_roots(file_id, page_num)?;
            match self.probe_dependent_roots(ctx, probe, roots)? {
                DependentCandidateOutcome::Continue => {}
                outcome => return Ok(outcome),
            }
        }
        Ok(DependentCandidateOutcome::Continue)
    }

    fn dependent_heap_roots(&self, file_id: FileId, page_num: PageNum) -> Result<Vec<RowLocation>> {
        let readable = self.buffer_pool.read_page(file_id, page_num)?;
        let data = readable.data();
        if !page::is_initialized(data) {
            return Ok(Vec::new());
        }
        let slot_count = page::next_slot(data)?;
        let mut members = HashSet::new();
        let mut member_owner = HashMap::new();
        let mut has_multiple_owners = false;
        let mut hot_successors = HashMap::new();
        for slot_num in 0..slot_count {
            match page::slot_state(data, slot_num)? {
                page::LinePointer::Redirect(target) => {
                    if page::slot_state(data, target)? != page::LinePointer::Normal {
                        return Err(storage_internal(
                            "redirect line pointer target is not a NORMAL tuple",
                        ));
                    }
                    has_multiple_owners |= member_owner.insert(target, slot_num).is_some();
                    members.insert(target);
                }
                page::LinePointer::Normal => {
                    let bytes = page::read_row(data, slot_num)?.ok_or_else(|| {
                        storage_internal("NORMAL line pointer has no tuple bytes")
                    })?;
                    let (_xmin, _xmax, t_ctid, infomask) = decode_mvcc_header(&bytes)?;
                    if infomask & crate::codec::HOT_UPDATED == 0 {
                        continue;
                    }
                    if t_ctid.0 != page_num {
                        return Err(storage_internal(
                            "HOT_UPDATED tuple points to a different heap page",
                        ));
                    }
                    if page::slot_state(data, t_ctid.1)? != page::LinePointer::Normal {
                        return Err(storage_internal(
                            "HOT_UPDATED successor slot is not a NORMAL tuple",
                        ));
                    }
                    let successor = page::read_row(data, t_ctid.1)?
                        .ok_or_else(|| storage_internal("HOT successor is not a live tuple"))?;
                    let (_x, _xm, _t, successor_infomask) = decode_mvcc_header(&successor)?;
                    if successor_infomask & crate::codec::HEAP_ONLY != 0 {
                        has_multiple_owners |= member_owner.insert(t_ctid.1, slot_num).is_some();
                        members.insert(t_ctid.1);
                        hot_successors.insert(slot_num, t_ctid.1);
                    } else {
                        return Err(storage_internal(
                            "HOT_UPDATED successor is not marked HEAP_ONLY",
                        ));
                    }
                }
                page::LinePointer::Dead | page::LinePointer::Unused => {}
            }
        }
        let mut validated = HashSet::new();
        for start in hot_successors.keys().copied() {
            if validated.contains(&start) {
                continue;
            }
            let mut path = HashSet::new();
            let mut current = start;
            while let Some(successor) = hot_successors.get(&current).copied() {
                if validated.contains(&current) {
                    break;
                }
                if !path.insert(current) {
                    return Err(storage_internal("cyclic HOT chain in dependent-row probe"));
                }
                current = successor;
            }
            validated.extend(path);
        }
        if has_multiple_owners {
            return Err(storage_internal(
                "heap chain member has multiple incoming owners",
            ));
        }
        let mut roots = Vec::new();
        for slot_num in 0..slot_count {
            match page::slot_state(data, slot_num)? {
                page::LinePointer::Normal if !members.contains(&slot_num) => {
                    let bytes = page::read_row(data, slot_num)?.ok_or_else(|| {
                        storage_internal("NORMAL line pointer has no tuple bytes")
                    })?;
                    let (_xmin, _xmax, _t_ctid, infomask) = decode_mvcc_header(&bytes)?;
                    if infomask & crate::codec::HEAP_ONLY != 0 {
                        return Err(storage_internal(
                            "heap-only tuple is not reachable from a HOT root or redirect",
                        ));
                    }
                    roots.push(RowLocation {
                        file_id,
                        page_num,
                        slot_num,
                    });
                }
                page::LinePointer::Redirect(_) => roots.push(RowLocation {
                    file_id,
                    page_num,
                    slot_num,
                }),
                page::LinePointer::Normal | page::LinePointer::Dead | page::LinePointer::Unused => {
                }
            }
        }
        Ok(roots)
    }

    fn probe_dependent_roots(
        &self,
        ctx: &StatementContext,
        probe: DependentProbeContext<'_>,
        roots: Vec<RowLocation>,
    ) -> Result<DependentCandidateOutcome> {
        let mut seen = HashSet::new();
        for root in roots {
            ctx.cancel.check()?;
            if probe.indexed {
                self.validate_referential_index_root(probe.schema, root)?;
            }
            for (location, physical) in
                self.collect_referential_chain_versions(probe.schema, root)?
            {
                ctx.cancel.check()?;
                if !seen.insert(location) {
                    continue;
                }
                match self.probe_dependent_candidate(ctx, probe, root, location, physical)? {
                    DependentCandidateOutcome::Continue => {}
                    outcome => return Ok(outcome),
                }
            }
        }
        Ok(DependentCandidateOutcome::Continue)
    }

    fn collect_referential_chain_versions(
        &self,
        schema: &TableSchema,
        root: RowLocation,
    ) -> Result<Vec<(RowLocation, crate::codec::DecodedPhysicalRow)>> {
        if root.file_id != heap_file_id(schema.storage_id) {
            return Err(storage_internal(
                "foreign-key access index points outside its table heap",
            ));
        }
        let versions = self.collect_chain_versions(schema, root)?;
        if versions
            .last()
            .is_some_and(|(_, physical)| physical.header.infomask & crate::codec::HOT_UPDATED != 0)
        {
            return Err(storage_internal(
                "foreign-key probe encountered an invalid HOT successor",
            ));
        }
        Ok(versions)
    }

    fn validate_referential_index_root(
        &self,
        schema: &TableSchema,
        root: RowLocation,
    ) -> Result<()> {
        if root.file_id != heap_file_id(schema.storage_id) {
            return Err(storage_internal(
                "foreign-key access index points outside its table heap",
            ));
        }
        let readable = self.buffer_pool.read_page(root.file_id, root.page_num)?;
        let data = readable.data();
        if page::slot_state(data, root.slot_num)? != page::LinePointer::Normal {
            return Ok(());
        }
        let bytes = page::read_row(data, root.slot_num)?
            .ok_or_else(|| storage_internal("NORMAL line pointer has no tuple bytes"))?;
        let (_xmin, _xmax, _t_ctid, infomask) = decode_mvcc_header(&bytes)?;
        if infomask & crate::codec::HEAP_ONLY != 0 {
            return Err(storage_internal(
                "foreign-key access index points directly to a heap-only tuple",
            ));
        }
        Ok(())
    }

    fn probe_dependent_candidate(
        &self,
        ctx: &StatementContext,
        probe: DependentProbeContext<'_>,
        identity_root: RowLocation,
        location: RowLocation,
        physical: crate::codec::DecodedPhysicalRow,
    ) -> Result<DependentCandidateOutcome> {
        let creator = self.creator_state(ctx, &physical.header);
        if matches!(creator, CurrentVersion::Dead) {
            return Ok(DependentCandidateOutcome::Continue);
        }
        let initial = self.current_version(ctx, &physical.header);
        let xmin = physical.header.xmin;
        let row = self.materialize_probe_row(
            ctx,
            probe.relations,
            probe.schema,
            physical,
            matches!(creator, CurrentVersion::Wait(_)).then_some(xmin),
        )?;
        let (candidate_key, _) = row_key_for_columns(probe.schema, probe.columns, &row)?;
        if candidate_key != *probe.key {
            if probe.indexed && !matches!(initial, CurrentVersion::Dead) {
                return Err(storage_internal(
                    "foreign-key child index points to a live row with a different key",
                ));
            }
            return Ok(DependentCandidateOutcome::Continue);
        }
        match initial {
            CurrentVersion::Dead => return Ok(DependentCandidateOutcome::Continue),
            CurrentVersion::Wait(blocker) => {
                return Ok(DependentCandidateOutcome::Wait(blocker));
            }
            CurrentVersion::Live => {}
        }
        match self.current_version_header(ctx, xmin, location)? {
            CurrentVersion::Dead => return Ok(DependentCandidateOutcome::Restart),
            CurrentVersion::Wait(blocker) => {
                return Ok(DependentCandidateOutcome::Wait(blocker));
            }
            CurrentVersion::Live => {}
        }
        let identity = common::RowIdentity {
            row_id: RowId {
                page_num: location.page_num,
                slot_num: location.slot_num,
            },
            xmin,
            key: storage_identity_key_for_row(probe.schema, &row, identity_root)?,
        };
        match self.resolve_latest_row_version(ctx, probe.relations, probe.schema, &identity)? {
            LatestRowVersion::WouldBlock(blocker) => Ok(DependentCandidateOutcome::Wait(blocker)),
            LatestRowVersion::Deleted => Ok(DependentCandidateOutcome::Restart),
            LatestRowVersion::Live { row: current, .. } => {
                let (current_key, _) =
                    row_key_for_columns(probe.schema, probe.columns, current.row())?;
                if current_key != *probe.key {
                    return Ok(DependentCandidateOutcome::Restart);
                }
                if probe
                    .excluded
                    .is_some_and(|excluded| excluded == current.identity())
                {
                    return Ok(DependentCandidateOutcome::Continue);
                }
                self.ensure_current_visible_to_retained_snapshot(
                    ctx,
                    probe.schema,
                    current.identity(),
                )?;
                Ok(DependentCandidateOutcome::Found)
            }
        }
    }

    fn creator_state(
        &self,
        ctx: &StatementContext,
        header: &crate::codec::MvccHeader,
    ) -> CurrentVersion {
        if ctx.live_txns.contains(&header.xmin) || header.infomask & common::XMIN_COMMITTED != 0 {
            return CurrentVersion::Live;
        }
        if header.infomask & common::XMIN_ABORTED != 0 {
            return CurrentVersion::Dead;
        }
        match self.txn_status_view().status(header.xmin) {
            TxnStatus::Aborted => CurrentVersion::Dead,
            TxnStatus::Committed => CurrentVersion::Live,
            TxnStatus::InProgress => CurrentVersion::Wait(header.xmin),
        }
    }

    fn current_version_header(
        &self,
        ctx: &StatementContext,
        expected_xmin: u64,
        location: RowLocation,
    ) -> Result<CurrentVersion> {
        let readable = self
            .buffer_pool
            .read_page(location.file_id, location.page_num)?;
        if page::slot_state(readable.data(), location.slot_num)? != page::LinePointer::Normal {
            return Ok(CurrentVersion::Dead);
        }
        let bytes = page::read_row(readable.data(), location.slot_num)?
            .ok_or_else(|| storage_internal("foreign-key candidate row disappeared"))?;
        let (xmin, xmax, _t_ctid, infomask) = decode_mvcc_header(&bytes)?;
        if xmin != expected_xmin {
            return Ok(CurrentVersion::Dead);
        }
        Ok(self.current_version(
            ctx,
            &crate::codec::MvccHeader {
                xmin,
                xmax,
                t_ctid: crate::codec::INVALID_TID,
                infomask,
            },
        ))
    }

    fn current_version(
        &self,
        ctx: &StatementContext,
        header: &crate::codec::MvccHeader,
    ) -> CurrentVersion {
        let status = self.txn_status_view();
        let creator = if ctx.live_txns.contains(&header.xmin)
            || header.infomask & common::XMIN_COMMITTED != 0
        {
            TxnStatus::Committed
        } else if header.infomask & common::XMIN_ABORTED != 0 {
            TxnStatus::Aborted
        } else {
            status.status(header.xmin)
        };
        match creator {
            TxnStatus::Aborted => return CurrentVersion::Dead,
            TxnStatus::InProgress => return CurrentVersion::Wait(header.xmin),
            TxnStatus::Committed => {}
        }
        if header.xmax == common::INVALID_XID {
            return CurrentVersion::Live;
        }
        if ctx.live_txns.contains(&header.xmax) || header.infomask & common::XMAX_COMMITTED != 0 {
            return CurrentVersion::Dead;
        }
        if header.infomask & common::XMAX_ABORTED != 0 {
            return CurrentVersion::Live;
        }
        match status.status(header.xmax) {
            TxnStatus::Aborted => CurrentVersion::Live,
            TxnStatus::Committed => CurrentVersion::Dead,
            TxnStatus::InProgress => CurrentVersion::Wait(header.xmax),
        }
    }

    fn materialize_current_row(
        &self,
        ctx: &StatementContext,
        relations: &PageBackedRelationSnapshot,
        schema: &TableSchema,
        physical: crate::codec::DecodedPhysicalRow,
    ) -> Result<Row> {
        self.materialize_probe_row(ctx, relations, schema, physical, None)
    }

    fn materialize_probe_row(
        &self,
        ctx: &StatementContext,
        relations: &PageBackedRelationSnapshot,
        schema: &TableSchema,
        physical: crate::codec::DecodedPhysicalRow,
        provisional_creator: Option<u64>,
    ) -> Result<Row> {
        let mut current = ctx.clone();
        current.snapshot = Arc::new(Snapshot::sees_all_committed());
        if let Some(creator) = provisional_creator
            && !current.live_txns.contains(&creator)
        {
            let mut live = current.live_txns.to_vec();
            live.push(creator);
            current.live_txns = Arc::from(live);
        }
        self.materialize_physical_row(&current, relations, schema, physical)
    }

    fn ensure_current_visible_to_retained_snapshot(
        &self,
        ctx: &StatementContext,
        schema: &TableSchema,
        identity: &common::RowIdentity,
    ) -> Result<()> {
        if ctx.isolation == common::IsolationLevel::ReadCommitted {
            return Ok(());
        }
        let file_id = heap_file_id(schema.storage_id);
        let readable = self
            .buffer_pool
            .read_page(file_id, identity.row_id.page_num)?;
        let bytes = page::read_row(readable.data(), identity.row_id.slot_num)?
            .ok_or_else(|| storage_internal("foreign-key current row disappeared"))?;
        let (xmin, xmax, _t_ctid, infomask) = decode_mvcc_header(&bytes)?;
        if is_visible(
            xmin,
            xmax,
            infomask,
            &ctx.snapshot,
            ctx.live_txns.as_ref(),
            self.txn_status_view(),
        ) {
            return Ok(());
        }
        Err(DbError::execute(
            SqlState::SerializationFailure,
            "could not serialize access due to concurrent foreign key change",
        ))
    }
}
