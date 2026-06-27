use super::*;

impl PageBackedStorageEngine {
    pub(super) fn write_new_row(
        &self,
        schema: &TableSchema,
        row: &Row,
        txn_id: u64,
    ) -> Result<RowLocation> {
        let row_bytes = encode_row(schema, row, txn_id)?;
        if row_bytes.len() + page_overhead() > buffer::PAGE_SIZE {
            return Err(DbError::storage(
                SqlState::InternalError,
                "row is too large for a data page",
            ));
        }

        let file_id = schema.id;
        // Hold the per-heap-file structural latch across the WHOLE free-space search
        // + allocate + insert (Milestone E2a). This makes "find space / extend /
        // insert / log" atomic against another inserter on the same table heap,
        // closing the TOCTOU where the read-check-drop-rewrite below would let two
        // concurrent inserters both target the same last slot. The latch wraps the
        // existing-page scan, the `new_page` extension, and `log_insert`; it is
        // dropped on return so a later index insert takes its own latch (rule 1: never
        // two structural latches at once). Contended under E2b's concurrent writers:
        // same-heap inserters serialize here. (Lock order: structural latch → frame
        // latch inside `read_page`/`write_page`/`new_page` → WAL mutex inside the
        // appends.)
        let latch = self.structural_latch(file_id);
        let _heap_guard = latch.lock();
        for page_num in self.table_page_nums(file_id)? {
            let readable = self.buffer_pool.read_page(file_id, page_num)?;
            let has_space = page::has_space_for(readable.data(), row_bytes.len())?;
            drop(readable);
            if has_space {
                let mut writable = self.buffer_pool.write_page(file_id, page_num, txn_id)?;
                let slot_num =
                    self.log_insert(&mut writable, txn_id, file_id, page_num, &row_bytes)?;
                return Ok(RowLocation {
                    file_id,
                    page_num,
                    slot_num,
                });
            }
        }

        // Allocate a fresh page. HeapInit is the page's own redo base, so a new
        // page never needs a separate full-page image.
        let mut writable = self.buffer_pool.new_page(file_id, txn_id)?;
        let page_num = writable.page_num();
        let init_lsn = self.wal.append(WalRecord {
            lsn: 0,
            txn_id,
            kind: WalRecordKind::HeapInit { file_id, page_num },
        })?;
        page::init_page(writable.data_mut(), page_num);
        page::set_page_lsn(writable.data_mut(), init_lsn);
        let slot_num = self.log_insert(&mut writable, txn_id, file_id, page_num, &row_bytes)?;
        Ok(RowLocation {
            file_id,
            page_num,
            slot_num,
        })
    }
    /// Insert a row into a pinned page and log its redo record: a full-page image
    /// on the first modification since the last checkpoint (torn-page protection),
    /// otherwise a `HeapInsert` delta. Stamps the page-LSN with the record's LSN.
    fn log_insert(
        &self,
        guard: &mut PageWriteGuard,
        txn_id: u64,
        file_id: FileId,
        page_num: PageNum,
        row_bytes: &[u8],
    ) -> Result<u16> {
        if guard.take_needs_fpi() {
            let slot_num = page::insert_row(guard.data_mut(), row_bytes)?;
            let lsn = self.wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::FullPageImage {
                    file_id,
                    page_num,
                    image: guard.data().to_vec(),
                },
            })?;
            page::set_page_lsn(guard.data_mut(), lsn);
            Ok(slot_num)
        } else {
            // Insert into the buffer FIRST, then log the slot id it actually landed
            // in. `insert_row` recycles an UNUSED slot id before appending (F3b), so
            // the produced slot is no longer predictable as `next_slot`; logging the
            // real slot keeps the `HeapInsert` redo exact (its redo re-runs
            // `insert_row` and asserts the same slot id is reproduced). Mutating the
            // buffer before appending the record mirrors the FPI arm above and is
            // WAL-safe: the page-LSN is stamped with the record's LSN below, so the
            // dirty page cannot be flushed ahead of its WAL record.
            let slot_num = page::insert_row(guard.data_mut(), row_bytes)?;
            let lsn = self.wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::HeapInsert {
                    file_id,
                    page_num,
                    slot: slot_num,
                    row_bytes: row_bytes.to_vec(),
                },
            })?;
            page::set_page_lsn(guard.data_mut(), lsn);
            Ok(slot_num)
        }
    }
    /// Write a HOT heap-only successor tuple onto **the predecessor's own page**
    /// (`page_num`), or return `Ok(None)` when the page has no room (so the caller
    /// falls back to a normal fully-indexed update). This is the placement half of
    /// the HOT-update fast path (`docs/specs/mvcc.md` §10 Milestone H2): unlike
    /// [`Self::write_new_row`] (which picks *any* page with space), HOT must keep the
    /// new version on the predecessor's page so the bounded `t_ctid` walk (H1) reaches
    /// it from the indexed root without a new index entry.
    ///
    /// The tuple is encoded with [`crate::codec::HEAP_ONLY`] set in its header
    /// (`xmin = txn_id`, `xmax = invalid`, `t_ctid = self`), so the bit is carried
    /// into the logged `HeapInsert` image and redone on recovery (the row bytes are
    /// the source of truth for `infomask`). It is logged exactly like
    /// [`Self::log_insert`] (a `FullPageImage` on first touch since the checkpoint,
    /// else a `HeapInsert` delta), so recovery reinstalls it identically.
    ///
    /// **Latching.** Takes the per-heap structural latch then the frame write latch
    /// for `page_num` (lock order structural → frame → WAL), both released on return.
    /// The space peek is done **before** consuming the page's first-touch FPI flag,
    /// so a no-room fall-back does not perturb the page's WAL state.
    /// `prune_horizon`: when `Some(horizon)` and the page has no room, run the H3
    /// update-path prune on this page (under the latch already held) to reclaim space
    /// from this row's committed-dead HOT prefix (and any other prunable chain on the
    /// page), then retry the same-page insert once. The prune mutates only this single
    /// latched page and never marks a root `DEAD` (so it needs no index vacuum —
    /// `classify_page_for_prune(.., allow_dead_roots = false)`); a fully-dead chain is
    /// left for VACUUM. Lock-free readers re-resolve through line pointers (incl. any
    /// new `REDIRECT`), so they stay correct. A stale/smaller `horizon` only reclaims
    /// less. `None` disables the prune (the non-HOT-update callers).
    fn try_hot_insert_on_page(
        &self,
        schema: &TableSchema,
        page_num: PageNum,
        row: &Row,
        txn_id: u64,
        prune_horizon: Option<u64>,
    ) -> Result<Option<RowLocation>> {
        let file_id = schema.id;
        let row_bytes =
            crate::codec::encode_row_with_infomask(schema, row, txn_id, crate::codec::HEAP_ONLY)?;

        let latch = self.structural_latch(file_id);
        let _heap_guard = latch.lock();
        let mut guard = self.buffer_pool.write_page(file_id, page_num, txn_id)?;

        // Peek whether the new tuple fits on THIS page before touching any WAL state
        // (so a fall-back leaves the page's first-touch FPI flag intact).
        if !page::has_space_for(guard.data(), row_bytes.len())? {
            // Update-path pruning (H3): try to reclaim same-page room by collapsing
            // this page's committed-dead HOT prefixes, then retry once. The prune logs
            // its own FullPageImage under `txn_id` (idempotent PageLSN-gated redo); it
            // only reclaims dead-to-all versions, so it is correct regardless of this
            // txn's outcome.
            let Some(horizon) = prune_horizon else {
                return Ok(None);
            };
            let plan = self.classify_page_for_prune(guard.data(), horizon, false)?;
            if plan.is_empty() {
                return Ok(None);
            }
            self.apply_prune_plan(&mut guard, &plan, file_id, page_num, txn_id)?;
            if !page::has_space_for(guard.data(), row_bytes.len())? {
                // Still no room after pruning ⇒ fall back to a normal update. The prune
                // already happened and is logged; the page is just denser now.
                return Ok(None);
            }
        }

        let slot_num = self.log_insert(&mut guard, txn_id, file_id, page_num, &row_bytes)?;
        Ok(Some(RowLocation {
            file_id,
            page_num,
            slot_num,
        }))
    }
    /// Stamp `xmax = txn_id` and `t_ctid` on the version at `location` **in place**
    /// and log its redo record (a full-page image on first touch since the last
    /// checkpoint, else a `HeapUpdateHeader` delta). The line pointer stays
    /// `NORMAL`: the tuple is physically present and is hidden purely by visibility
    /// once the stamping transaction commits (`docs/specs/mvcc.md` §3.2 invariant
    /// 1). `infomask` is carried through unchanged (no hint bits set here — that is
    /// the optional commit 10).
    ///
    /// This is the shared "mark a version superseded" write for both MVCC writes:
    /// `DELETE` passes `t_ctid = INVALID_TID` (a delete has no successor version);
    /// `UPDATE` passes `t_ctid = new_tid`, the forward version-chain pointer to the
    /// new tuple (invariant 5). It never removes the tuple or its index entries
    /// (VACUUM reclaims them, Milestone F).
    ///
    /// **First-updater-wins conflict check (E1b, `docs/specs/mvcc.md` §7.3).**
    /// `xmax` doubles as the row lock. Under the `write_page` frame latch — and
    /// **before** appending any WAL record or mutating the page — this re-reads the
    /// target version's *current physical* header (`xmax`/`infomask`) and runs
    /// [`common::write_conflict`]. The read-classify-stamp sequence is atomic on the
    /// frame latch: two concurrent writers racing to claim this version serialize on
    /// the latch, so the loser observes the winner's just-stamped `xmax` and aborts
    /// with [`SqlState::SerializationFailure`] (`40001`) — no WAL is appended and the
    /// header is left untouched on conflict. Checking `xmax` earlier (e.g. at
    /// `locate_visible_version` time) and stamping later under a fresh latch would be
    /// a TOCTOU race that defeats first-updater-wins, so the check lives here, inside
    /// the latch, next to the stamp. As of E2b (concurrent writers) this is
    /// load-bearing: when two writers race to delete/update the same version, the
    /// loser observes the winner's `xmax` and aborts with `40001`.
    pub(super) fn stamp_xmax_logged(
        &self,
        location: RowLocation,
        t_ctid: (PageNum, u16),
        infomask: u16,
        txn_id: u64,
    ) -> Result<()> {
        let mut guard = self
            .buffer_pool
            .write_page(location.file_id, location.page_num, txn_id)?;

        // Atomic first-updater-wins check: read the version's CURRENT physical
        // `xmax`/`infomask` under this frame latch and classify against the live
        // CLOG. A `Conflict` (the deleter committed-after-my-snapshot or is another
        // in-flight writer) fails fast — returning here appends NO WAL record and
        // leaves the header unstamped, so the winning writer's `xmax` stands.
        let current = page::read_row(guard.data(), location.slot_num)?
            .ok_or_else(|| storage_internal("cannot stamp xmax on a non-live slot"))?;
        let (_xmin, current_xmax, _t_ctid, current_infomask) =
            crate::codec::decode_mvcc_header(&current)?;
        if write_conflict(
            current_xmax,
            current_infomask,
            txn_id,
            self.txn_status_view(),
        ) == WriteConflict::Conflict
        {
            return Err(DbError::execute(
                SqlState::SerializationFailure,
                "could not serialize access due to concurrent update",
            ));
        }

        if guard.take_needs_fpi() {
            // Mutate the header first, then capture the page in a full-page image.
            // Keep the existing page-LSN on this in-place stamp; the FPI append
            // below assigns the record's LSN as the new page-LSN.
            let current_lsn = page::page_lsn(guard.data());
            page::set_tuple_header(
                guard.data_mut(),
                location.slot_num,
                txn_id,
                t_ctid,
                infomask,
                current_lsn,
            )?;
            let lsn = self.wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::FullPageImage {
                    file_id: location.file_id,
                    page_num: location.page_num,
                    image: guard.data().to_vec(),
                },
            })?;
            page::set_page_lsn(guard.data_mut(), lsn);
        } else {
            let lsn = self.wal.append(WalRecord {
                lsn: 0,
                txn_id,
                kind: WalRecordKind::HeapUpdateHeader {
                    file_id: location.file_id,
                    page_num: location.page_num,
                    slot: location.slot_num,
                    xmax: txn_id,
                    t_ctid,
                    infomask,
                },
            })?;
            page::set_tuple_header(
                guard.data_mut(),
                location.slot_num,
                txn_id,
                t_ctid,
                infomask,
                lsn,
            )?;
        }
        Ok(())
    }
    /// Attempt the HOT-update fast path (`docs/specs/mvcc.md` §10 Milestone H2) for
    /// an `UPDATE` whose visible predecessor is at `previous_location` (`infomask` its
    /// current header hints). Returns:
    ///
    /// - `Ok(Some(true))` — the HOT update was performed (the caller returns it).
    /// - `Ok(None)` — NOT eligible; the caller falls back to the normal fully-indexed
    ///   update path.
    ///
    /// Eligible iff BOTH:
    /// 1. **No indexed column changed.** The new row's key equals the predecessor's
    ///    for the primary key (already enforced by the caller — a PK change is
    ///    rejected) AND for every secondary index ([`secondary_index_key`]). If all
    ///    index keys match, only non-indexed columns differ.
    /// 2. **Same-page room.** The new heap-only tuple, encoded, fits in the free space
    ///    of the predecessor's own page ([`Self::try_hot_insert_on_page`] returns
    ///    `Some`). Reusing an `UNUSED` slot or appending both count. **Update-path
    ///    pruning (H3):** if there is no same-page room, the engine first runs the H3
    ///    prune on that page (collapsing its committed-dead HOT prefixes under the heap
    ///    latch it already holds, `gc_horizon` threaded in) and retries the same-page
    ///    insert; only if there is STILL no room does it fall back to a normal update.
    ///    The prune mutates only the single latched page and never marks a root `DEAD`
    ///    (no index vacuum), so lock-free readers — which re-resolve through line
    ///    pointers, incl. `REDIRECT` — stay correct, and the writer never takes the
    ///    exclusive guard. A stale/smaller `gc_horizon` only prunes less.
    ///
    /// When eligible: write the heap-only successor on the predecessor's page, then
    /// stamp the predecessor `xmax = txn`, `t_ctid → new`, and `HOT_UPDATED` via
    /// [`Self::stamp_xmax_logged`] (which keeps the atomic first-updater-wins check —
    /// a concurrent claimer yields `40001`). NO index entries are inserted: the index
    /// still points at the chain root, and the H1 bounded walk reaches the new version.
    ///
    /// **Orphan-on-conflict safety.** The heap-only tuple is placed BEFORE the
    /// stamp-with-conflict-check, mirroring the non-HOT path: on a `40001` the
    /// just-written heap-only tuple is left unreferenced (no predecessor `t_ctid`
    /// points at it, and it has no index entry), so its aborting `xmin` makes it
    /// invisible via CLOG ⇒ dead-to-all ⇒ reclaimable by VACUUM — harmless, exactly
    /// like the non-HOT orphan.
    pub(super) fn try_hot_update(
        &self,
        ctx: &StatementContext,
        schema: &TableSchema,
        table: TableId,
        previous_location: RowLocation,
        infomask: u16,
        row: &Row,
    ) -> Result<Option<bool>> {
        // Eligibility (1): no indexed column changed. Read the predecessor's CURRENT
        // physical row (not a snapshot read — we need its actual indexed values) and
        // compare every secondary index's key against the new row's. The primary key
        // is already known unchanged (the caller rejects a PK change). A missing
        // predecessor here means it was reclaimed under us — not eligible.
        let Some(previous_row) = self.read_location(schema, previous_location)? else {
            return Ok(None);
        };
        for index in self.table_indexes(table)? {
            let (old_key, _) = secondary_index_key(schema, &index, &previous_row)?;
            let (new_key, _) = secondary_index_key(schema, &index, row)?;
            if old_key != new_key {
                // An indexed column changed ⇒ the new version needs its own index
                // entry ⇒ not a HOT update; fall back.
                return Ok(None);
            }
        }

        // Eligibility (2): the new heap-only tuple fits on the predecessor's page —
        // possibly after the H3 update-path prune reclaims same-page room from this
        // page's committed-dead HOT prefixes (`Some(ctx.gc_horizon)`). The prune keeps
        // the visible predecessor (the live tail `L`) NORMAL at its stable slot id, so
        // `previous_location` is still valid for the stamp below. `None` (no room even
        // after pruning) ⇒ fall back to a normal update.
        let Some(new_location) = self.try_hot_insert_on_page(
            schema,
            previous_location.page_num,
            row,
            ctx.txn_id,
            Some(ctx.gc_horizon),
        )?
        else {
            return Ok(None);
        };

        // Stamp the predecessor: xmax = txn, t_ctid → the new heap-only tuple, and
        // HOT_UPDATED set (preserving its other infomask hints). This keeps the atomic
        // first-updater-wins check; on a `40001` the heap-only tuple written above is a
        // harmless orphan (see this method's doc). The new tuple is on the SAME page as
        // the predecessor by construction, so the H1 walk's same-page `HOT_UPDATED →
        // HEAP_ONLY` step reaches it.
        let new_tid = (new_location.page_num, new_location.slot_num);
        self.stamp_xmax_logged(
            previous_location,
            new_tid,
            infomask | crate::codec::HOT_UPDATED,
            ctx.txn_id,
        )?;

        // No index entries: the index keeps pointing at the chain root; the new
        // heap-only version is reached only by the bounded `t_ctid` walk from it. This
        // is the whole point of HOT — the un-indexed in-place version.
        Ok(Some(true))
    }
}
