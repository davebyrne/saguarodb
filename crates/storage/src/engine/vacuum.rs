use super::*;

impl PageBackedStorageEngine {
    /// The heap-prune VACUUM pass (`docs/specs/mvcc.md` §9, Milestone F2b): for every
    /// heap page of `schema`'s table, physically reclaim the tuples that are
    /// dead-to-everyone at `horizon` and return their TIDs. Reclaiming an aborted or
    /// committed-deleted version's space is what bounds heap bloat once the system has
    /// MVCC versions (`DELETE`/`UPDATE` only *tombstone* in milestones B–E).
    ///
    /// For each page, every `NORMAL` slot's tuple is classified with
    /// [`common::is_dead_to_all`] (its `xmin`/`xmax`/`infomask` from
    /// [`crate::codec::decode_mvcc_header`], settled against the live CLOG via
    /// [`Self::txn_status_view`]). Only dead-to-all slots are pruned: a live version
    /// (`xmax == INVALID_XID`), an in-flight deleter, and a committed delete at or above
    /// the horizon are all left `NORMAL` (the predicate's aborted-creator-any-age /
    /// committed-delete-below-horizon asymmetry — §9).
    ///
    /// **Abort-cleanup (F4c root-cause, `docs/specs/mvcc.md` §5.4 / §9 F4c).** A KEPT
    /// slot whose deleter is *definitively aborted* (`xmax != INVALID_XID` and the
    /// `XMAX_ABORTED` hint or `status(xmax) == Aborted`) is the surviving predecessor of
    /// an aborted UPDATE/DELETE — it stays live (the delete rolled back) and is NOT
    /// reclaimed, but its `xmax = T` is the only on-disk reference to the aborted `T` as a
    /// *deleter*. Its header is reset IN PLACE — `xmax → INVALID_XID`, `t_ctid → INVALID`,
    /// `HOT_UPDATED` + settled `XMAX_*` cleared (preserving `xmin`/`XMIN_*`/`HEAP_ONLY`) —
    /// so a full pass leaves no surviving reference to `T` (as deleter, mirroring the
    /// aborted-creator reclaim), licensing the F4c floor-advance for ALL aborted
    /// UPDATE/DELETE, not just inserts. VACUUM holds the exclusive guard, so `xmax`'s
    /// status is settled (never reset an in-progress xmax).
    ///
    /// A page that had any dead slot OR any abort-cleanup reset is rewritten — the resets
    /// applied FIRST, then [`page::prune_and_compact`] (dead slots → `DEAD`, survivors
    /// compacted, offsets/`free_start`/PageLSN/checksum rewritten) — and logged as a
    /// single **unconditional** `FullPageImage`: a prune+compact relocates survivors and
    /// is not expressible as a delta, so it is never gated on `take_needs_fpi` (mirrors
    /// `btree::log_full_page`); the in-place header resets fold into the same image. A
    /// page with neither is skipped entirely — no WAL record, no mutation. Survivors are
    /// byte-identical at their stable slot ids (`prune_and_compact`'s contract), and the
    /// resets keep the tuple at its slot id and length, so no index entry is touched (the
    /// line pointer stays addressable; `DEAD → UNUSED` reclaim and index vacuum are F3,
    /// not done here).
    ///
    /// **Full-extent scan.** Iterates `0..page_count` of the heap file via
    /// [`BufferPool::page_count`], faulting each page in (resident or from disk), rather
    /// than only the resident pages [`Self::table_page_nums`] reports — an evicted page
    /// holding dead tuples must still be vacuumed, else GC is incomplete.
    ///
    /// **Latching (lock order: structural → frame → WAL).** Per page, takes the
    /// per-heap structural latch then the frame write latch, releasing both before the
    /// next page (never held across pages). VACUUM runs under the exclusive
    /// checkpoint guard, so no writer runs during the pass; using the same latch
    /// order here keeps the page-level primitive consistent with normal heap
    /// mutations.
    ///
    /// **`vacuum_txn` = 0 (the recovery/maintenance convention).** Pages are dirtied
    /// and logged under txn id `0`, the same id recovery uses for non-transactional
    /// page work (`fetch_for_redo`; `apply_drop_table_without_wal`'s "txn 0 means no
    /// rollback tracking"). VACUUM is maintenance, not a user transaction: its
    /// reclamation must never be undone by an abort and must not depend on a user
    /// commit. A `FullPageImage` is unconditional torn-page repair — recovery's redo
    /// arm reinstalls it purely by PageLSN gating (`page_lsn(data) >= lsn` skips it,
    /// else `copy_from_slice` + force the record LSN), independent of the record's
    /// `txn_id` — so a crash mid-VACUUM leaves every pruned page either pre-prune or
    /// exactly the compacted image, never torn.
    pub(crate) fn vacuum_heap(
        &self,
        schema: &TableSchema,
        horizon: u64,
    ) -> Result<(Vec<RowLocation>, usize)> {
        // A table's heap file id is its table id (no high bit; see `heap::index_file_id`).
        let file_id = schema.id;
        let page_count = self.buffer_pool.page_count(file_id)?;
        let latch = self.structural_latch(file_id);

        // `reclaimed` are the DEAD-root TIDs (need F3a + F3b); `freed_count` additionally
        // counts the heap-only chain members freed straight to UNUSED (no index entry),
        // for the VACUUM command tag.
        let mut reclaimed: Vec<RowLocation> = Vec::new();
        let mut freed_count: usize = 0;
        for page_num in 0..page_count {
            if self.buffer_pool.is_page_abandoned(file_id, page_num) {
                continue;
            }
            {
                let guard = self.buffer_pool.read_page(file_id, page_num)?;
                if !page::is_initialized(guard.data()) {
                    continue;
                }
            }

            // Lock order: structural latch → frame write latch → (WAL mutex inside the
            // append). Both are released at the end of each iteration so no latch is
            // held across pages (rule 1: never two structural latches; forward-looking
            // for a concurrent VACUUM).
            let _heap_guard = latch.lock();
            let mut guard = self.buffer_pool.write_page(file_id, page_num, VACUUM_TXN)?;

            // Chain-aware classification (H3): compute, for THIS page, the line-pointer
            // rewrites (root → REDIRECT / DEAD, heap-only member → UNUSED) and the
            // in-place header resets (abort-cleanup). Pure read over the page bytes.
            // `allow_dead_roots = true`: VACUUM may mark a fully-dead chain's root DEAD
            // (it then runs F3a/F3b on the returned TIDs).
            let plan = self.classify_page_for_prune(guard.data(), horizon, true)?;
            if plan.is_empty() {
                continue;
            }

            // Apply the plan to this page (resets → free → redirect → dead → compact)
            // and log the result as a single unconditional FullPageImage under
            // VACUUM_TXN (see `apply_prune_plan`).
            self.apply_prune_plan(&mut guard, &plan, file_id, page_num, VACUUM_TXN)?;

            // Only the DEAD roots carry index entries that F3a must remove and F3b must
            // reclaim DEAD → UNUSED. REDIRECT roots keep a LIVE index entry (F3a skips
            // them) and heap-only members freed to UNUSED never had an entry.
            freed_count += plan.free_to_unused.len();
            for slot in plan.dead_roots {
                reclaimed.push(RowLocation {
                    file_id,
                    page_num,
                    slot_num: slot,
                });
            }
        }

        Ok((reclaimed, freed_count))
    }
    /// Apply a [`PagePrunePlan`] to one already-write-latched heap page and log the
    /// result as a SINGLE unconditional `FullPageImage` under `txn_id`, stamping the
    /// FPI's LSN as the new PageLSN (the `vacuum_heap` / `btree::log_full_page`
    /// pattern). Shared by VACUUM (`vacuum_heap`, `txn_id = VACUUM_TXN`) and the
    /// update-path prune (`try_hot_update`, the writer's own `txn_id`). Order on the
    /// page:
    /// 1. **Header resets** (abort-cleanup) — in place, BEFORE compaction relocates
    ///    survivors: clear `xmax → INVALID`, `t_ctid → INVALID`, the `HOT_UPDATED` /
    ///    settled-`XMAX_*` hint bits (the exact live, never-deleted header shape),
    ///    preserving every other bit (`xmin`/`XMIN_*`/`HEAP_ONLY`). Keeps the tuple at
    ///    its stable slot id and length, so no index entry is touched.
    /// 2. **Free heap-only members** (`free_to_unused`) → `UNUSED` directly (no index
    ///    entry — the key HOT win).
    /// 3. **Redirect collapsed roots** (`redirect_roots`) → `REDIRECT` to the live tail
    ///    (its index entry now resolves via the redirect; the target stays `NORMAL`).
    /// 4. **Mark fully-dead roots** (`dead_roots`) → `DEAD` (F3a strips the entry, F3b
    ///    reclaims the slot). Empty on the update path (`allow_dead_roots = false`).
    /// 5. **Compact** — relocate NORMAL survivors' bytes contiguously, reclaiming the
    ///    bytes freed by every now-non-`NORMAL` slot. Survivors keep their stable slot
    ///    ids (index-referenced slots are NEVER renumbered — only tuple BYTES move).
    ///
    /// A crash mid-apply leaves the page either pre-apply or exactly this image
    /// (PageLSN-gated idempotent redo), never torn.
    ///
    /// **Atomicity (durability defense-in-depth).** Every mutation is applied to a
    /// SCRATCH copy of the page bytes; the finished, checksum-stamped image is written
    /// back into the frame only after EVERY step (resets, frees, redirects, mark-dead,
    /// compact) and the WAL FullPageImage append succeed. On any error the frame is
    /// left byte-identical to its pre-apply image (a stale, valid checksum), so a
    /// malformed plan — e.g. a slot listed twice in `free_to_unused`, or a slot both
    /// freed and redirected — can NEVER leave the page half-mutated with a mismatched
    /// checksum. The Part 1 fix makes such a plan unreachable; this guarantees the page
    /// stays intact even against a future planning bug.
    pub(super) fn apply_prune_plan(
        &self,
        guard: &mut PageWriteGuard,
        plan: &PagePrunePlan,
        file_id: FileId,
        page_num: PageNum,
        txn_id: u64,
    ) -> Result<()> {
        // Build the post-prune image on a scratch copy first; the live frame is touched
        // only after the whole sequence (incl. the WAL append) has succeeded.
        let mut scratch = *guard.data();
        let provisional_lsn = page::page_lsn(&scratch);
        for &slot in &plan.reset_slots {
            let cleared_bits =
                crate::codec::HOT_UPDATED | common::XMAX_ABORTED | common::XMAX_COMMITTED;
            let tuple = page::read_row(&scratch, slot)?
                .ok_or_else(|| storage_internal("abort-cleanup slot is not live"))?;
            let (_xmin, _xmax, _t_ctid, infomask) = crate::codec::decode_mvcc_header(&tuple)?;
            page::set_tuple_header(
                &mut scratch,
                slot,
                common::INVALID_XID,
                crate::codec::INVALID_TID,
                infomask & !cleared_bits,
                provisional_lsn,
            )?;
        }
        if !plan.free_to_unused.is_empty() {
            page::free_slots_to_unused(&mut scratch, &plan.free_to_unused)?;
        }
        for &(root_slot, target_slot) in &plan.redirect_roots {
            page::set_redirect(&mut scratch, root_slot, target_slot)?;
        }
        if !plan.dead_roots.is_empty() {
            page::mark_slots_dead(&mut scratch, &plan.dead_roots)?;
        }
        page::compact(&mut scratch, provisional_lsn)?;
        let fpi_lsn = self.wal.append(WalRecord {
            lsn: 0,
            txn_id,
            kind: fpi_record_kind(&self.compression, file_id, page_num, &scratch),
        })?;
        page::set_page_lsn(&mut scratch, fpi_lsn);
        // All steps succeeded: publish the finished image into the live frame in one
        // shot. The frame was never touched before this point, so an earlier error left
        // it intact.
        *guard.data_mut() = scratch;
        Ok(())
    }
    /// Chain-aware HOT prune plan for ONE heap page (`docs/specs/mvcc.md` §9 / §10
    /// Milestone H3). Reads the page's slots (no mutation) and classifies every HOT
    /// chain rooted on the page, returning the line-pointer rewrites and in-place
    /// header resets `vacuum_heap` then applies under the frame latch.
    ///
    /// **What a chain is here.** Every index entry points at a chain ROOT — a stable
    /// indexed slot that is either `NORMAL` and NOT `HEAP_ONLY` (an independently
    /// indexed version: a non-HOT row, or the HOT-chain head) or a `REDIRECT` (a
    /// previously-collapsed root). A `HEAP_ONLY` `NORMAL` slot is a chain MEMBER with
    /// NO index entry of its own, reached only by walking `t_ctid` from its root
    /// (`HOT_UPDATED → HEAP_ONLY`, same page) — the H1 segment rule. A non-HOT row is
    /// a one-member chain, so the same collapse logic subsumes it.
    ///
    /// **Per chain, in order:**
    /// 1. **Abort truncation.** A HOT update that ABORTED appended a `HEAP_ONLY`
    ///    successor whose creator (`xmin`) aborted; an aborted UPDATE never committed,
    ///    so such a successor is always the chain TAIL. Where a `HOT_UPDATED` member's
    ///    successor has an aborted creator, the update rolled back: reset that member
    ///    in place (un-HOT — drop `xmax`/`t_ctid`/`HOT_UPDATED`) and free the aborted
    ///    successor (and anything past it) directly to `UNUSED`. This is the chain-aware
    ///    form of the F4c abort-cleanup (it leaves NO on-disk reference to the aborted
    ///    txn, as deleter or creator), and it truncates the chain before step 2.
    /// 2. **Committed-dead prefix collapse.** On the truncated chain, find `L` = the
    ///    first member that is NOT `is_dead_to_all(horizon)` — the live tail's head.
    ///    - **No `L` (whole chain dead-to-all):** the root slot → `DEAD` (F3a strips its
    ///      index entry, F3b reclaims it `DEAD → UNUSED`); every `HEAP_ONLY` member →
    ///      `UNUSED` directly.
    ///    - **`L` is a later member (the head died, a live tail survives):** the root
    ///      slot → `REDIRECT` to `L`'s slot (its index entry now resolves via the
    ///      redirect — H1 follows it); every dead `HEAP_ONLY` member strictly before `L`
    ///      → `UNUSED` directly; for a `NORMAL` root, the dead head IS the root slot, so
    ///      it simply becomes the `REDIRECT` (its bytes reclaimed by compaction).
    ///    - **`L` is the head (already live):** nothing to collapse.
    /// 3. **Abort-cleanup of a kept root.** A live chain head/root whose own `xmax` is a
    ///    DEFINITIVELY aborted deleter (a non-HOT aborted UPDATE/DELETE's surviving
    ///    predecessor) is reset in place (F4c), exactly as before H3.
    ///
    /// Deadness is re-derived per member here under the frame latch via
    /// [`common::is_dead_to_all`]; VACUUM holds the exclusive guard, so every `xmin`/
    /// `xmax` status is settled (never reset/redirect against an in-flight txn).
    pub(super) fn classify_page_for_prune(
        &self,
        data: &[u8; buffer::PAGE_SIZE],
        horizon: u64,
        allow_dead_roots: bool,
    ) -> Result<PagePrunePlan> {
        let status = self.txn_status_view();
        let slot_count = page::next_slot(data)?;

        // First pass: which NORMAL slots are chain MEMBERS reached only via a root, NOT
        // independent chain ROOTS? Two ways a slot is a member: (a) it is the same-page
        // `HEAP_ONLY` `t_ctid` target of a `HOT_UPDATED` tuple (the H1 segment rule), or
        // (b) it is the target of a `REDIRECT` line pointer (a previously-collapsed
        // root's live head). Everything else that is NORMAL-non-member or REDIRECT is a
        // chain ROOT. A HEAP_ONLY slot has no index entry, so it is never a root.
        //
        // Marking the REDIRECT target as a member is essential: it is a `NORMAL`
        // (often non-`HEAP_ONLY`) slot reached only through the redirect line pointer,
        // never through a readable `HOT_UPDATED → t_ctid` step. Without this, a
        // re-collapse (more HOT updates grow the chain from the redirect target, then
        // VACUUM runs again) would treat that target as its OWN independent root in the
        // second pass — planning the same physical chain twice (once via the REDIRECT
        // root, once via the target). The duplicated plan frees a slot to `UNUSED` more
        // than once / frees a redirected slot, which `apply_prune_plan` then rejects
        // mid-page (`docs/specs/mvcc.md` §9/§10 H3).
        let mut is_member: HashSet<u16> = HashSet::new();
        for slot in 0..slot_count {
            // (b) A REDIRECT's target is a member (reached via the redirect, not a root).
            if let page::LinePointer::Redirect(target) = page::slot_state(data, slot)? {
                is_member.insert(target);
                continue;
            }
            let Some(bytes) = page::read_row(data, slot)? else {
                continue;
            };
            let (_xmin, _xmax, t_ctid, infomask) = crate::codec::decode_mvcc_header(&bytes)?;
            if infomask & crate::codec::HOT_UPDATED == 0 {
                continue;
            }
            let (succ_page, succ_slot) = t_ctid;
            if succ_page != page::page_id(data) {
                continue;
            }
            // (a) A same-page HEAP_ONLY `t_ctid` successor of a HOT_UPDATED tuple.
            if let page::LinePointer::Normal = page::slot_state(data, succ_slot)? {
                let succ = page::read_row(data, succ_slot)?
                    .ok_or_else(|| storage_internal("HOT successor is not a live tuple"))?;
                let (_x, _xm, _t, succ_infomask) = crate::codec::decode_mvcc_header(&succ)?;
                if succ_infomask & crate::codec::HEAP_ONLY != 0 {
                    is_member.insert(succ_slot);
                }
            }
        }

        let mut plan = PagePrunePlan::default();
        // Second pass: process each ROOT's chain.
        for root_slot in 0..slot_count {
            let state = page::slot_state(data, root_slot)?;
            let head_slot = match state {
                // A NORMAL non-member slot is a chain root (the head tuple lives in the
                // root slot itself). A NORMAL member is reached via its root — skip.
                page::LinePointer::Normal => {
                    if is_member.contains(&root_slot) {
                        continue;
                    }
                    root_slot
                }
                // A REDIRECT root's head tuple lives at the redirect's target slot.
                page::LinePointer::Redirect(target) => match page::slot_state(data, target)? {
                    page::LinePointer::Normal => target,
                    _ => {
                        return Err(storage_internal(
                            "redirect line pointer target is not a NORMAL tuple",
                        ));
                    }
                },
                page::LinePointer::Dead | page::LinePointer::Unused => continue,
            };

            // Walk the chain from `head_slot`, collecting (slot, xmin, xmax, infomask).
            let chain = self.collect_prune_chain(data, head_slot)?;
            self.plan_chain(
                root_slot,
                &chain,
                horizon,
                status,
                allow_dead_roots,
                &mut plan,
            )?;
        }
        Ok(plan)
    }
    /// Walk a HOT chain from `head_slot` (already resolved through any REDIRECT),
    /// returning each member as `(slot, xmin, xmax, infomask)` in chain order. Follows
    /// the H1 segment rule (a same-page `HEAP_ONLY` successor of a `HOT_UPDATED`
    /// tuple); a cyclic `t_ctid` (corruption) is a structured error, not a spin.
    fn collect_prune_chain(
        &self,
        data: &[u8; buffer::PAGE_SIZE],
        head_slot: u16,
    ) -> Result<Vec<ChainMember>> {
        let slot_count = page::next_slot(data)?;
        let mut visited: HashSet<u16> = HashSet::with_capacity(slot_count as usize);
        let mut chain = Vec::new();
        let mut current = head_slot;
        loop {
            if !visited.insert(current) {
                return Err(storage_internal("cyclic HOT chain detected"));
            }
            let bytes = page::read_row(data, current)?
                .ok_or_else(|| storage_internal("HOT chain member is not a live tuple"))?;
            let (xmin, xmax, t_ctid, infomask) = crate::codec::decode_mvcc_header(&bytes)?;
            chain.push(ChainMember {
                slot: current,
                xmin,
                xmax,
                infomask,
            });
            if infomask & crate::codec::HOT_UPDATED == 0 {
                break;
            }
            let (succ_page, succ_slot) = t_ctid;
            if succ_page != page::page_id(data) {
                break;
            }
            match page::slot_state(data, succ_slot)? {
                page::LinePointer::Normal => {}
                _ => break,
            }
            let succ = page::read_row(data, succ_slot)?
                .ok_or_else(|| storage_internal("HOT successor is not a live tuple"))?;
            let (_x, _xm, _t, succ_infomask) = crate::codec::decode_mvcc_header(&succ)?;
            if succ_infomask & crate::codec::HEAP_ONLY == 0 {
                break;
            }
            current = succ_slot;
        }
        Ok(chain)
    }
    /// Plan one chain's collapse into `plan` (see [`Self::classify_page_for_prune`]'s
    /// per-chain rules). `root_slot` is the index-referenced slot; `chain` is the
    /// physical members from the head down. When `allow_dead_roots` is false (the
    /// update path, which cannot run index vacuum), a chain whose collapse would mark
    /// the root `DEAD` is left entirely untouched for VACUUM instead.
    fn plan_chain(
        &self,
        root_slot: u16,
        chain: &[ChainMember],
        horizon: u64,
        status: &dyn TxnStatusView,
        allow_dead_roots: bool,
        plan: &mut PagePrunePlan,
    ) -> Result<()> {
        if chain.is_empty() {
            return Ok(());
        }

        // The update path (`allow_dead_roots = false`) MUST NOT mark a root DEAD: that
        // needs index vacuum (F3a) + line-pointer reclaim (F3b), which run under other
        // structural latches it does not hold. So a chain whose collapse would mark the
        // root DEAD is staged into a scratch plan and applied ONLY if no DEAD root
        // resulted; otherwise the chain is left entirely untouched for VACUUM. (The
        // chain being updated always has a live member, so this only ever skips OTHER
        // fully-dead chains on the same page.)
        let mut staged = std::mem::take(plan);
        let before = (
            staged.reset_slots.len(),
            staged.free_to_unused.len(),
            staged.redirect_roots.len(),
            staged.dead_roots.len(),
        );

        let result = self.plan_chain_inner(root_slot, chain, horizon, status, &mut staged);

        if !allow_dead_roots && (result.is_err() || staged.dead_roots.len() != before.3) {
            // Roll back this chain's staged actions and leave it for VACUUM.
            staged.reset_slots.truncate(before.0);
            staged.free_to_unused.truncate(before.1);
            staged.redirect_roots.truncate(before.2);
            staged.dead_roots.truncate(before.3);
            *plan = staged;
            return Ok(());
        }
        *plan = staged;
        result
    }
    /// The collapse body for one chain (see [`Self::plan_chain`]); always allowed to
    /// schedule a DEAD root. [`Self::plan_chain`] wraps it to honor `allow_dead_roots`.
    fn plan_chain_inner(
        &self,
        root_slot: u16,
        chain: &[ChainMember],
        horizon: u64,
        status: &dyn TxnStatusView,
        plan: &mut PagePrunePlan,
    ) -> Result<()> {
        // Step 1 — abort truncation. Find the first member whose successor's creator
        // aborted (an aborted HOT update's rolled-back successor); reset that member
        // in place (un-HOT) and free the aborted suffix to UNUSED. An aborted UPDATE
        // never committed, so the aborted successor is always the chain TAIL — the
        // truncation removes a suffix and leaves a clean live-or-dead prefix.
        let mut live_len = chain.len();
        for i in 0..chain.len() {
            if chain[i].infomask & crate::codec::HOT_UPDATED == 0 {
                break;
            }
            let Some(succ) = chain.get(i + 1) else { break };
            let succ_aborted =
                succ.infomask & common::XMIN_ABORTED != 0 || status.is_aborted(succ.xmin);
            if succ_aborted {
                // Reset member `i` (the surviving predecessor): un-HOT it.
                plan.reset_slots.push(chain[i].slot);
                // Free the aborted successor and everything past it (all HEAP_ONLY)
                // straight to UNUSED — no index entry, dead-end orphans.
                for member in &chain[i + 1..] {
                    plan.free_to_unused.push(member.slot);
                }
                live_len = i + 1;
                break;
            }
        }
        let chain = &chain[..live_len];

        // Step 2 — committed-dead prefix collapse on the (truncated) chain. After a
        // reset above, member `live_len-1` has xmax INVALID, so it is not dead-to-all
        // and becomes the live tail head `L`.
        let l_index = chain
            .iter()
            .position(|m| !common::is_dead_to_all(m.xmin, m.xmax, m.infomask, horizon, status));

        match l_index {
            None => {
                // Whole (truncated) chain is dead-to-all → reclaim the entire chain.
                // The root slot → DEAD (F3a strips its entry, F3b reclaims it). For a
                // NORMAL root, the head tuple IS the root slot. Every other (HEAP_ONLY)
                // member → UNUSED directly.
                plan.dead_roots.push(root_slot);
                for member in chain {
                    if member.slot != root_slot {
                        plan.free_to_unused.push(member.slot);
                    }
                }
            }
            Some(0) => {
                // The head is already live (`L` == head). Nothing to collapse. A
                // non-HOT aborted-deleter on this kept head is abort-cleaned in step 3.
                let head = &chain[0];
                self.maybe_abort_cleanup_kept(head, status, plan);
            }
            Some(l) => {
                // The head (and the committed-dead prefix before `L`) died, but a live
                // tail survives at `chain[l]`. Re-point the root slot to `L` (REDIRECT),
                // and free every dead member strictly before `L` to UNUSED. For a NORMAL
                // root, the dead head IS the root slot and simply becomes the REDIRECT
                // (its bytes reclaimed by compaction), so it is NOT freed separately.
                plan.redirect_roots.push((root_slot, chain[l].slot));
                for member in &chain[..l] {
                    if member.slot != root_slot {
                        plan.free_to_unused.push(member.slot);
                    }
                }
                // The live tail's head may itself carry a non-HOT aborted-deleter stamp.
                self.maybe_abort_cleanup_kept(&chain[l], status, plan);
            }
        }
        Ok(())
    }
    /// If a KEPT (live) chain member's own `xmax` is a DEFINITIVELY aborted deleter —
    /// the surviving predecessor of a non-HOT aborted UPDATE/DELETE — schedule its
    /// in-place abort-cleanup (F4c). Skips a member already scheduled for a reset by
    /// step 1's abort truncation (its `xmax` would be reset to INVALID). VACUUM holds
    /// the exclusive guard, so `xmax`'s status is settled.
    fn maybe_abort_cleanup_kept(
        &self,
        member: &ChainMember,
        status: &dyn TxnStatusView,
        plan: &mut PagePrunePlan,
    ) {
        if plan.reset_slots.contains(&member.slot) {
            return;
        }
        let deleter_aborted = member.xmax != common::INVALID_XID
            && (member.infomask & common::XMAX_ABORTED != 0 || status.is_aborted(member.xmax));
        if deleter_aborted {
            plan.reset_slots.push(member.slot);
        }
    }
    /// Index VACUUM (`docs/specs/mvcc.md` §9, Milestone F3a): remove every index
    /// entry — across the table's primary-key index and every live secondary index —
    /// whose value (the heap `RowLocation`/TID) is in `dead_tids`. `dead_tids` are the
    /// TIDs `vacuum_heap` pruned to `DEAD`; their index entries still dangle (pointing
    /// at a now-DEAD slot) and must be removed before the line pointers can be
    /// reclaimed `DEAD → UNUSED` (F3b).
    ///
    /// Entries are matched by **dead-TID membership, not by key**: after the heap
    /// prune compacted the page the dead tuple's key bytes are gone, so the key cannot
    /// be recomputed; the index leaf's stored value (the TID) is the only handle left.
    /// Each index is vacuumed in a single leaf-chain walk (`BTree::remove_values_in`),
    /// shifting matching entries out of each leaf under its frame write latch and
    /// logging a `FullPageImage` of every changed leaf — the `vacuum_heap` /
    /// `btree::log_full_page` crash-safety pattern, redone by PageLSN gating regardless
    /// of txn id. The pass runs under the maintenance txn id (`0`, [`VACUUM_TXN`]) so
    /// its removals are never undone by an abort.
    ///
    /// **Latching.** Each index is vacuumed under *its own* per-index structural latch,
    /// acquired and released around that index's whole walk and never held while
    /// another index's latch is taken (rule 1: never two structural latches at once).
    /// The per-leaf write latch a removal takes inside `remove_values_in` is mutually
    /// exclusive with a concurrent lock-free scanner's per-leaf read latch on the same
    /// leaf, and no leaf is merged/freed and no right-sibling link is rewritten, so a
    /// concurrent scanner can neither miss nor duplicate a live entry (B-link safe).
    ///
    /// Called by [`vacuum`](Self::vacuum) as F4a's middle phase (F2b → **F3a** →
    /// F3b). It does **not** reclaim line pointers `DEAD → UNUSED` (F3b); the slots
    /// stay `DEAD` until that later step.
    pub(crate) fn vacuum_indexes(
        &self,
        schema: &TableSchema,
        dead_tids: &HashSet<RowLocation>,
    ) -> Result<()> {
        if dead_tids.is_empty() {
            return Ok(());
        }

        // Primary-key index, under its own structural latch (released before the next).
        let pk_file_id = index_file_id(schema.id);
        {
            let latch = self.structural_latch(pk_file_id);
            let _pk_guard = latch.lock();
            self.btree(pk_file_id)
                .remove_values_in(VACUUM_TXN, dead_tids)?;
        }

        // Every live secondary index, each under its own structural latch (one at a
        // time — rule 1: never two structural latches simultaneously).
        for index in self.table_indexes(schema.id)? {
            let secondary_file_id = secondary_index_file_id(index.id);
            let latch = self.structural_latch(secondary_file_id);
            let _index_guard = latch.lock();
            self.secondary_btree(index.id)
                .remove_values_in(VACUUM_TXN, dead_tids)?;
        }

        Ok(())
    }
    /// Line-pointer reclaim, the third VACUUM phase (`docs/specs/mvcc.md` §9,
    /// Milestone F3b): flip each `dead_tid`'s heap line pointer `DEAD → UNUSED`,
    /// freeing its slot id for reuse by a future `insert_row`. `dead_tids` are the
    /// TIDs `vacuum_heap` (F2b) pruned to `DEAD` and `vacuum_indexes` (F3a) has since
    /// stripped of every index entry; reclaiming them to `UNUSED` is what bounds the
    /// slot array under delete→vacuum→insert churn (a `DEAD` line pointer is dead
    /// weight `insert_row` will not recycle).
    ///
    /// **Ordering invariant — F2b → F3a → F3b.** This MUST run only after
    /// `vacuum_indexes` removed every index entry for these TIDs. The invariant is
    /// the safety hinge for slot reuse: `insert_row` recycles an `UNUSED` slot id,
    /// so an `UNUSED` slot must have *no* dangling index entry, or a stale entry
    /// would resolve to the new tuple written into the reclaimed slot (silent
    /// corruption). [`vacuum`](Self::vacuum) (F4a) enforces the F2b → F3a → F3b order
    /// by calling these three phases in sequence on one set of dead TIDs.
    /// `page::reclaim_line_pointers` debug-asserts each slot is currently `DEAD` (a
    /// `NORMAL`/`UNUSED`/out-of-bounds slot is a hard error), which catches the gross
    /// misordering of reclaiming a never-pruned slot, though it cannot by itself
    /// prove the *index* entries are gone — that is F4a's ordering responsibility.
    ///
    /// **Per page, lock order structural → frame → WAL.** TIDs are grouped by heap
    /// page; each page is reclaimed under the per-heap structural latch then the
    /// frame write latch (released before the next page, never held across pages —
    /// rule 1), and logged as a single unconditional `FullPageImage` under the
    /// maintenance txn id (`0`, [`VACUUM_TXN`]), the same crash-safety pattern as
    /// `vacuum_heap`/`vacuum_indexes`: recovery reinstalls the reclaimed page purely
    /// by PageLSN gating, independent of the record's `txn_id`. A reclaim
    /// (slot → `UNUSED`) followed by a later insert-into-reused-slot (`HeapInsert`)
    /// replay in LSN order to the final state (the new row at that slot), so a crash
    /// mid-reclaim leaves the page either pre-reclaim or exactly the reclaimed image,
    /// never torn.
    ///
    /// Called by [`vacuum`](Self::vacuum) as F4a's final phase (F2b → F3a → **F3b**).
    pub(crate) fn reclaim_line_pointers(
        &self,
        schema: &TableSchema,
        dead_tids: &HashSet<RowLocation>,
    ) -> Result<()> {
        if dead_tids.is_empty() {
            return Ok(());
        }

        // A table's heap file id is its table id (no high bit; see `heap::index_file_id`).
        let file_id = schema.id;
        let latch = self.structural_latch(file_id);

        // Group the dead slots by heap page so each page is rewritten once. A TID
        // from another file (an index TID) is a caller bug — these are heap TIDs that
        // `vacuum_heap` returned for this table's heap file.
        let mut by_page: BTreeMap<PageNum, Vec<u16>> = BTreeMap::new();
        for tid in dead_tids {
            debug_assert_eq!(
                tid.file_id, file_id,
                "reclaim_line_pointers expects heap TIDs for this table's heap file",
            );
            if tid.file_id == file_id {
                by_page.entry(tid.page_num).or_default().push(tid.slot_num);
            }
        }

        for (page_num, slots) in by_page {
            // Lock order: structural latch → frame write latch → (WAL mutex inside the
            // append). Both released at the end of each iteration so no latch is held
            // across pages (rule 1; forward-looking for a concurrent VACUUM).
            let _heap_guard = latch.lock();
            let mut guard = self.buffer_pool.write_page(file_id, page_num, VACUUM_TXN)?;

            // Flip DEAD → UNUSED, then log the reclaimed page as a single unconditional
            // FullPageImage and stamp the FPI's LSN as the new page-LSN (the
            // `vacuum_heap` / `btree::log_full_page` pattern). `reclaim_line_pointers`
            // stamps a provisional LSN; the FPI append overwrites it with the record's
            // LSN so redo gating is exact.
            let provisional_lsn = page::page_lsn(guard.data());
            page::reclaim_line_pointers(guard.data_mut(), &slots, provisional_lsn)?;
            let image = *guard.data();
            let fpi_lsn = self.wal.append(WalRecord {
                lsn: 0,
                txn_id: VACUUM_TXN,
                kind: fpi_record_kind(&self.compression, file_id, page_num, &image),
            })?;
            page::set_page_lsn(guard.data_mut(), fpi_lsn);
        }

        Ok(())
    }
    /// VACUUM one table (`docs/specs/mvcc.md` §9, §10 Milestone F4a): the live
    /// orchestration that ties the three reclamation phases together in their
    /// mandatory order — heap-prune (F2b) → index-vacuum (F3a) → line-pointer
    /// reclaim (F3b) — and returns the number of heap tuples reclaimed (for the
    /// `VACUUM` command tag / observability). `horizon` is the GC horizon
    /// (`ServerComponents::gc_horizon`), the minimum `xmin` advertised by any live
    /// snapshot; a version with `xmax < horizon` is dead to every current and
    /// future snapshot ([`common::is_dead_to_all`]).
    ///
    /// **The order is the safety invariant (F3b's hinge).** `vacuum_heap` returns the
    /// TIDs it pruned to `DEAD`; `vacuum_indexes` must strip every index entry for
    /// those TIDs **before** `reclaim_line_pointers` flips them `DEAD → UNUSED`,
    /// because `insert_row` recycles an `UNUSED` slot — a dangling index entry over a
    /// reclaimed-then-reused slot would resolve to the wrong (new) tuple (silent
    /// corruption). Running the three calls in this fixed sequence on one dead-TID
    /// set is exactly what discharges that precondition. When the heap prune finds
    /// nothing dead, the index and line-pointer phases are skipped (an empty set is a
    /// documented no-op for both, but skipping avoids even the empty-set call).
    ///
    /// **Safety against data loss (the horizon-under-the-guard argument).** The caller
    /// runs this under the EXCLUSIVE checkpoint guard, so NO writer executes during
    /// the pass: no committed-deleter can appear mid-pass, and `horizon` is captured
    /// once (after acquiring the guard) as the min advertised `xmin` over all live
    /// snapshots — INCLUDING lock-free readers, which advertise their `xmin`. So every
    /// version this reclaims has `xmax < horizon`, meaning its delete committed before
    /// any still-live snapshot's `xmin`; no current snapshot can see it live, and any
    /// reader that starts mid-pass freezes `xmin >= horizon` (the deleter is in its
    /// settled past). VACUUM therefore never reclaims a version a snapshot needs.
    pub fn vacuum(&self, schema: &TableSchema, horizon: u64) -> Result<usize> {
        // Phase F2b — heap-prune dead-to-all tuples + collapse HOT chains, collecting
        // the DEAD-root TIDs (the slots whose index entries F3a must strip and F3b must
        // reclaim) and the total count of reclaimed slots (DEAD roots + heap-only
        // members freed straight to UNUSED, which carry no index entry — the HOT win).
        let (dead, freed_in_chains) = self.vacuum_heap(schema, horizon)?;
        let reclaimed = dead.len() + freed_in_chains;
        if !dead.is_empty() {
            let dead: HashSet<RowLocation> = dead.into_iter().collect();
            // Phase F3a — strip every PK + secondary index entry for the DEAD-root TIDs.
            // REDIRECT roots are NOT in this set (their index entry stays live), and
            // heap-only members freed to UNUSED never had an entry, so neither reaches
            // F3a/F3b — exactly the H3 invariant (`docs/specs/mvcc.md` §9/§10 H3).
            self.vacuum_indexes(schema, &dead)?;
            // Phase F3b — reclaim the now entry-free line pointers DEAD → UNUSED.
            // MUST follow F3a (above): see this method's ordering invariant.
            self.reclaim_line_pointers(schema, &dead)?;
        }
        Ok(reclaimed)
    }
}
