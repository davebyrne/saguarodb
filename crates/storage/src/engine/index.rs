use super::*;

impl PageBackedStorageEngine {
    /// Insert `(entry_key, location)` into a secondary index, enforcing uniqueness
    /// for a unique index. The secondary key is the indexed column(s) alone (no pk
    /// tiebreaker); duplicate indexed values are disambiguated by the heap TID in
    /// `(key, tid)` order. A unique index rejects a duplicate non-NULL indexed value
    /// via the shared visibility-aware [`Self::unique_conflict_kind`] check (it
    /// conflicts only with an alive-or-potentially-alive version; dead/aborted
    /// versions are ignored). A committed-live duplicate raises
    /// [`SqlState::UniqueViolation`] (`23505`); a value held only by another
    /// in-progress inserter makes the caller drop the structural latch, wait on
    /// that transaction, and re-check. A NULL indexed value never participates in a
    /// unique constraint (SQL treats NULLs as distinct), so the check is skipped
    /// when `has_null`; distinct NULL rows coexist because their heap TIDs differ.
    pub(super) fn insert_secondary_entry(
        &self,
        ctx: &StatementContext,
        table_schema: &TableSchema,
        index: &IndexSchema,
        entry_key: &Key,
        has_null: bool,
        location: &RowLocation,
    ) -> Result<()> {
        let secondary = self.secondary_btree(index.id);
        // Hold this secondary index's structural latch across the uniqueness check
        // AND the insert atomically (Milestone E2a). For a unique secondary the scan
        // (`unique_conflict_kind`) and the mutation (`insert`, including any split /
        // root split) must be under ONE latch hold, or two concurrent inserts of the
        // same value could both pass the check and both insert a duplicate. For a
        // non-unique secondary there is no check, but the latch still serializes the
        // split protocol against another structural writer on this same index. The
        // latch is released on return, before the caller takes any other structural
        // latch (rule 1: never two structural latches at once). Contended under E2b's
        // concurrent writers: same-secondary writers serialize here.
        let latch = self.structural_latch(secondary_index_file_id(index.id));
        loop {
            let guard = latch.lock();
            if index.unique && !has_null {
                match self.unique_conflict_kind(
                    &secondary,
                    entry_key,
                    table_schema,
                    &ctx.live_txns,
                )? {
                    UniqueConflict::Violation => return Err(duplicate_unique_index(&index.name)),
                    // Drop the structural latch before blocking on the in-progress
                    // holder (it may itself be waiting on this latch), then re-check.
                    UniqueConflict::WouldBlock(blocker) => {
                        drop(guard);
                        self.wait_for_conflict(ctx, blocker)?;
                        continue;
                    }
                    UniqueConflict::None => {}
                }
            }
            // Check (if any) and the insert run under the same latch hold.
            return secondary.insert(ctx.txn_id, entry_key, location);
        }
    }
    /// Whether any existing version indexed under `key` in `index_btree` **conflicts**
    /// with a unique-constraint insert by `current_txns` — the shared,
    /// visibility-aware uniqueness check for the primary-key index and unique
    /// secondary indexes (`docs/specs/mvcc.md` §6/§7.3). It replaces the temporary
    /// presence-probes (B2 commits 3–4): "any entry for the key" became "the
    /// strongest [`UniqueConflict`] across the *alive-or-potentially-alive* versions
    /// for the key".
    ///
    /// This is a **liveness ("dirty") check, not a snapshot read**: it consults the
    /// CLOG (`TxnStatusView`) + the tuple's `infomask` hint bits — never a
    /// [`Snapshot`] — so it sees concurrently in-flight and already-committed state,
    /// not just what `current_txns`'s snapshot would observe. Each candidate TID from
    /// `scan_key` is a (possibly HOT) root: its `REDIRECT` + bounded HOT chain is
    /// resolved ([`Self::collect_chain_versions`]) and EVERY physically-present member
    /// is classified at its *physical* tuple header (NOT via [`Self::read_visible_row`],
    /// which would wrongly hide non-visible-but-alive versions). Resolving the chain is
    /// essential after a HOT update + VACUUM collapses the root to a `REDIRECT`: the
    /// live version is then a heap-only successor, and reading the redirect root
    /// directly yields no bytes — so without this resolution a duplicate of the
    /// unchanged key would bypass the constraint. A reclaimed (`DEAD`/`UNUSED`) root
    /// contributes no members and no conflict. The per-candidate decision is
    /// [`common::classify_unique_conflict`]: a creator-aborted or committed-deleted
    /// (incl. deleted-by-me) version is [`UniqueConflict::None`] and ignored; a
    /// committed/own/frozen-live version is a definite [`UniqueConflict::Violation`]
    /// (`23505`); a version created by another still-running txn is
    /// [`UniqueConflict::WouldBlock`] (block on that creator, then re-check —
    /// `docs/specs/deadlock.md`).
    ///
    /// **Precedence `Violation > WouldBlock > None`** (returns the strongest across
    /// candidates): a single committed-live duplicate is a definite `23505` even if
    /// another candidate is only in-flight; only when no candidate is a definite
    /// duplicate but at least one is in-flight do we return `WouldBlock`.
    ///
    /// Under the current shared-writer model, `WouldBlock` is a normal concurrent
    /// uniqueness outcome: callers must not hold an index structural latch while
    /// waiting, because the blocker may need that latch to finish.
    pub(super) fn unique_conflict_kind(
        &self,
        index_btree: &BTree<'_, RowLocation>,
        key: &Key,
        schema: &TableSchema,
        current_txns: &[u64],
    ) -> Result<UniqueConflict> {
        let status = self.txn_status_view();
        let mut strongest = UniqueConflict::None;
        for location in index_btree.scan_key(key)? {
            // Each index TID is a (possibly HOT) root. Resolve a `REDIRECT` (a
            // VACUUM-collapsed root whose original tuple was pruned) and walk the bounded
            // HOT chain via [`Self::collect_chain_versions`], so we examine EVERY
            // physically-present version that shares this key — not just the bytes at the
            // root slot. This is load-bearing after a HOT update + VACUUM: the live
            // version is a heap-only successor reached through the redirect, and reading
            // the redirect root directly yields no bytes (it would be wrongly treated as a
            // reclaimed slot, so a duplicate of the unchanged key would slip past the
            // unique constraint). A reclaimed (`DEAD`/`UNUSED`) root yields no members and
            // contributes no conflict. Each member is still classified at its *physical*
            // header (NOT snapshot visibility): a unique conflict may be with an in-flight
            // or committed-but-not-yet-visible version.
            for (_member_loc, decoded) in self.collect_chain_versions(schema, location)? {
                match classify_unique_conflict(
                    decoded.header.xmin,
                    decoded.header.xmax,
                    decoded.header.infomask,
                    current_txns,
                    status,
                ) {
                    // A committed-live duplicate is definitive; nothing outranks it.
                    UniqueConflict::Violation => return Ok(UniqueConflict::Violation),
                    // An in-flight candidate is the strongest seen so far, but a later
                    // candidate could still be a definite Violation, so keep scanning.
                    UniqueConflict::WouldBlock(b) => strongest = UniqueConflict::WouldBlock(b),
                    UniqueConflict::None => {}
                }
            }
        }
        Ok(strongest)
    }
}
