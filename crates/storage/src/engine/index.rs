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
    /// in-progress inserter raises [`SqlState::SerializationFailure`] (`40001`,
    /// retry — §7.3). A NULL indexed value never participates in a unique constraint
    /// (SQL treats NULLs as distinct), so the check is skipped when `has_null`;
    /// distinct NULL rows coexist because their heap TIDs differ.
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
        let _index_guard = latch.lock();
        if index.unique && !has_null {
            match self.unique_conflict_kind(&secondary, entry_key, table_schema, ctx.txn_id)? {
                UniqueConflict::Violation => return Err(duplicate_unique_index(&index.name)),
                UniqueConflict::InFlight => return Err(unique_conflict_retry()),
                UniqueConflict::None => {}
            }
        }
        secondary.insert(ctx.txn_id, entry_key, location)
    }
    /// Whether any existing version indexed under `key` in `index_btree` **conflicts**
    /// with a unique-constraint insert by `current_txn` — the shared,
    /// visibility-aware uniqueness check for the primary-key index and unique
    /// secondary indexes (`docs/specs/mvcc.md` §6/§7.3). It replaces the temporary
    /// presence-probes (B2 commits 3–4): "any entry for the key" became "the
    /// strongest [`UniqueConflict`] across the *alive-or-potentially-alive* versions
    /// for the key".
    ///
    /// This is a **liveness ("dirty") check, not a snapshot read**: it consults the
    /// CLOG (`TxnStatusView`) + the tuple's `infomask` hint bits — never a
    /// [`Snapshot`] — so it sees concurrently in-flight and already-committed state,
    /// not just what `current_txn`'s snapshot would observe. Each candidate TID from
    /// `scan_key` is read at the *physical* tuple header (NOT via
    /// [`Self::read_visible_row`], which would wrongly hide non-visible-but-alive
    /// versions); a DEAD/UNUSED line pointer (`read_row` ⇒ `None`) is a reclaimed
    /// slot and contributes no conflict. The per-candidate decision is
    /// [`common::classify_unique_conflict`]: a creator-aborted or committed-deleted
    /// (incl. deleted-by-me) version is [`UniqueConflict::None`] and ignored; a
    /// committed/own/frozen-live version is a definite [`UniqueConflict::Violation`]
    /// (`23505`); a version created by another still-running txn is
    /// [`UniqueConflict::InFlight`] (`40001`, "retry").
    ///
    /// **Precedence `Violation > InFlight > None`** (returns the strongest across
    /// candidates): a single committed-live duplicate is a definite `23505` even if
    /// another candidate is only in-flight; only when no candidate is a definite
    /// duplicate but at least one is in-flight do we return `InFlight`.
    ///
    /// While writers are serialized (Stage 1) no concurrent uncommitted inserter
    /// exists, so this never returns `InFlight` at runtime and every index entry is a
    /// committed, non-deleted tuple — it returns `Violation` exactly when the old
    /// presence-probe / boolean check did, so existing uniqueness behavior is
    /// unchanged. The `InFlight` arm becomes load-bearing once writers run
    /// concurrently (Milestone E2b).
    pub(super) fn unique_conflict_kind(
        &self,
        index_btree: &BTree<'_, RowLocation>,
        key: &Key,
        schema: &TableSchema,
        current_txn: u64,
    ) -> Result<UniqueConflict> {
        let status = self.txn_status_view();
        let mut strongest = UniqueConflict::None;
        for location in index_btree.scan_key(key)? {
            let readable = self
                .buffer_pool
                .read_page(location.file_id, location.page_num)?;
            let Some(bytes) = page::read_row(readable.data(), location.slot_num)? else {
                // DEAD/UNUSED line pointer: the slot was reclaimed; no conflict.
                continue;
            };
            let decoded = decode_row(schema, &bytes)?;
            match classify_unique_conflict(
                decoded.xmin,
                decoded.xmax,
                decoded.infomask,
                current_txn,
                status,
            ) {
                // A committed-live duplicate is definitive; nothing outranks it.
                UniqueConflict::Violation => return Ok(UniqueConflict::Violation),
                // An in-flight candidate is the strongest seen so far, but a later
                // candidate could still be a definite Violation, so keep scanning.
                UniqueConflict::InFlight => strongest = UniqueConflict::InFlight,
                UniqueConflict::None => {}
            }
        }
        Ok(strongest)
    }
}
