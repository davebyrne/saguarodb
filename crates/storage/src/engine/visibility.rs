use super::*;

fn toast_value_for_column(schema: &TableSchema, column: usize, raw: Vec<u8>) -> Result<Value> {
    let column = schema.columns.get(column).ok_or_else(|| {
        crate::toast::toast_corruption("TOAST physical value references a missing column")
    })?;
    match column.data_type {
        DataType::Text => String::from_utf8(raw)
            .map(Value::Text)
            .map_err(|_| crate::toast::toast_corruption("TOAST text value is not valid UTF-8")),
        DataType::Bytea => Ok(Value::Bytes(raw)),
        DataType::Array(ref element_type) => {
            let array = crate::codec::decode_array_payload(&raw)?;
            if array.element_type() != element_type.element_type() {
                return Err(crate::toast::toast_corruption(
                    "TOAST array element type does not match its column",
                ));
            }
            Ok(Value::Array(array))
        }
        _ => Err(crate::toast::toast_corruption(
            "TOAST physical value references a non-varlena column",
        )),
    }
}

fn validate_toast_sample_column(schema: &TableSchema, column: usize) -> Result<DataType> {
    let column = schema.columns.get(column).ok_or_else(|| {
        crate::toast::toast_corruption("TOAST physical value references a missing column")
    })?;
    match column.data_type {
        DataType::Text | DataType::Bytea | DataType::Array(_) => Ok(column.data_type.clone()),
        _ => Err(crate::toast::toast_corruption(
            "TOAST physical value references a non-varlena column",
        )),
    }
}

fn copy_bounded_sample(raw: &[u8], remaining: usize) -> Option<Vec<u8>> {
    if raw.is_empty() || remaining == 0 {
        return None;
    }
    Some(raw[..raw.len().min(remaining)].to_vec())
}

fn raw_len_within_remaining(raw_len: u32, remaining: usize) -> Option<usize> {
    let raw_len = raw_len as usize;
    (raw_len > 0 && raw_len <= remaining).then_some(raw_len)
}

fn external_stream_header_len(codec: u8) -> Result<usize> {
    match codec {
        compress::CODEC_NONE | compress::CODEC_ZSTD => Ok(4),
        compress::CODEC_ZSTD_DICT => Ok(8),
        other => Err(crate::toast::toast_corruption(format!(
            "unknown external TOAST stream codec {other}"
        ))),
    }
}

impl PageBackedStorageEngine {
    /// Read the *current physical* row at `location`, ignoring snapshot visibility
    /// and without materializing TOAST values. Returns `None` when the line pointer is
    /// not a live tuple.
    pub(super) fn read_location_physical(
        &self,
        schema: &TableSchema,
        location: RowLocation,
    ) -> Result<Option<crate::codec::DecodedPhysicalRow>> {
        let readable = self
            .buffer_pool
            .read_page(location.file_id, location.page_num)?;
        let Some(bytes) = page::read_row(readable.data(), location.slot_num)? else {
            return Ok(None);
        };
        decode_physical_row(schema, &bytes).map(Some)
    }

    pub(super) fn materialize_physical_row(
        &self,
        ctx: &StatementContext,
        relations: &PageBackedRelationSnapshot,
        schema: &TableSchema,
        physical: crate::codec::DecodedPhysicalRow,
    ) -> Result<Row> {
        let mut values = Vec::with_capacity(physical.values.len());
        for physical_value in physical.values {
            match physical_value {
                DecodedPhysicalValue::Null => values.push(Value::Null),
                DecodedPhysicalValue::Value(value) => values.push(value),
                DecodedPhysicalValue::Compressed {
                    column,
                    codec,
                    dict_id,
                    raw_len,
                    raw_crc32,
                    payload,
                } => {
                    validate_array_toast_raw_len(schema, column, raw_len)?;
                    let raw = self
                        .materialize_toast_payload(codec, dict_id, raw_len, raw_crc32, &payload)?;
                    values.push(toast_value_for_column(schema, column, raw)?);
                }
                DecodedPhysicalValue::External { column, pointer } => {
                    validate_array_toast_raw_len(schema, column, pointer.raw_len)?;
                    let stream = self.read_toast_stream(ctx, relations, schema, &pointer)?;
                    let (dict_id, raw_crc32, payload) =
                        crate::toast::parse_external_stream(pointer.codec, &stream)?;
                    let raw = self.materialize_toast_payload(
                        pointer.codec,
                        dict_id,
                        pointer.raw_len,
                        raw_crc32,
                        payload,
                    )?;
                    values.push(toast_value_for_column(schema, column, raw)?);
                }
            }
        }
        Ok(Row { values })
    }

    pub(super) fn sample_toast_physical_value(
        &self,
        ctx: &StatementContext,
        relations: &PageBackedRelationSnapshot,
        schema: &TableSchema,
        column: usize,
        physical_value: &crate::codec::DecodedPhysicalValue,
        remaining: usize,
    ) -> Result<Option<Vec<u8>>> {
        match physical_value {
            DecodedPhysicalValue::Null => Ok(None),
            DecodedPhysicalValue::Value(Value::Text(text)) => {
                Ok(copy_bounded_sample(text.as_bytes(), remaining))
            }
            DecodedPhysicalValue::Value(Value::Bytes(bytes)) => {
                Ok(copy_bounded_sample(bytes, remaining))
            }
            DecodedPhysicalValue::Value(Value::Array(array)) => {
                let raw = crate::codec::encode_array_payload(array)?;
                Ok(copy_bounded_sample(&raw, remaining))
            }
            DecodedPhysicalValue::Value(_) => Ok(None),
            DecodedPhysicalValue::Compressed {
                column: physical_column,
                codec,
                dict_id,
                raw_len,
                raw_crc32,
                payload,
            } => {
                if *physical_column != column {
                    return Err(crate::toast::toast_corruption(
                        "TOAST physical value column does not match decoded slot",
                    ));
                }
                let data_type = validate_toast_sample_column(schema, column)?;
                let declared_raw_len = *raw_len;
                validate_array_toast_raw_len(schema, column, declared_raw_len)?;
                let Some(sample_len) = raw_len_within_remaining(declared_raw_len, remaining) else {
                    return Ok(None);
                };
                let raw = self.materialize_toast_payload(
                    *codec,
                    *dict_id,
                    declared_raw_len,
                    *raw_crc32,
                    payload,
                )?;
                if raw.len() != sample_len {
                    return Err(crate::toast::toast_corruption(
                        "materialized TOAST sample length does not match metadata",
                    ));
                }
                validate_toast_sample_raw(&data_type, &raw)?;
                Ok(Some(raw))
            }
            DecodedPhysicalValue::External {
                column: physical_column,
                pointer,
            } => {
                if *physical_column != column {
                    return Err(crate::toast::toast_corruption(
                        "TOAST physical value column does not match decoded slot",
                    ));
                }
                let data_type = validate_toast_sample_column(schema, column)?;
                validate_array_toast_raw_len(schema, column, pointer.raw_len)?;
                let Some(sample_len) = raw_len_within_remaining(pointer.raw_len, remaining) else {
                    return Ok(None);
                };
                let header_len = external_stream_header_len(pointer.codec)?;
                if (pointer.stored_len as usize) > remaining.saturating_add(header_len) {
                    return Ok(None);
                }
                let stream = self.read_toast_stream(ctx, relations, schema, pointer)?;
                let (dict_id, raw_crc32, payload) =
                    crate::toast::parse_external_stream(pointer.codec, &stream)?;
                let raw = self.materialize_toast_payload(
                    pointer.codec,
                    dict_id,
                    pointer.raw_len,
                    raw_crc32,
                    payload,
                )?;
                if raw.len() != sample_len {
                    return Err(crate::toast::toast_corruption(
                        "materialized TOAST sample length does not match metadata",
                    ));
                }
                validate_toast_sample_raw(&data_type, &raw)?;
                Ok(Some(raw))
            }
        }
    }

    fn materialize_toast_payload(
        &self,
        codec: u8,
        dict_id: Option<u32>,
        raw_len: u32,
        raw_crc32: u32,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        let raw = self
            .compression
            .decompress_value(codec, dict_id, payload, raw_len as usize)?;
        if crc32fast::hash(&raw) != raw_crc32 {
            return Err(crate::toast::toast_corruption(
                "TOAST value raw CRC32 does not match stored metadata",
            ));
        }
        Ok(raw)
    }

    /// Resolve an index entry's TID — possibly a HOT root — to the heap slot of the
    /// single version **visible** to `snapshot` from `current_txns`, reading the
    /// `location` page once under a read latch (pure: no page mutation; pruning is
    /// the UPDATE/VACUUM path's job, `mvcc.md` §10 Milestone H). The two-step
    /// resolution (`mvcc.md` §5.2, §10 Milestone H1) is:
    ///
    /// 1. **REDIRECT resolution.** If `location.slot_num` is a `REDIRECT` line
    ///    pointer (a HOT root whose original tuple was pruned), follow it to its
    ///    same-page target. The target MUST be `NORMAL`: a redirect-to-redirect or
    ///    redirect-to-dead is corruption and returns a structured error rather than
    ///    looping. A `DEAD`/`UNUSED` root slot resolves to no version (`Ok(None)`).
    /// 2. **Bounded HOT-chain walk.** From the resolved root tuple, walk the forward
    ///    `t_ctid` chain, returning the first version [`is_visible`] accepts. THE
    ///    correctness invariant: the walk follows `t_ctid` into a successor **only
    ///    when the current tuple is `HOT_UPDATED` and the successor is `HEAP_ONLY`**
    ///    on the same page — i.e. it stays strictly within one HOT-chain segment. It
    ///    STOPS at any successor that is independently indexed (not `HEAP_ONLY`),
    ///    because that successor is reachable via its OWN index entry; following it
    ///    here would let one visible row be returned through two index entries
    ///    (double-count). Termination is guaranteed by a visited-slot set (so a
    ///    cyclic `t_ctid` from corruption errors instead of spinning).
    ///
    /// Returns the visible version's `(RowLocation, infomask)`; `None` when no
    /// version in the chain is visible (deleted/aborted/never-present) or the root
    /// slot is reclaimed. With no HOT tuples in the heap yet (H2/H3 unimplemented),
    /// every root is `NORMAL` with `t_ctid = INVALID_TID`, so this resolves the root
    /// slot itself and the walk is a single step — behavior-identical to the prior
    /// single-tuple visibility check.
    fn resolve_visible_in_chain(
        &self,
        location: RowLocation,
        snapshot: &Snapshot,
        current_txns: &[u64],
    ) -> Result<Option<(RowLocation, u16)>> {
        let readable = self
            .buffer_pool
            .read_page(location.file_id, location.page_num)?;
        let data = readable.data();
        let page_num = location.page_num;
        let file_id = location.file_id;

        // Step 1: resolve a REDIRECT root to its same-page NORMAL target.
        let mut current_slot = match page::slot_state(data, location.slot_num)? {
            page::LinePointer::Normal => location.slot_num,
            page::LinePointer::Redirect(target) => {
                // A REDIRECT always points within the same page at a NORMAL slot.
                match page::slot_state(data, target)? {
                    page::LinePointer::Normal => target,
                    _ => {
                        return Err(storage_internal(
                            "redirect line pointer target is not a NORMAL tuple",
                        ));
                    }
                }
            }
            // A reclaimed (DEAD/UNUSED) root slot resolves to no version.
            page::LinePointer::Dead | page::LinePointer::Unused => return Ok(None),
        };

        // Step 2: bounded HOT-chain walk from the resolved root. Termination is
        // guaranteed by `visited` (a cyclic `t_ctid` becomes a structured error, not
        // a spin); the slot count is only a capacity hint for that set.
        let slot_count = page::next_slot(data)?;
        let mut visited: HashSet<u16> = HashSet::with_capacity(slot_count as usize);
        loop {
            if !visited.insert(current_slot) {
                return Err(storage_internal("cyclic HOT chain detected"));
            }

            // The resolved root is NORMAL (step 1) and every followed successor is
            // validated NORMAL before we step onto it, so a missing tuple here is a
            // corrupt chain, not a skippable reclaimed slot.
            let Some(bytes) = page::read_row(data, current_slot)? else {
                return Err(storage_internal("HOT chain member is not a live tuple"));
            };
            let (xmin, xmax, t_ctid, infomask) = crate::codec::decode_mvcc_header(&bytes)?;
            if is_visible(
                xmin,
                xmax,
                infomask,
                snapshot,
                current_txns,
                self.txn_status_view(),
            ) {
                return Ok(Some((
                    RowLocation {
                        file_id,
                        page_num,
                        slot_num: current_slot,
                    },
                    infomask,
                )));
            }

            // Decide whether to follow `t_ctid` into a heap-only successor. Stop
            // unless: this tuple was HOT-updated, its successor is on THIS page, and
            // that successor is HEAP_ONLY (so it has no index entry of its own and is
            // reachable only here). Any other case — latest version, a non-HOT
            // successor, or an off-page successor — is independently indexed/absent,
            // so we must not cross into it (double-count guard).
            if infomask & crate::codec::HOT_UPDATED == 0 {
                return Ok(None);
            }
            let (succ_page, succ_slot) = t_ctid;
            if succ_page != page_num {
                return Ok(None);
            }
            // Peek the successor's header: only a HEAP_ONLY, NORMAL successor is part
            // of this HOT-chain segment. A non-HEAP_ONLY successor is independently
            // indexed (stop); a non-NORMAL successor under a HOT_UPDATED pointer is
            // corruption.
            match page::slot_state(data, succ_slot)? {
                page::LinePointer::Normal => {}
                _ => {
                    return Err(storage_internal(
                        "HOT_UPDATED successor slot is not a NORMAL tuple",
                    ));
                }
            }
            let Some(succ_bytes) = page::read_row(data, succ_slot)? else {
                return Err(storage_internal(
                    "HOT_UPDATED successor is not a live tuple",
                ));
            };
            let (_xmin, _xmax, _t_ctid, succ_infomask) =
                crate::codec::decode_mvcc_header(&succ_bytes)?;
            if succ_infomask & crate::codec::HEAP_ONLY == 0 {
                // The successor is independently indexed — it is reached via its own
                // index entry, so stop here (do not double-count it).
                return Ok(None);
            }
            current_slot = succ_slot;
        }
    }
    /// Collect the physically-present versions of the HOT chain rooted at `root`, in
    /// chain order: the resolved root tuple plus every heap-only successor reached by
    /// the bounded `t_ctid` walk (the same `HOT_UPDATED → HEAP_ONLY`, same-page,
    /// stop-at-independently-indexed rule as [`Self::resolve_visible_in_chain`], but
    /// gathering ALL members instead of returning the first visible one). Each element
    /// is `(RowLocation, DecodedPhysicalRow)` for a `NORMAL` member.
    ///
    /// Used by `create_index`'s HOT broken-chain check (`docs/specs/mvcc.md` §10
    /// Milestone H2) — a non-HOT root resolves to a one-element vec (so a plain
    /// single-version table is untouched); a HOT chain yields its root + heap-only
    /// members so the build can test whether two not-dead-to-all versions disagree on
    /// the new index's key — and by [`Self::unique_conflict_kind`] to examine every
    /// physically-present version sharing an index key. The walk is a pure read whose
    /// physical view is stable because it holds the page read latch for its duration
    /// (`create_index` additionally runs under the exclusive guard). A `DEAD`/`UNUSED`
    /// root resolves to no versions (`Ok(vec![])`); a corrupt chain (cycle, bad
    /// redirect, non-NORMAL HOT successor) is a structured error, never a spin.
    pub(super) fn collect_chain_versions(
        &self,
        schema: &TableSchema,
        root: RowLocation,
    ) -> Result<Vec<(RowLocation, crate::codec::DecodedPhysicalRow)>> {
        let readable = self.buffer_pool.read_page(root.file_id, root.page_num)?;
        let data = readable.data();
        let page_num = root.page_num;
        let file_id = root.file_id;

        // Step 1: resolve a REDIRECT root to its same-page NORMAL target (mirrors
        // `resolve_visible_in_chain`).
        let mut current_slot = match page::slot_state(data, root.slot_num)? {
            page::LinePointer::Normal => root.slot_num,
            page::LinePointer::Redirect(target) => match page::slot_state(data, target)? {
                page::LinePointer::Normal => target,
                _ => {
                    return Err(storage_internal(
                        "redirect line pointer target is not a NORMAL tuple",
                    ));
                }
            },
            page::LinePointer::Dead | page::LinePointer::Unused => return Ok(Vec::new()),
        };

        let slot_count = page::next_slot(data)?;
        let mut visited: HashSet<u16> = HashSet::with_capacity(slot_count as usize);
        let mut versions = Vec::new();
        loop {
            if !visited.insert(current_slot) {
                return Err(storage_internal("cyclic HOT chain detected"));
            }
            let Some(bytes) = page::read_row(data, current_slot)? else {
                return Err(storage_internal("HOT chain member is not a live tuple"));
            };
            let decoded = decode_physical_row(schema, &bytes)?;
            let infomask = decoded.header.infomask;
            let t_ctid = decoded.header.t_ctid;
            versions.push((
                RowLocation {
                    file_id,
                    page_num,
                    slot_num: current_slot,
                },
                decoded,
            ));

            // Follow only a same-page HEAP_ONLY successor of a HOT_UPDATED tuple — the
            // bounded HOT-chain segment.
            if infomask & crate::codec::HOT_UPDATED == 0 {
                return Ok(versions);
            }
            let (succ_page, succ_slot) = t_ctid;
            if succ_page != page_num {
                return Ok(versions);
            }
            match page::slot_state(data, succ_slot)? {
                page::LinePointer::Normal => {}
                _ => {
                    return Err(storage_internal(
                        "HOT_UPDATED successor slot is not a NORMAL tuple",
                    ));
                }
            }
            let Some(succ_bytes) = page::read_row(data, succ_slot)? else {
                return Err(storage_internal(
                    "HOT_UPDATED successor is not a live tuple",
                ));
            };
            let (_xmin, _xmax, _t_ctid, succ_infomask) =
                crate::codec::decode_mvcc_header(&succ_bytes)?;
            if succ_infomask & crate::codec::HEAP_ONLY == 0 {
                // Independently indexed successor: stop (it is its own root).
                return Ok(versions);
            }
            current_slot = succ_slot;
        }
    }
    /// Resolve a (possibly HOT) index entry to its visible heap version and read it,
    /// returning the **resolved heap location** alongside the row so callers stamp
    /// the right `RowId` (the live chain member, not the pruned root). Routes through
    /// [`Self::resolve_visible_in_chain`] (REDIRECT + bounded `t_ctid` walk +
    /// [`is_visible`], `docs/specs/mvcc.md` §6, §10 Milestone H1): an invisible chain
    /// (or a reclaimed root slot) yields `None` and is skipped by the caller — never
    /// an error. Under the degenerate autocommit snapshot every committed row and own
    /// write is visible, so this filters nothing; with no HOT tuples in the heap yet
    /// (H2/H3 unimplemented), the resolution is the prior single-tuple check at the
    /// index TID itself.
    pub(super) fn read_visible_row(
        &self,
        ctx: &StatementContext,
        relations: &PageBackedRelationSnapshot,
        schema: &TableSchema,
        location: RowLocation,
    ) -> Result<Option<(RowLocation, Row)>> {
        let Some((resolved, _infomask)) =
            self.resolve_visible_in_chain(location, &ctx.snapshot, &ctx.live_txns)?
        else {
            return Ok(None);
        };
        // The resolved slot is the NORMAL, visible chain member; read its bytes.
        let readable = self
            .buffer_pool
            .read_page(resolved.file_id, resolved.page_num)?;
        let Some(bytes) = page::read_row(readable.data(), resolved.slot_num)? else {
            return Ok(None);
        };
        let physical = decode_physical_row(schema, &bytes)?;
        drop(readable);
        let row = self.materialize_physical_row(ctx, relations, schema, physical)?;
        Ok(Some((resolved, row)))
    }
    /// Locate the single version of `key` visible to `snapshot` from `current_txns`
    /// and return its heap location together with the version's current `infomask`
    /// (`docs/specs/mvcc.md` §6). The primary-key index may carry an entry per
    /// version (B4); each candidate TID is decoded at its *physical* header and the
    /// visibility predicate ([`is_visible`]) settles which one this snapshot sees.
    /// Under snapshot isolation at most one version of a key is visible, so the
    /// first visible candidate is the row the executor matched. Returns `None` when
    /// no version is visible (already deleted, aborted, or never present) — the
    /// caller treats that as "no row" (a no-op delete). A DEAD/UNUSED line pointer
    /// (`read_row` ⇒ `None`) is a reclaimed slot and is skipped.
    pub(super) fn locate_visible_version(
        &self,
        index_btree: &BTree<'_, RowLocation>,
        key: &Key,
        snapshot: &Snapshot,
        current_txns: &[u64],
    ) -> Result<Option<(RowLocation, u16)>> {
        for location in index_btree.scan_key(key)? {
            // Each index entry's TID is a (possibly HOT) root: resolve REDIRECT +
            // the bounded `t_ctid` chain to the version this snapshot sees. Returns
            // the heap location of the visible chain member (which UPDATE/DELETE then
            // stamp), not the index TID — so a HOT-updated row is stamped at the live
            // heap-only version, not its pruned root.
            if let Some(resolved) =
                self.resolve_visible_in_chain(location, snapshot, current_txns)?
            {
                return Ok(Some(resolved));
            }
        }
        Ok(None)
    }
    pub(super) fn table_page_nums(&self, file_id: FileId) -> Result<Vec<PageNum>> {
        let mut pages: Vec<_> = self
            .buffer_pool
            .iter_pages()?
            .filter(|info| info.file_id == file_id && page::is_initialized(&info.data.0))
            .map(|info| info.page_num)
            .collect();
        pages.sort_unstable();
        Ok(pages)
    }
}

fn validate_toast_sample_raw(data_type: &DataType, raw: &[u8]) -> Result<()> {
    match data_type {
        DataType::Text => std::str::from_utf8(raw)
            .map(|_| ())
            .map_err(|_| crate::toast::toast_corruption("TOAST text value is not valid UTF-8")),
        DataType::Bytea => Ok(()),
        DataType::Array(element_type) => {
            let array = crate::codec::decode_array_payload(raw)?;
            if array.element_type() != element_type.element_type() {
                return Err(crate::toast::toast_corruption(
                    "TOAST sample array element type does not match its column",
                ));
            }
            Ok(())
        }
        _ => Err(crate::toast::toast_corruption(
            "TOAST sample references a non-varlena column",
        )),
    }
}

fn validate_array_toast_raw_len(schema: &TableSchema, column: usize, raw_len: u32) -> Result<()> {
    let column = schema.columns.get(column).ok_or_else(|| {
        crate::toast::toast_corruption("TOAST physical value references a missing column")
    })?;
    if matches!(column.data_type, DataType::Array(_)) {
        crate::codec::validate_decoded_array_payload_len(raw_len as usize)?;
    }
    Ok(())
}
