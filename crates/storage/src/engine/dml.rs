use super::*;

/// The outcome of a [`PageBackedStorageEngine::stamp_xmax_logged`] attempt: the
/// version was stamped, or its `xmax` row lock is held by an in-progress writer
/// (`WouldBlock(holder)`) that the caller must block on before retrying the stamp
/// (`docs/specs/deadlock.md`). A committed-superseded conflict is an `Err(40001)`
/// instead, not a variant here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StampOutcome {
    Stamped,
    WouldBlock(u64),
}

enum ToastStreamPayload {
    RawColumn,
    Owned(std::sync::Arc<[u8]>),
}

struct ToastCandidate {
    column: usize,
    raw_len: u32,
    raw_crc32: u32,
    stream_codec: u8,
    stream_dict_id: Option<u32>,
    stream_payload: ToastStreamPayload,
    current_stored_len: usize,
}

struct CompressedToastValue {
    codec: u8,
    dict_id: Option<u32>,
    payload: std::sync::Arc<[u8]>,
}

enum ToastColumnPlan {
    Null,
    Value,
    Varlena(ToastVarlenaPlan),
}

enum ToastVarlenaPlan {
    Plain,
    Compressed {
        codec: u8,
        dict_id: Option<u32>,
        raw_len: u32,
        raw_crc32: u32,
        payload: std::sync::Arc<[u8]>,
    },
    External(ToastPointer),
}

fn plain_prepared_values(row: &Row) -> Vec<crate::codec::PreparedColumnValue> {
    row.values
        .iter()
        .cloned()
        .map(|value| match value {
            Value::Null => crate::codec::PreparedColumnValue::Null,
            other => crate::codec::PreparedColumnValue::Value(other),
        })
        .collect()
}

fn validate_logical_index_keys_fit(
    storage: &PageBackedStorageEngine,
    schema: &TableSchema,
    row: &Row,
) -> Result<()> {
    crate::btree::validate_index_key_fits(&key_for_row(schema, row)?)?;
    for index in storage.table_indexes(schema.id)? {
        let (key, _has_null) = secondary_index_key(schema, &index, row)?;
        crate::btree::validate_index_key_fits(&key)?;
    }
    Ok(())
}

fn toastable_raw_bytes<'a>(data_type: &DataType, value: &'a Value) -> Option<&'a [u8]> {
    match (data_type, value) {
        (DataType::Text, Value::Text(text)) => Some(text.as_bytes()),
        (DataType::Bytea, Value::Bytes(bytes)) => Some(bytes),
        _ => None,
    }
}

fn supported_varlena_len(len: usize) -> Result<u32> {
    let len = u32::try_from(len).map_err(|_| {
        DbError::storage(
            SqlState::ProgramLimitExceeded,
            "varlena value exceeds the supported length",
        )
    })?;
    if len > crate::codec::VARLENA_LEN_MASK {
        return Err(DbError::storage(
            SqlState::ProgramLimitExceeded,
            "varlena value exceeds the supported length",
        ));
    }
    Ok(len)
}

fn inline_compressed_stored_len(payload_len: usize) -> usize {
    1 + 4 + 4 + 4 + payload_len
}

fn planned_row_meets_toast_goal(
    schema: &TableSchema,
    row: &Row,
    plans: &[ToastColumnPlan],
) -> Result<bool> {
    let len = planned_row_len(schema, row, plans)?;
    Ok(len <= schema.toast.tuple_target as usize && row_len_fits_page(len))
}

fn ensure_planned_row_fits_page(
    schema: &TableSchema,
    row: &Row,
    plans: &[ToastColumnPlan],
) -> Result<()> {
    ensure_row_len_fits_page(planned_row_len(schema, row, plans)?)
}

fn ensure_row_len_fits_page(row_len: usize) -> Result<()> {
    if !row_len_fits_page(row_len) {
        return Err(DbError::storage(
            SqlState::ProgramLimitExceeded,
            "row is too large for a data page",
        ));
    }
    Ok(())
}

fn row_len_fits_page(row_len: usize) -> bool {
    row_len
        .checked_add(page_overhead())
        .is_some_and(|len| len <= buffer::PAGE_SIZE)
}

fn planned_row_len(schema: &TableSchema, row: &Row, plans: &[ToastColumnPlan]) -> Result<usize> {
    if row.values.len() != schema.columns.len() || plans.len() != schema.columns.len() {
        return Err(DbError::storage(
            SqlState::DatatypeMismatch,
            format!(
                "row has {} values but table {} has {} columns",
                row.values.len(),
                schema.name,
                schema.columns.len()
            ),
        ));
    }

    let mut len = 1 + crate::codec::V2_MVCC_HEADER_LEN + crate::codec::null_bitmap_len(plans.len());
    for ((column, value), plan) in schema.columns.iter().zip(&row.values).zip(plans) {
        len = checked_row_len_add(
            len,
            planned_column_len(column, value, plan, schema.toast_table_id.is_some())?,
        )?;
    }
    Ok(len)
}

fn planned_column_len(
    column: &ColumnDef,
    value: &Value,
    plan: &ToastColumnPlan,
    has_toast_relation: bool,
) -> Result<usize> {
    match plan {
        ToastColumnPlan::Null => {
            if !column.nullable {
                return Err(DbError::storage(
                    SqlState::NotNullViolation,
                    format!("column {} cannot be NULL", column.name),
                ));
            }
            Ok(0)
        }
        ToastColumnPlan::Value => logical_value_v3_len(column, value),
        ToastColumnPlan::Varlena(varlena) => {
            if column.data_type != DataType::Text && column.data_type != DataType::Bytea {
                return Err(DbError::storage(
                    SqlState::DatatypeMismatch,
                    format!("value type does not match column {}", column.name),
                ));
            }
            match varlena {
                ToastVarlenaPlan::Plain => {
                    let raw = toastable_raw_bytes(&column.data_type, value).ok_or_else(|| {
                        DbError::storage(
                            SqlState::DatatypeMismatch,
                            format!("value type does not match column {}", column.name),
                        )
                    })?;
                    supported_varlena_len(raw.len())?;
                    checked_varlena_len_add(4, raw.len())
                }
                ToastVarlenaPlan::Compressed { payload, .. } => {
                    let stored_len = inline_compressed_stored_len(payload.len());
                    supported_varlena_len(stored_len)?;
                    checked_varlena_len_add(4, stored_len)
                }
                ToastVarlenaPlan::External(pointer) => {
                    if !has_toast_relation {
                        return Err(crate::toast::toast_corruption(
                            "external toast value requires a hidden TOAST relation",
                        ));
                    }
                    pointer.encode()?;
                    checked_varlena_len_add(4, crate::codec::TOAST_POINTER_LEN)
                }
            }
        }
    }
}

fn logical_value_v3_len(column: &ColumnDef, value: &Value) -> Result<usize> {
    match value {
        Value::Null => Err(DbError::storage(
            SqlState::InternalError,
            "NULL should be represented by ToastColumnPlan::Null",
        )),
        Value::Integer(_) if column.data_type == DataType::Integer => Ok(8),
        Value::Float(_) if column.data_type == DataType::Double => Ok(8),
        Value::Real(_) if column.data_type == DataType::Real => Ok(4),
        Value::Numeric(_) if matches!(column.data_type, DataType::Numeric { .. }) => Ok(20),
        Value::Text(value) if column.data_type == DataType::Text => {
            supported_varlena_len(value.len())?;
            checked_varlena_len_add(4, value.len())
        }
        Value::Boolean(_) if column.data_type == DataType::Boolean => Ok(1),
        Value::Date(_) if column.data_type == DataType::Date => Ok(8),
        Value::Timestamp(_) if column.data_type == DataType::Timestamp => Ok(8),
        Value::Time(_) if column.data_type == DataType::Time => Ok(8),
        Value::TimestampTz(_) if column.data_type == DataType::TimestampTz => Ok(8),
        Value::Interval(_) if column.data_type == DataType::Interval => Ok(16),
        Value::Bytes(value) if column.data_type == DataType::Bytea => {
            supported_varlena_len(value.len())?;
            checked_varlena_len_add(4, value.len())
        }
        Value::Uuid(_) if column.data_type == DataType::Uuid => Ok(16),
        _ => Err(DbError::storage(
            SqlState::DatatypeMismatch,
            format!("value type does not match column {}", column.name),
        )),
    }
}

fn checked_varlena_len_add(prefix_len: usize, payload_len: usize) -> Result<usize> {
    prefix_len.checked_add(payload_len).ok_or_else(|| {
        DbError::storage(
            SqlState::ProgramLimitExceeded,
            "varlena value length overflows usize",
        )
    })
}

fn checked_row_len_add(row_len: usize, column_len: usize) -> Result<usize> {
    row_len.checked_add(column_len).ok_or_else(|| {
        DbError::storage(SqlState::ProgramLimitExceeded, "row length overflows usize")
    })
}

fn external_stream_stored_len(codec: u8, dict_id: Option<u32>, payload_len: usize) -> Result<u32> {
    let header_len: usize = match codec {
        compress::CODEC_NONE | compress::CODEC_ZSTD => {
            if dict_id.is_some() {
                return Err(crate::toast::toast_corruption(
                    "dict-less TOAST stream codec cannot carry a dictionary id",
                ));
            }
            4
        }
        compress::CODEC_ZSTD_DICT => match dict_id {
            Some(0) | None => {
                return Err(crate::toast::toast_corruption(
                    "zstd-dict TOAST stream is missing dictionary id",
                ));
            }
            Some(_) => 8,
        },
        _ => {
            return Err(crate::toast::toast_corruption(
                "unknown external TOAST stream codec",
            ));
        }
    };
    let len = header_len.checked_add(payload_len).ok_or_else(|| {
        DbError::storage(
            SqlState::ProgramLimitExceeded,
            "external TOAST stream length overflows usize",
        )
    })?;
    supported_varlena_len(len)
}

fn candidate_stream_payload<'a>(
    schema: &TableSchema,
    row: &'a Row,
    candidate: &'a ToastCandidate,
) -> Result<&'a [u8]> {
    match &candidate.stream_payload {
        ToastStreamPayload::RawColumn => {
            let column = schema.columns.get(candidate.column).ok_or_else(|| {
                DbError::storage(SqlState::InternalError, "TOAST candidate column is invalid")
            })?;
            let value = row.values.get(candidate.column).ok_or_else(|| {
                DbError::storage(
                    SqlState::InternalError,
                    "TOAST candidate row value is missing",
                )
            })?;
            toastable_raw_bytes(&column.data_type, value).ok_or_else(|| {
                DbError::storage(
                    SqlState::InternalError,
                    "TOAST raw candidate no longer references a varlena value",
                )
            })
        }
        ToastStreamPayload::Owned(payload) => Ok(payload),
    }
}

fn materialize_prepared_values(
    schema: &TableSchema,
    row: &Row,
    plans: &[ToastColumnPlan],
) -> Result<Vec<crate::codec::PreparedColumnValue>> {
    if row.values.len() != schema.columns.len() || plans.len() != schema.columns.len() {
        return Err(DbError::storage(
            SqlState::DatatypeMismatch,
            format!(
                "row has {} values but table {} has {} columns",
                row.values.len(),
                schema.name,
                schema.columns.len()
            ),
        ));
    }

    let mut values = Vec::with_capacity(plans.len());
    for ((column, value), plan) in schema.columns.iter().zip(&row.values).zip(plans) {
        match plan {
            ToastColumnPlan::Null => values.push(crate::codec::PreparedColumnValue::Null),
            ToastColumnPlan::Value => {
                values.push(crate::codec::PreparedColumnValue::Value(value.clone()));
            }
            ToastColumnPlan::Varlena(ToastVarlenaPlan::Plain) => {
                let raw = toastable_raw_bytes(&column.data_type, value).ok_or_else(|| {
                    DbError::storage(
                        SqlState::DatatypeMismatch,
                        format!("value type does not match column {}", column.name),
                    )
                })?;
                values.push(crate::codec::PreparedColumnValue::Varlena(
                    crate::codec::VarlenaPhysical::Plain(raw.to_vec()),
                ));
            }
            ToastColumnPlan::Varlena(ToastVarlenaPlan::Compressed {
                codec,
                dict_id,
                raw_len,
                raw_crc32,
                payload,
            }) => {
                values.push(crate::codec::PreparedColumnValue::Varlena(
                    crate::codec::VarlenaPhysical::Compressed {
                        codec: *codec,
                        dict_id: *dict_id,
                        raw_len: *raw_len,
                        raw_crc32: *raw_crc32,
                        payload: payload.to_vec(),
                    },
                ));
            }
            ToastColumnPlan::Varlena(ToastVarlenaPlan::External(pointer)) => {
                values.push(crate::codec::PreparedColumnValue::Varlena(
                    crate::codec::VarlenaPhysical::External(pointer.clone()),
                ));
            }
        }
    }
    Ok(values)
}

impl PageBackedStorageEngine {
    pub(crate) fn prepare_row_for_storage(
        &self,
        ctx: &StatementContext,
        schema: &TableSchema,
        header: &crate::codec::MvccHeader,
        row: &Row,
    ) -> Result<Vec<u8>> {
        validate_logical_index_keys_fit(self, schema, row)?;
        if matches!(schema.relation_kind, RelationKind::Toast { .. })
            || schema.toast_table_id.is_none()
        {
            let values = plain_prepared_values(row);
            let bytes = crate::codec::encode_row_v3_prepared(schema, header, &values)?;
            ensure_row_len_fits_page(bytes.len())?;
            return Ok(bytes);
        }

        let (mut plans, mut candidates) = self.prepare_inline_toast_candidates(schema, row)?;
        if planned_row_meets_toast_goal(schema, row, &plans)? {
            let values = materialize_prepared_values(schema, row, &plans)?;
            let bytes = crate::codec::encode_row_v3_prepared(schema, header, &values)?;
            ensure_row_len_fits_page(bytes.len())?;
            return Ok(bytes);
        }
        if schema.toast.mode == common::ToastMode::Off {
            ensure_planned_row_fits_page(schema, row, &plans)?;
            let values = materialize_prepared_values(schema, row, &plans)?;
            let bytes = crate::codec::encode_row_v3_prepared(schema, header, &values)?;
            ensure_row_len_fits_page(bytes.len())?;
            return Ok(bytes);
        }

        candidates.sort_by(|a, b| {
            b.current_stored_len
                .cmp(&a.current_stored_len)
                .then_with(|| a.column.cmp(&b.column))
        });
        let mut planned = Vec::new();
        for (candidate_index, candidate) in candidates.iter().enumerate() {
            let payload = candidate_stream_payload(schema, row, candidate)?;
            let stored_len = external_stream_stored_len(
                candidate.stream_codec,
                candidate.stream_dict_id,
                payload.len(),
            )?;
            let pointer = ToastPointer {
                value_id: crate::toast::FIRST_TOAST_VALUE_ID,
                raw_len: candidate.raw_len,
                stored_len,
                codec: candidate.stream_codec,
            };
            pointer.encode()?;
            plans[candidate.column] = ToastColumnPlan::Varlena(ToastVarlenaPlan::External(pointer));
            planned.push(candidate_index);
            if planned_row_meets_toast_goal(schema, row, &plans)? {
                break;
            }
        }
        ensure_planned_row_fits_page(schema, row, &plans)?;

        for candidate_index in planned {
            let candidate = &candidates[candidate_index];
            let payload = candidate_stream_payload(schema, row, candidate)?;
            let stream = crate::toast::build_external_stream(
                candidate.stream_codec,
                candidate.stream_dict_id,
                candidate.raw_crc32,
                payload,
            )?;
            let pointer = self.write_toast_stream(
                ctx,
                schema,
                candidate.raw_len,
                candidate.stream_codec,
                &stream,
            )?;
            plans[candidate.column] = ToastColumnPlan::Varlena(ToastVarlenaPlan::External(pointer));
        }

        let values = materialize_prepared_values(schema, row, &plans)?;
        let bytes = crate::codec::encode_row_v3_prepared(schema, header, &values)?;
        ensure_row_len_fits_page(bytes.len())?;
        Ok(bytes)
    }

    fn prepare_inline_toast_candidates(
        &self,
        schema: &TableSchema,
        row: &Row,
    ) -> Result<(Vec<ToastColumnPlan>, Vec<ToastCandidate>)> {
        let mut plans = Vec::with_capacity(row.values.len());
        let mut candidates = Vec::new();
        for (column_index, value) in row.values.iter().enumerate() {
            let Some(column) = schema.columns.get(column_index) else {
                plans.push(ToastColumnPlan::Value);
                continue;
            };
            if matches!(value, Value::Null) {
                plans.push(ToastColumnPlan::Null);
                continue;
            }
            let Some(raw) = toastable_raw_bytes(&column.data_type, value) else {
                plans.push(ToastColumnPlan::Value);
                continue;
            };
            let raw_len = supported_varlena_len(raw.len())?;
            let raw_crc32 = crc32fast::hash(raw);
            let mut plan = ToastVarlenaPlan::Plain;
            let mut stream_codec = compress::CODEC_NONE;
            let mut stream_dict_id = None;
            let mut stream_payload = ToastStreamPayload::RawColumn;
            let mut current_stored_len = raw.len();

            if raw.len() >= schema.toast.min_value_size as usize
                && schema.toast.compression != common::ToastCompression::None
                && let Some(compressed) = self.try_toast_value_compression(schema, raw)?
                && raw.len()
                    >= inline_compressed_stored_len(compressed.payload.len())
                        .saturating_add(common::ToastOptions::MIN_TOAST_COMPRESSION_SAVINGS)
            {
                current_stored_len = inline_compressed_stored_len(compressed.payload.len());
                stream_codec = compressed.codec;
                stream_dict_id = compressed.dict_id;
                stream_payload = ToastStreamPayload::Owned(compressed.payload.clone());
                plan = ToastVarlenaPlan::Compressed {
                    codec: compressed.codec,
                    dict_id: compressed.dict_id,
                    raw_len,
                    raw_crc32,
                    payload: compressed.payload,
                };
            }

            if raw.len() >= schema.toast.min_value_size as usize {
                candidates.push(ToastCandidate {
                    column: column_index,
                    raw_len,
                    raw_crc32,
                    stream_codec,
                    stream_dict_id,
                    stream_payload,
                    current_stored_len,
                });
            }
            plans.push(ToastColumnPlan::Varlena(plan));
        }
        Ok((plans, candidates))
    }

    fn try_toast_value_compression(
        &self,
        schema: &TableSchema,
        raw: &[u8],
    ) -> Result<Option<CompressedToastValue>> {
        match schema.toast.compression {
            common::ToastCompression::None => Ok(None),
            common::ToastCompression::Zstd => Ok(Some(CompressedToastValue {
                codec: compress::CODEC_ZSTD,
                dict_id: None,
                payload: compress::compress_value_zstd(raw)?.into(),
            })),
            common::ToastCompression::ZstdDict => {
                if let Some(dict_id) = schema.toast.active_dict_id {
                    Ok(Some(CompressedToastValue {
                        codec: compress::CODEC_ZSTD_DICT,
                        dict_id: Some(dict_id),
                        payload: self
                            .compression
                            .compress_value_zstd_dict(dict_id, raw)?
                            .into(),
                    }))
                } else {
                    Ok(Some(CompressedToastValue {
                        codec: compress::CODEC_ZSTD,
                        dict_id: None,
                        payload: compress::compress_value_zstd(raw)?.into(),
                    }))
                }
            }
        }
    }

    pub(super) fn write_new_row_bytes(
        &self,
        schema: &TableSchema,
        row_bytes: &[u8],
        txn_id: u64,
    ) -> Result<RowLocation> {
        if row_bytes.len() + page_overhead() > buffer::PAGE_SIZE {
            return Err(DbError::storage(
                SqlState::ProgramLimitExceeded,
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
                    self.log_insert(&mut writable, txn_id, file_id, page_num, row_bytes)?;
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
        let init_lsn = match self.wal.append(WalRecord {
            lsn: 0,
            txn_id,
            kind: WalRecordKind::HeapInit { file_id, page_num },
        }) {
            Ok(lsn) => lsn,
            Err(err) => {
                self.buffer_pool.abandon_unpublished_new_page(writable)?;
                return Err(err);
            }
        };
        // The HeapInit record now durably references this page: it can no longer be
        // abandoned, only reclaimed by VACUUM after its tuples die.
        writable.publish();
        page::init_page(writable.data_mut(), page_num);
        page::set_page_lsn(writable.data_mut(), init_lsn);
        let slot_num = self.log_insert(&mut writable, txn_id, file_id, page_num, row_bytes)?;
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
            let mut image = *guard.data();
            let slot_num = match page::insert_row(&mut image, row_bytes) {
                Ok(slot_num) => slot_num,
                Err(err) => {
                    guard.restore_needs_fpi();
                    return Err(err);
                }
            };
            let record = WalRecord {
                lsn: 0,
                txn_id,
                kind: fpi_record_kind(&self.compression, file_id, page_num, &image),
            };
            let lsn = match self.wal.append(record) {
                Ok(lsn) => lsn,
                Err(err) => {
                    guard.restore_needs_fpi();
                    return Err(err);
                }
            };
            page::set_page_lsn(&mut image, lsn);
            *guard.data_mut() = image;
            Ok(slot_num)
        } else {
            let mut image = *guard.data();
            let slot_num = page::insert_row(&mut image, row_bytes)?;
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
            page::set_page_lsn(&mut image, lsn);
            *guard.data_mut() = image;
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
            let plan = self.classify_page_for_prune(schema, guard.data(), horizon, false)?;
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
        current_txns: &[u64],
    ) -> Result<StampOutcome> {
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
        match write_conflict(
            current_xmax,
            current_infomask,
            current_txns,
            self.txn_status_view(),
        ) {
            WriteConflict::Proceed => {}
            // The deleter committed since my snapshot ⇒ the row changed under me.
            WriteConflict::Conflict => {
                return Err(DbError::execute(
                    SqlState::SerializationFailure,
                    "could not serialize access due to concurrent update",
                ));
            }
            // An in-progress writer holds the lock: return WITHOUT stamping (the
            // frame latch drops on return) so the caller blocks on that holder and
            // re-attempts the stamp (`docs/specs/deadlock.md`).
            WriteConflict::WouldBlock(holder) => {
                return Ok(StampOutcome::WouldBlock(holder));
            }
        }

        if guard.take_needs_fpi() {
            let mut image = *guard.data();
            let current_lsn = page::page_lsn(&image);
            if let Err(err) = page::set_tuple_header(
                &mut image,
                location.slot_num,
                txn_id,
                t_ctid,
                infomask,
                current_lsn,
            ) {
                guard.restore_needs_fpi();
                return Err(err);
            }
            let record = WalRecord {
                lsn: 0,
                txn_id,
                kind: fpi_record_kind(
                    &self.compression,
                    location.file_id,
                    location.page_num,
                    &image,
                ),
            };
            let lsn = match self.wal.append(record) {
                Ok(lsn) => lsn,
                Err(err) => {
                    guard.restore_needs_fpi();
                    return Err(err);
                }
            };
            page::set_page_lsn(&mut image, lsn);
            *guard.data_mut() = image;
        } else {
            let mut image = *guard.data();
            page::set_tuple_header(
                &mut image,
                location.slot_num,
                txn_id,
                t_ctid,
                infomask,
                page::page_lsn(guard.data()),
            )?;
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
            page::set_page_lsn(&mut image, lsn);
            *guard.data_mut() = image;
        }
        Ok(StampOutcome::Stamped)
    }

    /// Block until the in-progress `blocker` holding a row/key lock this statement
    /// wants has finished, so the caller can re-attempt the conflict check
    /// (`docs/specs/deadlock.md`). Delegates to the lock manager installed on the
    /// statement context; returns `Err` on deadlock (`40P01`) or cancel (`57014`).
    /// The waiter is the statement's writing xid (`ctx.txn_id`).
    pub(super) fn wait_for_conflict(&self, ctx: &StatementContext, blocker: u64) -> Result<()> {
        ctx.conflict_waiter
            .wait_for(ctx.txn_id, blocker, ctx.cancel.as_ref())
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
        let Some(previous_row) = self.read_location_materialized(ctx, schema, previous_location)?
        else {
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
        while let StampOutcome::WouldBlock(blocker) = self.stamp_xmax_logged(
            previous_location,
            new_tid,
            infomask | crate::codec::HOT_UPDATED,
            ctx.txn_id,
            &ctx.live_txns,
        )? {
            // An in-progress writer holds the predecessor's lock: wait for it, then
            // re-attempt the stamp (the heap-only successor is already written).
            self.wait_for_conflict(ctx, blocker)?;
        }

        // No index entries: the index keeps pointing at the chain root; the new
        // heap-only version is reached only by the bounded `t_ctid` walk from it. This
        // is the whole point of HOT — the un-indexed in-place version.
        Ok(Some(true))
    }
}
