use common::{
    ColumnId, CompressionSetting, FileId, IndexId, IndexSchema, Lsn, PageNum, SequenceId,
    SequenceSchema, TableId, TableSchema, ToastOptions,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub txn_id: u64,
    pub kind: WalRecordKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalRecordKind {
    // Logical (structured) records, JSON payloads.
    CreateTable {
        schema: TableSchema,
    },
    DropTable {
        table: TableId,
    },
    UpdateTableSchema {
        schema: TableSchema,
        indexes: Vec<IndexSchema>,
    },
    CreateIndex {
        schema: IndexSchema,
    },
    DropIndex {
        index: IndexId,
    },
    CreateSequence {
        schema: SequenceSchema,
    },
    DropSequence {
        sequence: SequenceId,
    },
    /// Non-transactional sequence advance produced by `nextval`. Recovery replays
    /// it regardless of the writer transaction's eventual outcome so rolled-back
    /// statements still leave sequence gaps.
    SequenceAdvance {
        sequence: SequenceId,
        value: i64,
    },
    /// Non-transactional sequence state change produced by `setval`. Recovery
    /// replays it regardless of the writer transaction's eventual outcome.
    SetSequenceValue {
        sequence: SequenceId,
        value: i64,
        is_called: bool,
    },
    Commit,
    /// Commit of a transaction that had savepoint subtransactions: marks the
    /// top-level transaction (`txn_id` in the header) AND every committed (live or
    /// released, i.e. not-rolled-back) subxid committed, atomically in one durable
    /// record. Recovery marks `txn_id` and each `subxids` entry `Committed`. A
    /// rolled-back subxid is instead recorded by its own `Abort` record (header
    /// `txn_id` = the subxid) and is NOT in `subxids`. See `docs/specs/savepoints.md`
    /// §5; a no-savepoint commit uses the plain `Commit` record (unchanged format).
    CommitWithSubxids {
        subxids: Vec<u64>,
    },
    /// Marks a transaction aborted. Payload is empty; the `txn_id` is in the
    /// `WalRecord` header, mirroring `Commit`. Recovery rebuilds the CLOG from
    /// `Commit`/`Abort` records and never redoes an aborted transaction's data.
    /// `ROLLBACK TO SAVEPOINT` appends one of these per rolled-back subxid (header
    /// `txn_id` = the subxid), reusing this record.
    Abort,
    Checkpoint {
        redo_lsn: Lsn,
    },
    /// Physiological redo: initialize a fresh heap page.
    HeapInit {
        file_id: FileId,
        page_num: PageNum,
    },
    /// Physiological redo: write an encoded row into a slot on a heap page.
    HeapInsert {
        file_id: FileId,
        page_num: PageNum,
        slot: u16,
        row_bytes: Vec<u8>,
    },
    /// Physiological redo: mark a slot dead on a heap page.
    HeapDelete {
        file_id: FileId,
        page_num: PageNum,
        slot: u16,
    },
    /// Physiological redo: an in-place mutation of a v2 tuple header — set the
    /// `xmax`, forward `t_ctid` pointer, and `infomask` of the live tuple at
    /// `slot` without relocating it (fixed-width header fields ⇒ same-size
    /// mutation). Emitted by `UPDATE`/`DELETE` version stamping (Milestone B
    /// commits 8–9); recovery applies it PageLSN-gated like the other heap
    /// records (see `docs/specs/mvcc.md` §5.3).
    HeapUpdateHeader {
        file_id: FileId,
        page_num: PageNum,
        slot: u16,
        xmax: u64,
        t_ctid: (PageNum, u16),
        infomask: u16,
    },
    /// Torn-page protection: a full page image, reinstalled during redo before
    /// any later delta for the same page is applied.
    FullPageImage {
        file_id: FileId,
        page_num: PageNum,
        image: Vec<u8>,
    },
    /// Compressed full-page image. `payload` decompresses to exactly PAGE_SIZE
    /// bytes via the codec/dict named here; emitted only when smaller than raw.
    FullPageImageCompressed {
        file_id: FileId,
        page_num: PageNum,
        codec: u8,
        dict_id: u32,
        payload: Vec<u8>,
    },
    /// Installs an immutable per-table compression dictionary. Replay writes the
    /// dict file if absent and registers it, so later records can resolve it.
    CreateDictionary {
        dict_id: u32,
        table_id: TableId,
        bytes: Vec<u8>,
    },
    /// DDL: updates a table's compression setting + active dictionary
    /// (CLOG-gated on replay like other DDL).
    AlterTableCompression {
        table_id: TableId,
        compression: CompressionSetting,
        active_dict_id: Option<u32>,
    },
    /// DDL: updates a table's TOAST policy and linked hidden TOAST relation
    /// (CLOG-gated on replay like other DDL).
    AlterTableToast {
        table_id: TableId,
        toast: ToastOptions,
        toast_table_id: Option<TableId>,
    },
    /// DDL: swaps a table and its dependent physical storage generations.
    TruncateTable {
        table_id: TableId,
        new_table_storage_id: FileId,
        new_toast_storage_id: Option<(TableId, FileId)>,
        new_index_storage_ids: Vec<(IndexId, FileId)>,
    },
    /// DDL: updates a user table's primary-key column list. The derived storage
    /// identity B-tree is rebuilt from heap rows when this committed logical
    /// record is applied.
    AlterTablePrimaryKey {
        table_id: TableId,
        primary_key: Vec<ColumnId>,
    },
}

/// Whether `kind` is a replayable page-mutation record (anything recovery
/// applies), i.e. every record except the `Commit` / `CommitWithSubxids` / `Abort`
/// / `Checkpoint` metadata markers. Redo-all recovery (`docs/specs/mvcc.md` §8)
/// replays these and skips the markers (the markers feed the CLOG, not the heap).
pub fn is_redo_operation(kind: &WalRecordKind) -> bool {
    !matches!(
        kind,
        WalRecordKind::Commit
            | WalRecordKind::CommitWithSubxids { .. }
            | WalRecordKind::Abort
            | WalRecordKind::Checkpoint { .. }
    )
}

impl WalRecord {
    /// A committed-operation record for WAL tests (LSN assignment, commit
    /// tracking, replay, truncation). `value` only distinguishes records.
    pub fn insert_for_test(txn_id: u64, value: i64) -> Self {
        Self {
            lsn: 0,
            txn_id,
            kind: WalRecordKind::HeapInsert {
                file_id: 1,
                page_num: 0,
                slot: 0,
                row_bytes: value.to_le_bytes().to_vec(),
            },
        }
    }
}
