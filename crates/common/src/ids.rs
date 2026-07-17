use serde::{Deserialize, Serialize};

pub type TableId = u32;
pub type SchemaId = u32;
pub const PUBLIC_SCHEMA_ID: SchemaId = 1;
pub const FIRST_USER_SCHEMA_ID: SchemaId = 2;
pub type ColumnId = u16;
/// Durable identity of a column within its containing relation. Unlike the
/// dense [`ColumnId`] storage ordinal, this value is never renumbered or reused.
pub type ColumnObjectId = u32;
/// Stable PostgreSQL-compatible OID of a built-in scalar function.
pub type FunctionId = u32;
/// Globally durable identity of a catalog constraint.
pub type ConstraintId = u32;
/// Globally durable identity of a catalog index. Zero is reserved for the
/// synthetic primary-key index identity; user index IDs are never reused.
pub type IndexId = u32;
pub type SequenceId = u32;
pub const PRIMARY_KEY_INDEX_ID: IndexId = 0;
pub type BindingId = u32;
pub type PageNum = u32;
pub type FileId = u32;
pub type Lsn = u64;

/// The two byte positions associated with a WAL record. `replay_from` is the
/// record-start boundary and `record_lsn` is the stored end boundary stamped on
/// pages.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WalPosition {
    pub replay_from: Lsn,
    pub record_lsn: Lsn,
}

impl WalPosition {
    pub fn new(replay_from: Lsn, record_lsn: Lsn) -> crate::Result<Self> {
        if replay_from >= record_lsn {
            return Err(crate::DbError::wal(
                crate::SqlState::InternalError,
                "WAL record position must have a start before its end",
            ));
        }
        Ok(Self {
            replay_from,
            record_lsn,
        })
    }
}

/// Earliest WAL record needed to reconstruct one dirty page.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirtyPageEntry {
    pub file_id: FileId,
    pub page_num: PageNum,
    pub rec_lsn: Lsn,
}

impl PartialOrd for DirtyPageEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DirtyPageEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.file_id, self.page_num).cmp(&(other.file_id, other.page_num))
    }
}

/// Transaction identifier. MVCC tuple headers stamp the creator (`xmin`) and
/// deleter (`xmax`) of each row version with one of these.
pub type TxnId = u64;

/// Reserved transaction id meaning "no transaction": a live (un-deleted) row's
/// `xmax`, or an absent transaction reference. Never a real allocated id.
pub const INVALID_XID: TxnId = 0;

/// Reserved transaction id meaning "always committed / always visible". Pre-MVCC
/// (row format v1) tuples decode with `xmin = FROZEN_XID` so they are visible to
/// every snapshot, and VACUUM freezes settled tuples to this id (later milestone).
pub const FROZEN_XID: TxnId = 2;

/// The transaction-id allocator must assign real ids strictly greater than this
/// reserved range so they never collide with `INVALID_XID`/`FROZEN_XID`. Wiring
/// the allocator above this floor is Milestone A3's responsibility.
pub const FIRST_NORMAL_XID: TxnId = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RowId {
    pub page_num: PageNum,
    pub slot_num: u16,
}
