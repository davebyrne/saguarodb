use serde::{Deserialize, Serialize};

pub type TableId = u32;
pub type SchemaId = u32;
pub const PUBLIC_SCHEMA_ID: SchemaId = 1;
pub const FIRST_USER_SCHEMA_ID: SchemaId = 2;
pub type ColumnId = u16;
pub type IndexId = u32;
pub type SequenceId = u32;
pub const PRIMARY_KEY_INDEX_ID: IndexId = 0;
pub type BindingId = u32;
pub type PageNum = u32;
pub type FileId = u32;
pub type Lsn = u64;

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
