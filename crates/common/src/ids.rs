use serde::{Deserialize, Serialize};

pub type TableId = u32;
pub type ColumnId = u16;
pub type IndexId = u32;
pub const PRIMARY_KEY_INDEX_ID: IndexId = 0;
pub type BindingId = u32;
pub type PageNum = u32;
pub type FileId = u32;
pub type Lsn = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RowId {
    pub page_num: PageNum,
    pub slot_num: u16,
}
