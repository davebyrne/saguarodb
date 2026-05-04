use common::{FileId, PageNum};

pub const PAGE_SIZE: usize = 8192;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageData(pub [u8; PAGE_SIZE]);

impl Default for PageData {
    fn default() -> Self {
        Self([0; PAGE_SIZE])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageInfo {
    pub file_id: FileId,
    pub page_num: PageNum,
    pub data: PageData,
    pub is_dirty: bool,
}
