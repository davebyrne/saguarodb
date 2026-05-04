use common::{FileId, PageNum, Result};

use crate::PageData;

pub trait PageLoader: Send + Sync {
    fn load_page(&self, file_id: FileId, page_num: PageNum) -> Result<Option<PageData>>;
}
