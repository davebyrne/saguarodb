use serde::{Deserialize, Serialize};

use crate::Lsn;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageFlushInfo {
    pub dirty_txn_id: u64,
    pub page_lsn: Option<Lsn>,
}

pub trait FlushPolicy: Send + Sync {
    fn can_flush(&self, info: &PageFlushInfo) -> bool;
}
