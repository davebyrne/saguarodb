use serde::{Deserialize, Serialize};

use crate::Lsn;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageFlushInfo {
    pub dirty_txn_id: u64,
    pub page_lsn: Option<Lsn>,
}

pub trait FlushPolicy: Send + Sync {
    fn can_flush(&self, info: &PageFlushInfo) -> bool;

    /// Make every WAL record durable up to the present, so a dirty page about to be
    /// written to its home satisfies write-ahead logging (its describing records are
    /// on disk before the page is). Called by the buffer pool's eviction (steal)
    /// path before it flushes a stolen dirty page.
    ///
    /// Before Milestone D the flush gate only stole *committed* pages, whose WAL
    /// (including the `Commit`) was already durable, so this was unnecessary. With
    /// the relaxed gate (`docs/specs/mvcc.md` §8) an *uncommitted* page may be
    /// stolen, and its records are not yet flushed — so the steal must force the WAL
    /// first. The default is a no-op for policies with no WAL (tests).
    fn ensure_durable(&self) -> crate::Result<()> {
        Ok(())
    }
}
