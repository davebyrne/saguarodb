#![cfg_attr(
    not(test),
    deny(
        clippy::disallowed_macros,
        clippy::expect_used,
        clippy::panic,
        clippy::todo,
        clippy::unimplemented,
        clippy::unreachable,
        clippy::unwrap_used
    )
)]

mod loader;
mod page;
mod pool;

pub use loader::{PageLoader, PageStore};
pub use page::{PAGE_SIZE, PageData, PageInfo};
pub use pool::{
    BufferPool, CheckpointBatchStats, CheckpointExclusiveGuard, CheckpointFence,
    CheckpointSharedGuard, MemoryBufferPool, PageReadGuard, PageWriteGuard,
};
