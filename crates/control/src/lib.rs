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

mod control;
mod manifest;

pub use control::{ControlStore, FileControlStore};
pub use manifest::{ControlData, MAX_DIRTY_PAGES};

#[cfg(test)]
mod tests {
    use super::{ControlData, ControlStore, FileControlStore};

    fn control(lsn: u64, tables: Vec<u32>, catalog: &[u8]) -> ControlData {
        ControlData {
            checkpoint_end_lsn: lsn,
            page_redo_lsn: lsn,
            catalog_redo_lsn: lsn,
            dirty_pages: Vec::new(),
            tables,
            catalog: catalog.to_vec(),
            page_size: 8192,
        }
    }

    #[test]
    fn store_then_load_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileControlStore::open(dir.path(), 8192).unwrap();
        store.store(control(55, vec![1, 2], b"catalog")).unwrap();

        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.checkpoint_end_lsn, 55);
        assert_eq!(loaded.tables, vec![1, 2]);
        assert_eq!(loaded.catalog, b"catalog");
        assert!(dir.path().join("manifest.dat").exists());
    }

    #[test]
    fn load_returns_none_without_control_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileControlStore::open(dir.path(), 8192).unwrap();
        assert!(store.load().unwrap().is_none());
    }

    #[test]
    fn store_overwrites_previous_control_record() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileControlStore::open(dir.path(), 8192).unwrap();
        store.store(control(10, vec![1], b"old")).unwrap();
        store.store(control(20, vec![1, 3], b"new")).unwrap();

        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.checkpoint_end_lsn, 20);
        assert_eq!(loaded.tables, vec![1, 3]);
        assert_eq!(loaded.catalog, b"new");
    }
}
