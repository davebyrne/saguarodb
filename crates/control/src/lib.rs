mod control;
mod manifest;

pub use control::{ControlStore, FileControlStore};
pub use manifest::ControlData;

#[cfg(test)]
mod tests {
    use super::{ControlStore, FileControlStore};

    #[test]
    fn store_then_load_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileControlStore::open(dir.path()).unwrap();
        store.store(55, &[1, 2], b"catalog").unwrap();

        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.checkpoint_lsn, 55);
        assert_eq!(loaded.tables, vec![1, 2]);
        assert_eq!(loaded.catalog, b"catalog");
        assert!(dir.path().join("manifest.dat").exists());
    }

    #[test]
    fn load_returns_none_without_control_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileControlStore::open(dir.path()).unwrap();
        assert!(store.load().unwrap().is_none());
    }

    #[test]
    fn store_overwrites_previous_control_record() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileControlStore::open(dir.path()).unwrap();
        store.store(10, &[1], b"old").unwrap();
        store.store(20, &[1, 3], b"new").unwrap();

        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.checkpoint_lsn, 20);
        assert_eq!(loaded.tables, vec![1, 3]);
        assert_eq!(loaded.catalog, b"new");
    }
}
