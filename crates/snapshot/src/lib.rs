mod manager;
mod manifest;
mod writer;

pub use manager::{FileSnapshotManager, LoadedSnapshot, SnapshotManager};
pub use writer::{SnapshotPage, SnapshotWriter};

pub use manifest::SnapshotMetadata;

#[cfg(test)]
mod tests {
    use buffer::{BufferPool, MemoryBufferPool, PageData};
    use common::ErrorKind;

    use super::{FileSnapshotManager, SnapshotManager, SnapshotPage};

    #[test]
    fn first_snapshot_creates_manifest_and_generation_directory() {
        let dir = tempfile::tempdir().unwrap();
        let pool = MemoryBufferPool::empty(8);
        let manager = FileSnapshotManager::open(dir.path()).unwrap();

        let mut writer = manager.begin_snapshot().unwrap();
        writer.write_catalog(br#"{"tables":[]}"#).unwrap();
        let metadata = manager.commit_snapshot(writer, 55).unwrap();
        assert_eq!(metadata.generation, 1);
        assert_eq!(metadata.checkpoint_lsn, 55);

        let loaded = manager.load_current(&pool).unwrap().unwrap();
        assert_eq!(loaded.metadata.generation, 1);
        assert_eq!(loaded.metadata.checkpoint_lsn, 55);
        assert_eq!(loaded.catalog_bytes, br#"{"tables":[]}"#);
        assert!(dir.path().join("manifest.dat").exists());
        assert!(dir.path().join("snap_1").exists());
    }

    #[test]
    fn orphan_generation_directory_is_removed_without_touching_current_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileSnapshotManager::open(dir.path()).unwrap();
        create_committed_snapshot(&manager, 1, 10);
        std::fs::create_dir(dir.path().join("snap_99")).unwrap();

        manager.cleanup_old_snapshots().unwrap();

        assert!(dir.path().join("snap_1").exists());
        assert!(!dir.path().join("snap_99").exists());
    }

    #[test]
    fn table_pages_load_with_preserved_sparse_page_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileSnapshotManager::open(dir.path()).unwrap();
        let pool = MemoryBufferPool::empty(8);
        let mut page_three = PageData::default();
        page_three.0[0] = 3;
        let mut page_zero = PageData::default();
        page_zero.0[0] = 9;

        let mut writer = manager.begin_snapshot().unwrap();
        writer.write_catalog(b"catalog").unwrap();
        writer
            .write_table(
                7,
                &[
                    SnapshotPage {
                        page_num: 3,
                        data: page_three.clone(),
                    },
                    SnapshotPage {
                        page_num: 0,
                        data: page_zero.clone(),
                    },
                ],
            )
            .unwrap();
        manager.commit_snapshot(writer, 88).unwrap();

        let loaded = manager.load_current(&pool).unwrap().unwrap();
        assert_eq!(loaded.metadata.tables, vec![7]);
        assert_eq!(pool.read_page(7, 0).unwrap().data()[0], 9);
        assert_eq!(pool.read_page(7, 3).unwrap().data()[0], 3);
        assert!(pool.read_page(7, 1).is_err());
    }

    #[test]
    fn load_current_errors_if_buffer_cannot_retain_loaded_snapshot_pages() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileSnapshotManager::open(dir.path()).unwrap();
        let mut page_zero = PageData::default();
        page_zero.0[0] = 1;
        let mut page_one = PageData::default();
        page_one.0[0] = 2;

        let mut writer = manager.begin_snapshot().unwrap();
        writer.write_catalog(b"catalog").unwrap();
        writer
            .write_table(
                7,
                &[
                    SnapshotPage {
                        page_num: 0,
                        data: page_zero,
                    },
                    SnapshotPage {
                        page_num: 1,
                        data: page_one,
                    },
                ],
            )
            .unwrap();
        manager.commit_snapshot(writer, 88).unwrap();

        let err = manager
            .load_current(&MemoryBufferPool::empty(1))
            .unwrap_err();

        assert_eq!(err.kind, ErrorKind::Storage);
    }

    #[test]
    fn current_table_pages_returns_sorted_page_numbered_snapshot_data() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileSnapshotManager::open(dir.path()).unwrap();
        let mut page_two = PageData::default();
        page_two.0[0] = 2;
        let mut page_five = PageData::default();
        page_five.0[0] = 5;

        let mut writer = manager.begin_snapshot().unwrap();
        writer.write_catalog(b"catalog").unwrap();
        writer
            .write_table(
                4,
                &[
                    SnapshotPage {
                        page_num: 5,
                        data: page_five.clone(),
                    },
                    SnapshotPage {
                        page_num: 2,
                        data: page_two.clone(),
                    },
                ],
            )
            .unwrap();
        manager.commit_snapshot(writer, 100).unwrap();

        let pages = manager.current_table_pages(4).unwrap();
        assert_eq!(
            pages.iter().map(|page| page.page_num).collect::<Vec<_>>(),
            vec![2, 5]
        );
        assert_eq!(pages[0].data, page_two);
        assert_eq!(pages[1].data, page_five);
        assert!(manager.current_table_pages(99).unwrap().is_empty());
    }

    #[test]
    fn write_table_rejects_duplicate_page_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileSnapshotManager::open(dir.path()).unwrap();
        let mut writer = manager.begin_snapshot().unwrap();

        let err = writer
            .write_table(
                4,
                &[
                    SnapshotPage {
                        page_num: 2,
                        data: PageData::default(),
                    },
                    SnapshotPage {
                        page_num: 2,
                        data: PageData::default(),
                    },
                ],
            )
            .unwrap_err();

        assert_eq!(err.kind, ErrorKind::Storage);
    }

    #[test]
    fn stale_manifest_tmp_does_not_replace_current_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileSnapshotManager::open(dir.path()).unwrap();
        create_committed_snapshot(&manager, 1, 10);

        let mut writer = manager.begin_snapshot().unwrap();
        writer.write_catalog(b"new").unwrap();
        let tmp_path = dir.path().join("manifest.dat.tmp");
        std::fs::write(&tmp_path, b"not a committed manifest").unwrap();

        let loaded = manager
            .load_current(&MemoryBufferPool::empty(8))
            .unwrap()
            .unwrap();
        assert_eq!(loaded.metadata.generation, 1);
        assert_eq!(loaded.metadata.checkpoint_lsn, 10);
        assert_eq!(loaded.catalog_bytes, b"snapshot");
    }

    #[test]
    fn second_committed_snapshot_becomes_current_without_deleting_old_generation() {
        let dir = tempfile::tempdir().unwrap();
        let manager = FileSnapshotManager::open(dir.path()).unwrap();
        create_committed_snapshot(&manager, 1, 10);

        let mut writer = manager.begin_snapshot().unwrap();
        writer.write_catalog(b"new snapshot").unwrap();
        manager.commit_snapshot(writer, 20).unwrap();

        let loaded = manager
            .load_current(&MemoryBufferPool::empty(8))
            .unwrap()
            .unwrap();
        assert_eq!(loaded.metadata.generation, 2);
        assert_eq!(loaded.metadata.checkpoint_lsn, 20);
        assert_eq!(loaded.catalog_bytes, b"new snapshot");
        assert!(dir.path().join("snap_1").exists());
        assert!(dir.path().join("snap_2").exists());
    }

    fn create_committed_snapshot(manager: &FileSnapshotManager, _generation: u64, lsn: u64) {
        let mut writer = manager.begin_snapshot().unwrap();
        writer.write_catalog(b"snapshot").unwrap();
        manager.commit_snapshot(writer, lsn).unwrap();
    }
}
