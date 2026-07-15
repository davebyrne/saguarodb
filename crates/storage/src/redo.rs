use buffer::PAGE_SIZE;
use common::{DbError, Lsn, Result, SqlState};
use wal::WalRecordKind;

use crate::page;

/// Apply a physiological redo record to a page buffer, gated by the page-LSN so
/// replay is idempotent: a record whose effect is already present
/// (`page_lsn(data) >= lsn`) is skipped and `Ok(false)` is returned.
///
/// `data` is the in-memory buffer for the record's `(file_id, page_num)`. The
/// caller supplies a freshly zeroed buffer for the first record on a new page
/// (`HeapInit` / `FullPageImage`) and the page loaded from its home for later
/// deltas. Records are replayed in ascending LSN order.
pub fn apply_physical_redo(
    data: &mut [u8; PAGE_SIZE],
    lsn: Lsn,
    kind: &WalRecordKind,
) -> Result<bool> {
    if page::page_lsn(data) >= lsn {
        return Ok(false);
    }
    match kind {
        WalRecordKind::HeapInit { page_num, .. } => {
            page::init_page(data, *page_num);
            page::set_page_lsn(data, lsn);
        }
        WalRecordKind::HeapInsert {
            slot, row_bytes, ..
        } => {
            let produced = page::insert_row(data, row_bytes)?;
            if produced != *slot {
                return Err(redo_error(format!(
                    "redo heap-insert slot mismatch: expected {slot}, produced {produced}"
                )));
            }
            page::set_page_lsn(data, lsn);
        }
        WalRecordKind::HeapDelete { slot, .. } => {
            page::delete_row(data, *slot)?;
            page::set_page_lsn(data, lsn);
        }
        WalRecordKind::HeapUpdateHeader {
            slot,
            xmax,
            t_ctid,
            infomask,
            ..
        } => {
            // `set_tuple_header` mutates the v2 header in place and stamps the
            // page-LSN itself (no separate `set_page_lsn` like the siblings).
            page::set_tuple_header(data, *slot, *xmax, *t_ctid, *infomask, lsn)?;
        }
        WalRecordKind::FullPageImage { image, .. } => {
            if image.len() != PAGE_SIZE {
                return Err(redo_error(format!(
                    "redo full-page image has {} bytes, expected {PAGE_SIZE}",
                    image.len()
                )));
            }
            data.copy_from_slice(image);
            // The image carries its own page-LSN, but force it to the record's
            // LSN so gating is exact regardless of the image contents.
            page::set_page_lsn(data, lsn);
        }
        _ => return Err(redo_error("not a physiological redo record")),
    }
    Ok(true)
}

fn redo_error(message: impl Into<String>) -> DbError {
    DbError::storage(SqlState::InternalError, message)
}

#[cfg(test)]
mod tests {
    use buffer::PageData;
    use common::{
        ColumnDef, CompressionSetting, DataType, RelationKind, TableSchema, ToastOptions, Value,
        XMAX_COMMITTED,
    };
    use wal::WalRecordKind;

    use super::apply_physical_redo;
    use crate::codec::{decode_row, encode_row};
    use crate::page;

    fn live_row_count(data: &[u8; buffer::PAGE_SIZE]) -> usize {
        let slots = page::next_slot(data).unwrap();
        (0..slots)
            .filter(|slot| page::read_row(data, *slot).unwrap().is_some())
            .count()
    }

    fn header_schema() -> TableSchema {
        TableSchema {
            id: 1,
            schema_id: common::PUBLIC_SCHEMA_ID,
            storage_id: 1,
            name: "t".to_string(),
            columns: vec![
                ColumnDef {
                    id: 0,
                    object_id: 1,
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
                ColumnDef {
                    id: 1,
                    object_id: 2,
                    name: "note".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    max_length: None,
                    default: None,
                    pg_type: None,
                },
            ],
            primary_key: vec![0],
            schema_version: common::INITIAL_SCHEMA_VERSION,
            compression: CompressionSetting::None,
            active_dict_id: None,
            toast: ToastOptions::legacy_catalog_default(),
            toast_table_id: None,
            relation_kind: RelationKind::User,
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            next_foreign_key_id: 0,
            next_column_object_id: u32::MAX,
        }
    }

    fn header_row() -> common::Row {
        common::Row {
            values: vec![Value::Integer(42), Value::Text("hi".to_string())],
        }
    }

    /// Seed a fresh page with one v2 tuple at slot 0 stamped `xmin = 7`,
    /// returning the page and the inserted slot.
    fn page_with_one_tuple() -> (PageData, u16) {
        let mut data = PageData::default();
        apply_physical_redo(
            &mut data.0,
            1,
            &WalRecordKind::HeapInit {
                file_id: 1,
                page_num: 0,
            },
        )
        .unwrap();
        let row_bytes = encode_row(&header_schema(), &header_row(), 7).unwrap();
        apply_physical_redo(
            &mut data.0,
            2,
            &WalRecordKind::HeapInsert {
                file_id: 1,
                page_num: 0,
                slot: 0,
                row_bytes,
            },
        )
        .unwrap();
        (data, 0)
    }

    #[test]
    fn heap_init_initializes_and_stamps_lsn() {
        let mut data = PageData::default();
        let applied = apply_physical_redo(
            &mut data.0,
            5,
            &WalRecordKind::HeapInit {
                file_id: 1,
                page_num: 3,
            },
        )
        .unwrap();

        assert!(applied);
        assert_eq!(page::page_lsn(&data.0), 5);
        page::validate(&data.0).unwrap();
    }

    #[test]
    fn heap_insert_is_idempotent_under_gating() {
        let mut data = PageData::default();
        apply_physical_redo(
            &mut data.0,
            5,
            &WalRecordKind::HeapInit {
                file_id: 1,
                page_num: 0,
            },
        )
        .unwrap();
        let insert = WalRecordKind::HeapInsert {
            file_id: 1,
            page_num: 0,
            slot: 0,
            row_bytes: vec![1, 2, 3],
        };

        assert!(apply_physical_redo(&mut data.0, 6, &insert).unwrap());
        assert_eq!(page::read_row(&data.0, 0).unwrap(), Some(vec![1, 2, 3]));
        assert_eq!(page::page_lsn(&data.0), 6);

        // Re-applying the same record is skipped (page-LSN already >= record LSN).
        assert!(!apply_physical_redo(&mut data.0, 6, &insert).unwrap());
        assert_eq!(live_row_count(&data.0), 1);
    }

    #[test]
    fn heap_delete_marks_slot_dead() {
        let mut data = PageData::default();
        apply_physical_redo(
            &mut data.0,
            1,
            &WalRecordKind::HeapInit {
                file_id: 1,
                page_num: 0,
            },
        )
        .unwrap();
        apply_physical_redo(
            &mut data.0,
            2,
            &WalRecordKind::HeapInsert {
                file_id: 1,
                page_num: 0,
                slot: 0,
                row_bytes: vec![9],
            },
        )
        .unwrap();

        apply_physical_redo(
            &mut data.0,
            3,
            &WalRecordKind::HeapDelete {
                file_id: 1,
                page_num: 0,
                slot: 0,
            },
        )
        .unwrap();
        assert_eq!(page::read_row(&data.0, 0).unwrap(), None);
    }

    #[test]
    fn heap_update_header_mutates_in_place_and_stamps_lsn() {
        let (mut data, slot) = page_with_one_tuple();

        let record = WalRecordKind::HeapUpdateHeader {
            file_id: 1,
            page_num: 0,
            slot,
            xmax: 99,
            t_ctid: (4, 5),
            infomask: XMAX_COMMITTED,
        };
        assert!(apply_physical_redo(&mut data.0, 3, &record).unwrap());
        assert_eq!(page::page_lsn(&data.0), 3);
        page::validate(&data.0).unwrap();

        // The three header fields are now set; xmin and the row payload survive.
        let bytes = page::read_row(&data.0, slot).unwrap().unwrap();
        let decoded = decode_row(&header_schema(), &bytes).unwrap();
        assert_eq!(decoded.xmax, 99);
        assert_eq!(decoded.t_ctid, (4, 5));
        assert_eq!(decoded.infomask, XMAX_COMMITTED);
        assert_eq!(decoded.xmin, 7);
        assert_eq!(decoded.row, header_row());
    }

    #[test]
    fn heap_update_header_is_idempotent_under_gating() {
        let (mut data, slot) = page_with_one_tuple();
        let record = WalRecordKind::HeapUpdateHeader {
            file_id: 1,
            page_num: 0,
            slot,
            xmax: 99,
            t_ctid: (4, 5),
            infomask: XMAX_COMMITTED,
        };

        assert!(apply_physical_redo(&mut data.0, 3, &record).unwrap());
        let after_first = data.0;

        // Re-applying the same record (page-LSN already >= record LSN) is a no-op
        // and leaves the page byte-for-byte unchanged.
        assert!(!apply_physical_redo(&mut data.0, 3, &record).unwrap());
        assert_eq!(data.0, after_first);

        // A record older than the page-LSN is likewise skipped without mutating.
        let stale = WalRecordKind::HeapUpdateHeader {
            file_id: 1,
            page_num: 0,
            slot,
            xmax: 123,
            t_ctid: (7, 8),
            infomask: 0,
        };
        assert!(!apply_physical_redo(&mut data.0, 2, &stale).unwrap());
        assert_eq!(data.0, after_first);
    }

    #[test]
    fn full_page_image_installs_and_gates() {
        let mut image = PageData::default();
        page::init_page(&mut image.0, 2);
        page::set_page_lsn(&mut image.0, 10);
        let record = WalRecordKind::FullPageImage {
            file_id: 1,
            page_num: 2,
            image: image.0.to_vec(),
        };

        let mut data = PageData::default();
        assert!(apply_physical_redo(&mut data.0, 10, &record).unwrap());
        assert_eq!(data.0, image.0);

        // Same-LSN replay is skipped and leaves the page untouched.
        assert!(!apply_physical_redo(&mut data.0, 10, &record).unwrap());
        assert_eq!(data.0, image.0);
    }

    #[test]
    fn full_page_image_rejects_wrong_size() {
        let mut data = PageData::default();
        let err = apply_physical_redo(
            &mut data.0,
            5,
            &WalRecordKind::FullPageImage {
                file_id: 1,
                page_num: 0,
                image: vec![0u8; 100],
            },
        )
        .unwrap_err();
        assert!(err.message.contains("full-page image"));
    }

    #[test]
    fn heap_insert_rejects_slot_mismatch() {
        let mut data = PageData::default();
        apply_physical_redo(
            &mut data.0,
            5,
            &WalRecordKind::HeapInit {
                file_id: 1,
                page_num: 0,
            },
        )
        .unwrap();

        let err = apply_physical_redo(
            &mut data.0,
            6,
            &WalRecordKind::HeapInsert {
                file_id: 1,
                page_num: 0,
                slot: 5,
                row_bytes: vec![1],
            },
        )
        .unwrap_err();
        assert!(err.message.contains("slot mismatch"));
    }

    #[test]
    fn older_record_is_skipped() {
        let mut data = PageData::default();
        apply_physical_redo(
            &mut data.0,
            10,
            &WalRecordKind::HeapInit {
                file_id: 1,
                page_num: 0,
            },
        )
        .unwrap();

        // A record older than the page-LSN is ignored.
        let applied = apply_physical_redo(
            &mut data.0,
            5,
            &WalRecordKind::HeapInsert {
                file_id: 1,
                page_num: 0,
                slot: 0,
                row_bytes: vec![1],
            },
        )
        .unwrap();
        assert!(!applied);
        assert_eq!(live_row_count(&data.0), 0);
    }
}
