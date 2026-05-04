use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use buffer::PageData;
use common::{DbError, Lsn, PageNum, Result, SqlState, TableId};

use crate::SnapshotMetadata;
use crate::manager::table_file_name;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotPage {
    pub page_num: PageNum,
    pub data: PageData,
}

pub struct SnapshotWriter {
    generation: u64,
    generation_dir: PathBuf,
    table_ids: BTreeSet<TableId>,
    written_paths: Vec<PathBuf>,
    catalog_written: bool,
}

impl SnapshotWriter {
    pub(crate) fn new(generation: u64, generation_dir: PathBuf) -> Result<Self> {
        Ok(Self {
            generation,
            generation_dir,
            table_ids: BTreeSet::new(),
            written_paths: Vec::new(),
            catalog_written: false,
        })
    }

    pub fn write_table(&mut self, table: TableId, pages: &[SnapshotPage]) -> Result<()> {
        if self.table_ids.contains(&table) {
            return Err(corrupt_snapshot(format!(
                "table {table} was already written to snapshot generation {}",
                self.generation
            )));
        }

        let mut sorted_pages = pages.to_vec();
        sorted_pages.sort_by_key(|page| page.page_num);
        for pair in sorted_pages.windows(2) {
            if pair[0].page_num == pair[1].page_num {
                return Err(corrupt_snapshot(format!(
                    "duplicate page number {} for table {table}",
                    pair[0].page_num
                )));
            }
        }

        let page_count = u32::try_from(sorted_pages.len()).map_err(|_| {
            corrupt_snapshot(format!(
                "table {table} has too many pages for snapshot generation {}",
                self.generation
            ))
        })?;
        let path = self.generation_dir.join(table_file_name(table));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|err| {
                DbError::io(format!(
                    "failed to create snapshot table file {}: {err}",
                    path.display()
                ))
            })?;
        file.write_all(&page_count.to_le_bytes()).map_err(|err| {
            DbError::io(format!(
                "failed to write snapshot table file {}: {err}",
                path.display()
            ))
        })?;
        for page in &sorted_pages {
            file.write_all(&page.page_num.to_le_bytes())
                .and_then(|()| file.write_all(&page.data.0))
                .map_err(|err| {
                    DbError::io(format!(
                        "failed to write snapshot table file {}: {err}",
                        path.display()
                    ))
                })?;
        }

        self.table_ids.insert(table);
        self.written_paths.push(path);
        Ok(())
    }

    pub fn write_catalog(&mut self, catalog: &[u8]) -> Result<()> {
        if self.catalog_written {
            return Err(corrupt_snapshot(format!(
                "catalog was already written to snapshot generation {}",
                self.generation
            )));
        }

        let path = self.generation_dir.join("catalog.dat");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|err| {
                DbError::io(format!(
                    "failed to create snapshot catalog file {}: {err}",
                    path.display()
                ))
            })?;
        file.write_all(catalog).map_err(|err| {
            DbError::io(format!(
                "failed to write snapshot catalog file {}: {err}",
                path.display()
            ))
        })?;

        self.catalog_written = true;
        self.written_paths.push(path);
        Ok(())
    }

    pub(crate) fn generation_dir(&self) -> &Path {
        &self.generation_dir
    }

    pub(crate) fn written_paths(&self) -> &[PathBuf] {
        &self.written_paths
    }

    pub(crate) fn metadata(&self, checkpoint_lsn: Lsn) -> Result<SnapshotMetadata> {
        if !self.catalog_written {
            return Err(corrupt_snapshot(format!(
                "snapshot generation {} is missing catalog.dat",
                self.generation
            )));
        }
        Ok(SnapshotMetadata {
            generation: self.generation,
            checkpoint_lsn,
            tables: self.table_ids.iter().copied().collect(),
        })
    }
}

fn corrupt_snapshot(message: impl Into<String>) -> DbError {
    DbError::storage(SqlState::InternalError, message)
}
