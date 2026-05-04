use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use buffer::{BufferPool, PAGE_SIZE, PageData};
use common::{DbError, Lsn, PageNum, Result, SqlState, TableId};

use crate::manifest::{decode_manifest, encode_manifest};
use crate::{SnapshotMetadata, SnapshotPage, SnapshotWriter};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadedSnapshot {
    pub metadata: SnapshotMetadata,
    pub catalog_bytes: Vec<u8>,
}

pub trait SnapshotManager: Send + Sync {
    fn load_current(&self, buffer_pool: &dyn BufferPool) -> Result<Option<LoadedSnapshot>>;
    fn current_table_pages(&self, table: TableId) -> Result<Vec<SnapshotPage>>;
    fn begin_snapshot(&self) -> Result<SnapshotWriter>;
    fn commit_snapshot(
        &self,
        writer: SnapshotWriter,
        checkpoint_lsn: Lsn,
    ) -> Result<SnapshotMetadata>;
    fn cleanup_old_snapshots(&self) -> Result<()>;
}

pub struct FileSnapshotManager {
    data_dir: PathBuf,
}

impl FileSnapshotManager {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(data_dir.as_ref()).map_err(|err| {
            DbError::io(format!(
                "failed to create snapshot data directory {}: {err}",
                data_dir.as_ref().display()
            ))
        })?;
        Ok(Self {
            data_dir: data_dir.as_ref().to_path_buf(),
        })
    }

    fn manifest_path(&self) -> PathBuf {
        self.data_dir.join("manifest.dat")
    }

    fn manifest_tmp_path(&self) -> PathBuf {
        self.data_dir.join("manifest.dat.tmp")
    }

    fn snapshot_dir(&self, generation: u64) -> PathBuf {
        self.data_dir.join(snapshot_dir_name(generation))
    }

    fn load_manifest(&self) -> Result<Option<SnapshotMetadata>> {
        match fs::read(self.manifest_path()) {
            Ok(bytes) => decode_manifest(&bytes).map(Some),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(DbError::io(format!(
                "failed to read snapshot manifest {}: {err}",
                self.manifest_path().display()
            ))),
        }
    }

    fn next_generation(&self) -> Result<u64> {
        let current_generation = self
            .load_manifest()?
            .map(|metadata| metadata.generation)
            .unwrap_or(0);
        let existing_generation = existing_snapshot_generations(&self.data_dir)?
            .into_iter()
            .max()
            .unwrap_or(0);
        Ok(current_generation.max(existing_generation) + 1)
    }
}

impl SnapshotManager for FileSnapshotManager {
    fn load_current(&self, buffer_pool: &dyn BufferPool) -> Result<Option<LoadedSnapshot>> {
        let Some(metadata) = self.load_manifest()? else {
            return Ok(None);
        };

        let generation_dir = self.snapshot_dir(metadata.generation);
        let mut table_pages = Vec::with_capacity(metadata.tables.len());
        for table in &metadata.tables {
            table_pages.push((
                *table,
                read_table_pages(&generation_dir.join(table_file_name(*table)))?,
            ));
        }
        let catalog_bytes = read_required_file(&generation_dir.join("catalog.dat"), "catalog")?;

        for (table, pages) in &table_pages {
            for page in pages {
                buffer_pool.load_page(*table, page.page_num, page.data.clone())?;
            }
        }
        verify_loaded_pages_resident(buffer_pool, &table_pages)?;

        Ok(Some(LoadedSnapshot {
            metadata,
            catalog_bytes,
        }))
    }

    fn current_table_pages(&self, table: TableId) -> Result<Vec<SnapshotPage>> {
        let Some(metadata) = self.load_manifest()? else {
            return Ok(Vec::new());
        };
        if !metadata.tables.contains(&table) {
            return Ok(Vec::new());
        }

        read_table_pages(
            &self
                .snapshot_dir(metadata.generation)
                .join(table_file_name(table)),
        )
    }

    fn begin_snapshot(&self) -> Result<SnapshotWriter> {
        let generation = self.next_generation()?;
        let generation_dir = self.snapshot_dir(generation);
        fs::create_dir(&generation_dir).map_err(|err| {
            DbError::io(format!(
                "failed to create snapshot generation directory {}: {err}",
                generation_dir.display()
            ))
        })?;
        fsync_dir(&self.data_dir)?;
        SnapshotWriter::new(generation, generation_dir)
    }

    fn commit_snapshot(
        &self,
        writer: SnapshotWriter,
        checkpoint_lsn: Lsn,
    ) -> Result<SnapshotMetadata> {
        let metadata = writer.metadata(checkpoint_lsn)?;

        for path in writer.written_paths() {
            fsync_file(path)?;
        }
        fsync_dir(writer.generation_dir())?;

        let manifest_tmp_path = self.manifest_tmp_path();
        let manifest_bytes = encode_manifest(&metadata)?;
        {
            let mut manifest_tmp = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&manifest_tmp_path)
                .map_err(|err| {
                    DbError::io(format!(
                        "failed to open temporary snapshot manifest {}: {err}",
                        manifest_tmp_path.display()
                    ))
                })?;
            manifest_tmp.write_all(&manifest_bytes).map_err(|err| {
                DbError::io(format!(
                    "failed to write temporary snapshot manifest {}: {err}",
                    manifest_tmp_path.display()
                ))
            })?;
            manifest_tmp.sync_all().map_err(|err| {
                DbError::io(format!(
                    "failed to fsync temporary snapshot manifest {}: {err}",
                    manifest_tmp_path.display()
                ))
            })?;
        }

        fs::rename(&manifest_tmp_path, self.manifest_path()).map_err(|err| {
            DbError::io(format!(
                "failed to replace snapshot manifest {} with {}: {err}",
                self.manifest_path().display(),
                manifest_tmp_path.display()
            ))
        })?;
        fsync_dir(&self.data_dir)?;

        Ok(metadata)
    }

    fn cleanup_old_snapshots(&self) -> Result<()> {
        let current_generation = self.load_manifest()?.map(|metadata| metadata.generation);
        for entry in fs::read_dir(&self.data_dir).map_err(|err| {
            DbError::io(format!(
                "failed to read snapshot data directory {}: {err}",
                self.data_dir.display()
            ))
        })? {
            let entry = entry.map_err(|err| {
                DbError::io(format!(
                    "failed to read snapshot data directory entry {}: {err}",
                    self.data_dir.display()
                ))
            })?;
            let file_type = entry.file_type().map_err(|err| {
                DbError::io(format!(
                    "failed to inspect snapshot path {}: {err}",
                    entry.path().display()
                ))
            })?;
            if !file_type.is_dir() {
                continue;
            }
            let Some(generation) = parse_snapshot_dir_name(&entry.file_name()) else {
                continue;
            };
            if Some(generation) != current_generation {
                fs::remove_dir_all(entry.path()).map_err(|err| {
                    DbError::io(format!(
                        "failed to remove old snapshot directory {}: {err}",
                        entry.path().display()
                    ))
                })?;
            }
        }
        fsync_dir(&self.data_dir)
    }
}

pub(crate) fn table_file_name(table: TableId) -> String {
    format!("table_{table}.tbl")
}

fn snapshot_dir_name(generation: u64) -> String {
    format!("snap_{generation}")
}

fn existing_snapshot_generations(data_dir: &Path) -> Result<Vec<u64>> {
    let mut generations = Vec::new();
    for entry in fs::read_dir(data_dir).map_err(|err| {
        DbError::io(format!(
            "failed to read snapshot data directory {}: {err}",
            data_dir.display()
        ))
    })? {
        let entry = entry.map_err(|err| {
            DbError::io(format!(
                "failed to read snapshot data directory entry {}: {err}",
                data_dir.display()
            ))
        })?;
        let file_type = entry.file_type().map_err(|err| {
            DbError::io(format!(
                "failed to inspect snapshot path {}: {err}",
                entry.path().display()
            ))
        })?;
        if file_type.is_dir()
            && let Some(generation) = parse_snapshot_dir_name(&entry.file_name())
        {
            generations.push(generation);
        }
    }
    Ok(generations)
}

fn parse_snapshot_dir_name(name: &std::ffi::OsStr) -> Option<u64> {
    name.to_str()?.strip_prefix("snap_")?.parse().ok()
}

fn read_table_pages(path: &Path) -> Result<Vec<SnapshotPage>> {
    let bytes = read_required_file(path, "table snapshot")?;
    let mut offset = 0;
    let page_count = read_u32(&bytes, &mut offset, "table page count")? as usize;
    let expected_len = 4usize
        .checked_add(page_count.checked_mul(4 + PAGE_SIZE).ok_or_else(|| {
            corrupt_snapshot(format!(
                "table snapshot {} page count overflows",
                path.display()
            ))
        })?)
        .ok_or_else(|| {
            corrupt_snapshot(format!("table snapshot {} size overflows", path.display()))
        })?;
    if bytes.len() != expected_len {
        return Err(corrupt_snapshot(format!(
            "table snapshot {} has {} bytes but expected {expected_len}",
            path.display(),
            bytes.len()
        )));
    }

    let mut pages = Vec::with_capacity(page_count);
    let mut previous_page_num: Option<PageNum> = None;
    for _ in 0..page_count {
        let page_num = read_u32(&bytes, &mut offset, "table page number")?;
        if previous_page_num >= Some(page_num) {
            return Err(corrupt_snapshot(format!(
                "table snapshot {} has duplicate or unsorted page number {page_num}",
                path.display()
            )));
        }
        previous_page_num = Some(page_num);

        let end = offset + PAGE_SIZE;
        let raw = &bytes[offset..end];
        let mut data = PageData::default();
        data.0.copy_from_slice(raw);
        offset = end;
        pages.push(SnapshotPage { page_num, data });
    }
    Ok(pages)
}

fn verify_loaded_pages_resident(
    buffer_pool: &dyn BufferPool,
    table_pages: &[(TableId, Vec<SnapshotPage>)],
) -> Result<()> {
    let resident: BTreeMap<_, _> = buffer_pool
        .iter_pages()?
        .map(|info| ((info.file_id, info.page_num), info.data))
        .collect();

    for (table, pages) in table_pages {
        for page in pages {
            let key = (*table, page.page_num);
            if resident.get(&key) != Some(&page.data) {
                return Err(corrupt_snapshot(format!(
                    "snapshot page table={table} page={} was not resident after load_current; buffer pool is too small for v1 recovery directory rebuild",
                    page.page_num
                )));
            }
        }
    }
    Ok(())
}

fn read_u32(bytes: &[u8], offset: &mut usize, field: &str) -> Result<u32> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| corrupt_snapshot(format!("{field} offset overflows")))?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt_snapshot(format!("missing {field}")))?;
    let mut value = [0; 4];
    value.copy_from_slice(raw);
    *offset = end;
    Ok(u32::from_le_bytes(value))
}

fn read_required_file(path: &Path, description: &str) -> Result<Vec<u8>> {
    fs::read(path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            corrupt_snapshot(format!(
                "missing {description} file for current snapshot: {}",
                path.display()
            ))
        } else {
            DbError::io(format!(
                "failed to read {} {}: {err}",
                description,
                path.display()
            ))
        }
    })
}

fn fsync_file(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|err| {
            DbError::io(format!(
                "failed to fsync snapshot file {}: {err}",
                path.display()
            ))
        })
}

fn fsync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|dir| dir.sync_all())
        .map_err(|err| {
            DbError::io(format!(
                "failed to fsync snapshot directory {}: {err}",
                path.display()
            ))
        })
}

fn corrupt_snapshot(message: impl Into<String>) -> DbError {
    DbError::storage(SqlState::InternalError, message)
}
