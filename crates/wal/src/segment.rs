#![cfg_attr(
    not(test),
    deny(
        clippy::arithmetic_side_effects,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss,
        clippy::indexing_slicing
    )
)]

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use common::{CheckedSliceReader, DbError, Lsn, Result, SqlState};

pub(crate) const WAL_FORMAT_VERSION: u32 = 3;
pub(crate) const SEGMENT_PAYLOAD_BYTES: u64 = 16 * 1024 * 1024;
const SEGMENT_MAGIC: &[u8; 4] = b"SGWS";
pub(crate) const SEGMENT_HEADER_LEN: u64 = 32;
const META_MAGIC: &[u8; 4] = b"SGWM";
const META_VERSION: u32 = 2;
const META_LEN: usize = 36;

#[derive(Clone, Copy, Debug)]
pub(crate) struct WalMeta {
    pub(crate) replay_floor: Lsn,
    pub(crate) durable_end: Lsn,
}

pub(crate) fn wal_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("wal")
}

pub(crate) fn segment_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("{number:016X}.wal"))
}

fn meta_path(dir: &Path) -> PathBuf {
    dir.join("wal.meta")
}

pub(crate) fn initialize(data_dir: &Path) -> Result<()> {
    let dir = wal_dir(data_dir);
    if data_dir.join("wal.dat").exists() {
        return Err(wal_error(
            "unsupported legacy single-file WAL; rebuild the data directory",
        ));
    }
    let has_durable_state =
        data_dir.join("manifest.dat").exists() || data_dir.join("clog.dat").exists();
    if !dir.exists() && has_durable_state {
        return Err(wal_error(
            "segmented WAL is missing beside existing durable database state",
        ));
    }
    if !dir.exists() {
        fs::create_dir_all(&dir).map_err(|err| {
            DbError::io(format!(
                "failed to create WAL directory {}: {err}",
                dir.display()
            ))
        })?;
    }

    let metadata_exists = meta_path(&dir).exists();
    if metadata_exists {
        let meta = load_meta(&dir)?;
        let first_retained = meta.replay_floor / SEGMENT_PAYLOAD_BYTES;
        let first_path = segment_path(&dir, first_retained);
        if first_path.exists()
            && meta.replay_floor == 0
            && meta.durable_end == 0
            && !has_durable_state
        {
            fs::remove_file(&first_path).map_err(|err| {
                DbError::io(format!(
                    "failed to remove incomplete initial WAL segment {}: {err}",
                    first_path.display()
                ))
            })?;
            sync_dir(&dir)?;
            create_segment(&dir, 0)?;
        } else if !first_path.exists() {
            if meta.replay_floor != 0 || has_durable_state {
                return Err(wal_error(
                    "WAL segment is missing beside initialized durable state",
                ));
            }
            create_segment(&dir, 0)?;
        }
    } else {
        if has_durable_state {
            return Err(wal_error(
                "WAL metadata is missing beside existing durable database state",
            ));
        }
        if segment_path(&dir, 0).exists() {
            open_segment(&dir, 0)?;
        }
        write_meta(
            &dir,
            WalMeta {
                replay_floor: 0,
                durable_end: 0,
            },
        )?;
        if !segment_path(&dir, 0).exists() {
            create_segment(&dir, 0)?;
        }
    }
    sync_dir(data_dir)?;
    Ok(())
}

pub(crate) fn load_meta(dir: &Path) -> Result<WalMeta> {
    let path = meta_path(dir);
    let bytes = fs::read(&path).map_err(|err| {
        DbError::io(format!(
            "failed to read WAL metadata {}: {err}",
            path.display()
        ))
    })?;
    if bytes.len() != META_LEN {
        return Err(wal_error("WAL metadata length mismatch"));
    }
    let mut reader = CheckedSliceReader::new(&bytes);
    let magic = reader
        .take(4)
        .map_err(|_| wal_error("WAL metadata is incomplete"))?;
    if magic != META_MAGIC {
        return Err(wal_error("WAL metadata magic mismatch"));
    }
    let version = read_u32(&mut reader, "metadata version")?;
    if version != META_VERSION {
        return Err(wal_error(format!(
            "unsupported WAL metadata version {version}"
        )));
    }
    let wal_version = read_u32(&mut reader, "format version")?;
    if wal_version != WAL_FORMAT_VERSION {
        return Err(wal_error(format!(
            "unsupported WAL format version {wal_version}"
        )));
    }
    let payload_bytes = read_u32(&mut reader, "segment size")?;
    let expected_payload = u32::try_from(SEGMENT_PAYLOAD_BYTES)
        .map_err(|_| wal_error("WAL segment size does not fit u32"))?;
    if payload_bytes != expected_payload {
        return Err(wal_error("WAL segment size mismatch"));
    }
    let replay_floor = reader
        .read_u64_le()
        .map_err(|_| wal_error("WAL replay floor is incomplete"))?;
    let durable_end = reader
        .read_u64_le()
        .map_err(|_| wal_error("WAL durable end is incomplete"))?;
    let stored_crc = read_u32(&mut reader, "metadata checksum")?;
    let checksum_end = META_LEN
        .checked_sub(4)
        .ok_or_else(|| wal_error("WAL metadata checksum range underflows"))?;
    let checksummed = bytes
        .get(..checksum_end)
        .ok_or_else(|| wal_error("WAL metadata checksum range is invalid"))?;
    if crc32fast::hash(checksummed) != stored_crc {
        return Err(wal_error("WAL metadata checksum mismatch"));
    }
    Ok(WalMeta {
        replay_floor,
        durable_end,
    })
}

pub(crate) struct MetaWriteFailure {
    pub(crate) error: DbError,
    pub(crate) replacement_visible: bool,
}

pub(crate) fn write_meta(dir: &Path, meta: WalMeta) -> Result<()> {
    write_meta_with_outcome(dir, meta).map_err(|failure| failure.error)
}

pub(crate) fn write_meta_with_outcome(
    dir: &Path,
    meta: WalMeta,
) -> std::result::Result<(), MetaWriteFailure> {
    let mut replacement_visible = false;
    let result = (|| -> Result<()> {
        let mut bytes = Vec::with_capacity(META_LEN);
        bytes.extend_from_slice(META_MAGIC);
        bytes.extend_from_slice(&META_VERSION.to_le_bytes());
        bytes.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
        let payload_bytes = u32::try_from(SEGMENT_PAYLOAD_BYTES)
            .map_err(|_| wal_error("WAL segment size does not fit u32"))?;
        bytes.extend_from_slice(&payload_bytes.to_le_bytes());
        bytes.extend_from_slice(&meta.replay_floor.to_le_bytes());
        bytes.extend_from_slice(&meta.durable_end.to_le_bytes());
        bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
        let path = meta_path(dir);
        let tmp = dir.join("wal.meta.tmp");
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|err| {
                    DbError::io(format!(
                        "failed to open WAL metadata {}: {err}",
                        tmp.display()
                    ))
                })?;
            file.write_all(&bytes).map_err(|err| {
                DbError::io(format!(
                    "failed to write WAL metadata {}: {err}",
                    tmp.display()
                ))
            })?;
            file.sync_all().map_err(|err| {
                DbError::io(format!(
                    "failed to fsync WAL metadata {}: {err}",
                    tmp.display()
                ))
            })?;
        }
        fs::rename(&tmp, &path).map_err(|err| {
            DbError::io(format!(
                "failed to replace WAL metadata {} with {}: {err}",
                path.display(),
                tmp.display()
            ))
        })?;
        replacement_visible = true;
        sync_dir(dir)
    })();
    result.map_err(|error| MetaWriteFailure {
        error,
        replacement_visible,
    })
}

pub(crate) fn create_segment(dir: &Path, number: u64) -> Result<File> {
    let path = segment_path(dir, number);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|err| {
            DbError::io(format!(
                "failed to create WAL segment {}: {err}",
                path.display()
            ))
        })?;
    let creation = (|| {
        let header = encode_segment_header(number)?;
        file.write_all(&header).map_err(|err| {
            DbError::io(format!(
                "failed to write WAL segment header {}: {err}",
                path.display()
            ))
        })?;
        file.sync_all().map_err(|err| {
            DbError::io(format!(
                "failed to fsync WAL segment header {}: {err}",
                path.display()
            ))
        })
    })();
    if let Err(err) = creation {
        drop(file);
        fs::remove_file(&path).map_err(|cleanup| {
            DbError::io(format!(
                "failed to remove incomplete WAL segment {} after {err}: {cleanup}",
                path.display()
            ))
        })?;
        sync_dir(dir)?;
        return Err(err);
    }
    sync_dir(dir)?;
    Ok(file)
}

pub(crate) fn open_segment(dir: &Path, number: u64) -> Result<File> {
    let path = segment_path(dir, number);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|err| {
            DbError::io(format!(
                "failed to open WAL segment {}: {err}",
                path.display()
            ))
        })?;
    validate_segment_header(&mut file, number, &path)?;
    Ok(file)
}

pub(crate) fn discover_end(dir: &Path, replay_floor: Lsn, durable_end: Lsn) -> Result<Lsn> {
    let first = replay_floor / SEGMENT_PAYLOAD_BYTES;
    let durable_segment = durable_end / SEGMENT_PAYLOAD_BYTES;
    let last_with_durable_bytes =
        if durable_end > replay_floor && durable_end.is_multiple_of(SEGMENT_PAYLOAD_BYTES) {
            durable_segment
                .checked_sub(1)
                .ok_or_else(|| wal_error("WAL durable segment underflow"))?
        } else {
            durable_segment
        };
    // A successor whose logical start is at or beyond the durable end contains no
    // acknowledged bytes. Remove it by filename before opening its header: a crash
    // during rollover may have left a full-length but torn header, which is tail to
    // discard rather than corruption in the durable stream.
    let mut removed = false;
    for number in segment_numbers(dir)? {
        if number > last_with_durable_bytes {
            remove_segment(dir, number, "remove unacknowledged tail")?;
            removed = true;
        }
    }
    if removed {
        sync_dir(dir)?;
    }
    let mut highest = None;
    for number in segment_numbers(dir)? {
        if number >= first {
            highest = Some(highest.map_or(number, |current: u64| current.max(number)));
        }
    }
    let mut highest = highest.ok_or_else(|| wal_error("WAL has no segment at its replay floor"))?;
    loop {
        let path = segment_path(dir, highest);
        let len = path
            .metadata()
            .map_err(|err| {
                DbError::io(format!(
                    "failed to stat WAL segment {}: {err}",
                    path.display()
                ))
            })?
            .len();
        if len >= SEGMENT_HEADER_LEN || highest == first {
            break;
        }
        remove_segment(dir, highest, "remove incomplete final")?;
        sync_dir(dir)?;
        highest = highest
            .checked_sub(1)
            .ok_or_else(|| wal_error("WAL segment number underflow"))?;
    }
    let mut number = first;
    loop {
        let path = segment_path(dir, number);
        let file = open_segment(dir, number)?;
        let len = file
            .metadata()
            .map_err(|err| {
                DbError::io(format!(
                    "failed to stat WAL segment {}: {err}",
                    path.display()
                ))
            })?
            .len();
        let payload_len = len.checked_sub(SEGMENT_HEADER_LEN).ok_or_else(|| {
            wal_error(format!(
                "WAL segment {} is shorter than its header",
                path.display()
            ))
        })?;
        if payload_len > SEGMENT_PAYLOAD_BYTES {
            return Err(wal_error(format!(
                "WAL segment {} exceeds its fixed size",
                path.display()
            )));
        }
        if number < highest && payload_len != SEGMENT_PAYLOAD_BYTES {
            return Err(wal_error(format!(
                "non-final WAL segment {} is incomplete",
                path.display()
            )));
        }
        if number == highest {
            let base = number
                .checked_mul(SEGMENT_PAYLOAD_BYTES)
                .ok_or_else(|| wal_error("WAL segment position overflow"))?;
            return base
                .checked_add(payload_len)
                .ok_or_else(|| wal_error("WAL end position overflow"));
        }
        number = number
            .checked_add(1)
            .ok_or_else(|| wal_error("WAL segment number overflow"))?;
    }
}

fn segment_numbers(dir: &Path) -> Result<Vec<u64>> {
    let mut numbers = Vec::new();
    for entry in fs::read_dir(dir).map_err(|err| {
        DbError::io(format!(
            "failed to list WAL directory {}: {err}",
            dir.display()
        ))
    })? {
        let entry = entry.map_err(|err| DbError::io(format!("failed to list WAL entry: {err}")))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(".wal") else {
            continue;
        };
        if stem.len() != 16 {
            return Err(wal_error(format!("invalid WAL segment name {name}")));
        }
        let number = u64::from_str_radix(stem, 16)
            .map_err(|_| wal_error(format!("invalid WAL segment name {name}")))?;
        numbers
            .try_reserve(1)
            .map_err(|_| wal_error("cannot allocate WAL segment list"))?;
        numbers.push(number);
    }
    Ok(numbers)
}

pub(crate) struct SegmentReader {
    dir: PathBuf,
    position: Lsn,
    end: Lsn,
    file: Option<File>,
    file_number: Option<u64>,
}

impl SegmentReader {
    pub(crate) fn new(dir: PathBuf, position: Lsn, end: Lsn) -> Self {
        Self {
            dir,
            position,
            end,
            file: None,
            file_number: None,
        }
    }

    pub(crate) fn position(&self) -> Lsn {
        self.position
    }
    pub(crate) fn remaining(&self) -> u64 {
        self.end.saturating_sub(self.position)
    }

    pub(crate) fn read_exact_vec(&mut self, length: usize) -> Result<Option<Vec<u8>>> {
        let length_u64 =
            u64::try_from(length).map_err(|_| wal_error("WAL read length does not fit u64"))?;
        if self.remaining() < length_u64 {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| wal_error("cannot allocate WAL record buffer"))?;
        bytes.resize(length, 0);
        self.read_exact_into(&mut bytes)?;
        Ok(Some(bytes))
    }

    fn read_exact_into(&mut self, mut target: &mut [u8]) -> Result<()> {
        while !target.is_empty() {
            let number = self.position / SEGMENT_PAYLOAD_BYTES;
            let offset = self.position % SEGMENT_PAYLOAD_BYTES;
            self.ensure_file(number, offset)?;
            let available = SEGMENT_PAYLOAD_BYTES
                .checked_sub(offset)
                .ok_or_else(|| wal_error("WAL segment offset overflow"))?;
            let target_len = u64::try_from(target.len())
                .map_err(|_| wal_error("WAL read length does not fit u64"))?;
            let take_u64 = available.min(target_len);
            let take = usize::try_from(take_u64)
                .map_err(|_| wal_error("WAL read chunk does not fit usize"))?;
            let (chunk, rest) = target.split_at_mut(take);
            let file = self
                .file
                .as_mut()
                .ok_or_else(|| wal_error("WAL segment reader has no file"))?;
            file.read_exact(chunk)
                .map_err(|err| DbError::io(format!("failed to read WAL segment: {err}")))?;
            self.position = self
                .position
                .checked_add(take_u64)
                .ok_or_else(|| wal_error("WAL reader position overflow"))?;
            target = rest;
        }
        Ok(())
    }

    fn ensure_file(&mut self, number: u64, offset: u64) -> Result<()> {
        if self.file_number == Some(number) {
            return Ok(());
        }
        let mut file = open_segment(&self.dir, number)?;
        let physical = SEGMENT_HEADER_LEN
            .checked_add(offset)
            .ok_or_else(|| wal_error("WAL physical offset overflow"))?;
        file.seek(SeekFrom::Start(physical))
            .map_err(|err| DbError::io(format!("failed to seek WAL segment: {err}")))?;
        self.file = Some(file);
        self.file_number = Some(number);
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn write_stream(dir: &Path, start: Lsn, bytes: &[u8]) -> Result<File> {
    let mut position = start;
    let mut remaining = bytes;
    let mut active = open_or_create_for_position(dir, position)?;
    while !remaining.is_empty() {
        let offset = position % SEGMENT_PAYLOAD_BYTES;
        if offset == 0 && position != 0 {
            active
                .sync_all()
                .map_err(|err| DbError::io(format!("failed to fsync filled WAL segment: {err}")))?;
            let number = position / SEGMENT_PAYLOAD_BYTES;
            active = match open_segment(dir, number) {
                Ok(file) => file,
                Err(_) if !segment_path(dir, number).exists() => create_segment(dir, number)?,
                Err(err) => return Err(err),
            };
        }
        let physical = SEGMENT_HEADER_LEN
            .checked_add(offset)
            .ok_or_else(|| wal_error("WAL physical offset overflow"))?;
        active
            .seek(SeekFrom::Start(physical))
            .map_err(|err| DbError::io(format!("failed to seek WAL segment for append: {err}")))?;
        let available = SEGMENT_PAYLOAD_BYTES
            .checked_sub(offset)
            .ok_or_else(|| wal_error("WAL segment offset overflow"))?;
        let remaining_len = u64::try_from(remaining.len())
            .map_err(|_| wal_error("WAL append length does not fit u64"))?;
        let take_u64 = available.min(remaining_len);
        let take = usize::try_from(take_u64)
            .map_err(|_| wal_error("WAL append chunk does not fit usize"))?;
        let chunk = remaining
            .get(..take)
            .ok_or_else(|| wal_error("WAL append chunk is invalid"))?;
        active
            .write_all(chunk)
            .map_err(|err| DbError::io(format!("failed to append WAL segment: {err}")))?;
        position = position
            .checked_add(take_u64)
            .ok_or_else(|| wal_error("WAL append position overflow"))?;
        remaining = remaining
            .get(take..)
            .ok_or_else(|| wal_error("WAL append remainder is invalid"))?;
    }
    Ok(active)
}

pub(crate) fn append_stream(
    dir: &Path,
    active: &mut File,
    active_number: &mut u64,
    start: Lsn,
    bytes: &[u8],
) -> Result<()> {
    let mut position = start;
    let mut remaining = bytes;
    while !remaining.is_empty() {
        let number = position / SEGMENT_PAYLOAD_BYTES;
        if number != *active_number {
            // The previous segment must be durable before its successor's durable
            // header makes that successor discoverable after a crash. Otherwise a
            // torn final write in the previous segment can look like corruption in
            // the middle of the retained stream on restart.
            active.sync_all().map_err(|err| {
                DbError::io(format!(
                    "failed to fsync filled WAL segment before rollover: {err}"
                ))
            })?;
            *active = match open_segment(dir, number) {
                Ok(file) => file,
                Err(_) if !segment_path(dir, number).exists() => create_segment(dir, number)?,
                Err(err) => return Err(err),
            };
            *active_number = number;
        }
        let offset = position % SEGMENT_PAYLOAD_BYTES;
        let physical = SEGMENT_HEADER_LEN
            .checked_add(offset)
            .ok_or_else(|| wal_error("WAL physical offset overflow"))?;
        active
            .seek(SeekFrom::Start(physical))
            .map_err(|err| DbError::io(format!("failed to seek WAL segment for append: {err}")))?;
        let available = SEGMENT_PAYLOAD_BYTES
            .checked_sub(offset)
            .ok_or_else(|| wal_error("WAL segment offset overflow"))?;
        let remaining_len = u64::try_from(remaining.len())
            .map_err(|_| wal_error("WAL append length does not fit u64"))?;
        let take_u64 = available.min(remaining_len);
        let take = usize::try_from(take_u64)
            .map_err(|_| wal_error("WAL append chunk does not fit usize"))?;
        let chunk = remaining
            .get(..take)
            .ok_or_else(|| wal_error("WAL append chunk is invalid"))?;
        active
            .write_all(chunk)
            .map_err(|err| DbError::io(format!("failed to append WAL segment: {err}")))?;
        position = position
            .checked_add(take_u64)
            .ok_or_else(|| wal_error("WAL append position overflow"))?;
        remaining = remaining
            .get(take..)
            .ok_or_else(|| wal_error("WAL append remainder is invalid"))?;
    }
    Ok(())
}

#[cfg(test)]
fn open_or_create_for_position(dir: &Path, position: Lsn) -> Result<File> {
    open_segment(dir, active_segment_number(position)?)
}

pub(crate) fn active_segment_number(position: Lsn) -> Result<u64> {
    let number = if position > 0 && position.is_multiple_of(SEGMENT_PAYLOAD_BYTES) {
        position
            .checked_div(SEGMENT_PAYLOAD_BYTES)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| wal_error("WAL active segment underflow"))?
    } else {
        position / SEGMENT_PAYLOAD_BYTES
    };
    Ok(number)
}

pub(crate) fn truncate_stream(
    dir: &Path,
    position: Lsn,
    old_end: Lsn,
    replay_floor: Lsn,
) -> Result<(File, u64)> {
    // At an exact boundary the preceding segment normally owns `position`. Once
    // recycling advances the replay floor to that boundary, however, that segment
    // is intentionally gone and the empty successor is the retained active segment.
    let keep_preceding =
        position > 0 && position.is_multiple_of(SEGMENT_PAYLOAD_BYTES) && replay_floor < position;
    let keep_number = if keep_preceding {
        position
            .checked_div(SEGMENT_PAYLOAD_BYTES)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| wal_error("WAL truncate segment underflow"))?
    } else {
        position / SEGMENT_PAYLOAD_BYTES
    };
    let keep_payload = if keep_preceding {
        SEGMENT_PAYLOAD_BYTES
    } else {
        position % SEGMENT_PAYLOAD_BYTES
    };
    let old_last = old_end / SEGMENT_PAYLOAD_BYTES;
    let first_remove = keep_number
        .checked_add(1)
        .ok_or_else(|| wal_error("WAL segment number overflow"))?;
    if first_remove <= old_last {
        let mut number = old_last;
        loop {
            remove_segment(dir, number, "remove")?;
            if number == first_remove {
                break;
            }
            number = number
                .checked_sub(1)
                .ok_or_else(|| wal_error("WAL segment number underflow"))?;
        }
        sync_dir(dir)?;
    }
    let file = match open_segment(dir, keep_number) {
        Ok(file) => file,
        Err(_) if !segment_path(dir, keep_number).exists() => create_segment(dir, keep_number)?,
        Err(err) => return Err(err),
    };
    let keep_len = SEGMENT_HEADER_LEN
        .checked_add(keep_payload)
        .ok_or_else(|| wal_error("WAL truncate length overflow"))?;
    file.set_len(keep_len)
        .map_err(|err| DbError::io(format!("failed to truncate WAL segment: {err}")))?;
    file.sync_all()
        .map_err(|err| DbError::io(format!("failed to fsync truncated WAL segment: {err}")))?;
    Ok((file, keep_number))
}

pub(crate) fn recycle_segments(dir: &Path, old_floor: Lsn, floor: Lsn) -> Result<()> {
    let first_keep = floor / SEGMENT_PAYLOAD_BYTES;
    let mut number = old_floor / SEGMENT_PAYLOAD_BYTES;
    while number < first_keep {
        remove_segment(dir, number, "recycle")?;
        number = number
            .checked_add(1)
            .ok_or_else(|| wal_error("WAL segment number overflow"))?;
    }
    sync_dir(dir)
}

pub(crate) fn recycle_orphaned_segments(dir: &Path, floor: Lsn) -> Result<()> {
    let first_keep = floor / SEGMENT_PAYLOAD_BYTES;
    let mut removed = false;
    for number in segment_numbers(dir)? {
        if number < first_keep {
            remove_segment(dir, number, "recycle")?;
            removed = true;
        }
    }
    if removed {
        sync_dir(dir)?;
    }
    Ok(())
}

fn remove_segment(dir: &Path, number: u64, action: &str) -> Result<()> {
    let path = segment_path(dir, number);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(DbError::io(format!(
            "failed to {action} WAL segment {}: {err}",
            path.display()
        ))),
    }
}

fn encode_segment_header(number: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(
        usize::try_from(SEGMENT_HEADER_LEN)
            .map_err(|_| wal_error("segment header length does not fit usize"))?,
    );
    bytes.extend_from_slice(SEGMENT_MAGIC);
    bytes.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&number.to_le_bytes());
    let start = number
        .checked_mul(SEGMENT_PAYLOAD_BYTES)
        .ok_or_else(|| wal_error("WAL segment start overflow"))?;
    bytes.extend_from_slice(&start.to_le_bytes());
    let payload = u32::try_from(SEGMENT_PAYLOAD_BYTES)
        .map_err(|_| wal_error("WAL segment size does not fit u32"))?;
    bytes.extend_from_slice(&payload.to_le_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    Ok(bytes)
}

fn validate_segment_header(file: &mut File, number: u64, path: &Path) -> Result<()> {
    file.seek(SeekFrom::Start(0)).map_err(|err| {
        DbError::io(format!(
            "failed to seek WAL segment {}: {err}",
            path.display()
        ))
    })?;
    let header_len = usize::try_from(SEGMENT_HEADER_LEN)
        .map_err(|_| wal_error("WAL segment header length does not fit usize"))?;
    let mut bytes = vec![0; header_len];
    file.read_exact(&mut bytes).map_err(|err| {
        DbError::wal(
            SqlState::InternalError,
            format!("incomplete WAL segment header {}: {err}", path.display()),
        )
    })?;
    if bytes.get(..4) != Some(SEGMENT_MAGIC.as_slice()) {
        return Err(wal_error(format!(
            "WAL segment header magic mismatch: {}",
            path.display()
        )));
    }
    let checksum_start = header_len
        .checked_sub(4)
        .ok_or_else(|| wal_error("WAL segment header checksum range underflows"))?;
    let stored = bytes
        .get(checksum_start..)
        .ok_or_else(|| wal_error("WAL segment header checksum is missing"))?;
    let stored = u32::from_le_bytes(
        stored
            .try_into()
            .map_err(|_| wal_error("WAL segment checksum is malformed"))?,
    );
    if crc32fast::hash(
        bytes
            .get(..checksum_start)
            .ok_or_else(|| wal_error("WAL segment checksum range is invalid"))?,
    ) != stored
    {
        return Err(wal_error(format!(
            "WAL segment header checksum mismatch: {}",
            path.display()
        )));
    }
    let mut reader = CheckedSliceReader::at(&bytes, 4)
        .map_err(|_| wal_error("WAL segment header is malformed"))?;
    if read_u32(&mut reader, "segment format")? != WAL_FORMAT_VERSION {
        return Err(wal_error("unsupported WAL segment format"));
    }
    let stored_number = reader
        .read_u64_le()
        .map_err(|_| wal_error("WAL segment number is incomplete"))?;
    let start = reader
        .read_u64_le()
        .map_err(|_| wal_error("WAL segment start is incomplete"))?;
    let payload = read_u32(&mut reader, "segment payload size")?;
    let expected_start = number
        .checked_mul(SEGMENT_PAYLOAD_BYTES)
        .ok_or_else(|| wal_error("WAL segment start overflow"))?;
    if stored_number != number
        || start != expected_start
        || u64::from(payload) != SEGMENT_PAYLOAD_BYTES
    {
        return Err(wal_error(format!(
            "WAL segment header does not match filename: {}",
            path.display()
        )));
    }
    Ok(())
}

fn read_u32(reader: &mut CheckedSliceReader<'_>, field: &str) -> Result<u32> {
    reader
        .read_u32_le()
        .map_err(|_| wal_error(format!("WAL {field} is incomplete")))
}

pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|dir| dir.sync_all())
        .map_err(|err| {
            DbError::io(format!(
                "failed to fsync WAL directory {}: {err}",
                path.display()
            ))
        })
}

fn wal_error(message: impl Into<String>) -> DbError {
    DbError::wal(SqlState::InternalError, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initialization_resumes_after_directory_creation_only() {
        let data_dir = tempfile::tempdir().unwrap();
        fs::create_dir(data_dir.path().join("wal")).unwrap();

        initialize(data_dir.path()).unwrap();

        let dir = wal_dir(data_dir.path());
        assert!(meta_path(&dir).exists());
        assert!(segment_path(&dir, 0).exists());
    }

    #[test]
    fn initialization_resumes_after_metadata_creation() {
        let data_dir = tempfile::tempdir().unwrap();
        initialize(data_dir.path()).unwrap();
        let dir = wal_dir(data_dir.path());
        fs::remove_file(segment_path(&dir, 0)).unwrap();

        initialize(data_dir.path()).unwrap();

        assert!(segment_path(&dir, 0).exists());
        assert_eq!(load_meta(&dir).unwrap().replay_floor, 0);
    }

    #[test]
    fn initialization_replaces_every_short_initial_segment_header() {
        for length in 0..SEGMENT_HEADER_LEN {
            let data_dir = tempfile::tempdir().unwrap();
            initialize(data_dir.path()).unwrap();
            let dir = wal_dir(data_dir.path());
            let path = segment_path(&dir, 0);
            File::options()
                .write(true)
                .open(&path)
                .unwrap()
                .set_len(length)
                .unwrap();

            initialize(data_dir.path()).unwrap();

            open_segment(&dir, 0).unwrap();
        }
    }

    #[test]
    fn initialization_replaces_a_full_length_corrupt_initial_header() {
        let data_dir = tempfile::tempdir().unwrap();
        initialize(data_dir.path()).unwrap();
        let dir = wal_dir(data_dir.path());
        let path = segment_path(&dir, 0);
        File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .write_all(&vec![0; usize::try_from(SEGMENT_HEADER_LEN).unwrap()])
            .unwrap();

        initialize(data_dir.path()).unwrap();

        open_segment(&dir, 0).unwrap();
    }

    #[test]
    fn discovery_removes_every_short_final_rollover_header() {
        for length in 0..SEGMENT_HEADER_LEN {
            let data_dir = tempfile::tempdir().unwrap();
            initialize(data_dir.path()).unwrap();
            let dir = wal_dir(data_dir.path());
            File::options()
                .write(true)
                .open(segment_path(&dir, 0))
                .unwrap()
                .set_len(SEGMENT_HEADER_LEN + SEGMENT_PAYLOAD_BYTES)
                .unwrap();
            let segment = create_segment(&dir, 1).unwrap();
            segment.set_len(length).unwrap();
            segment.sync_all().unwrap();

            assert_eq!(
                discover_end(&dir, 0, SEGMENT_PAYLOAD_BYTES).unwrap(),
                SEGMENT_PAYLOAD_BYTES
            );
            assert!(!segment_path(&dir, 1).exists());
        }
    }

    #[test]
    fn discovery_removes_a_full_length_corrupt_unacknowledged_rollover_header() {
        let data_dir = tempfile::tempdir().unwrap();
        initialize(data_dir.path()).unwrap();
        let dir = wal_dir(data_dir.path());
        File::options()
            .write(true)
            .open(segment_path(&dir, 0))
            .unwrap()
            .set_len(SEGMENT_HEADER_LEN + SEGMENT_PAYLOAD_BYTES)
            .unwrap();
        let mut segment = create_segment(&dir, 1).unwrap();
        segment.seek(SeekFrom::Start(0)).unwrap();
        segment
            .write_all(&vec![0; usize::try_from(SEGMENT_HEADER_LEN).unwrap()])
            .unwrap();
        segment.sync_all().unwrap();

        assert_eq!(
            discover_end(&dir, 0, SEGMENT_PAYLOAD_BYTES).unwrap(),
            SEGMENT_PAYLOAD_BYTES
        );
        assert!(!segment_path(&dir, 1).exists());
    }

    #[test]
    fn stream_crosses_segment_boundary_without_header_bytes() {
        let data_dir = tempfile::tempdir().unwrap();
        initialize(data_dir.path()).unwrap();
        let dir = wal_dir(data_dir.path());
        let length = usize::try_from(SEGMENT_PAYLOAD_BYTES).unwrap() + 37;
        let bytes: Vec<u8> = (0..length).map(|index| (index % 251) as u8).collect();

        write_stream(&dir, 0, &bytes).unwrap().sync_all().unwrap();

        assert!(segment_path(&dir, 0).exists());
        assert!(segment_path(&dir, 1).exists());
        let mut reader = SegmentReader::new(dir, 0, u64::try_from(length).unwrap());
        assert_eq!(reader.read_exact_vec(length).unwrap().unwrap(), bytes);
    }

    #[test]
    fn recycling_removes_only_wholly_obsolete_segments() {
        let data_dir = tempfile::tempdir().unwrap();
        initialize(data_dir.path()).unwrap();
        let dir = wal_dir(data_dir.path());
        let length = usize::try_from(SEGMENT_PAYLOAD_BYTES).unwrap() + 1;
        let bytes = vec![7; length];
        write_stream(&dir, 0, &bytes).unwrap().sync_all().unwrap();

        recycle_segments(&dir, 0, SEGMENT_PAYLOAD_BYTES).unwrap();

        assert!(!segment_path(&dir, 0).exists());
        assert!(segment_path(&dir, 1).exists());
    }

    #[test]
    fn interrupted_cross_segment_tail_repair_remains_discoverable() {
        let data_dir = tempfile::tempdir().unwrap();
        initialize(data_dir.path()).unwrap();
        let dir = wal_dir(data_dir.path());
        let length = usize::try_from(SEGMENT_PAYLOAD_BYTES).unwrap() + 10;
        write_stream(&dir, 0, &vec![3; length])
            .unwrap()
            .sync_all()
            .unwrap();

        remove_segment(&dir, 1, "remove").unwrap();
        sync_dir(&dir).unwrap();

        assert_eq!(
            discover_end(&dir, 0, SEGMENT_PAYLOAD_BYTES).unwrap(),
            SEGMENT_PAYLOAD_BYTES
        );
    }

    #[test]
    fn orphan_cleanup_resumes_a_partially_completed_recycle() {
        let data_dir = tempfile::tempdir().unwrap();
        initialize(data_dir.path()).unwrap();
        let dir = wal_dir(data_dir.path());
        let length = usize::try_from(SEGMENT_PAYLOAD_BYTES * 2).unwrap() + 1;
        write_stream(&dir, 0, &vec![9; length])
            .unwrap()
            .sync_all()
            .unwrap();

        recycle_segments(&dir, SEGMENT_PAYLOAD_BYTES, SEGMENT_PAYLOAD_BYTES * 2).unwrap();
        assert!(segment_path(&dir, 0).exists());
        assert!(!segment_path(&dir, 1).exists());

        recycle_orphaned_segments(&dir, SEGMENT_PAYLOAD_BYTES * 2).unwrap();
        assert!(!segment_path(&dir, 0).exists());
        assert!(segment_path(&dir, 2).exists());
    }
}
