use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

use crc32fast::Hasher as Crc32;
use flate2::{Decompress, FlushDecompress, Status};
use rayon::prelude::*;
use sha1::{Digest, Sha1};

use crate::error::CloneError;
use crate::git_object::{self, TreeEntryMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectType {
    Commit,
    Tree,
    Blob,
    Tag,
}

impl ObjectType {
    pub const fn as_git_name(self) -> &'static str {
        match self {
            Self::Commit => "commit",
            Self::Tree => "tree",
            Self::Blob => "blob",
            Self::Tag => "tag",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectId([u8; 20]);

impl ObjectId {
    pub const fn from_bytes(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }

    pub fn parse_hex(hex_oid: &str) -> Result<Self, CloneError> {
        let bytes = hex::decode(hex_oid).map_err(|error| CloneError::ObjectParseFailed {
            oid: hex_oid.to_owned(),
            object_type: "object id",
            operation: "parsing hex object id",
            detail: error.to_string(),
        })?;
        if bytes.len() != 20 {
            return Err(CloneError::ObjectParseFailed {
                oid: hex_oid.to_owned(),
                object_type: "object id",
                operation: "parsing hex object id",
                detail: format!("expected 20 bytes, found {}", bytes.len()),
            });
        }
        let mut oid = [0u8; 20];
        oid.copy_from_slice(&bytes);
        Ok(Self(oid))
    }

    pub const fn as_bytes(self) -> [u8; 20] {
        self.0
    }

    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

#[derive(Debug, Clone)]
pub struct ObjectBytes {
    pub object_type: ObjectType,
    pub data: Arc<[u8]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaBase {
    Offset(u64),
    Oid(ObjectId),
}

#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub object_type: ObjectType,
    pub pack_inflated_size: u64,
    pub pack_offset: u64,
    pub compressed_start: usize,
    pub compressed_len: usize,
    pub crc32: u32,
    pub delta_base: Option<DeltaBase>,
}

pub trait ObjectReader: Sync {
    fn read_object(&self, oid: ObjectId) -> Result<ObjectBytes, CloneError>;
    fn stream_blob(&self, oid: ObjectId, out: &mut dyn Write) -> Result<u64, CloneError>;
}

#[derive(Debug, Clone)]
pub struct PackIndex {
    pack_path: PathBuf,
    pack: PackStorage,
    meta_by_oid: HashMap<ObjectId, ObjectMeta>,
    oid_by_offset: HashMap<u64, ObjectId>,
    state_by_oid: HashMap<ObjectId, ObjectDataState>,
    retained_object_count: usize,
    retained_object_bytes: usize,
    spilled_object_count: usize,
    spilled_object_bytes: usize,
    reconstructed_object_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[derive(Debug, Clone)]
pub struct PackStorage {
    inner: PackStorageInner,
}

#[derive(Debug, Clone)]
enum PackStorageInner {
    #[cfg(any(test, not(unix)))]
    Memory(Arc<[u8]>),
    FileBacked {
        file: Arc<File>,
        len: u64,
    },
}

#[derive(Debug)]
pub struct PackIngestReport {
    pub index: PackIndex,
    pub scan_ms: u128,
    pub resolve_ms: u128,
    pub idx_write_ms: u128,
    pub object_state_ms: u128,
    pub object_count: usize,
    pub base_object_count: usize,
    pub delta_count: usize,
    pub offset_delta_count: usize,
    pub ref_delta_count: usize,
    pub declared_inflated_bytes: u64,
    pub checkout_needed_blob_count: usize,
    pub checkout_ready_blob_count: usize,
    pub checkout_ready_blob_bytes: usize,
    pub checkout_spilled_blob_count: usize,
    pub checkout_spilled_blob_bytes: usize,
    pub checkout_missing_blob_count: usize,
    pub pipeline_frame_count: Option<usize>,
    pub pipeline_checkout_wait_ms: Option<u128>,
    pub pipeline_checkout_wait_count: Option<usize>,
    pub pipeline_checkout_wait_max_ms: Option<u128>,
    pub pipeline_peak_pending_delta_count: Option<usize>,
    pub pipeline_resolver_wall_ms: Option<u128>,
    pub pipeline_resolver_wait_for_frame_ms: Option<u128>,
    pub pipeline_queue_peak_depth: Option<usize>,
    pub pipeline_arena_spill_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PackIngestOptions {
    pub checkout_hint: Option<CheckoutHint>,
}

#[derive(Debug, Clone, Copy)]
pub struct CheckoutHint {
    pub default_commit: ObjectId,
}

#[derive(Debug)]
pub enum PipelineEvent {
    Frames(Vec<ObjectFrame>),
    Finished {
        checksum: [u8; 20],
        pack_bytes: u64,
        scan_ms: u128,
    },
}

#[derive(Debug, Clone)]
pub struct PipelineObjectStore {
    pack_path: PathBuf,
    inner: Arc<PipelineObjectStoreInner>,
}

#[derive(Debug)]
struct PipelineObjectStoreInner {
    state: Mutex<PipelineObjectStoreState>,
    ready: Condvar,
    wait_ms: AtomicU64,
    wait_count: AtomicUsize,
    wait_max_ms: AtomicU64,
}

#[derive(Debug, Default, Clone)]
struct PipelineObjectStoreState {
    meta_by_oid: HashMap<ObjectId, ObjectMeta>,
    state_by_oid: HashMap<ObjectId, ObjectDataState>,
    complete: bool,
    failure: Option<PipelineFailure>,
    retained_object_count: usize,
    retained_object_bytes: usize,
    spilled_object_count: usize,
    spilled_object_bytes: usize,
}

#[derive(Debug, Clone)]
struct PipelineFailure {
    operation: &'static str,
    detail: String,
}

#[derive(Debug, Clone)]
enum ObjectDataState {
    Resident(Arc<[u8]>),
    Spilled {
        path: PathBuf,
        offset: u64,
        len: u64,
    },
    Reconstructable,
}

#[derive(Debug)]
struct ObjectStateBuild {
    state_by_oid: HashMap<ObjectId, ObjectDataState>,
    retained_object_count: usize,
    retained_object_bytes: usize,
    spilled_object_count: usize,
    spilled_object_bytes: usize,
    checkout_needed_blob_count: usize,
    checkout_ready_blob_count: usize,
    checkout_ready_blob_bytes: usize,
    checkout_spilled_blob_count: usize,
    checkout_spilled_blob_bytes: usize,
    checkout_missing_blob_count: usize,
}

impl PackIndex {
    pub const fn retained_object_count(&self) -> usize {
        self.retained_object_count
    }

    pub const fn retained_object_bytes(&self) -> usize {
        self.retained_object_bytes
    }

    pub const fn spilled_object_count(&self) -> usize {
        self.spilled_object_count
    }

    pub const fn spilled_object_bytes(&self) -> usize {
        self.spilled_object_bytes
    }

    pub fn reconstructed_object_count(&self) -> usize {
        self.reconstructed_object_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl PipelineObjectStore {
    pub fn new(pack_path: &Path) -> Self {
        Self {
            pack_path: pack_path.to_owned(),
            inner: Arc::new(PipelineObjectStoreInner {
                state: Mutex::new(PipelineObjectStoreState::default()),
                ready: Condvar::new(),
                wait_ms: AtomicU64::new(0),
                wait_count: AtomicUsize::new(0),
                wait_max_ms: AtomicU64::new(0),
            }),
        }
    }

    pub fn checkout_wait_ms(&self) -> u128 {
        u128::from(self.inner.wait_ms.load(Ordering::Relaxed))
    }

    pub fn checkout_wait_count(&self) -> usize {
        self.inner.wait_count.load(Ordering::Relaxed)
    }

    pub fn checkout_wait_max_ms(&self) -> u128 {
        u128::from(self.inner.wait_max_ms.load(Ordering::Relaxed))
    }

    fn publish_object(
        &self,
        oid: ObjectId,
        meta: ObjectMeta,
        object_state: ObjectDataState,
        retained_bytes: usize,
        spilled_bytes: usize,
    ) -> Result<(), CloneError> {
        let mut state = self.lock_state("publishing pipeline object")?;
        state.meta_by_oid.insert(oid, meta);
        match &object_state {
            ObjectDataState::Resident(_) => {
                state.retained_object_count += 1;
                state.retained_object_bytes =
                    state.retained_object_bytes.saturating_add(retained_bytes);
            }
            ObjectDataState::Spilled { .. } => {
                state.spilled_object_count += 1;
                state.spilled_object_bytes =
                    state.spilled_object_bytes.saturating_add(spilled_bytes);
            }
            ObjectDataState::Reconstructable => {}
        }
        state.state_by_oid.insert(oid, object_state);
        drop(state);
        self.inner.ready.notify_all();
        Ok(())
    }

    fn finish(&self) -> Result<PipelineObjectStoreState, CloneError> {
        let mut state = self.lock_state("finishing pipeline object store")?;
        state.complete = true;
        self.inner.ready.notify_all();
        Ok(state.clone())
    }

    pub(crate) fn fail(&self, operation: &'static str, detail: String) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.failure = Some(PipelineFailure { operation, detail });
            state.complete = true;
            self.inner.ready.notify_all();
        }
    }

    fn lock_state(
        &self,
        operation: &'static str,
    ) -> Result<std::sync::MutexGuard<'_, PipelineObjectStoreState>, CloneError> {
        self.inner
            .state
            .lock()
            .map_err(|error| CloneError::PackIndexFailed {
                path: self.pack_path.clone(),
                operation,
                detail: error.to_string(),
            })
    }

    fn wait_for<T>(
        &self,
        oid: ObjectId,
        expected_type: &'static str,
        mut read: impl FnMut(&PipelineObjectStoreState) -> Option<T>,
    ) -> Result<T, CloneError> {
        let start = Instant::now();
        let mut state = self.lock_state("waiting for pipeline object")?;
        let mut waited = false;
        loop {
            if let Some(value) = read(&state) {
                let elapsed_ms = u64_saturating_from_u128(start.elapsed().as_millis());
                self.inner.wait_ms.fetch_add(elapsed_ms, Ordering::Relaxed);
                if waited {
                    self.inner.wait_count.fetch_add(1, Ordering::Relaxed);
                    update_atomic_max(&self.inner.wait_max_ms, elapsed_ms);
                }
                return Ok(value);
            }
            if let Some(failure) = &state.failure {
                return Err(CloneError::PackIndexFailed {
                    path: self.pack_path.clone(),
                    operation: failure.operation,
                    detail: failure.detail.clone(),
                });
            }
            if state.complete {
                return Err(CloneError::ObjectLookupFailed {
                    oid: oid.to_hex(),
                    expected_type,
                    detail: "pipeline completed before the object became available".to_owned(),
                });
            }
            waited = true;
            state = self
                .inner
                .ready
                .wait(state)
                .map_err(|error| CloneError::PackIndexFailed {
                    path: self.pack_path.clone(),
                    operation: "waiting for pipeline object",
                    detail: error.to_string(),
                })?;
        }
    }
}

impl ObjectReader for PipelineObjectStore {
    fn read_object(&self, oid: ObjectId) -> Result<ObjectBytes, CloneError> {
        let (meta, object_state) = self.wait_for(oid, "object", |state| {
            Some((
                state.meta_by_oid.get(&oid)?.clone(),
                state.state_by_oid.get(&oid)?.clone(),
            ))
        })?;
        let data = match object_state {
            ObjectDataState::Resident(data) => data,
            ObjectDataState::Spilled { path, offset, len } => {
                read_spilled_object(&path, offset, len)?
            }
            ObjectDataState::Reconstructable => {
                return Err(CloneError::ObjectLookupFailed {
                    oid: oid.to_hex(),
                    expected_type: "resident pipeline object",
                    detail: "pipeline object was marked reconstructable before final index was available"
                        .to_owned(),
                });
            }
        };
        Ok(ObjectBytes {
            object_type: meta.object_type,
            data,
        })
    }

    fn stream_blob(&self, oid: ObjectId, out: &mut dyn Write) -> Result<u64, CloneError> {
        let (meta, object_state) = self.wait_for(oid, "blob", |state| {
            Some((
                state.meta_by_oid.get(&oid)?.clone(),
                state.state_by_oid.get(&oid)?.clone(),
            ))
        })?;
        if meta.object_type != ObjectType::Blob {
            return Err(CloneError::ObjectLookupFailed {
                oid: oid.to_hex(),
                expected_type: "blob",
                detail: format!("found {}", meta.object_type.as_git_name()),
            });
        }
        match object_state {
            ObjectDataState::Resident(data) => {
                out.write_all(&data)
                    .map_err(|error| CloneError::PackIndexFailed {
                        path: self.pack_path.clone(),
                        operation: "streaming resident pipeline blob",
                        detail: error.to_string(),
                    })?;
                Ok(data.len() as u64)
            }
            ObjectDataState::Spilled { path, offset, len } => {
                let mut file = File::open(&path).map_err(|error| CloneError::PackIndexFailed {
                    path: path.clone(),
                    operation: "opening spilled pipeline blob",
                    detail: error.to_string(),
                })?;
                copy_exact_range(&path, &mut file, offset, len, out)?;
                Ok(len)
            }
            ObjectDataState::Reconstructable => Err(CloneError::ObjectLookupFailed {
                oid: oid.to_hex(),
                expected_type: "resident pipeline blob",
                detail: "pipeline blob was marked reconstructable before final index was available"
                    .to_owned(),
            }),
        }
    }
}

impl PackStorage {
    #[cfg(any(test, not(unix)))]
    pub const fn from_memory(pack: Arc<[u8]>) -> Self {
        Self {
            inner: PackStorageInner::Memory(pack),
        }
    }

    pub fn open_file_backed(pack_path: &Path) -> Result<Self, CloneError> {
        #[cfg(unix)]
        {
            let file = File::open(pack_path).map_err(|error| CloneError::PackIndexFailed {
                path: pack_path.to_owned(),
                operation: "opening pack file for file-backed access",
                detail: error.to_string(),
            })?;
            let len = file
                .metadata()
                .map_err(|error| CloneError::PackIndexFailed {
                    path: pack_path.to_owned(),
                    operation: "reading file-backed pack metadata",
                    detail: error.to_string(),
                })?
                .len();
            Ok(Self {
                inner: PackStorageInner::FileBacked {
                    file: Arc::new(file),
                    len,
                },
            })
        }

        #[cfg(not(unix))]
        {
            read_pack_arc(pack_path).map(Self::from_memory)
        }
    }

    fn inflate_frame(
        &self,
        pack_path: &Path,
        frame: &ObjectFrame,
    ) -> Result<Arc<[u8]>, CloneError> {
        match &self.inner {
            #[cfg(any(test, not(unix)))]
            PackStorageInner::Memory(pack) => inflate_frame(pack_path, pack.as_ref(), frame),
            PackStorageInner::FileBacked { file, len } => {
                let compressed_end = checked_frame_end(pack_path, frame, *len)?;
                let mut compressed = vec![0u8; frame.compressed_len];
                read_exact_at(
                    pack_path,
                    file,
                    frame.compressed_start as u64,
                    &mut compressed,
                    "reading compressed object range",
                )?;
                debug_assert_eq!(
                    compressed_end,
                    frame.compressed_start as u64 + compressed.len() as u64
                );
                inflate_compressed_frame(pack_path, frame, &compressed)
            }
        }
    }
}

impl ObjectReader for PackIndex {
    fn read_object(&self, oid: ObjectId) -> Result<ObjectBytes, CloneError> {
        let meta = self
            .meta_by_oid
            .get(&oid)
            .ok_or_else(|| CloneError::ObjectLookupFailed {
                oid: oid.to_hex(),
                expected_type: "object",
                detail: "object was not present in the fetched pack".to_owned(),
            })?;
        let data = match self.state_by_oid.get(&oid) {
            Some(ObjectDataState::Resident(data)) => Arc::clone(data),
            Some(ObjectDataState::Spilled { path, offset, len }) => {
                read_spilled_object(path, *offset, *len)?
            }
            Some(ObjectDataState::Reconstructable) | None => {
                self.reconstructed_object_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.reconstruct_object(oid, 0)?
            }
        };
        Ok(ObjectBytes {
            object_type: meta.object_type,
            data,
        })
    }

    fn stream_blob(&self, oid: ObjectId, out: &mut dyn Write) -> Result<u64, CloneError> {
        let meta = self
            .meta_by_oid
            .get(&oid)
            .ok_or_else(|| CloneError::ObjectLookupFailed {
                oid: oid.to_hex(),
                expected_type: "blob",
                detail: "object was not present in the fetched pack".to_owned(),
            })?;
        if meta.object_type != ObjectType::Blob {
            return Err(CloneError::ObjectLookupFailed {
                oid: oid.to_hex(),
                expected_type: "blob",
                detail: format!("found {}", meta.object_type.as_git_name()),
            });
        }
        match self.state_by_oid.get(&oid) {
            Some(ObjectDataState::Resident(data)) => {
                out.write_all(data)
                    .map_err(|error| CloneError::PackIndexFailed {
                        path: self.pack_path.clone(),
                        operation: "streaming resident blob",
                        detail: error.to_string(),
                    })?;
                Ok(data.len() as u64)
            }
            Some(ObjectDataState::Spilled { path, offset, len }) => {
                let mut file = File::open(path).map_err(|error| CloneError::PackIndexFailed {
                    path: path.clone(),
                    operation: "opening spilled blob",
                    detail: error.to_string(),
                })?;
                copy_exact_range(path, &mut file, *offset, *len, out)?;
                Ok(*len)
            }
            Some(ObjectDataState::Reconstructable) | None => {
                self.reconstructed_object_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let data = self.reconstruct_object(oid, 0)?;
                out.write_all(&data)
                    .map_err(|error| CloneError::PackIndexFailed {
                        path: self.pack_path.clone(),
                        operation: "streaming reconstructed blob",
                        detail: error.to_string(),
                    })?;
                Ok(data.len() as u64)
            }
        }
    }
}

impl PackIndex {
    fn reconstruct_object(&self, oid: ObjectId, depth: usize) -> Result<Arc<[u8]>, CloneError> {
        if depth > 128 {
            return Err(CloneError::PackIndexFailed {
                path: self.pack_path.clone(),
                operation: "reconstructing object",
                detail: format!("delta chain for {} exceeded 128 objects", oid.to_hex()),
            });
        }
        let meta = self
            .meta_by_oid
            .get(&oid)
            .ok_or_else(|| CloneError::ObjectLookupFailed {
                oid: oid.to_hex(),
                expected_type: "object",
                detail: "object metadata was not present in the fetched pack".to_owned(),
            })?;
        let frame = ObjectFrame {
            offset: meta.pack_offset,
            compressed_start: meta.compressed_start,
            compressed_len: meta.compressed_len,
            crc32: meta.crc32,
            encoded: match meta.delta_base {
                None => EncodedObjectKind::Base(meta.object_type),
                Some(DeltaBase::Offset(base_offset)) => {
                    EncodedObjectKind::OffsetDelta { base_offset }
                }
                Some(DeltaBase::Oid(base_oid)) => EncodedObjectKind::RefDelta {
                    base_oid: base_oid.as_bytes(),
                },
            },
            inflated: None,
            declared_size: meta.pack_inflated_size,
        };
        let payload = self.pack.inflate_frame(&self.pack_path, &frame)?;
        match meta.delta_base {
            None => Ok(payload),
            Some(DeltaBase::Offset(base_offset)) => {
                let base_oid = self.oid_by_offset.get(&base_offset).ok_or_else(|| {
                    CloneError::PackIndexFailed {
                        path: self.pack_path.clone(),
                        operation: "reconstructing offset delta",
                        detail: format!("base offset {base_offset} was not indexed"),
                    }
                })?;
                let base = self.read_cached_or_reconstruct(*base_oid, depth + 1)?;
                Ok(apply_delta(&self.pack_path, &base, &payload)?.into())
            }
            Some(DeltaBase::Oid(base_oid)) => {
                let base = self.read_cached_or_reconstruct(base_oid, depth + 1)?;
                Ok(apply_delta(&self.pack_path, &base, &payload)?.into())
            }
        }
    }

    fn read_cached_or_reconstruct(
        &self,
        oid: ObjectId,
        depth: usize,
    ) -> Result<Arc<[u8]>, CloneError> {
        match self.state_by_oid.get(&oid) {
            Some(ObjectDataState::Resident(data)) => Ok(Arc::clone(data)),
            Some(ObjectDataState::Spilled { path, offset, len }) => {
                read_spilled_object(path, *offset, *len)
            }
            Some(ObjectDataState::Reconstructable) | None => self.reconstruct_object(oid, depth),
        }
    }
}

fn read_spilled_object(path: &Path, offset: u64, len: u64) -> Result<Arc<[u8]>, CloneError> {
    let mut file = File::open(path).map_err(|error| CloneError::PackIndexFailed {
        path: path.to_owned(),
        operation: "opening spilled object",
        detail: error.to_string(),
    })?;
    let mut data = Vec::with_capacity(usize_from_u64(
        path,
        "allocating spilled object buffer",
        len,
    )?);
    copy_exact_range(path, &mut file, offset, len, &mut data)?;
    if data.len() as u64 != len {
        return Err(CloneError::PackIndexFailed {
            path: path.to_owned(),
            operation: "reading spilled object",
            detail: format!("read {} bytes, expected {len}", data.len()),
        });
    }
    Ok(data.into())
}

fn copy_exact_range(
    path: &Path,
    file: &mut File,
    offset: u64,
    len: u64,
    out: &mut dyn Write,
) -> Result<(), CloneError> {
    use std::io::{Seek, SeekFrom};

    file.seek(SeekFrom::Start(offset))
        .map_err(|error| CloneError::PackIndexFailed {
            path: path.to_owned(),
            operation: "seeking spilled object",
            detail: error.to_string(),
        })?;
    let mut remaining = len;
    let mut buffer = [0u8; 8192];
    while remaining > 0 {
        let to_read = buffer.len().min(usize_from_u64(
            path,
            "reading spilled object range",
            remaining,
        )?);
        let read =
            file.read(&mut buffer[..to_read])
                .map_err(|error| CloneError::PackIndexFailed {
                    path: path.to_owned(),
                    operation: "reading spilled object",
                    detail: error.to_string(),
                })?;
        if read == 0 {
            return Err(CloneError::PackIndexFailed {
                path: path.to_owned(),
                operation: "reading spilled object",
                detail: format!("spilled object ended with {remaining} bytes remaining"),
            });
        }
        out.write_all(&buffer[..read])
            .map_err(|error| CloneError::PackIndexFailed {
                path: path.to_owned(),
                operation: "copying spilled object",
                detail: error.to_string(),
            })?;
        remaining -= read as u64;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodedObjectKind {
    Base(ObjectType),
    OffsetDelta { base_offset: u64 },
    RefDelta { base_oid: [u8; 20] },
}

#[derive(Debug, Clone)]
pub struct ObjectFrame {
    pub offset: u64,
    pub compressed_start: usize,
    pub compressed_len: usize,
    pub crc32: u32,
    pub encoded: EncodedObjectKind,
    pub inflated: Option<Arc<[u8]>>,
    pub declared_size: u64,
}

#[derive(Debug)]
pub struct PackScan {
    pub checksum: [u8; 20],
    pub frames: Vec<ObjectFrame>,
}

#[derive(Debug, Clone)]
struct ResolvedObject {
    object_type: ObjectType,
    data: Arc<[u8]>,
    size: u64,
    oid: [u8; 20],
}

#[derive(Debug, Clone)]
struct ResolvedFrame {
    frame_index: usize,
    offset: u64,
    crc32: u32,
    object: ResolvedObject,
}

#[derive(Debug)]
struct IndexEntry {
    oid: [u8; 20],
    crc32: u32,
    offset: u64,
}

#[derive(Debug)]
struct DeltaAdjacency {
    children_by_offset: HashMap<u64, Vec<usize>>,
    children_by_oid: HashMap<[u8; 20], Vec<usize>>,
    delta_count: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct PackFrameMetrics {
    object_count: usize,
    base_object_count: usize,
    delta_count: usize,
    offset_delta_count: usize,
    ref_delta_count: usize,
    declared_inflated_bytes: u64,
}

#[cfg(test)]
pub fn ingest_pack(pack_path: &Path, index_path: &Path) -> Result<PackIngestReport, CloneError> {
    let pack = read_pack_arc(pack_path)?;

    let scan_payload = if env_bool("FCL_LOW_MEMORY") {
        ScanPayload::MetadataOnly
    } else {
        ScanPayload::Inflate
    };
    let scan_start = Instant::now();
    let scan = scan_pack(pack_path, pack.as_ref(), scan_payload)?;
    let scan_ms = scan_start.elapsed().as_millis();

    ingest_scanned_pack(
        pack_path,
        index_path,
        PackStorage::from_memory(pack),
        &scan,
        scan_ms,
        PackIngestOptions::default(),
    )
}

pub fn ingest_fetched_pack(
    pack_path: &Path,
    index_path: &Path,
    checksum: [u8; 20],
    options: PackIngestOptions,
) -> Result<PackIngestReport, CloneError> {
    let scan_payload = if env_bool("FCL_LOW_MEMORY") {
        ScanPayload::MetadataOnly
    } else {
        ScanPayload::Inflate
    };
    let scan_start = Instant::now();
    let scan = scan_pack_file_windowed(pack_path, scan_payload, checksum)?;
    let scan_ms = scan_start.elapsed().as_millis();

    let pack = PackStorage::open_file_backed(pack_path)?;
    ingest_scanned_pack(pack_path, index_path, pack, &scan, scan_ms, options)
}

#[cfg(test)]
pub fn read_pack_arc(pack_path: &Path) -> Result<Arc<[u8]>, CloneError> {
    let pack = fs::read(pack_path).map_err(|error| CloneError::PackIndexFailed {
        path: pack_path.to_owned(),
        operation: "reading pack file",
        detail: error.to_string(),
    })?;
    Ok(Arc::<[u8]>::from(pack))
}

pub fn ingest_scanned_pack(
    pack_path: &Path,
    index_path: &Path,
    pack: PackStorage,
    scan: &PackScan,
    scan_ms: u128,
    options: PackIngestOptions,
) -> Result<PackIngestReport, CloneError> {
    let resolve_start = Instant::now();
    let frame_metrics = pack_frame_metrics(&scan.frames);
    let resolved = resolve_inflated_frames(
        pack_path,
        &pack,
        &scan.frames,
        options.checkout_hint.is_some(),
    )?;
    let resolve_ms = resolve_start.elapsed().as_millis();

    let mut entries = resolved
        .iter()
        .map(|frame| IndexEntry {
            oid: frame.object.oid,
            crc32: frame.crc32,
            offset: frame.offset,
        })
        .collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.oid.cmp(&right.oid));
    let idx_write_start = Instant::now();
    write_idx_v2(index_path, &entries, &scan.checksum)?;
    let idx_write_ms = idx_write_start.elapsed().as_millis();

    let object_state_start = Instant::now();
    let mut meta_by_oid = HashMap::with_capacity(resolved.len());
    let mut oid_by_offset = HashMap::with_capacity(resolved.len());
    for frame in &resolved {
        let oid = ObjectId::from_bytes(frame.object.oid);
        let source = &scan.frames[frame.frame_index];
        let delta_base = match source.encoded {
            EncodedObjectKind::Base(_) => None,
            EncodedObjectKind::OffsetDelta { base_offset } => Some(DeltaBase::Offset(base_offset)),
            EncodedObjectKind::RefDelta { base_oid } => {
                Some(DeltaBase::Oid(ObjectId::from_bytes(base_oid)))
            }
        };
        oid_by_offset.insert(frame.offset, oid);
        meta_by_oid.insert(
            oid,
            ObjectMeta {
                object_type: frame.object.object_type,
                pack_inflated_size: source.declared_size,
                pack_offset: frame.offset,
                compressed_start: source.compressed_start,
                compressed_len: source.compressed_len,
                crc32: frame.crc32,
                delta_base,
            },
        );
    }
    let spill_dir = index_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("fcl-spill");
    let checkout_needed_blobs = checkout_needed_blobs(options.checkout_hint, &resolved)?;
    let cache = build_object_states(pack_path, &spill_dir, resolved, &checkout_needed_blobs)?;
    let object_state_ms = object_state_start.elapsed().as_millis();

    Ok(PackIngestReport {
        index: PackIndex {
            pack_path: pack_path.to_owned(),
            pack,
            meta_by_oid,
            oid_by_offset,
            state_by_oid: cache.state_by_oid,
            retained_object_count: cache.retained_object_count,
            retained_object_bytes: cache.retained_object_bytes,
            spilled_object_count: cache.spilled_object_count,
            spilled_object_bytes: cache.spilled_object_bytes,
            reconstructed_object_count: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        },
        scan_ms,
        resolve_ms,
        idx_write_ms,
        object_state_ms,
        object_count: frame_metrics.object_count,
        base_object_count: frame_metrics.base_object_count,
        delta_count: frame_metrics.delta_count,
        offset_delta_count: frame_metrics.offset_delta_count,
        ref_delta_count: frame_metrics.ref_delta_count,
        declared_inflated_bytes: frame_metrics.declared_inflated_bytes,
        checkout_needed_blob_count: cache.checkout_needed_blob_count,
        checkout_ready_blob_count: cache.checkout_ready_blob_count,
        checkout_ready_blob_bytes: cache.checkout_ready_blob_bytes,
        checkout_spilled_blob_count: cache.checkout_spilled_blob_count,
        checkout_spilled_blob_bytes: cache.checkout_spilled_blob_bytes,
        checkout_missing_blob_count: cache.checkout_missing_blob_count,
        pipeline_frame_count: None,
        pipeline_checkout_wait_ms: None,
        pipeline_checkout_wait_count: None,
        pipeline_checkout_wait_max_ms: None,
        pipeline_peak_pending_delta_count: None,
        pipeline_resolver_wall_ms: None,
        pipeline_resolver_wait_for_frame_ms: None,
        pipeline_queue_peak_depth: None,
        pipeline_arena_spill_bytes: None,
    })
}

pub fn ingest_pack_pipeline(
    pack_path: &Path,
    index_path: &Path,
    receiver: &Receiver<PipelineEvent>,
    store: PipelineObjectStore,
) -> Result<PackIngestReport, CloneError> {
    let start = Instant::now();
    let spill_dir = index_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("fcl-spill");
    let mut resolver = PipelineResolver::new(pack_path, index_path, &spill_dir, store)?;
    let mut wait_for_frame_ms = 0u128;
    let (checksum, scan_ms) = loop {
        let wait_start = Instant::now();
        let event = receiver
            .recv()
            .map_err(|error| CloneError::PackIndexFailed {
                path: pack_path.to_owned(),
                operation: "receiving pipeline object frame",
                detail: error.to_string(),
            })?;
        wait_for_frame_ms += wait_start.elapsed().as_millis();
        match event {
            PipelineEvent::Frames(frames) => {
                resolver.update_queue_peak_depth(frames.len());
                for frame in frames {
                    resolver.accept_frame(frame)?;
                }
            }
            PipelineEvent::Finished {
                checksum: received_checksum,
                pack_bytes,
                scan_ms: received_scan_ms,
            } => {
                let _ = pack_bytes;
                break (received_checksum, received_scan_ms);
            }
        }
    };
    resolver.finish(
        checksum,
        scan_ms,
        start.elapsed().as_millis(),
        wait_for_frame_ms,
    )
}

struct PipelineResolver {
    pack_path: PathBuf,
    index_path: PathBuf,
    spill_dir: PathBuf,
    store: PipelineObjectStore,
    frames: Vec<ObjectFrame>,
    entries: Vec<IndexEntry>,
    oid_by_offset: HashMap<u64, ObjectId>,
    resolved_by_offset: HashMap<u64, ResolvedObject>,
    resolved_by_oid: HashMap<[u8; 20], ResolvedObject>,
    pending_by_offset: HashMap<u64, Vec<PendingDelta>>,
    pending_by_oid: HashMap<[u8; 20], Vec<PendingDelta>>,
    frame_count: usize,
    peak_pending_delta_count: usize,
    queue_peak_depth: usize,
    arena_spill_bytes: u64,
    resident_blob_bytes: u64,
    resident_blob_limit: u64,
    spill_blobs_over_budget: bool,
}

#[derive(Debug)]
struct PendingDelta {
    frame_index: usize,
    frame: ObjectFrame,
}

impl PipelineResolver {
    fn new(
        pack_path: &Path,
        index_path: &Path,
        spill_dir: &Path,
        store: PipelineObjectStore,
    ) -> Result<Self, CloneError> {
        fs::create_dir_all(spill_dir).map_err(|error| CloneError::PackIndexFailed {
            path: spill_dir.to_owned(),
            operation: "creating pipeline spill directory",
            detail: error.to_string(),
        })?;
        let resident_blob_limit =
            optional_usize_env("FCL_CHECKOUT_BLOB_CACHE_BYTES")?.unwrap_or(256 * 1024 * 1024);
        Ok(Self {
            pack_path: pack_path.to_owned(),
            index_path: index_path.to_owned(),
            spill_dir: spill_dir.to_owned(),
            store,
            frames: Vec::new(),
            entries: Vec::new(),
            oid_by_offset: HashMap::new(),
            resolved_by_offset: HashMap::new(),
            resolved_by_oid: HashMap::new(),
            pending_by_offset: HashMap::new(),
            pending_by_oid: HashMap::new(),
            frame_count: 0,
            peak_pending_delta_count: 0,
            queue_peak_depth: 0,
            arena_spill_bytes: 0,
            resident_blob_bytes: 0,
            resident_blob_limit: u64::try_from(resident_blob_limit).unwrap_or(u64::MAX),
            spill_blobs_over_budget: env_bool("FCL_PIPELINE_SPILL_BLOBS")
                || env_bool("FCL_SPILL_BLOBS"),
        })
    }

    fn accept_frame(&mut self, frame: ObjectFrame) -> Result<(), CloneError> {
        let frame_index = self.frames.len();
        self.frames.push(frame.clone());
        self.frame_count += 1;
        match frame.encoded {
            EncodedObjectKind::Base(object_type) => {
                let payload = frame_payload_from_frame(&self.pack_path, &frame)?;
                let resolved = ResolvedFrame {
                    frame_index,
                    offset: frame.offset,
                    crc32: frame.crc32,
                    object: resolve_base_object(object_type, payload),
                };
                self.publish_resolved(&resolved)?;
            }
            EncodedObjectKind::OffsetDelta { base_offset } => {
                if let Some(base) = self.resolved_by_offset.get(&base_offset).cloned() {
                    self.resolve_delta(frame_index, &frame, &base)?;
                } else {
                    self.pending_by_offset
                        .entry(base_offset)
                        .or_default()
                        .push(PendingDelta { frame_index, frame });
                    self.update_peak_pending();
                }
            }
            EncodedObjectKind::RefDelta { base_oid } => {
                if let Some(base) = self.resolved_by_oid.get(&base_oid).cloned() {
                    self.resolve_delta(frame_index, &frame, &base)?;
                } else {
                    self.pending_by_oid
                        .entry(base_oid)
                        .or_default()
                        .push(PendingDelta { frame_index, frame });
                    self.update_peak_pending();
                }
            }
        }
        Ok(())
    }

    fn resolve_delta(
        &mut self,
        frame_index: usize,
        frame: &ObjectFrame,
        base: &ResolvedObject,
    ) -> Result<(), CloneError> {
        let delta = frame_payload_from_frame(&self.pack_path, frame)?;
        let data = apply_delta(&self.pack_path, &base.data, &delta)?;
        let resolved = ResolvedFrame {
            frame_index,
            offset: frame.offset,
            crc32: frame.crc32,
            object: resolve_base_object(base.object_type, data.into()),
        };
        self.publish_resolved(&resolved)
    }

    fn publish_resolved(&mut self, resolved: &ResolvedFrame) -> Result<(), CloneError> {
        let source = &self.frames[resolved.frame_index];
        let oid = ObjectId::from_bytes(resolved.object.oid);
        let delta_base = match source.encoded {
            EncodedObjectKind::Base(_) => None,
            EncodedObjectKind::OffsetDelta { base_offset } => Some(DeltaBase::Offset(base_offset)),
            EncodedObjectKind::RefDelta { base_oid } => {
                Some(DeltaBase::Oid(ObjectId::from_bytes(base_oid)))
            }
        };
        let meta = ObjectMeta {
            object_type: resolved.object.object_type,
            pack_inflated_size: source.declared_size,
            pack_offset: resolved.offset,
            compressed_start: source.compressed_start,
            compressed_len: source.compressed_len,
            crc32: resolved.crc32,
            delta_base,
        };
        let data_len = resolved.object.data.len();
        let object_state =
            self.pipeline_object_state(&resolved.object.data, resolved.object.object_type)?;
        let retained_bytes = if matches!(object_state, ObjectDataState::Resident(_)) {
            data_len
        } else {
            0
        };
        let spilled_bytes = if matches!(object_state, ObjectDataState::Spilled { .. }) {
            data_len
        } else {
            0
        };
        self.store
            .publish_object(oid, meta, object_state, retained_bytes, spilled_bytes)?;
        self.entries.push(IndexEntry {
            oid: resolved.object.oid,
            crc32: resolved.crc32,
            offset: resolved.offset,
        });
        self.oid_by_offset.insert(resolved.offset, oid);
        self.resolved_by_offset
            .insert(resolved.offset, resolved.object.clone());
        self.resolved_by_oid
            .insert(resolved.object.oid, resolved.object.clone());
        if let Some(children) = self.pending_by_offset.remove(&resolved.offset) {
            for child in children {
                self.resolve_delta(child.frame_index, &child.frame, &resolved.object)?;
            }
        }
        if let Some(children) = self.pending_by_oid.remove(&resolved.object.oid) {
            for child in children {
                self.resolve_delta(child.frame_index, &child.frame, &resolved.object)?;
            }
        }
        Ok(())
    }

    fn pipeline_object_state(
        &mut self,
        data: &Arc<[u8]>,
        object_type: ObjectType,
    ) -> Result<ObjectDataState, CloneError> {
        if object_type != ObjectType::Blob {
            return Ok(ObjectDataState::Resident(Arc::clone(data)));
        }
        let data_len = data.len();
        let data_len_u64 = u64::try_from(data_len).unwrap_or(u64::MAX);
        if self.resident_blob_bytes.saturating_add(data_len_u64) <= self.resident_blob_limit {
            self.resident_blob_bytes = self.resident_blob_bytes.saturating_add(data_len_u64);
            return Ok(ObjectDataState::Resident(Arc::clone(data)));
        }
        if !self.spill_blobs_over_budget {
            self.resident_blob_bytes = self.resident_blob_bytes.saturating_add(data_len_u64);
            return Ok(ObjectDataState::Resident(Arc::clone(data)));
        }
        let spilled = spill_object(&self.pack_path, &self.spill_dir, data)?;
        self.arena_spill_bytes = self.arena_spill_bytes.saturating_add(data_len_u64);
        Ok(ObjectDataState::Spilled {
            path: spilled.path,
            offset: spilled.offset,
            len: data_len as u64,
        })
    }

    fn finish(
        mut self,
        checksum: [u8; 20],
        scan_ms: u128,
        resolve_ms: u128,
        wait_for_frame_ms: u128,
    ) -> Result<PackIngestReport, CloneError> {
        let pending = self.pending_by_offset.values().map(Vec::len).sum::<usize>()
            + self.pending_by_oid.values().map(Vec::len).sum::<usize>();
        if pending != 0 {
            return Err(CloneError::PackIndexFailed {
                path: self.pack_path.clone(),
                operation: "resolving deltas",
                detail: format!("{pending} delta objects could not find resolved bases"),
            });
        }
        self.entries
            .sort_unstable_by(|left, right| left.oid.cmp(&right.oid));
        let idx_write_start = Instant::now();
        write_idx_v2(&self.index_path, &self.entries, &checksum)?;
        let idx_write_ms = idx_write_start.elapsed().as_millis();
        let object_state_start = Instant::now();
        let store_state = self.store.finish()?;
        let object_state_ms = object_state_start.elapsed().as_millis();
        let pack = PackStorage::open_file_backed(&self.pack_path)?;
        let frame_metrics = pack_frame_metrics(&self.frames);
        Ok(PackIngestReport {
            index: PackIndex {
                pack_path: self.pack_path,
                pack,
                meta_by_oid: store_state.meta_by_oid,
                oid_by_offset: self.oid_by_offset,
                state_by_oid: store_state.state_by_oid,
                retained_object_count: store_state.retained_object_count,
                retained_object_bytes: store_state.retained_object_bytes,
                spilled_object_count: store_state.spilled_object_count,
                spilled_object_bytes: store_state.spilled_object_bytes,
                reconstructed_object_count: Arc::new(AtomicUsize::new(0)),
            },
            scan_ms,
            resolve_ms,
            idx_write_ms,
            object_state_ms,
            object_count: frame_metrics.object_count,
            base_object_count: frame_metrics.base_object_count,
            delta_count: frame_metrics.delta_count,
            offset_delta_count: frame_metrics.offset_delta_count,
            ref_delta_count: frame_metrics.ref_delta_count,
            declared_inflated_bytes: frame_metrics.declared_inflated_bytes,
            checkout_needed_blob_count: 0,
            checkout_ready_blob_count: 0,
            checkout_ready_blob_bytes: 0,
            checkout_spilled_blob_count: 0,
            checkout_spilled_blob_bytes: 0,
            checkout_missing_blob_count: 0,
            pipeline_frame_count: Some(self.frame_count),
            pipeline_checkout_wait_ms: Some(self.store.checkout_wait_ms()),
            pipeline_checkout_wait_count: Some(self.store.checkout_wait_count()),
            pipeline_checkout_wait_max_ms: Some(self.store.checkout_wait_max_ms()),
            pipeline_peak_pending_delta_count: Some(self.peak_pending_delta_count),
            pipeline_resolver_wall_ms: Some(resolve_ms),
            pipeline_resolver_wait_for_frame_ms: Some(wait_for_frame_ms),
            pipeline_queue_peak_depth: Some(self.queue_peak_depth),
            pipeline_arena_spill_bytes: Some(self.arena_spill_bytes),
        })
    }

    fn update_queue_peak_depth(&mut self, depth: usize) {
        self.queue_peak_depth = self.queue_peak_depth.max(depth);
    }

    fn update_peak_pending(&mut self) {
        let pending = self.pending_by_offset.values().map(Vec::len).sum::<usize>()
            + self.pending_by_oid.values().map(Vec::len).sum::<usize>();
        self.peak_pending_delta_count = self.peak_pending_delta_count.max(pending);
    }
}

fn frame_payload_from_frame(
    pack_path: &Path,
    frame: &ObjectFrame,
) -> Result<Arc<[u8]>, CloneError> {
    frame.inflated.as_ref().map_or_else(
        || {
            Err(CloneError::PackIndexFailed {
                path: pack_path.to_owned(),
                operation: "reading pipeline frame payload",
                detail: "streaming pipeline frame did not carry inflated payload".to_owned(),
            })
        },
        |inflated| Ok(Arc::clone(inflated)),
    )
}

fn build_object_states(
    pack_path: &Path,
    spill_dir: &Path,
    resolved: Vec<ResolvedFrame>,
    checkout_needed_blobs: &HashSet<ObjectId>,
) -> Result<ObjectStateBuild, CloneError> {
    let resident_limit = optional_usize_env("FCL_OBJECT_CACHE_BYTES")?.unwrap_or(512 * 1024 * 1024);
    let checkout_resident_limit =
        optional_usize_env("FCL_CHECKOUT_BLOB_CACHE_BYTES")?.unwrap_or(256 * 1024 * 1024);
    let max_spill_bytes = optional_usize_env("FCL_MAX_SPILL_BYTES")?;
    let spill_blobs = env_bool("FCL_SPILL_BLOBS");
    let configured_spill_dir = std::env::var_os("FCL_SPILL_DIR").map(PathBuf::from);
    let spill_dir = configured_spill_dir.as_deref().unwrap_or(spill_dir);

    let mut state_by_oid = HashMap::with_capacity(resolved.len());
    let mut retained_object_count = 0usize;
    let mut retained_object_bytes = 0usize;
    let mut spilled_object_count = 0usize;
    let mut spilled_object_bytes = 0usize;
    let mut checkout_resident_bytes = 0usize;
    let mut checkout_ready_blob_count = 0usize;
    let mut checkout_ready_blob_bytes = 0usize;
    let mut checkout_spilled_blob_count = 0usize;
    let mut checkout_spilled_blob_bytes = 0usize;
    let mut checkout_missing_blob_count = 0usize;

    for frame in resolved {
        let oid = ObjectId::from_bytes(frame.object.oid);
        let data_len = frame.object.data.len();
        let is_checkout_needed_blob =
            frame.object.object_type == ObjectType::Blob && checkout_needed_blobs.contains(&oid);
        let should_keep_resident = frame.object.object_type != ObjectType::Blob
            && retained_object_bytes.saturating_add(data_len) <= resident_limit;

        let state = if is_checkout_needed_blob && data_len == 0 && frame.object.size != 0 {
            checkout_missing_blob_count += 1;
            ObjectDataState::Reconstructable
        } else if is_checkout_needed_blob
            && checkout_resident_bytes.saturating_add(data_len) <= checkout_resident_limit
        {
            retained_object_count += 1;
            retained_object_bytes += data_len;
            checkout_resident_bytes += data_len;
            checkout_ready_blob_count += 1;
            checkout_ready_blob_bytes += data_len;
            ObjectDataState::Resident(frame.object.data)
        } else if is_checkout_needed_blob {
            let spilled = spill_object(pack_path, spill_dir, &frame.object.data)?;
            spilled_object_count += 1;
            spilled_object_bytes = spilled_object_bytes.saturating_add(data_len);
            checkout_spilled_blob_count += 1;
            checkout_spilled_blob_bytes = checkout_spilled_blob_bytes.saturating_add(data_len);
            checkout_ready_blob_count += 1;
            checkout_ready_blob_bytes += data_len;
            enforce_max_spill_bytes(max_spill_bytes, spilled_object_bytes)?;
            ObjectDataState::Spilled {
                path: spilled.path,
                offset: spilled.offset,
                len: data_len as u64,
            }
        } else if should_keep_resident {
            retained_object_count += 1;
            retained_object_bytes += data_len;
            ObjectDataState::Resident(frame.object.data)
        } else if frame.object.object_type == ObjectType::Blob && spill_blobs {
            let spilled = spill_object(pack_path, spill_dir, &frame.object.data)?;
            spilled_object_count += 1;
            spilled_object_bytes = spilled_object_bytes.saturating_add(data_len);
            enforce_max_spill_bytes(max_spill_bytes, spilled_object_bytes)?;
            ObjectDataState::Spilled {
                path: spilled.path,
                offset: spilled.offset,
                len: data_len as u64,
            }
        } else {
            ObjectDataState::Reconstructable
        };
        state_by_oid.insert(oid, state);
    }

    Ok(ObjectStateBuild {
        state_by_oid,
        retained_object_count,
        retained_object_bytes,
        spilled_object_count,
        spilled_object_bytes,
        checkout_needed_blob_count: checkout_needed_blobs.len(),
        checkout_ready_blob_count,
        checkout_ready_blob_bytes,
        checkout_spilled_blob_count,
        checkout_spilled_blob_bytes,
        checkout_missing_blob_count,
    })
}

fn pack_frame_metrics(frames: &[ObjectFrame]) -> PackFrameMetrics {
    let mut metrics = PackFrameMetrics {
        object_count: frames.len(),
        ..PackFrameMetrics::default()
    };
    for frame in frames {
        metrics.declared_inflated_bytes = metrics
            .declared_inflated_bytes
            .saturating_add(frame.declared_size);
        match frame.encoded {
            EncodedObjectKind::Base(_) => metrics.base_object_count += 1,
            EncodedObjectKind::OffsetDelta { .. } => {
                metrics.delta_count += 1;
                metrics.offset_delta_count += 1;
            }
            EncodedObjectKind::RefDelta { .. } => {
                metrics.delta_count += 1;
                metrics.ref_delta_count += 1;
            }
        }
    }
    metrics
}

fn update_atomic_max(value: &AtomicU64, candidate: u64) {
    let mut current = value.load(Ordering::Relaxed);
    while candidate > current {
        match value.compare_exchange(current, candidate, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(previous) => current = previous,
        }
    }
}

fn enforce_max_spill_bytes(
    max: Option<usize>,
    spilled_object_bytes: usize,
) -> Result<(), CloneError> {
    if let Some(max_spill_bytes) = max
        && spilled_object_bytes > max_spill_bytes
    {
        return Err(CloneError::CloneLimitExceeded {
            operation: "spilling object data",
            detail: format!(
                "FCL_MAX_SPILL_BYTES is {max_spill_bytes}, but spilled object data reached {spilled_object_bytes} bytes"
            ),
        });
    }
    Ok(())
}

fn checkout_needed_blobs(
    checkout_hint: Option<CheckoutHint>,
    resolved: &[ResolvedFrame],
) -> Result<HashSet<ObjectId>, CloneError> {
    let Some(checkout_hint) = checkout_hint else {
        return Ok(HashSet::new());
    };
    let objects_by_oid = resolved
        .iter()
        .map(|frame| (ObjectId::from_bytes(frame.object.oid), frame))
        .collect::<HashMap<_, _>>();
    let commit = objects_by_oid
        .get(&checkout_hint.default_commit)
        .ok_or_else(|| CloneError::ObjectLookupFailed {
            oid: checkout_hint.default_commit.to_hex(),
            expected_type: "commit",
            detail: "default branch commit was not present in the fetched pack".to_owned(),
        })?;
    if commit.object.object_type != ObjectType::Commit {
        return Err(CloneError::ObjectLookupFailed {
            oid: checkout_hint.default_commit.to_hex(),
            expected_type: "commit",
            detail: format!("found {}", commit.object.object_type.as_git_name()),
        });
    }
    let root_tree_oid =
        git_object::parse_commit_tree_oid(checkout_hint.default_commit, &commit.object.data)?;
    let mut needed_blobs = HashSet::new();
    let mut queued_trees = vec![root_tree_oid];
    let mut seen_trees = HashSet::new();
    while let Some(tree_oid) = queued_trees.pop() {
        if !seen_trees.insert(tree_oid) {
            continue;
        }
        let tree = objects_by_oid
            .get(&tree_oid)
            .ok_or_else(|| CloneError::ObjectLookupFailed {
                oid: tree_oid.to_hex(),
                expected_type: "tree",
                detail: "default branch tree was not present in the fetched pack".to_owned(),
            })?;
        if tree.object.object_type != ObjectType::Tree {
            return Err(CloneError::ObjectLookupFailed {
                oid: tree_oid.to_hex(),
                expected_type: "tree",
                detail: format!("found {}", tree.object.object_type.as_git_name()),
            });
        }
        for entry in git_object::parse_tree_entries(tree_oid, &tree.object.data)? {
            match entry.mode {
                TreeEntryMode::Directory => queued_trees.push(entry.oid),
                TreeEntryMode::File | TreeEntryMode::Executable | TreeEntryMode::Symlink => {
                    needed_blobs.insert(entry.oid);
                }
                TreeEntryMode::Gitlink => {}
            }
        }
    }
    Ok(needed_blobs)
}

#[derive(Debug, Clone)]
struct SpilledObject {
    path: PathBuf,
    offset: u64,
}

fn spill_object(
    pack_path: &Path,
    spill_dir: &Path,
    data: &[u8],
) -> Result<SpilledObject, CloneError> {
    fs::create_dir_all(spill_dir).map_err(|error| CloneError::PackIndexFailed {
        path: spill_dir.to_owned(),
        operation: "creating object spill directory",
        detail: error.to_string(),
    })?;
    let path = spill_dir.join("fcl-spill.dat");
    let offset = fs::metadata(&path).map_or(0, |metadata| metadata.len());
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|error| CloneError::PackIndexFailed {
            path: path.clone(),
            operation: "opening object spill arena",
            detail: error.to_string(),
        })?;
    file.write_all(data)
        .map_err(|error| CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation: "spilling object data",
            detail: format!("{}: {error}", path.display()),
        })?;
    Ok(SpilledObject { path, offset })
}

fn optional_usize_env(name: &'static str) -> Result<Option<usize>, CloneError> {
    let Some(raw) = std::env::var_os(name) else {
        return Ok(None);
    };
    let raw = raw.to_string_lossy();
    let value = raw
        .parse::<usize>()
        .map_err(|error| CloneError::CloneLimitExceeded {
            operation: "parsing clone safety limit",
            detail: format!("{name} must be an unsigned byte count, got `{raw}`: {error}"),
        })?;
    Ok(Some(value))
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn u64_saturating_from_u128(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[derive(Debug, Clone, Copy)]
pub enum ScanPayload {
    Inflate,
    MetadataOnly,
}

#[derive(Debug)]
pub struct StreamingPackScanner {
    pack_path: PathBuf,
    payload: ScanPayload,
    state: ScannerState,
    header: [u8; 12],
    header_len: usize,
    pack_offset: usize,
    declared_objects: Option<usize>,
    frames: Vec<ObjectFrame>,
    current: Option<PartialObjectFrame>,
    trailer_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScannerState {
    PackHeader,
    ObjectHeader,
    OffsetDeltaBase,
    RefDeltaBase,
    CompressedPayload,
    Trailer,
}

#[derive(Debug)]
struct PartialObjectFrame {
    object_start: usize,
    kind_id: u8,
    declared_size: u64,
    size_shift: u32,
    encoded: Option<EncodedObjectKind>,
    compressed_start: usize,
    compressed_len: usize,
    crc32: Crc32,
    offset_delta_distance: u64,
    offset_delta_started: bool,
    ref_delta_base_oid: [u8; 20],
    ref_delta_base_len: usize,
    decompressor: Decompress,
    inflated: Vec<u8>,
}

impl StreamingPackScanner {
    pub fn new(pack_path: &Path) -> Self {
        Self::with_payload(pack_path, ScanPayload::MetadataOnly)
    }

    pub fn with_payload(pack_path: &Path, payload: ScanPayload) -> Self {
        Self {
            pack_path: pack_path.to_owned(),
            payload,
            state: ScannerState::PackHeader,
            header: [0; 12],
            header_len: 0,
            pack_offset: 0,
            declared_objects: None,
            frames: Vec::new(),
            current: None,
            trailer_len: 0,
        }
    }

    pub fn feed(&mut self, mut bytes: &[u8]) -> Result<(), CloneError> {
        self.feed_inner(&mut bytes, None)
    }

    pub fn feed_collect(&mut self, mut bytes: &[u8]) -> Result<Vec<ObjectFrame>, CloneError> {
        let mut frames = Vec::new();
        self.feed_inner(&mut bytes, Some(&mut frames))?;
        Ok(frames)
    }

    fn feed_inner(
        &mut self,
        bytes: &mut &[u8],
        mut completed: Option<&mut Vec<ObjectFrame>>,
    ) -> Result<(), CloneError> {
        while !bytes.is_empty() {
            match self.state {
                ScannerState::PackHeader => self.feed_pack_header(bytes)?,
                ScannerState::ObjectHeader => self.feed_object_header(bytes)?,
                ScannerState::OffsetDeltaBase => self.feed_offset_delta_base(bytes)?,
                ScannerState::RefDeltaBase => self.feed_ref_delta_base(bytes)?,
                ScannerState::CompressedPayload => {
                    if let Some(frame) = self.feed_compressed_payload(bytes)?
                        && let Some(completed) = completed.as_deref_mut()
                    {
                        completed.push(frame);
                    }
                }
                ScannerState::Trailer => {
                    self.feed_trailer(bytes)?;
                    *bytes = &[];
                }
            }
        }
        Ok(())
    }

    pub fn finish(self, checksum: [u8; 20]) -> Result<PackScan, CloneError> {
        let Some(declared_objects) = self.declared_objects else {
            return Err(CloneError::PackIndexFailed {
                path: self.pack_path,
                operation: "scanning pack",
                detail: "pack header was incomplete".to_owned(),
            });
        };
        if self.frames.len() != declared_objects {
            return Err(CloneError::PackIndexFailed {
                path: self.pack_path,
                operation: "scanning pack",
                detail: format!(
                    "scanned {} objects but pack declared {declared_objects}",
                    self.frames.len()
                ),
            });
        }
        if self.trailer_len != 20 {
            return Err(CloneError::PackIndexFailed {
                path: self.pack_path,
                operation: "scanning pack",
                detail: format!("pack trailer was {} bytes, expected 20", self.trailer_len),
            });
        }
        Ok(PackScan {
            checksum,
            frames: self.frames,
        })
    }

    fn feed_pack_header(&mut self, bytes: &mut &[u8]) -> Result<(), CloneError> {
        let needed = self.header.len() - self.header_len;
        let take = needed.min(bytes.len());
        self.header[self.header_len..self.header_len + take].copy_from_slice(&bytes[..take]);
        self.header_len += take;
        self.pack_offset += take;
        *bytes = &bytes[take..];
        if self.header_len != self.header.len() {
            return Ok(());
        }
        if &self.header[0..4] != b"PACK" {
            return Err(CloneError::PackIndexFailed {
                path: self.pack_path.clone(),
                operation: "validating pack header",
                detail: "file does not start with PACK".to_owned(),
            });
        }
        let version = u32::from_be_bytes([
            self.header[4],
            self.header[5],
            self.header[6],
            self.header[7],
        ]);
        if version != 2 && version != 3 {
            return Err(CloneError::PackIndexFailed {
                path: self.pack_path.clone(),
                operation: "parsing pack header",
                detail: format!("unsupported pack version {version}"),
            });
        }
        let count = u32::from_be_bytes([
            self.header[8],
            self.header[9],
            self.header[10],
            self.header[11],
        ]) as usize;
        enforce_max_objects(count)?;
        self.declared_objects = Some(count);
        self.frames = Vec::with_capacity(count);
        self.state = if count == 0 {
            ScannerState::Trailer
        } else {
            ScannerState::ObjectHeader
        };
        Ok(())
    }

    fn feed_object_header(&mut self, bytes: &mut &[u8]) -> Result<(), CloneError> {
        let Some(byte) = take_byte(bytes) else {
            return Ok(());
        };
        if self.current.is_none() {
            let kind_id = (byte >> 4) & 0b111;
            let mut crc32 = Crc32::new();
            crc32.update(&[byte]);
            self.current = Some(PartialObjectFrame {
                object_start: self.pack_offset,
                kind_id,
                declared_size: u64::from(byte & 0b1111),
                size_shift: 4,
                encoded: None,
                compressed_start: 0,
                compressed_len: 0,
                crc32,
                offset_delta_distance: 0,
                offset_delta_started: false,
                ref_delta_base_oid: [0; 20],
                ref_delta_base_len: 0,
                decompressor: Decompress::new(true),
                inflated: Vec::new(),
            });
        } else if let Some(current) = self.current.as_mut() {
            current.crc32.update(&[byte]);
            current.declared_size |= u64::from(byte & 0x7f) << current.size_shift;
            current.size_shift += 7;
        }
        self.pack_offset += 1;
        if byte & 0x80 != 0 {
            return Ok(());
        }
        self.finish_object_header()
    }

    fn finish_object_header(&mut self) -> Result<(), CloneError> {
        let current = self
            .current
            .as_mut()
            .ok_or_else(|| CloneError::PackIndexFailed {
                path: self.pack_path.clone(),
                operation: "parsing object header",
                detail: "object header state was missing".to_owned(),
            })?;
        match current.kind_id {
            1 => self.begin_compressed_payload(EncodedObjectKind::Base(ObjectType::Commit)),
            2 => self.begin_compressed_payload(EncodedObjectKind::Base(ObjectType::Tree)),
            3 => self.begin_compressed_payload(EncodedObjectKind::Base(ObjectType::Blob)),
            4 => self.begin_compressed_payload(EncodedObjectKind::Base(ObjectType::Tag)),
            6 => {
                self.state = ScannerState::OffsetDeltaBase;
                Ok(())
            }
            7 => {
                self.state = ScannerState::RefDeltaBase;
                Ok(())
            }
            other => Err(CloneError::PackIndexFailed {
                path: self.pack_path.clone(),
                operation: "parsing object header",
                detail: format!(
                    "object at offset {} used unknown type {other}",
                    current.object_start
                ),
            }),
        }
    }

    fn feed_offset_delta_base(&mut self, bytes: &mut &[u8]) -> Result<(), CloneError> {
        let Some(byte) = take_byte(bytes) else {
            return Ok(());
        };
        let current = self
            .current
            .as_mut()
            .ok_or_else(|| CloneError::PackIndexFailed {
                path: self.pack_path.clone(),
                operation: "parsing offset delta base",
                detail: "offset delta state was missing".to_owned(),
            })?;
        current.crc32.update(&[byte]);
        if current.offset_delta_started {
            current.offset_delta_distance =
                ((current.offset_delta_distance + 1) << 7) | u64::from(byte & 0x7f);
        } else {
            current.offset_delta_distance = u64::from(byte & 0x7f);
            current.offset_delta_started = true;
        }
        self.pack_offset += 1;
        if byte & 0x80 != 0 {
            return Ok(());
        }
        let base_offset = (current.object_start as u64)
            .checked_sub(current.offset_delta_distance)
            .ok_or_else(|| CloneError::PackIndexFailed {
                path: self.pack_path.clone(),
                operation: "parsing offset delta base",
                detail: format!(
                    "delta at offset {} points before the pack",
                    current.object_start
                ),
            })?;
        self.begin_compressed_payload(EncodedObjectKind::OffsetDelta { base_offset })
    }

    fn feed_ref_delta_base(&mut self, bytes: &mut &[u8]) -> Result<(), CloneError> {
        let base_oid = {
            let current = self
                .current
                .as_mut()
                .ok_or_else(|| CloneError::PackIndexFailed {
                    path: self.pack_path.clone(),
                    operation: "parsing ref delta base",
                    detail: "ref delta state was missing".to_owned(),
                })?;
            let needed = 20 - current.ref_delta_base_len;
            let take = needed.min(bytes.len());
            current.ref_delta_base_oid
                [current.ref_delta_base_len..current.ref_delta_base_len + take]
                .copy_from_slice(&bytes[..take]);
            current.crc32.update(&bytes[..take]);
            current.ref_delta_base_len += take;
            self.pack_offset += take;
            *bytes = &bytes[take..];
            if current.ref_delta_base_len != 20 {
                return Ok(());
            }
            current.ref_delta_base_oid
        };
        self.begin_compressed_payload(EncodedObjectKind::RefDelta { base_oid })
    }

    fn begin_compressed_payload(&mut self, encoded: EncodedObjectKind) -> Result<(), CloneError> {
        let current = self
            .current
            .as_mut()
            .ok_or_else(|| CloneError::PackIndexFailed {
                path: self.pack_path.clone(),
                operation: "scanning object frame",
                detail: "object state was missing".to_owned(),
            })?;
        current.encoded = Some(encoded);
        current.compressed_start = self.pack_offset;
        self.state = ScannerState::CompressedPayload;
        Ok(())
    }

    fn feed_compressed_payload(
        &mut self,
        bytes: &mut &[u8],
    ) -> Result<Option<ObjectFrame>, CloneError> {
        let current = self
            .current
            .as_mut()
            .ok_or_else(|| CloneError::PackIndexFailed {
                path: self.pack_path.clone(),
                operation: "scanning compressed object",
                detail: "compressed object state was missing".to_owned(),
            })?;
        let before_in = current.decompressor.total_in();
        let before_out = current.decompressor.total_out();
        let mut output = [0u8; 8192];
        let status = current
            .decompressor
            .decompress(bytes, &mut output, FlushDecompress::None)
            .map_err(|error| CloneError::PackIndexFailed {
                path: self.pack_path.clone(),
                operation: "scanning compressed object",
                detail: error.to_string(),
            })?;
        let consumed = usize_from_u64(
            &self.pack_path,
            "tracking compressed input",
            current.decompressor.total_in() - before_in,
        )?;
        let inflated = current.decompressor.total_out() - before_out;
        let written = usize_from_u64(&self.pack_path, "tracking inflated output", inflated)?;
        if matches!(self.payload, ScanPayload::Inflate) && written > 0 {
            current.inflated.extend_from_slice(&output[..written]);
        }
        if consumed > 0 {
            current.crc32.update(&bytes[..consumed]);
            current.compressed_len += consumed;
            self.pack_offset += consumed;
            *bytes = &bytes[consumed..];
        }
        if status == Status::StreamEnd {
            if current.decompressor.total_out() != current.declared_size {
                return Err(CloneError::PackIndexFailed {
                    path: self.pack_path.clone(),
                    operation: "scanning compressed object",
                    detail: format!(
                        "object at offset {} inflated to {} bytes, expected {}",
                        current.object_start,
                        current.decompressor.total_out(),
                        current.declared_size
                    ),
                });
            }
            return self.finish_frame().map(Some);
        }
        if consumed == 0 && inflated == 0 {
            return Err(CloneError::PackIndexFailed {
                path: self.pack_path.clone(),
                operation: "scanning compressed object",
                detail: "decompressor made no progress".to_owned(),
            });
        }
        Ok(None)
    }

    fn finish_frame(&mut self) -> Result<ObjectFrame, CloneError> {
        let current = self
            .current
            .take()
            .ok_or_else(|| CloneError::PackIndexFailed {
                path: self.pack_path.clone(),
                operation: "scanning object frame",
                detail: "object state was missing".to_owned(),
            })?;
        let encoded = current.encoded.ok_or_else(|| CloneError::PackIndexFailed {
            path: self.pack_path.clone(),
            operation: "scanning object frame",
            detail: "object encoding state was missing".to_owned(),
        })?;
        let inflated = if matches!(self.payload, ScanPayload::Inflate) {
            Some(Arc::from(current.inflated))
        } else {
            None
        };
        let frame = ObjectFrame {
            offset: current.object_start as u64,
            compressed_start: current.compressed_start,
            compressed_len: current.compressed_len,
            crc32: current.crc32.finalize(),
            encoded,
            inflated,
            declared_size: current.declared_size,
        };
        self.frames.push(frame.clone());
        let declared_objects =
            self.declared_objects
                .ok_or_else(|| CloneError::PackIndexFailed {
                    path: self.pack_path.clone(),
                    operation: "scanning pack",
                    detail: "pack object count was missing".to_owned(),
                })?;
        self.state = if self.frames.len() == declared_objects {
            ScannerState::Trailer
        } else {
            ScannerState::ObjectHeader
        };
        Ok(frame)
    }

    fn feed_trailer(&mut self, bytes: &[u8]) -> Result<(), CloneError> {
        self.trailer_len += bytes.len();
        self.pack_offset += bytes.len();
        if self.trailer_len > 20 {
            return Err(CloneError::PackIndexFailed {
                path: self.pack_path.clone(),
                operation: "scanning pack",
                detail: format!("pack trailer exceeded 20 bytes: {}", self.trailer_len),
            });
        }
        Ok(())
    }
}

fn take_byte(bytes: &mut &[u8]) -> Option<u8> {
    let (byte, rest) = bytes.split_first()?;
    *bytes = rest;
    Some(*byte)
}

#[cfg(test)]
fn scan_pack(pack_path: &Path, pack: &[u8], payload: ScanPayload) -> Result<PackScan, CloneError> {
    let checksum = validate_pack(pack_path, pack)?;
    scan_pack_with_checksum(pack_path, pack, payload, checksum)
}

#[cfg(test)]
fn scan_pack_with_checksum(
    pack_path: &Path,
    pack: &[u8],
    payload: ScanPayload,
    checksum: [u8; 20],
) -> Result<PackScan, CloneError> {
    validate_pack_header(pack_path, pack)?;
    if pack[pack.len() - 20..] != checksum {
        return Err(CloneError::PackChecksumMismatch {
            path: pack_path.to_owned(),
            expected: hex::encode(checksum),
            actual: hex::encode(&pack[pack.len() - 20..]),
        });
    }
    let version = u32::from_be_bytes([pack[4], pack[5], pack[6], pack[7]]);
    if version != 2 && version != 3 {
        return Err(CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation: "parsing pack header",
            detail: format!("unsupported pack version {version}"),
        });
    }

    let count = u32::from_be_bytes([pack[8], pack[9], pack[10], pack[11]]) as usize;
    enforce_max_objects(count)?;
    let mut offset = 12usize;
    let mut frames = Vec::with_capacity(count);

    for _ in 0..count {
        let object_start = offset;
        let (kind_id, declared_size, next_offset) = parse_object_header(pack_path, pack, offset)?;
        offset = next_offset;

        let encoded = match kind_id {
            1 => EncodedObjectKind::Base(ObjectType::Commit),
            2 => EncodedObjectKind::Base(ObjectType::Tree),
            3 => EncodedObjectKind::Base(ObjectType::Blob),
            4 => EncodedObjectKind::Base(ObjectType::Tag),
            6 => {
                let (base_offset, next_offset) =
                    parse_offset_delta_base(pack_path, pack, object_start, offset)?;
                offset = next_offset;
                EncodedObjectKind::OffsetDelta { base_offset }
            }
            7 => {
                if pack.len() - offset < 20 {
                    return Err(CloneError::PackIndexFailed {
                        path: pack_path.to_owned(),
                        operation: "parsing ref delta base",
                        detail: format!(
                            "ref delta at offset {object_start} has a truncated base oid"
                        ),
                    });
                }
                let mut base_oid = [0u8; 20];
                base_oid.copy_from_slice(&pack[offset..offset + 20]);
                offset += 20;
                EncodedObjectKind::RefDelta { base_oid }
            }
            other => {
                return Err(CloneError::PackIndexFailed {
                    path: pack_path.to_owned(),
                    operation: "parsing object header",
                    detail: format!("object at offset {object_start} used unknown type {other}"),
                });
            }
        };

        let compressed_start = offset;
        let (inflated, compressed_len) = match payload {
            ScanPayload::Inflate => {
                let (inflated, compressed_len) =
                    inflate_next_frame(pack_path, pack, offset, declared_size)?;
                (Some(inflated), compressed_len)
            }
            ScanPayload::MetadataOnly => (None, scan_zlib_stream_len(pack_path, pack, offset)?),
        };
        offset += compressed_len;
        if offset > pack.len() - 20 {
            return Err(CloneError::PackIndexFailed {
                path: pack_path.to_owned(),
                operation: "scanning object frame",
                detail: format!("object at offset {object_start} overlaps the pack trailer"),
            });
        }
        let mut crc = Crc32::new();
        crc.update(&pack[object_start..offset]);
        frames.push(ObjectFrame {
            offset: object_start as u64,
            compressed_start,
            compressed_len,
            crc32: crc.finalize(),
            encoded,
            inflated,
            declared_size,
        });
    }

    if offset != pack.len() - 20 {
        return Err(CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation: "scanning pack",
            detail: format!(
                "scan ended at offset {offset}, expected trailer at {}",
                pack.len() - 20
            ),
        });
    }

    Ok(PackScan { checksum, frames })
}

fn scan_pack_file_windowed(
    pack_path: &Path,
    payload: ScanPayload,
    checksum: [u8; 20],
) -> Result<PackScan, CloneError> {
    let mut file = File::open(pack_path).map_err(|error| CloneError::PackIndexFailed {
        path: pack_path.to_owned(),
        operation: "opening pack file for windowed scan",
        detail: error.to_string(),
    })?;
    let mut scanner = StreamingPackScanner::with_payload(pack_path, payload);
    let mut buffer = vec![0u8; env_usize("FCL_SCAN_BUFFER", 1024 * 1024)];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| CloneError::PackIndexFailed {
                path: pack_path.to_owned(),
                operation: "reading pack file for windowed scan",
                detail: error.to_string(),
            })?;
        if read == 0 {
            break;
        }
        scanner.feed(&buffer[..read])?;
    }
    scanner.finish(checksum)
}

#[cfg(test)]
#[cfg(test)]
fn validate_pack(pack_path: &Path, pack: &[u8]) -> Result<[u8; 20], CloneError> {
    validate_pack_header(pack_path, pack)?;
    let expected = &pack[pack.len() - 20..];
    let actual = Sha1::digest(&pack[..pack.len() - 20]);
    if expected != actual.as_slice() {
        return Err(CloneError::PackChecksumMismatch {
            path: pack_path.to_owned(),
            expected: hex::encode(expected),
            actual: hex::encode(actual),
        });
    }
    let mut checksum = [0u8; 20];
    checksum.copy_from_slice(expected);
    Ok(checksum)
}

#[cfg(test)]
fn validate_pack_header(pack_path: &Path, pack: &[u8]) -> Result<(), CloneError> {
    if pack.len() < 32 || &pack[0..4] != b"PACK" {
        return Err(CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation: "validating pack header",
            detail: "file does not start with PACK".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
fn parse_object_header(
    pack_path: &Path,
    pack: &[u8],
    mut offset: usize,
) -> Result<(u8, u64, usize), CloneError> {
    let first = *pack
        .get(offset)
        .ok_or_else(|| CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation: "parsing object header",
            detail: "object header is truncated".to_owned(),
        })?;
    offset += 1;
    let kind = (first >> 4) & 0b111;
    let mut size = u64::from(first & 0b1111);
    let mut shift = 4;
    let mut byte = first;
    while byte & 0x80 != 0 {
        byte = *pack
            .get(offset)
            .ok_or_else(|| CloneError::PackIndexFailed {
                path: pack_path.to_owned(),
                operation: "parsing object header",
                detail: "object size varint is truncated".to_owned(),
            })?;
        offset += 1;
        size |= u64::from(byte & 0x7f) << shift;
        shift += 7;
    }
    Ok((kind, size, offset))
}

#[cfg(test)]
fn parse_offset_delta_base(
    pack_path: &Path,
    pack: &[u8],
    object_start: usize,
    mut offset: usize,
) -> Result<(u64, usize), CloneError> {
    let mut byte = *pack
        .get(offset)
        .ok_or_else(|| CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation: "parsing offset delta base",
            detail: "offset delta base is truncated".to_owned(),
        })?;
    offset += 1;
    let mut distance = u64::from(byte & 0x7f);
    while byte & 0x80 != 0 {
        byte = *pack
            .get(offset)
            .ok_or_else(|| CloneError::PackIndexFailed {
                path: pack_path.to_owned(),
                operation: "parsing offset delta base",
                detail: "offset delta base varint is truncated".to_owned(),
            })?;
        offset += 1;
        distance = ((distance + 1) << 7) | u64::from(byte & 0x7f);
    }
    let base_offset =
        (object_start as u64)
            .checked_sub(distance)
            .ok_or_else(|| CloneError::PackIndexFailed {
                path: pack_path.to_owned(),
                operation: "parsing offset delta base",
                detail: format!("delta at offset {object_start} points before the pack"),
            })?;
    Ok((base_offset, offset))
}

#[cfg(test)]
fn inflate_next_frame(
    pack_path: &Path,
    pack: &[u8],
    offset: usize,
    declared_size: u64,
) -> Result<(Arc<[u8]>, usize), CloneError> {
    let mut decompressor = Decompress::new(true);
    let mut output = Vec::with_capacity(usize_from_u64(
        pack_path,
        "allocating inflated object buffer",
        declared_size,
    )?);
    let mut chunk = [0u8; 8192];

    loop {
        let before_in = decompressor.total_in();
        let before_out = decompressor.total_out();
        let status = decompressor
            .decompress(
                &pack[offset + usize_from_u64(pack_path, "tracking compressed input", before_in)?
                    ..pack.len() - 20],
                &mut chunk,
                FlushDecompress::None,
            )
            .map_err(|error| CloneError::PackIndexFailed {
                path: pack_path.to_owned(),
                operation: "inflating object while scanning pack",
                detail: error.to_string(),
            })?;
        let written = usize_from_u64(
            pack_path,
            "tracking inflated output",
            decompressor.total_out() - before_out,
        )?;
        output.extend_from_slice(&chunk[..written]);
        if status == Status::StreamEnd {
            if output.len() as u64 != declared_size {
                return Err(CloneError::PackIndexFailed {
                    path: pack_path.to_owned(),
                    operation: "inflating object while scanning pack",
                    detail: format!(
                        "inflated to {} bytes, expected {declared_size}",
                        output.len()
                    ),
                });
            }
            return Ok((
                output.into(),
                usize_from_u64(
                    pack_path,
                    "tracking compressed input",
                    decompressor.total_in(),
                )?,
            ));
        }
        if decompressor.total_in() == before_in && decompressor.total_out() == before_out {
            return Err(CloneError::PackIndexFailed {
                path: pack_path.to_owned(),
                operation: "inflating object while scanning pack",
                detail: "decompressor made no progress".to_owned(),
            });
        }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "keeps delta scheduling state in one place"
)]
fn resolve_inflated_frames(
    pack_path: &Path,
    pack: &PackStorage,
    frames: &[ObjectFrame],
    keep_blob_data: bool,
) -> Result<Vec<ResolvedFrame>, CloneError> {
    let adjacency = build_delta_adjacency(frames);
    let offset_to_frame_index = frames
        .iter()
        .enumerate()
        .map(|(index, frame)| (frame.offset, index))
        .collect::<HashMap<_, _>>();
    let mut unresolved_children = vec![0usize; frames.len()];
    let mut ref_children_counted = vec![false; frames.len()];
    for (base_offset, children) in &adjacency.children_by_offset {
        if let Some(base_index) = offset_to_frame_index.get(base_offset) {
            unresolved_children[*base_index] =
                unresolved_children[*base_index].saturating_add(children.len());
        }
    }
    let base_results = frames
        .par_iter()
        .enumerate()
        .filter_map(|(frame_index, frame)| match frame.encoded {
            EncodedObjectKind::Base(object_type) => Some(
                frame_payload(pack_path, pack, frame).map(|payload| ResolvedFrame {
                    frame_index,
                    offset: frame.offset,
                    crc32: frame.crc32,
                    object: resolve_base_object(object_type, payload),
                }),
            ),
            EncodedObjectKind::OffsetDelta { .. } | EncodedObjectKind::RefDelta { .. } => None,
        })
        .collect::<Vec<_>>();

    let mut resolved = collect_results(base_results)?;
    let mut resolved_by_offset = HashMap::new();
    let mut resolved_by_oid = HashMap::new();
    for (index, frame) in resolved.iter().enumerate() {
        resolved_by_offset.insert(frame.offset, index);
        resolved_by_oid.insert(frame.object.oid, index);
    }
    for frame in frames {
        if let EncodedObjectKind::RefDelta { base_oid } = frame.encoded
            && let Some(base_index) = resolved_by_oid.get(&base_oid)
        {
            let base_frame_index = resolved[*base_index].frame_index;
            unresolved_children[base_frame_index] =
                unresolved_children[base_frame_index].saturating_add(1);
            ref_children_counted[base_frame_index] = true;
        }
    }

    let mut queued = vec![false; frames.len()];
    let mut ready = Vec::new();
    for frame in &resolved {
        enqueue_delta_children(frame, &adjacency, &mut queued, &mut ready);
    }

    let mut resolved_delta_count = 0usize;
    while resolved_delta_count < adjacency.delta_count {
        if ready.is_empty() {
            return Err(CloneError::PackIndexFailed {
                path: pack_path.to_owned(),
                operation: "resolving deltas",
                detail: format!(
                    "{} delta objects could not find resolved bases",
                    adjacency.delta_count - resolved_delta_count
                ),
            });
        }

        let current_ready = std::mem::take(&mut ready);

        let round_results = current_ready
            .par_iter()
            .map(|frame_index| {
                let frame = &frames[*frame_index];
                let (base_frame_index, base) =
                    delta_base(frame, &resolved, &resolved_by_offset, &resolved_by_oid)
                        .ok_or_else(|| CloneError::PackIndexFailed {
                            path: pack_path.to_owned(),
                            operation: "resolving delta base",
                            detail: format!(
                                "delta at offset {} lost its resolved base",
                                frame.offset
                            ),
                        })?;
                let delta = frame_payload(pack_path, pack, frame)?;
                let data = apply_delta(pack_path, &base.data, &delta)?;
                Ok((
                    ResolvedFrame {
                        frame_index: *frame_index,
                        offset: frame.offset,
                        crc32: frame.crc32,
                        object: resolve_base_object(base.object_type, data.into()),
                    },
                    base_frame_index,
                ))
            })
            .collect::<Vec<_>>();

        for (frame, base_frame_index) in collect_results(round_results)? {
            if !keep_blob_data {
                release_delta_base_if_done(
                    &mut resolved,
                    &mut unresolved_children,
                    base_frame_index,
                );
            }
            let index = resolved.len();
            resolved_by_offset.insert(frame.offset, index);
            resolved_by_oid.insert(frame.object.oid, index);
            count_ref_delta_children_once(
                &frame,
                &adjacency,
                &mut unresolved_children,
                &mut ref_children_counted,
            );
            enqueue_delta_children(&frame, &adjacency, &mut queued, &mut ready);
            resolved.push(frame);
            resolved_delta_count += 1;
        }
    }

    if resolved.len() != frames.len() {
        return Err(CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation: "resolving pack",
            detail: format!(
                "resolved {} objects but pack declared {}",
                resolved.len(),
                frames.len()
            ),
        });
    }

    Ok(resolved)
}

fn count_ref_delta_children_once(
    resolved: &ResolvedFrame,
    adjacency: &DeltaAdjacency,
    unresolved_children: &mut [usize],
    ref_children_counted: &mut [bool],
) {
    if ref_children_counted[resolved.frame_index] {
        return;
    }
    if let Some(children) = adjacency.children_by_oid.get(&resolved.object.oid) {
        unresolved_children[resolved.frame_index] =
            unresolved_children[resolved.frame_index].saturating_add(children.len());
    }
    ref_children_counted[resolved.frame_index] = true;
}

fn release_delta_base_if_done(
    resolved: &mut [ResolvedFrame],
    unresolved_children: &mut [usize],
    base_frame_index: usize,
) {
    if env_bool("FCL_SPILL_BLOBS") {
        return;
    }
    if unresolved_children[base_frame_index] > 0 {
        unresolved_children[base_frame_index] -= 1;
    }
    if unresolved_children[base_frame_index] != 0 {
        return;
    }
    for frame in resolved {
        if frame.frame_index == base_frame_index && frame.object.object_type == ObjectType::Blob {
            frame.object.data = Arc::<[u8]>::from([]);
            return;
        }
    }
}

fn frame_payload(
    pack_path: &Path,
    pack: &PackStorage,
    frame: &ObjectFrame,
) -> Result<Arc<[u8]>, CloneError> {
    frame.inflated.as_ref().map_or_else(
        || pack.inflate_frame(pack_path, frame),
        |inflated| Ok(Arc::clone(inflated)),
    )
}

#[cfg(test)]
fn scan_zlib_stream_len(pack_path: &Path, pack: &[u8], offset: usize) -> Result<usize, CloneError> {
    let mut decompressor = Decompress::new(true);
    let mut output = [0u8; 8192];

    loop {
        let before_in = decompressor.total_in();
        let before_out = decompressor.total_out();
        let status = decompressor
            .decompress(
                &pack[offset + usize_from_u64(pack_path, "tracking compressed input", before_in)?
                    ..pack.len() - 20],
                &mut output,
                FlushDecompress::None,
            )
            .map_err(|error| CloneError::PackIndexFailed {
                path: pack_path.to_owned(),
                operation: "scanning compressed object",
                detail: error.to_string(),
            })?;
        if status == Status::StreamEnd {
            return usize_from_u64(
                pack_path,
                "tracking compressed input",
                decompressor.total_in(),
            );
        }
        if decompressor.total_in() == before_in && decompressor.total_out() == before_out {
            return Err(CloneError::PackIndexFailed {
                path: pack_path.to_owned(),
                operation: "scanning compressed object",
                detail: "decompressor made no progress".to_owned(),
            });
        }
    }
}

#[cfg(any(test, not(unix)))]
fn inflate_frame(
    pack_path: &Path,
    pack: &[u8],
    frame: &ObjectFrame,
) -> Result<Arc<[u8]>, CloneError> {
    checked_frame_end(pack_path, frame, pack.len() as u64)?;
    let compressed = &pack[frame.compressed_start..frame.compressed_start + frame.compressed_len];
    inflate_compressed_frame(pack_path, frame, compressed)
}

fn inflate_compressed_frame(
    pack_path: &Path,
    frame: &ObjectFrame,
    compressed: &[u8],
) -> Result<Arc<[u8]>, CloneError> {
    let mut decompressor = Decompress::new(true);
    let mut output = Vec::with_capacity(usize_from_u64(
        pack_path,
        "allocating inflated object buffer",
        frame.declared_size,
    )?);
    let mut chunk = [0u8; 8192];

    loop {
        let before_in = decompressor.total_in();
        let before_out = decompressor.total_out();
        let status = decompressor
            .decompress(
                &compressed[usize_from_u64(pack_path, "tracking compressed input", before_in)?..],
                &mut chunk,
                FlushDecompress::None,
            )
            .map_err(|error| CloneError::PackIndexFailed {
                path: pack_path.to_owned(),
                operation: "inflating object",
                detail: error.to_string(),
            })?;
        let written = usize_from_u64(
            pack_path,
            "tracking inflated output",
            decompressor.total_out() - before_out,
        )?;
        output.extend_from_slice(&chunk[..written]);
        if status == Status::StreamEnd {
            if output.len() as u64 != frame.declared_size {
                return Err(CloneError::PackIndexFailed {
                    path: pack_path.to_owned(),
                    operation: "inflating object",
                    detail: format!(
                        "object at offset {} inflated to {} bytes, expected {}",
                        frame.offset,
                        output.len(),
                        frame.declared_size
                    ),
                });
            }
            return Ok(output.into());
        }
        if decompressor.total_in() == before_in && decompressor.total_out() == before_out {
            return Err(CloneError::PackIndexFailed {
                path: pack_path.to_owned(),
                operation: "inflating object",
                detail: "decompressor made no progress".to_owned(),
            });
        }
    }
}

fn checked_frame_end(
    pack_path: &Path,
    frame: &ObjectFrame,
    pack_len: u64,
) -> Result<u64, CloneError> {
    let compressed_start =
        u64::try_from(frame.compressed_start).map_err(|error| CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation: "reading compressed object range",
            detail: format!(
                "compressed start {} did not fit u64: {error}",
                frame.compressed_start
            ),
        })?;
    let compressed_len =
        u64::try_from(frame.compressed_len).map_err(|error| CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation: "reading compressed object range",
            detail: format!(
                "compressed length {} did not fit u64: {error}",
                frame.compressed_len
            ),
        })?;
    let compressed_end = compressed_start
        .checked_add(compressed_len)
        .ok_or_else(|| CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation: "reading compressed object range",
            detail: format!(
                "compressed range {}..+{} overflowed u64",
                frame.compressed_start, frame.compressed_len
            ),
        })?;
    if compressed_end > pack_len {
        return Err(CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation: "reading compressed object range",
            detail: format!(
                "object at offset {} uses compressed range {}..{}, but pack is {pack_len} bytes",
                frame.offset, frame.compressed_start, compressed_end
            ),
        });
    }
    Ok(compressed_end)
}

#[cfg(unix)]
fn read_exact_at(
    pack_path: &Path,
    file: &File,
    offset: u64,
    buffer: &mut [u8],
    operation: &'static str,
) -> Result<(), CloneError> {
    let mut read = 0usize;
    while read < buffer.len() {
        match file.read_at(&mut buffer[read..], offset + read as u64) {
            Ok(0) => {
                return Err(CloneError::PackIndexFailed {
                    path: pack_path.to_owned(),
                    operation,
                    detail: format!(
                        "short read at offset {offset}: read {read} of {} bytes",
                        buffer.len()
                    ),
                });
            }
            Ok(bytes) => read += bytes,
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => {
                return Err(CloneError::PackIndexFailed {
                    path: pack_path.to_owned(),
                    operation,
                    detail: format!(
                        "failed at offset {} after reading {read} of {} bytes: {error}",
                        offset + read as u64,
                        buffer.len()
                    ),
                });
            }
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn read_exact_at(
    pack_path: &Path,
    file: &File,
    offset: u64,
    buffer: &mut [u8],
    operation: &'static str,
) -> Result<(), CloneError> {
    use std::io::{Seek, SeekFrom};

    let mut file = file
        .try_clone()
        .map_err(|error| CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation,
            detail: format!("cloning file handle for positional read failed: {error}"),
        })?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|error| CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation,
            detail: format!("seeking to offset {offset} failed: {error}"),
        })?;
    file.read_exact(buffer)
        .map_err(|error| CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation,
            detail: format!(
                "failed to read {} bytes at offset {offset}: {error}",
                buffer.len()
            ),
        })
}

fn env_bool(name: &str) -> bool {
    matches!(
        std::env::var(name).as_deref(),
        Ok("1" | "true" | "TRUE" | "yes" | "on")
    )
}

fn enforce_max_objects(count: usize) -> Result<(), CloneError> {
    let Some(raw) = std::env::var_os("FCL_MAX_OBJECTS") else {
        return Ok(());
    };
    let raw = raw.to_string_lossy();
    let max = raw
        .parse::<usize>()
        .map_err(|error| CloneError::CloneLimitExceeded {
            operation: "parsing clone safety limit",
            detail: format!(
                "FCL_MAX_OBJECTS must be an unsigned object count, got `{raw}`: {error}"
            ),
        })?;
    if count > max {
        return Err(CloneError::CloneLimitExceeded {
            operation: "checking pack object count",
            detail: format!("FCL_MAX_OBJECTS is {max}, but the pack declares {count} objects"),
        });
    }
    Ok(())
}

fn build_delta_adjacency(frames: &[ObjectFrame]) -> DeltaAdjacency {
    let mut children_by_offset = HashMap::<u64, Vec<usize>>::new();
    let mut children_by_oid = HashMap::<[u8; 20], Vec<usize>>::new();
    let mut delta_count = 0usize;

    for (index, frame) in frames.iter().enumerate() {
        match frame.encoded {
            EncodedObjectKind::Base(_) => {}
            EncodedObjectKind::OffsetDelta { base_offset } => {
                children_by_offset
                    .entry(base_offset)
                    .or_default()
                    .push(index);
                delta_count += 1;
            }
            EncodedObjectKind::RefDelta { base_oid } => {
                children_by_oid.entry(base_oid).or_default().push(index);
                delta_count += 1;
            }
        }
    }

    DeltaAdjacency {
        children_by_offset,
        children_by_oid,
        delta_count,
    }
}

fn enqueue_delta_children(
    resolved: &ResolvedFrame,
    adjacency: &DeltaAdjacency,
    queued: &mut [bool],
    ready: &mut Vec<usize>,
) {
    if let Some(children) = adjacency.children_by_offset.get(&resolved.offset) {
        for child in children {
            if !queued[*child] {
                queued[*child] = true;
                ready.push(*child);
            }
        }
    }
    if let Some(children) = adjacency.children_by_oid.get(&resolved.object.oid) {
        for child in children {
            if !queued[*child] {
                queued[*child] = true;
                ready.push(*child);
            }
        }
    }
}

fn collect_results<T>(results: Vec<Result<T, CloneError>>) -> Result<Vec<T>, CloneError> {
    results.into_iter().collect()
}

fn delta_base<'a>(
    frame: &ObjectFrame,
    resolved: &'a [ResolvedFrame],
    resolved_by_offset: &HashMap<u64, usize>,
    resolved_by_oid: &HashMap<[u8; 20], usize>,
) -> Option<(usize, &'a ResolvedObject)> {
    let index = match frame.encoded {
        EncodedObjectKind::Base(_) => return None,
        EncodedObjectKind::OffsetDelta { base_offset } => resolved_by_offset.get(&base_offset),
        EncodedObjectKind::RefDelta { base_oid } => resolved_by_oid.get(&base_oid),
    }?;
    Some((resolved[*index].frame_index, &resolved[*index].object))
}

fn resolve_base_object(object_type: ObjectType, data: Arc<[u8]>) -> ResolvedObject {
    let size = data.len() as u64;
    let mut hasher = Sha1::new();
    hasher.update(object_type.as_git_name().as_bytes());
    hasher.update(b" ");
    hasher.update(data.len().to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(&data);
    let digest = hasher.finalize();
    let mut oid = [0u8; 20];
    oid.copy_from_slice(&digest);
    ResolvedObject {
        object_type,
        data,
        size,
        oid,
    }
}

fn apply_delta(pack_path: &Path, base: &[u8], delta: &[u8]) -> Result<Vec<u8>, CloneError> {
    let mut cursor = 0usize;
    let source_size = read_delta_size(pack_path, delta, &mut cursor)?;
    if source_size != base.len() {
        return Err(CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation: "applying delta",
            detail: format!(
                "delta expected base size {source_size}, found {}",
                base.len()
            ),
        });
    }
    let target_size = read_delta_size(pack_path, delta, &mut cursor)?;
    let mut output = Vec::with_capacity(target_size);

    while cursor < delta.len() {
        let command = delta[cursor];
        cursor += 1;
        if command & 0x80 != 0 {
            let mut copy_offset = 0usize;
            let mut copy_size = 0usize;
            if command & 0x01 != 0 {
                copy_offset |= usize::from(delta_byte(pack_path, delta, &mut cursor)?);
            }
            if command & 0x02 != 0 {
                copy_offset |= usize::from(delta_byte(pack_path, delta, &mut cursor)?) << 8;
            }
            if command & 0x04 != 0 {
                copy_offset |= usize::from(delta_byte(pack_path, delta, &mut cursor)?) << 16;
            }
            if command & 0x08 != 0 {
                copy_offset |= usize::from(delta_byte(pack_path, delta, &mut cursor)?) << 24;
            }
            if command & 0x10 != 0 {
                copy_size |= usize::from(delta_byte(pack_path, delta, &mut cursor)?);
            }
            if command & 0x20 != 0 {
                copy_size |= usize::from(delta_byte(pack_path, delta, &mut cursor)?) << 8;
            }
            if command & 0x40 != 0 {
                copy_size |= usize::from(delta_byte(pack_path, delta, &mut cursor)?) << 16;
            }
            if copy_size == 0 {
                copy_size = 0x10000;
            }
            let end = copy_offset + copy_size;
            if end > base.len() {
                return Err(CloneError::PackIndexFailed {
                    path: pack_path.to_owned(),
                    operation: "applying delta",
                    detail: "delta copy command reads past base object".to_owned(),
                });
            }
            output.extend_from_slice(&base[copy_offset..end]);
        } else if command != 0 {
            let literal_len = usize::from(command);
            if delta.len() - cursor < literal_len {
                return Err(CloneError::PackIndexFailed {
                    path: pack_path.to_owned(),
                    operation: "applying delta",
                    detail: "delta literal command is truncated".to_owned(),
                });
            }
            output.extend_from_slice(&delta[cursor..cursor + literal_len]);
            cursor += literal_len;
        } else {
            return Err(CloneError::PackIndexFailed {
                path: pack_path.to_owned(),
                operation: "applying delta",
                detail: "delta command 0 is reserved".to_owned(),
            });
        }
    }

    if output.len() != target_size {
        return Err(CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation: "applying delta",
            detail: format!(
                "delta produced {} bytes but declared {target_size}",
                output.len()
            ),
        });
    }

    Ok(output)
}

fn read_delta_size(
    pack_path: &Path,
    delta: &[u8],
    cursor: &mut usize,
) -> Result<usize, CloneError> {
    let mut size = 0usize;
    let mut shift = 0;
    loop {
        let byte = delta_byte(pack_path, delta, cursor)?;
        size |= usize::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(size);
        }
        shift += 7;
    }
}

fn delta_byte(pack_path: &Path, delta: &[u8], cursor: &mut usize) -> Result<u8, CloneError> {
    let byte = *delta
        .get(*cursor)
        .ok_or_else(|| CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation: "reading delta byte",
            detail: "delta data is truncated".to_owned(),
        })?;
    *cursor += 1;
    Ok(byte)
}

fn write_idx_v2(
    index_path: &Path,
    entries: &[IndexEntry],
    pack_checksum: &[u8; 20],
) -> Result<(), CloneError> {
    validate_unique_objects(index_path, entries)?;

    let mut idx = Vec::new();
    idx.extend_from_slice(&[0xff, b't', b'O', b'c']);
    idx.extend_from_slice(&2u32.to_be_bytes());

    let mut fanout = [0u32; 256];
    for entry in entries {
        fanout[usize::from(entry.oid[0])] += 1;
    }
    let mut running = 0u32;
    for count in fanout {
        running += count;
        idx.extend_from_slice(&running.to_be_bytes());
    }
    for entry in entries {
        idx.extend_from_slice(&entry.oid);
    }
    for entry in entries {
        idx.extend_from_slice(&entry.crc32.to_be_bytes());
    }

    let mut large_offsets = Vec::new();
    for entry in entries {
        if entry.offset <= 0x7fff_ffff {
            let offset =
                u32::try_from(entry.offset).map_err(|error| CloneError::PackIndexFailed {
                    path: index_path.to_owned(),
                    operation: "writing pack index",
                    detail: format!("small offset {} did not fit idx v2: {error}", entry.offset),
                })?;
            idx.extend_from_slice(&offset.to_be_bytes());
        } else {
            let large_index = u32::try_from(large_offsets.len()).map_err(|error| {
                CloneError::PackIndexFailed {
                    path: index_path.to_owned(),
                    operation: "writing pack index",
                    detail: format!("too many large offsets for idx v2: {error}"),
                }
            })?;
            idx.extend_from_slice(&(0x8000_0000 | large_index).to_be_bytes());
            large_offsets.push(entry.offset);
        }
    }
    for offset in large_offsets {
        idx.extend_from_slice(&offset.to_be_bytes());
    }

    idx.extend_from_slice(pack_checksum);
    let idx_checksum = Sha1::digest(&idx);
    idx.extend_from_slice(&idx_checksum);

    std::fs::write(index_path, idx).map_err(|error| CloneError::PackIndexFailed {
        path: index_path.to_owned(),
        operation: "writing pack index",
        detail: error.to_string(),
    })
}

fn usize_from_u64(path: &Path, operation: &'static str, value: u64) -> Result<usize, CloneError> {
    usize::try_from(value).map_err(|error| CloneError::PackIndexFailed {
        path: path.to_owned(),
        operation,
        detail: format!("value {value} does not fit in memory on this platform: {error}"),
    })
}

fn validate_unique_objects(index_path: &Path, entries: &[IndexEntry]) -> Result<(), CloneError> {
    for window in entries.windows(2) {
        if window[0].oid == window[1].oid {
            return Err(CloneError::PackIndexFailed {
                path: index_path.to_owned(),
                operation: "writing pack index",
                detail: format!("duplicate object id {}", hex::encode(window[0].oid)),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        CheckoutHint, EncodedObjectKind, ObjectDataState, ObjectType, PackIndex, PackIngestOptions,
        PackStorage, ScanPayload, ingest_pack, ingest_scanned_pack, scan_pack, validate_pack,
    };
    use crate::checkout::materialize_default_branch;
    use crate::pack::{ObjectId, ObjectReader};
    use crate::repo::RepoLayout;
    use std::collections::HashSet;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn ingest_scanned_pack_should_match_complete_ingest() {
        let temp = test_temp_dir("ingest-scanned-pack");
        let repo = temp.join("repo");
        fs::create_dir(&repo).expect("repo directory should be created");
        run_git(&repo, &["init"]);
        run_git(&repo, &["config", "user.name", "fcl test"]);
        run_git(&repo, &["config", "user.email", "fcl@example.invalid"]);
        fs::write(repo.join("file.txt"), "alpha\n").expect("file should be written");
        run_git(&repo, &["add", "."]);
        run_git(&repo, &["commit", "-m", "initial"]);
        fs::write(repo.join("file.txt"), "alpha\nbeta\ngamma\n").expect("file should be updated");
        fs::write(repo.join("other.txt"), "alpha\nbeta\n").expect("second file should be written");
        run_git(&repo, &["add", "."]);
        run_git(&repo, &["commit", "-m", "update"]);
        run_git(&repo, &["repack", "-ad"]);

        let pack_path = only_pack_path(&repo);
        let complete_idx = temp.join("complete.idx");
        let scanned_idx = temp.join("scanned.idx");
        let complete = ingest_pack(&pack_path, &complete_idx).expect("complete ingest should work");

        let pack = fs::read(&pack_path).expect("pack should be readable");
        let pack = Arc::<[u8]>::from(pack);
        let scan_payload = if std::env::var("FCL_LOW_MEMORY").is_ok() {
            ScanPayload::MetadataOnly
        } else {
            ScanPayload::Inflate
        };
        let scan = scan_pack(&pack_path, pack.as_ref(), scan_payload).expect("scan should work");
        let scanned = ingest_scanned_pack(
            &pack_path,
            &scanned_idx,
            PackStorage::from_memory(pack),
            &scan,
            0,
            PackIngestOptions::default(),
        )
        .expect("scanned ingest should work");

        assert_eq!(
            fs::read(complete_idx).expect("complete idx should exist"),
            fs::read(scanned_idx).expect("scanned idx should exist")
        );
        assert_same_index_metadata(&complete.index, &scanned.index);

        fs::remove_dir_all(temp).expect("test temp directory should be removed");
    }

    #[test]
    fn streaming_pack_scanner_should_match_metadata_scan_for_chunk_boundaries() {
        let temp = test_temp_dir("streaming-pack-scanner");
        let repo = build_packed_test_repo(&temp);
        let pack_path = only_pack_path(&repo);
        let pack = fs::read(&pack_path).expect("pack should be readable");
        let expected = scan_pack(&pack_path, &pack, ScanPayload::MetadataOnly)
            .expect("metadata scan should work");

        for chunk_size in [1, 2, 3, 7, 8192, pack.len()] {
            let actual = scan_streaming(&pack_path, &pack, chunk_size);
            assert_same_scan_metadata(&expected, &actual, chunk_size);
        }

        fs::remove_dir_all(temp).expect("test temp directory should be removed");
    }

    #[test]
    fn streaming_pack_scanner_should_cover_offset_delta_frames() {
        let temp = test_temp_dir("streaming-pack-offset-delta");
        let repo = build_packed_test_repo(&temp);
        let pack_path = only_pack_path(&repo);
        let pack = fs::read(&pack_path).expect("pack should be readable");
        let actual = scan_streaming(&pack_path, &pack, 1);

        assert!(
            actual
                .frames
                .iter()
                .any(|frame| matches!(frame.encoded, EncodedObjectKind::OffsetDelta { .. })),
            "test pack should include at least one offset delta"
        );

        fs::remove_dir_all(temp).expect("test temp directory should be removed");
    }

    #[test]
    fn file_backed_storage_should_inflate_scanned_frame() {
        let temp = test_temp_dir("file-backed-inflate");
        let repo = build_packed_test_repo(&temp);
        let pack_path = only_pack_path(&repo);
        let pack = fs::read(&pack_path).expect("pack should be readable");
        let scan = scan_pack(&pack_path, &pack, ScanPayload::MetadataOnly)
            .expect("metadata scan should work");
        let storage = PackStorage::open_file_backed(&pack_path)
            .expect("file-backed pack storage should open");
        let frame = scan.frames.first().expect("pack should contain a frame");

        let inflated = storage
            .inflate_frame(&pack_path, frame)
            .expect("file-backed frame should inflate");

        assert_eq!(inflated.len() as u64, frame.declared_size);
        fs::remove_dir_all(temp).expect("test temp directory should be removed");
    }

    #[test]
    fn file_backed_ingest_should_match_memory_ingest() {
        let temp = test_temp_dir("file-backed-ingest");
        let repo = build_packed_test_repo(&temp);
        let pack_path = only_pack_path(&repo);
        let pack = fs::read(&pack_path).expect("pack should be readable");
        let pack = Arc::<[u8]>::from(pack);
        let scan = scan_pack(&pack_path, pack.as_ref(), ScanPayload::MetadataOnly)
            .expect("metadata scan should work");
        let memory_idx = temp.join("memory.idx");
        let file_idx = temp.join("file.idx");
        let memory = ingest_scanned_pack(
            &pack_path,
            &memory_idx,
            PackStorage::from_memory(Arc::clone(&pack)),
            &scan,
            0,
            PackIngestOptions::default(),
        )
        .expect("memory ingest should work");
        let file_backed = ingest_scanned_pack(
            &pack_path,
            &file_idx,
            PackStorage::open_file_backed(&pack_path).expect("file-backed storage should open"),
            &scan,
            0,
            PackIngestOptions::default(),
        )
        .expect("file-backed ingest should work");

        assert_eq!(
            fs::read(memory_idx).expect("memory idx should exist"),
            fs::read(file_idx).expect("file-backed idx should exist")
        );
        assert_same_index_metadata(&memory.index, &file_backed.index);
        fs::remove_dir_all(temp).expect("test temp directory should be removed");
    }

    #[test]
    fn file_backed_index_should_reconstruct_blob_from_pack() {
        let temp = test_temp_dir("file-backed-reconstruct");
        let repo = build_packed_test_repo(&temp);
        let pack_path = only_pack_path(&repo);
        let pack = fs::read(&pack_path).expect("pack should be readable");
        let scan = scan_pack(&pack_path, &pack, ScanPayload::MetadataOnly)
            .expect("metadata scan should work");
        let blob_oid = ObjectId::parse_hex(&run_git_stdout(&repo, &["rev-parse", "HEAD:file.txt"]))
            .expect("blob oid should parse");
        let expected =
            fs::read(repo.join("file.txt")).expect("working tree blob should be readable");
        let mut report = ingest_scanned_pack(
            &pack_path,
            &temp.join("file-backed.idx"),
            PackStorage::open_file_backed(&pack_path).expect("file-backed storage should open"),
            &scan,
            0,
            PackIngestOptions::default(),
        )
        .expect("file-backed ingest should work");
        report
            .index
            .state_by_oid
            .insert(blob_oid, ObjectDataState::Reconstructable);
        let mut actual = Vec::new();

        report
            .index
            .stream_blob(blob_oid, &mut actual)
            .expect("blob should reconstruct from file-backed pack");

        assert_eq!(actual, expected);
        fs::remove_dir_all(temp).expect("test temp directory should be removed");
    }

    #[test]
    fn ingest_with_checkout_hint_should_retain_default_branch_blobs() {
        let temp = test_temp_dir("checkout-hint-retain");
        let repo = build_packed_test_repo(&temp);
        let pack_path = only_pack_path(&repo);
        let pack = fs::read(&pack_path).expect("pack should be readable");
        let scan = scan_pack(&pack_path, &pack, ScanPayload::MetadataOnly)
            .expect("metadata scan should work");
        let default_commit = ObjectId::parse_hex(&run_git_stdout(&repo, &["rev-parse", "HEAD"]))
            .expect("default commit oid should parse");
        let expected_blobs = checkout_blob_oids(&repo);
        let report = ingest_scanned_pack(
            &pack_path,
            &temp.join("checkout-hint.idx"),
            PackStorage::open_file_backed(&pack_path).expect("file-backed storage should open"),
            &scan,
            0,
            PackIngestOptions {
                checkout_hint: Some(CheckoutHint { default_commit }),
            },
        )
        .expect("checkout-hinted ingest should work");
        let checkout = temp.join("checkout");
        let layout = RepoLayout::create(&checkout).expect("checkout repo layout should be created");

        materialize_default_branch(&layout, &report.index, default_commit)
            .expect("checkout should materialize from retained blobs");

        assert_eq!(report.checkout_needed_blob_count, expected_blobs.len());
        assert_eq!(report.checkout_ready_blob_count, expected_blobs.len());
        assert_eq!(report.checkout_missing_blob_count, 0);
        assert_eq!(report.index.reconstructed_object_count(), 0);
        fs::remove_dir_all(temp).expect("test temp directory should be removed");
    }

    #[test]
    fn ingest_without_checkout_hint_should_preserve_reconstructable_blob_behavior() {
        let temp = test_temp_dir("checkout-hint-disabled");
        let repo = build_packed_test_repo(&temp);
        let pack_path = only_pack_path(&repo);
        let pack = fs::read(&pack_path).expect("pack should be readable");
        let scan = scan_pack(&pack_path, &pack, ScanPayload::MetadataOnly)
            .expect("metadata scan should work");
        let default_commit = ObjectId::parse_hex(&run_git_stdout(&repo, &["rev-parse", "HEAD"]))
            .expect("default commit oid should parse");
        let report = ingest_scanned_pack(
            &pack_path,
            &temp.join("without-checkout-hint.idx"),
            PackStorage::open_file_backed(&pack_path).expect("file-backed storage should open"),
            &scan,
            0,
            PackIngestOptions::default(),
        )
        .expect("default ingest should work");
        let checkout = temp.join("checkout");
        let layout = RepoLayout::create(&checkout).expect("checkout repo layout should be created");

        materialize_default_branch(&layout, &report.index, default_commit)
            .expect("checkout should materialize from reconstructable blobs");

        assert_eq!(report.checkout_needed_blob_count, 0);
        assert!(report.index.reconstructed_object_count() > 0);
        fs::remove_dir_all(temp).expect("test temp directory should be removed");
    }

    #[test]
    fn streaming_scanner_should_emit_completed_frames() {
        let temp = test_temp_dir("streaming-frame-events");
        let repo = build_packed_test_repo(&temp);
        let pack_path = only_pack_path(&repo);
        let pack = fs::read(&pack_path).expect("pack should be readable");
        let checksum = validate_pack(&pack_path, &pack).expect("pack checksum should validate");
        let expected = scan_pack(&pack_path, &pack, ScanPayload::MetadataOnly)
            .expect("metadata scan should work");
        let mut scanner = super::StreamingPackScanner::new(&pack_path);
        let mut emitted = Vec::new();

        for chunk in pack.chunks(31) {
            emitted.extend(
                scanner
                    .feed_collect(chunk)
                    .expect("streaming scanner should accept chunk"),
            );
        }
        let finished = scanner
            .finish(checksum)
            .expect("streaming scanner should finish");

        assert_eq!(emitted.len(), expected.frames.len());
        assert_same_scan_metadata(&finished, &expected, 31);
        fs::remove_dir_all(temp).expect("test temp directory should be removed");
    }

    #[test]
    fn windowed_scan_should_match_in_memory_scan() {
        let temp = test_temp_dir("windowed-scan");
        let repo = build_packed_test_repo(&temp);
        let pack_path = only_pack_path(&repo);
        let pack = fs::read(&pack_path).expect("pack should be readable");
        let checksum = validate_pack(&pack_path, &pack).expect("pack checksum should validate");
        let expected = scan_pack(&pack_path, &pack, ScanPayload::MetadataOnly)
            .expect("metadata scan should work");

        let actual =
            super::scan_pack_file_windowed(&pack_path, ScanPayload::MetadataOnly, checksum)
                .expect("windowed scan should work");

        assert_same_scan_metadata(&actual, &expected, 0);
        fs::remove_dir_all(temp).expect("test temp directory should be removed");
    }

    #[test]
    fn pipeline_resolver_should_match_sequential_index_metadata() {
        let temp = test_temp_dir("pipeline-resolver");
        let repo = build_packed_test_repo(&temp);
        let pack_path = only_pack_path(&repo);
        let pack = fs::read(&pack_path).expect("pack should be readable");
        let scan =
            scan_pack(&pack_path, &pack, ScanPayload::Inflate).expect("inflated scan should work");
        let sequential = ingest_scanned_pack(
            &pack_path,
            &temp.join("sequential.idx"),
            PackStorage::open_file_backed(&pack_path).expect("file-backed storage should open"),
            &scan,
            0,
            PackIngestOptions::default(),
        )
        .expect("sequential ingest should work");
        let (sender, receiver) = std::sync::mpsc::sync_channel(scan.frames.len() + 1);
        let store = super::PipelineObjectStore::new(&pack_path);

        sender
            .send(super::PipelineEvent::Frames(scan.frames.clone()))
            .expect("frames should send");
        sender
            .send(super::PipelineEvent::Finished {
                checksum: scan.checksum,
                pack_bytes: pack.len() as u64,
                scan_ms: 0,
            })
            .expect("finish should send");
        drop(sender);

        let pipeline =
            super::ingest_pack_pipeline(&pack_path, &temp.join("pipeline.idx"), &receiver, store)
                .expect("pipeline ingest should work");

        assert_same_pack_metadata(&pipeline.index, &sequential.index);
        fs::remove_dir_all(temp).expect("test temp directory should be removed");
    }

    #[test]
    fn pipeline_resolver_should_spill_after_cumulative_resident_blob_limit() {
        let temp = test_temp_dir("pipeline-resident-budget");
        let pack_path = temp.join("pack.pack");
        let index_path = temp.join("pack.idx");
        let spill_dir = temp.join("spill");
        let store = super::PipelineObjectStore::new(&pack_path);
        let mut resolver = super::PipelineResolver::new(&pack_path, &index_path, &spill_dir, store)
            .expect("resolver should be created");
        resolver.resident_blob_limit = 3;
        resolver.spill_blobs_over_budget = true;
        let first = Arc::<[u8]>::from([1u8, 2]);
        let second = Arc::<[u8]>::from([3u8, 4]);

        let first_state = resolver
            .pipeline_object_state(&first, ObjectType::Blob)
            .expect("first blob state should build");
        let second_state = resolver
            .pipeline_object_state(&second, ObjectType::Blob)
            .expect("second blob state should build");

        assert!(matches!(first_state, ObjectDataState::Resident(_)));
        assert!(matches!(second_state, ObjectDataState::Spilled { .. }));
        assert_eq!(resolver.resident_blob_bytes, 2);
        assert_eq!(resolver.arena_spill_bytes, 2);
        fs::remove_dir_all(temp).expect("test temp directory should be removed");
    }

    #[test]
    fn pipeline_resolver_should_keep_over_budget_blobs_resident_by_default() {
        let temp = test_temp_dir("pipeline-resident-over-budget");
        let pack_path = temp.join("pack.pack");
        let index_path = temp.join("pack.idx");
        let spill_dir = temp.join("spill");
        let store = super::PipelineObjectStore::new(&pack_path);
        let mut resolver = super::PipelineResolver::new(&pack_path, &index_path, &spill_dir, store)
            .expect("resolver should be created");
        resolver.resident_blob_limit = 3;
        let first = Arc::<[u8]>::from([1u8, 2]);
        let second = Arc::<[u8]>::from([3u8, 4]);

        let first_state = resolver
            .pipeline_object_state(&first, ObjectType::Blob)
            .expect("first blob state should build");
        let second_state = resolver
            .pipeline_object_state(&second, ObjectType::Blob)
            .expect("second blob state should build");

        assert!(matches!(first_state, ObjectDataState::Resident(_)));
        assert!(matches!(second_state, ObjectDataState::Resident(_)));
        assert_eq!(resolver.resident_blob_bytes, 4);
        assert_eq!(resolver.arena_spill_bytes, 0);
        fs::remove_dir_all(temp).expect("test temp directory should be removed");
    }

    fn test_temp_dir(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("fcl-{name}-{}-{stamp}", std::process::id()));
        fs::create_dir(&path).expect("test temp directory should be created");
        path
    }

    fn run_git(repo: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .expect("git should run");
        assert!(
            output.status.success(),
            "git {} failed: stdout=`{}` stderr=`{}`",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn run_git_stdout(repo: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .expect("git should run");
        assert!(
            output.status.success(),
            "git {} failed: stdout=`{}` stderr=`{}`",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    fn checkout_blob_oids(repo: &Path) -> HashSet<ObjectId> {
        run_git_stdout(repo, &["ls-tree", "-r", "HEAD"])
            .lines()
            .filter_map(|line| line.split_whitespace().nth(2))
            .map(|oid| ObjectId::parse_hex(oid).expect("blob oid should parse"))
            .collect()
    }

    fn build_packed_test_repo(temp: &Path) -> PathBuf {
        let repo = temp.join("repo");
        fs::create_dir(&repo).expect("repo directory should be created");
        run_git(&repo, &["init"]);
        run_git(&repo, &["config", "user.name", "fcl test"]);
        run_git(&repo, &["config", "user.email", "fcl@example.invalid"]);
        for index in 0..12 {
            fs::write(
                repo.join("file.txt"),
                format!("common prefix\nline {index}\ncommon suffix\n"),
            )
            .expect("file should be written");
            fs::write(
                repo.join(format!("similar-{index}.txt")),
                format!("common prefix\nline {index}\ncommon suffix\n"),
            )
            .expect("similar file should be written");
            run_git(&repo, &["add", "."]);
            run_git(&repo, &["commit", "-m", &format!("commit {index}")]);
        }
        run_git(&repo, &["repack", "-ad", "--depth=50", "--window=50"]);
        repo
    }

    fn scan_streaming(pack_path: &Path, pack: &[u8], chunk_size: usize) -> super::PackScan {
        let checksum = validate_pack(pack_path, pack).expect("pack checksum should validate");
        let mut scanner = super::StreamingPackScanner::new(pack_path);
        for chunk in pack.chunks(chunk_size) {
            scanner
                .feed(chunk)
                .expect("streaming scanner should accept chunk");
        }
        scanner
            .finish(checksum)
            .expect("streaming scanner should finish")
    }

    fn only_pack_path(repo: &Path) -> PathBuf {
        let pack_dir = repo.join(".git/objects/pack");
        let packs = fs::read_dir(&pack_dir)
            .expect("pack directory should be readable")
            .map(|entry| entry.expect("pack dir entry should be readable").path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "pack")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            packs.len(),
            1,
            "expected one pack in {}",
            pack_dir.display()
        );
        packs[0].clone()
    }

    fn assert_same_index_metadata(left: &PackIndex, right: &PackIndex) {
        assert_eq!(left.meta_by_oid.len(), right.meta_by_oid.len());
        assert_eq!(left.oid_by_offset, right.oid_by_offset);
        assert_eq!(left.retained_object_count, right.retained_object_count);
        assert_eq!(left.retained_object_bytes, right.retained_object_bytes);
        assert_eq!(left.spilled_object_count, right.spilled_object_count);
        assert_eq!(left.spilled_object_bytes, right.spilled_object_bytes);
        for (oid, left_meta) in &left.meta_by_oid {
            let right_meta = right
                .meta_by_oid
                .get(oid)
                .expect("right index should contain oid");
            assert_eq!(left_meta.object_type, right_meta.object_type);
            assert_eq!(left_meta.pack_inflated_size, right_meta.pack_inflated_size);
            assert_eq!(left_meta.pack_offset, right_meta.pack_offset);
            assert_eq!(left_meta.compressed_start, right_meta.compressed_start);
            assert_eq!(left_meta.compressed_len, right_meta.compressed_len);
            assert_eq!(left_meta.crc32, right_meta.crc32);
            assert_eq!(left_meta.delta_base, right_meta.delta_base);
        }
    }

    fn assert_same_pack_metadata(left: &PackIndex, right: &PackIndex) {
        assert_eq!(left.meta_by_oid.len(), right.meta_by_oid.len());
        assert_eq!(left.oid_by_offset, right.oid_by_offset);
        for (oid, left_meta) in &left.meta_by_oid {
            let right_meta = right
                .meta_by_oid
                .get(oid)
                .expect("right index should contain oid");
            assert_eq!(left_meta.object_type, right_meta.object_type);
            assert_eq!(left_meta.pack_inflated_size, right_meta.pack_inflated_size);
            assert_eq!(left_meta.pack_offset, right_meta.pack_offset);
            assert_eq!(left_meta.compressed_start, right_meta.compressed_start);
            assert_eq!(left_meta.compressed_len, right_meta.compressed_len);
            assert_eq!(left_meta.crc32, right_meta.crc32);
            assert_eq!(left_meta.delta_base, right_meta.delta_base);
        }
    }

    fn assert_same_scan_metadata(
        left: &super::PackScan,
        right: &super::PackScan,
        chunk_size: usize,
    ) {
        assert_eq!(left.checksum, right.checksum, "chunk_size={chunk_size}");
        assert_eq!(
            left.frames.len(),
            right.frames.len(),
            "chunk_size={chunk_size}"
        );
        for (left, right) in left.frames.iter().zip(&right.frames) {
            assert_eq!(left.offset, right.offset, "chunk_size={chunk_size}");
            assert_eq!(
                left.compressed_start, right.compressed_start,
                "chunk_size={chunk_size}"
            );
            assert_eq!(
                left.compressed_len, right.compressed_len,
                "chunk_size={chunk_size}"
            );
            assert_eq!(left.crc32, right.crc32, "chunk_size={chunk_size}");
            assert_eq!(left.encoded, right.encoded, "chunk_size={chunk_size}");
            assert_eq!(
                left.declared_size, right.declared_size,
                "chunk_size={chunk_size}"
            );
        }
    }
}
