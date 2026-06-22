use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crc32fast::Hasher as Crc32;
use flate2::{Decompress, FlushDecompress, Status};
use rayon::prelude::*;
use sha1::{Digest, Sha1};

use crate::error::CloneError;

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
    pub size: u64,
    pub pack_inflated_size: u64,
    pub pack_offset: u64,
    pub compressed_start: usize,
    pub compressed_len: usize,
    pub crc32: u32,
    pub delta_base: Option<DeltaBase>,
}

pub trait ObjectReader: Sync {
    fn get_meta(&self, oid: ObjectId) -> Option<&ObjectMeta>;
    fn read_object(&self, oid: ObjectId) -> Result<ObjectBytes, CloneError>;
    fn stream_blob(&self, oid: ObjectId, out: &mut dyn Write) -> Result<u64, CloneError>;
}

#[derive(Debug, Clone)]
pub struct PackIndex {
    pack_path: PathBuf,
    pack: Arc<[u8]>,
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
enum ObjectDataState {
    Resident(Arc<[u8]>),
    Spilled { path: PathBuf, len: u64 },
    Reconstructable,
}

#[derive(Debug)]
struct ObjectStateBuild {
    state_by_oid: HashMap<ObjectId, ObjectDataState>,
    retained_object_count: usize,
    retained_object_bytes: usize,
    spilled_object_count: usize,
    spilled_object_bytes: usize,
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

impl ObjectReader for PackIndex {
    fn get_meta(&self, oid: ObjectId) -> Option<&ObjectMeta> {
        self.meta_by_oid.get(&oid)
    }

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
            Some(ObjectDataState::Spilled { path, len }) => read_spilled_object(path, *len)?,
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
            Some(ObjectDataState::Spilled { path, len }) => {
                let mut file = File::open(path).map_err(|error| CloneError::PackIndexFailed {
                    path: path.clone(),
                    operation: "opening spilled blob",
                    detail: error.to_string(),
                })?;
                std::io::copy(&mut file, out).map_err(|error| CloneError::PackIndexFailed {
                    path: path.clone(),
                    operation: "streaming spilled blob",
                    detail: error.to_string(),
                })?;
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
        let payload = inflate_frame(&self.pack_path, self.pack.as_ref(), &frame)?;
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
            Some(ObjectDataState::Spilled { path, len }) => read_spilled_object(path, *len),
            Some(ObjectDataState::Reconstructable) | None => self.reconstruct_object(oid, depth),
        }
    }
}

fn read_spilled_object(path: &Path, len: u64) -> Result<Arc<[u8]>, CloneError> {
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
    file.read_to_end(&mut data)
        .map_err(|error| CloneError::PackIndexFailed {
            path: path.to_owned(),
            operation: "reading spilled object",
            detail: error.to_string(),
        })?;
    if data.len() as u64 != len {
        return Err(CloneError::PackIndexFailed {
            path: path.to_owned(),
            operation: "reading spilled object",
            detail: format!("read {} bytes, expected {len}", data.len()),
        });
    }
    Ok(data.into())
}

#[derive(Debug, Clone, Copy)]
enum EncodedObjectKind {
    Base(ObjectType),
    OffsetDelta { base_offset: u64 },
    RefDelta { base_oid: [u8; 20] },
}

#[derive(Debug, Clone)]
struct ObjectFrame {
    offset: u64,
    compressed_start: usize,
    compressed_len: usize,
    crc32: u32,
    encoded: EncodedObjectKind,
    inflated: Option<Arc<[u8]>>,
    declared_size: u64,
}

#[derive(Debug)]
struct PackScan {
    checksum: [u8; 20],
    frames: Vec<ObjectFrame>,
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

pub fn ingest_pack(pack_path: &Path, index_path: &Path) -> Result<PackIndex, CloneError> {
    let pack = fs::read(pack_path).map_err(|error| CloneError::PackIndexFailed {
        path: pack_path.to_owned(),
        operation: "reading pack file",
        detail: error.to_string(),
    })?;
    let pack = Arc::<[u8]>::from(pack);

    let scan_payload = if env_bool("FCL_LOW_MEMORY") {
        ScanPayload::MetadataOnly
    } else {
        ScanPayload::Inflate
    };
    let scan = scan_pack(pack_path, pack.as_ref(), scan_payload)?;
    let resolved = resolve_inflated_frames(pack_path, pack.as_ref(), &scan.frames)?;
    let mut entries = resolved
        .iter()
        .map(|frame| IndexEntry {
            oid: frame.object.oid,
            crc32: frame.crc32,
            offset: frame.offset,
        })
        .collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.oid.cmp(&right.oid));
    write_idx_v2(index_path, &entries, &scan.checksum)?;

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
                size: frame.object.size,
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
    let cache = build_object_states(pack_path, &spill_dir, resolved)?;
    Ok(PackIndex {
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
    })
}

fn build_object_states(
    pack_path: &Path,
    spill_dir: &Path,
    resolved: Vec<ResolvedFrame>,
) -> Result<ObjectStateBuild, CloneError> {
    let resident_limit = optional_usize_env("FCL_OBJECT_CACHE_BYTES")?.unwrap_or(512 * 1024 * 1024);
    let max_spill_bytes = optional_usize_env("FCL_MAX_SPILL_BYTES")?;
    let spill_blobs = env_bool("FCL_SPILL_BLOBS");
    let configured_spill_dir = std::env::var_os("FCL_SPILL_DIR").map(PathBuf::from);
    let spill_dir = configured_spill_dir.as_deref().unwrap_or(spill_dir);

    let mut state_by_oid = HashMap::with_capacity(resolved.len());
    let mut retained_object_count = 0usize;
    let mut retained_object_bytes = 0usize;
    let mut spilled_object_count = 0usize;
    let mut spilled_object_bytes = 0usize;

    for frame in resolved {
        let oid = ObjectId::from_bytes(frame.object.oid);
        let data_len = frame.object.data.len();
        let should_keep_resident = frame.object.object_type != ObjectType::Blob
            && retained_object_bytes.saturating_add(data_len) <= resident_limit;

        let state = if should_keep_resident {
            retained_object_count += 1;
            retained_object_bytes += data_len;
            ObjectDataState::Resident(frame.object.data)
        } else if frame.object.object_type == ObjectType::Blob && spill_blobs {
            let path = spill_object(pack_path, spill_dir, oid, &frame.object.data)?;
            spilled_object_count += 1;
            spilled_object_bytes = spilled_object_bytes.saturating_add(data_len);
            if let Some(max_spill_bytes) = max_spill_bytes
                && spilled_object_bytes > max_spill_bytes
            {
                return Err(CloneError::CloneLimitExceeded {
                    operation: "spilling object data",
                    detail: format!(
                        "FCL_MAX_SPILL_BYTES is {max_spill_bytes}, but spilled object data reached {spilled_object_bytes} bytes"
                    ),
                });
            }
            ObjectDataState::Spilled {
                path,
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
    })
}

fn spill_object(
    pack_path: &Path,
    spill_dir: &Path,
    oid: ObjectId,
    data: &[u8],
) -> Result<PathBuf, CloneError> {
    fs::create_dir_all(spill_dir).map_err(|error| CloneError::PackIndexFailed {
        path: spill_dir.to_owned(),
        operation: "creating object spill directory",
        detail: error.to_string(),
    })?;
    let path = spill_dir.join(oid.to_hex());
    fs::write(&path, data).map_err(|error| CloneError::PackIndexFailed {
        path: pack_path.to_owned(),
        operation: "spilling object data",
        detail: format!("{}: {error}", path.display()),
    })?;
    Ok(path)
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

#[derive(Debug, Clone, Copy)]
enum ScanPayload {
    Inflate,
    MetadataOnly,
}

fn scan_pack(pack_path: &Path, pack: &[u8], payload: ScanPayload) -> Result<PackScan, CloneError> {
    let checksum = validate_pack(pack_path, pack)?;
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

fn validate_pack(pack_path: &Path, pack: &[u8]) -> Result<[u8; 20], CloneError> {
    if pack.len() < 32 || &pack[0..4] != b"PACK" {
        return Err(CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation: "validating pack header",
            detail: "file does not start with PACK".to_owned(),
        });
    }
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
    pack: &[u8],
    frames: &[ObjectFrame],
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
            release_delta_base_if_done(&mut resolved, &mut unresolved_children, base_frame_index);
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
    pack: &[u8],
    frame: &ObjectFrame,
) -> Result<Arc<[u8]>, CloneError> {
    frame.inflated.as_ref().map_or_else(
        || inflate_frame(pack_path, pack, frame),
        |inflated| Ok(Arc::clone(inflated)),
    )
}

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

fn inflate_frame(
    pack_path: &Path,
    pack: &[u8],
    frame: &ObjectFrame,
) -> Result<Arc<[u8]>, CloneError> {
    let mut decompressor = Decompress::new(true);
    let mut output = Vec::with_capacity(usize_from_u64(
        pack_path,
        "allocating inflated object buffer",
        frame.declared_size,
    )?);
    let mut chunk = [0u8; 8192];
    let compressed = &pack[frame.compressed_start..frame.compressed_start + frame.compressed_len];

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
    let mut seen = HashSet::with_capacity(entries.len());
    for entry in entries {
        if !seen.insert(entry.oid) {
            return Err(CloneError::PackIndexFailed {
                path: index_path.to_owned(),
                operation: "writing pack index",
                detail: format!("duplicate object id {}", hex::encode(entry.oid)),
            });
        }
    }
    Ok(())
}
