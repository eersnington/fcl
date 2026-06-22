use std::fs::{self, File};
use std::path::{Component, Path, PathBuf};
use std::time::Instant;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

use rayon::{ThreadPoolBuilder, prelude::*};
use sha1::{Digest, Sha1};

use crate::error::CloneError;
use crate::pack::{ObjectId, ObjectReader, ObjectType};
use crate::protocol::{RemoteRef, RemoteRefs};
use crate::repo::RepoLayout;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TreeMode {
    File,
    Executable,
    Symlink,
    Directory,
    Gitlink,
}

impl TreeMode {
    const fn index_mode(self) -> u32 {
        match self {
            Self::File => 0o100_644,
            Self::Executable => 0o100_755,
            Self::Symlink => 0o120_000,
            Self::Directory => 0o040_000,
            Self::Gitlink => 0o160_000,
        }
    }
}

#[derive(Debug, Clone)]
struct TreeEntry {
    mode: TreeMode,
    name: String,
    oid: ObjectId,
}

#[derive(Debug, Clone)]
struct CheckoutEntry {
    path: PathBuf,
    mode: TreeMode,
    oid: ObjectId,
    size: u32,
    stat: FileStat,
}

#[derive(Debug, Clone)]
struct CheckoutManifestEntry {
    path: PathBuf,
    mode: TreeMode,
    oid: ObjectId,
    size: u64,
}

#[derive(Debug, Clone, Copy)]
struct FileStat {
    ctime_secs: u32,
    ctime_nanos: u32,
    mtime_secs: u32,
    mtime_nanos: u32,
    dev: u32,
    ino: u32,
    uid: u32,
    gid: u32,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CheckoutReport {
    pub manifest_ms: u128,
    pub dir_create_ms: u128,
    pub file_materialize_ms: u128,
    pub index_write_ms: u128,
    pub file_count: usize,
    pub dir_count: usize,
    pub blob_bytes: u64,
}

pub fn materialize_default_branch(
    repo: &RepoLayout,
    object_reader: &dyn ObjectReader,
    remote_refs: &RemoteRefs,
    selected_refs: &[RemoteRef],
) -> Result<CheckoutReport, CloneError> {
    let manifest_start = Instant::now();
    let head_ref =
        remote_refs
            .default_branch
            .as_deref()
            .ok_or_else(|| CloneError::CheckoutFailed {
                path: repo.root().to_owned(),
                operation: "resolving default branch",
                detail: "remote did not advertise a HEAD symref".to_owned(),
            })?;
    let head = selected_refs
        .iter()
        .find(|remote_ref| remote_ref.name == head_ref)
        .ok_or_else(|| CloneError::CheckoutFailed {
            path: repo.root().to_owned(),
            operation: "resolving default branch",
            detail: format!("HEAD points to `{head_ref}`, but that ref was not fetched"),
        })?;
    let commit_oid = ObjectId::parse_hex(&head.oid)?;
    let root_tree_oid = parse_commit_tree_oid(object_reader, commit_oid)?;
    let mut directories = Vec::new();
    let mut manifest = Vec::new();
    collect_tree_manifest(
        object_reader,
        root_tree_oid,
        Path::new(""),
        &mut directories,
        &mut manifest,
    )?;
    directories.sort_by_cached_key(|path| git_index_path_bytes(path));
    directories.dedup();
    let manifest_ms = manifest_start.elapsed().as_millis();
    let dir_count = directories.len();
    let file_count = manifest.len();
    let blob_bytes = manifest.iter().map(|entry| entry.size).sum();

    let dir_start = Instant::now();
    for directory in directories {
        let path = repo.root().join(&directory);
        fs::create_dir_all(&path).map_err(|error| CloneError::CheckoutFailed {
            path,
            operation: "creating directory",
            detail: error.to_string(),
        })?;
    }
    let dir_create_ms = dir_start.elapsed().as_millis();

    let file_start = Instant::now();
    let mut checkout_entries = materialize_manifest(repo.root(), &manifest, object_reader)?;
    let file_materialize_ms = file_start.elapsed().as_millis();

    let index_start = Instant::now();
    write_git_index(&repo.git_index_path(), &mut checkout_entries)?;
    Ok(CheckoutReport {
        manifest_ms,
        dir_create_ms,
        file_materialize_ms,
        index_write_ms: index_start.elapsed().as_millis(),
        file_count,
        dir_count,
        blob_bytes,
    })
}

pub fn index_existing_default_branch(
    repo: &RepoLayout,
    object_reader: &dyn ObjectReader,
    remote_refs: &RemoteRefs,
    selected_refs: &[RemoteRef],
) -> Result<CheckoutReport, CloneError> {
    let manifest_start = Instant::now();
    let head_ref =
        remote_refs
            .default_branch
            .as_deref()
            .ok_or_else(|| CloneError::CheckoutFailed {
                path: repo.root().to_owned(),
                operation: "resolving default branch",
                detail: "remote did not advertise a HEAD symref".to_owned(),
            })?;
    let head = selected_refs
        .iter()
        .find(|remote_ref| remote_ref.name == head_ref)
        .ok_or_else(|| CloneError::CheckoutFailed {
            path: repo.root().to_owned(),
            operation: "resolving default branch",
            detail: format!("HEAD points to `{head_ref}`, but that ref was not fetched"),
        })?;
    let commit_oid = ObjectId::parse_hex(&head.oid)?;
    let root_tree_oid = parse_commit_tree_oid(object_reader, commit_oid)?;
    let mut directories = Vec::new();
    let mut manifest = Vec::new();
    collect_tree_manifest(
        object_reader,
        root_tree_oid,
        Path::new(""),
        &mut directories,
        &mut manifest,
    )?;
    let manifest_ms = manifest_start.elapsed().as_millis();
    let dir_count = directories.len();
    let file_count = manifest.len();
    let blob_bytes = manifest.iter().map(|entry| entry.size).sum();

    let file_start = Instant::now();
    let mut checkout_entries = index_existing_manifest(repo.root(), &manifest)?;
    let file_materialize_ms = file_start.elapsed().as_millis();

    let index_start = Instant::now();
    write_git_index(&repo.git_index_path(), &mut checkout_entries)?;
    Ok(CheckoutReport {
        manifest_ms,
        dir_create_ms: 0,
        file_materialize_ms,
        index_write_ms: index_start.elapsed().as_millis(),
        file_count,
        dir_count,
        blob_bytes,
    })
}

fn parse_commit_tree_oid(
    object_reader: &dyn ObjectReader,
    commit_oid: ObjectId,
) -> Result<ObjectId, CloneError> {
    let object = object_reader.read_object(commit_oid)?;
    if object.object_type != ObjectType::Commit {
        return Err(CloneError::ObjectLookupFailed {
            oid: commit_oid.to_hex(),
            expected_type: "commit",
            detail: format!("found {}", object.object_type.as_git_name()),
        });
    }
    let text =
        std::str::from_utf8(&object.data).map_err(|error| CloneError::ObjectParseFailed {
            oid: commit_oid.to_hex(),
            object_type: "commit",
            operation: "reading commit as UTF-8",
            detail: error.to_string(),
        })?;
    let tree = text
        .lines()
        .find_map(|line| line.strip_prefix("tree "))
        .ok_or_else(|| CloneError::ObjectParseFailed {
            oid: commit_oid.to_hex(),
            object_type: "commit",
            operation: "finding tree header",
            detail: "commit did not contain a tree header".to_owned(),
        })?;
    ObjectId::parse_hex(tree)
}

fn collect_tree_manifest(
    object_reader: &dyn ObjectReader,
    tree_oid: ObjectId,
    relative_dir: &Path,
    directories: &mut Vec<PathBuf>,
    manifest: &mut Vec<CheckoutManifestEntry>,
) -> Result<(), CloneError> {
    let object = object_reader.read_object(tree_oid)?;
    if object.object_type != ObjectType::Tree {
        return Err(CloneError::ObjectLookupFailed {
            oid: tree_oid.to_hex(),
            expected_type: "tree",
            detail: format!("found {}", object.object_type.as_git_name()),
        });
    }

    for entry in parse_tree(tree_oid, &object.data)? {
        let relative_path = relative_dir.join(&entry.name);
        validate_relative_path(&relative_path)?;
        match entry.mode {
            TreeMode::Directory => {
                directories.push(relative_path.clone());
                collect_tree_manifest(
                    object_reader,
                    entry.oid,
                    &relative_path,
                    directories,
                    manifest,
                )?;
            }
            TreeMode::Gitlink => {}
            TreeMode::File | TreeMode::Executable | TreeMode::Symlink => {
                let object = object_reader.get_meta(entry.oid).ok_or_else(|| {
                    CloneError::ObjectLookupFailed {
                        oid: entry.oid.to_hex(),
                        expected_type: "blob",
                        detail: "object was not present in the fetched pack".to_owned(),
                    }
                })?;
                if object.object_type != ObjectType::Blob {
                    return Err(CloneError::ObjectLookupFailed {
                        oid: entry.oid.to_hex(),
                        expected_type: "blob",
                        detail: format!("found {}", object.object_type.as_git_name()),
                    });
                }
                manifest.push(CheckoutManifestEntry {
                    path: relative_path,
                    mode: entry.mode,
                    oid: entry.oid,
                    size: object.size,
                });
            }
        }
    }

    Ok(())
}

fn materialize_manifest(
    root: &Path,
    manifest: &[CheckoutManifestEntry],
    object_reader: &dyn ObjectReader,
) -> Result<Vec<CheckoutEntry>, CloneError> {
    let jobs = checkout_jobs()?;
    if let Some(jobs) = jobs {
        let pool = ThreadPoolBuilder::new()
            .num_threads(jobs)
            .build()
            .map_err(|error| CloneError::CheckoutFailed {
                path: root.to_owned(),
                operation: "creating checkout worker pool",
                detail: error.to_string(),
            })?;
        pool.install(|| {
            manifest
                .par_iter()
                .map(|entry| materialize_manifest_entry(root, entry, object_reader))
                .collect()
        })
    } else {
        manifest
            .par_iter()
            .map(|entry| materialize_manifest_entry(root, entry, object_reader))
            .collect()
    }
}

fn index_existing_manifest(
    root: &Path,
    manifest: &[CheckoutManifestEntry],
) -> Result<Vec<CheckoutEntry>, CloneError> {
    manifest
        .iter()
        .map(|entry| {
            let path = root.join(&entry.path);
            let metadata =
                fs::symlink_metadata(&path).map_err(|error| CloneError::CheckoutFailed {
                    path: path.clone(),
                    operation: "reading archive checkout metadata",
                    detail: error.to_string(),
                })?;
            Ok(CheckoutEntry {
                path: entry.path.clone(),
                mode: entry.mode,
                oid: entry.oid,
                size: index_size(&entry.path, entry.size)?,
                stat: FileStat::try_from_metadata(&path, &metadata)?,
            })
        })
        .collect()
}

fn checkout_jobs() -> Result<Option<usize>, CloneError> {
    let Some(raw) = std::env::var_os("FCL_CHECKOUT_JOBS") else {
        return Ok(None);
    };
    let raw = raw.to_string_lossy();
    let jobs = raw
        .parse::<usize>()
        .map_err(|error| CloneError::CheckoutFailed {
            path: PathBuf::from("."),
            operation: "parsing FCL_CHECKOUT_JOBS",
            detail: error.to_string(),
        })?;
    if jobs == 0 {
        return Err(CloneError::CheckoutFailed {
            path: PathBuf::from("."),
            operation: "parsing FCL_CHECKOUT_JOBS",
            detail: "value must be greater than 0".to_owned(),
        });
    }
    Ok(Some(jobs))
}

fn materialize_manifest_entry(
    root: &Path,
    entry: &CheckoutManifestEntry,
    object_reader: &dyn ObjectReader,
) -> Result<CheckoutEntry, CloneError> {
    let path = root.join(&entry.path);

    match entry.mode {
        TreeMode::File | TreeMode::Executable => {
            let mut file = File::create(&path).map_err(|error| CloneError::CheckoutFailed {
                path: path.clone(),
                operation: "creating file",
                detail: error.to_string(),
            })?;
            let written = object_reader.stream_blob(entry.oid, &mut file)?;
            if written != entry.size {
                return Err(CloneError::CheckoutFailed {
                    path,
                    operation: "streaming file",
                    detail: format!("wrote {written} bytes, expected {}", entry.size),
                });
            }
            #[cfg(unix)]
            if entry.mode == TreeMode::Executable {
                let mut permissions = fs::metadata(&path)
                    .map_err(|error| CloneError::CheckoutFailed {
                        path: path.clone(),
                        operation: "reading executable file metadata",
                        detail: error.to_string(),
                    })?
                    .permissions();
                permissions.set_mode(0o755);
                fs::set_permissions(&path, permissions).map_err(|error| {
                    CloneError::CheckoutFailed {
                        path: path.clone(),
                        operation: "setting executable permissions",
                        detail: error.to_string(),
                    }
                })?;
            }
        }
        TreeMode::Symlink => {
            let target = object_reader.read_object(entry.oid)?;
            if target.object_type != ObjectType::Blob {
                return Err(CloneError::ObjectLookupFailed {
                    oid: entry.oid.to_hex(),
                    expected_type: "blob",
                    detail: format!("found {}", target.object_type.as_git_name()),
                });
            }
            create_symlink(&path, target.data.as_ref())?;
        }
        TreeMode::Directory | TreeMode::Gitlink => {
            return Err(CloneError::CheckoutFailed {
                path: entry.path.clone(),
                operation: "materializing checkout entry",
                detail: format!(
                    "non-blob mode {:?} reached file materialization",
                    entry.mode
                ),
            });
        }
    }

    let metadata = fs::symlink_metadata(&path).map_err(|error| CloneError::CheckoutFailed {
        path: path.clone(),
        operation: "reading checkout metadata",
        detail: error.to_string(),
    })?;
    Ok(CheckoutEntry {
        path: entry.path.clone(),
        mode: entry.mode,
        oid: entry.oid,
        size: index_size(&entry.path, entry.size)?,
        stat: FileStat::try_from_metadata(&path, &metadata)?,
    })
}

#[cfg(unix)]
fn create_symlink(path: &Path, target: &[u8]) -> Result<(), CloneError> {
    use std::os::unix::fs::symlink;

    let target = std::str::from_utf8(target).map_err(|error| CloneError::CheckoutFailed {
        path: path.to_owned(),
        operation: "parsing symlink target",
        detail: error.to_string(),
    })?;
    symlink(target, path).map_err(|error| CloneError::CheckoutFailed {
        path: path.to_owned(),
        operation: "creating symlink",
        detail: error.to_string(),
    })
}

#[cfg(not(unix))]
fn create_symlink(path: &Path, _target: &[u8]) -> Result<(), CloneError> {
    Err(CloneError::CheckoutFailed {
        path: path.to_owned(),
        operation: "creating symlink",
        detail: "symlink checkout is not supported on this platform yet".to_owned(),
    })
}

fn parse_tree(tree_oid: ObjectId, data: &[u8]) -> Result<Vec<TreeEntry>, CloneError> {
    let mut entries = Vec::new();
    let mut cursor = 0usize;
    while cursor < data.len() {
        let mode_start = cursor;
        while cursor < data.len() && data[cursor] != b' ' {
            cursor += 1;
        }
        if cursor == data.len() {
            return tree_parse_error(tree_oid, "tree entry mode was not terminated by a space");
        }
        let mode = std::str::from_utf8(&data[mode_start..cursor]).map_err(|error| {
            CloneError::ObjectParseFailed {
                oid: tree_oid.to_hex(),
                object_type: "tree",
                operation: "parsing entry mode",
                detail: error.to_string(),
            }
        })?;
        cursor += 1;

        let name_start = cursor;
        while cursor < data.len() && data[cursor] != 0 {
            cursor += 1;
        }
        if cursor == data.len() {
            return tree_parse_error(tree_oid, "tree entry name was not NUL terminated");
        }
        let name = std::str::from_utf8(&data[name_start..cursor]).map_err(|error| {
            CloneError::ObjectParseFailed {
                oid: tree_oid.to_hex(),
                object_type: "tree",
                operation: "parsing entry name",
                detail: error.to_string(),
            }
        })?;
        cursor += 1;
        if data.len() - cursor < 20 {
            return tree_parse_error(tree_oid, "tree entry object id was truncated");
        }
        let mut oid = [0u8; 20];
        oid.copy_from_slice(&data[cursor..cursor + 20]);
        cursor += 20;

        entries.push(TreeEntry {
            mode: parse_tree_mode(tree_oid, mode)?,
            name: name.to_owned(),
            oid: ObjectId::from_bytes(oid),
        });
    }
    Ok(entries)
}

fn parse_tree_mode(tree_oid: ObjectId, mode: &str) -> Result<TreeMode, CloneError> {
    match mode {
        "100644" => Ok(TreeMode::File),
        "100755" => Ok(TreeMode::Executable),
        "120000" => Ok(TreeMode::Symlink),
        "040000" | "40000" => Ok(TreeMode::Directory),
        "160000" => Ok(TreeMode::Gitlink),
        other => Err(CloneError::ObjectParseFailed {
            oid: tree_oid.to_hex(),
            object_type: "tree",
            operation: "parsing entry mode",
            detail: format!("unsupported tree mode `{other}`"),
        }),
    }
}

fn tree_parse_error<T>(tree_oid: ObjectId, detail: &str) -> Result<T, CloneError> {
    Err(CloneError::ObjectParseFailed {
        oid: tree_oid.to_hex(),
        object_type: "tree",
        operation: "parsing tree entry",
        detail: detail.to_owned(),
    })
}

fn validate_relative_path(path: &Path) -> Result<(), CloneError> {
    for component in path.components() {
        match component {
            Component::Normal(name) if !name.is_empty() => {}
            _ => {
                return Err(CloneError::CheckoutFailed {
                    path: path.to_owned(),
                    operation: "validating checkout path",
                    detail: "tree entry contains an unsafe path component".to_owned(),
                });
            }
        }
    }
    Ok(())
}

fn write_git_index(index_path: &Path, entries: &mut [CheckoutEntry]) -> Result<(), CloneError> {
    entries.sort_by_cached_key(|entry| git_index_path_bytes(&entry.path));
    let mut index = Vec::new();
    index.extend_from_slice(b"DIRC");
    index.extend_from_slice(&2u32.to_be_bytes());
    let entry_count = u32::try_from(entries.len()).map_err(|error| CloneError::CheckoutFailed {
        path: index_path.to_owned(),
        operation: "writing git index header",
        detail: format!("index has too many entries for v2 format: {error}"),
    })?;
    index.extend_from_slice(&entry_count.to_be_bytes());

    for entry in entries {
        write_index_entry(&mut index, entry)?;
    }

    let checksum = Sha1::digest(&index);
    index.extend_from_slice(&checksum);
    fs::write(index_path, index).map_err(|error| CloneError::CheckoutFailed {
        path: index_path.to_owned(),
        operation: "writing git index",
        detail: error.to_string(),
    })
}

fn git_index_path_bytes(path: &Path) -> Vec<u8> {
    path.to_string_lossy().replace('\\', "/").into_bytes()
}

fn write_index_entry(index: &mut Vec<u8>, entry: &CheckoutEntry) -> Result<(), CloneError> {
    let start = index.len();
    index.extend_from_slice(&entry.stat.ctime_secs.to_be_bytes());
    index.extend_from_slice(&entry.stat.ctime_nanos.to_be_bytes());
    index.extend_from_slice(&entry.stat.mtime_secs.to_be_bytes());
    index.extend_from_slice(&entry.stat.mtime_nanos.to_be_bytes());
    index.extend_from_slice(&entry.stat.dev.to_be_bytes());
    index.extend_from_slice(&entry.stat.ino.to_be_bytes());
    index.extend_from_slice(&entry.mode.index_mode().to_be_bytes());
    index.extend_from_slice(&entry.stat.uid.to_be_bytes());
    index.extend_from_slice(&entry.stat.gid.to_be_bytes());
    index.extend_from_slice(&entry.size.to_be_bytes());
    index.extend_from_slice(&entry.oid.as_bytes());

    let path = entry.path.to_string_lossy();
    let path_bytes = path.as_bytes();
    if path_bytes.len() > 0xfff {
        return Err(CloneError::CheckoutFailed {
            path: entry.path.clone(),
            operation: "writing git index entry",
            detail: "path is too long for index v2 short flags".to_owned(),
        });
    }
    let path_len = u16::try_from(path_bytes.len()).map_err(|error| CloneError::CheckoutFailed {
        path: entry.path.clone(),
        operation: "writing git index entry",
        detail: format!("path length did not fit index v2 flags: {error}"),
    })?;
    index.extend_from_slice(&path_len.to_be_bytes());
    index.extend_from_slice(path_bytes);
    index.push(0);
    while !(index.len() - start).is_multiple_of(8) {
        index.push(0);
    }
    Ok(())
}

impl FileStat {
    #[cfg(unix)]
    fn try_from_metadata(path: &Path, metadata: &fs::Metadata) -> Result<Self, CloneError> {
        Ok(Self {
            ctime_secs: metadata_i64_to_u32(path, "ctime seconds", metadata.ctime())?,
            ctime_nanos: metadata_i64_to_u32(path, "ctime nanoseconds", metadata.ctime_nsec())?,
            mtime_secs: metadata_i64_to_u32(path, "mtime seconds", metadata.mtime())?,
            mtime_nanos: metadata_i64_to_u32(path, "mtime nanoseconds", metadata.mtime_nsec())?,
            dev: metadata_u64_to_u32(path, "device id", metadata.dev())?,
            ino: metadata_u64_to_u32(path, "inode", metadata.ino())?,
            uid: metadata.uid(),
            gid: metadata.gid(),
        })
    }

    #[cfg(not(unix))]
    fn try_from_metadata(_path: &Path, _metadata: &fs::Metadata) -> Result<Self, CloneError> {
        Ok(Self {
            ctime_secs: 0,
            ctime_nanos: 0,
            mtime_secs: 0,
            mtime_nanos: 0,
            dev: 0,
            ino: 0,
            uid: 0,
            gid: 0,
        })
    }
}

fn index_size(path: &Path, size: u64) -> Result<u32, CloneError> {
    u32::try_from(size).map_err(|error| CloneError::CheckoutFailed {
        path: path.to_owned(),
        operation: "writing git index entry",
        detail: format!("file size {size} does not fit index v2: {error}"),
    })
}

#[cfg(unix)]
fn metadata_i64_to_u32(path: &Path, field: &'static str, value: i64) -> Result<u32, CloneError> {
    u32::try_from(value).map_err(|error| CloneError::CheckoutFailed {
        path: path.to_owned(),
        operation: "encoding file metadata for git index",
        detail: format!("{field} value {value} does not fit index v2: {error}"),
    })
}

#[cfg(unix)]
fn metadata_u64_to_u32(path: &Path, field: &'static str, value: u64) -> Result<u32, CloneError> {
    u32::try_from(value).map_err(|error| CloneError::CheckoutFailed {
        path: path.to_owned(),
        operation: "encoding file metadata for git index",
        detail: format!("{field} value {value} does not fit index v2: {error}"),
    })
}
