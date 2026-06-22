use std::fs::{self, OpenOptions};
use std::path::{Component, Path, PathBuf};
use std::time::Instant;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

use rayon::{ThreadPoolBuilder, prelude::*};
use sha1::{Digest, Sha1};

use crate::error::CloneError;
use crate::pack::{ObjectId, ObjectReader, ObjectType};
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
    git_path: Vec<u8>,
    mode: TreeMode,
    oid: ObjectId,
    size: u32,
    stat: FileStat,
}

#[derive(Debug, Clone)]
struct CheckoutDirectory {
    path: PathBuf,
    git_path: Vec<u8>,
    depth: usize,
}

#[derive(Debug, Clone)]
struct CheckoutManifestEntry {
    path: PathBuf,
    git_path: Vec<u8>,
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
    commit_oid: ObjectId,
) -> Result<CheckoutReport, CloneError> {
    let manifest_start = Instant::now();
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

    let dir_start = Instant::now();
    create_checkout_directories(repo.root(), directories)?;
    let dir_create_ms = dir_start.elapsed().as_millis();

    let file_start = Instant::now();
    sort_manifest_for_checkout(&mut manifest);
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
    directories: &mut Vec<CheckoutDirectory>,
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
                directories.push(CheckoutDirectory {
                    git_path: git_index_path_bytes(&relative_path),
                    depth: path_depth(&relative_path),
                    path: relative_path.clone(),
                });
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
                    git_path: git_index_path_bytes(&relative_path),
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

fn create_checkout_directories(
    root: &Path,
    mut directories: Vec<CheckoutDirectory>,
) -> Result<(), CloneError> {
    directories.sort_by(|left, right| {
        left.depth
            .cmp(&right.depth)
            .then_with(|| left.git_path.cmp(&right.git_path))
    });
    directories.dedup_by(|left, right| left.git_path == right.git_path);

    for directory in directories {
        let path = root.join(&directory.path);
        fs::create_dir(&path).map_err(|error| CloneError::CheckoutFailed {
            path,
            operation: "creating checkout directory",
            detail: error.to_string(),
        })?;
    }
    Ok(())
}

fn sort_manifest_for_checkout(manifest: &mut [CheckoutManifestEntry]) {
    manifest.sort_by(|left, right| {
        parent_git_path(&left.git_path)
            .cmp(parent_git_path(&right.git_path))
            .then_with(|| left.git_path.cmp(&right.git_path))
    });
}

fn parent_git_path(path: &[u8]) -> &[u8] {
    path.iter()
        .rposition(|byte| *byte == b'/')
        .map_or(&[], |index| &path[..index])
}

fn path_depth(path: &Path) -> usize {
    path.components()
        .filter(|component| matches!(component, Component::Normal(_)))
        .count()
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

    let metadata = match entry.mode {
        TreeMode::File | TreeMode::Executable => {
            materialize_regular_file(&path, entry, object_reader)?
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
            fs::symlink_metadata(&path).map_err(|error| CloneError::CheckoutFailed {
                path: path.clone(),
                operation: "reading checkout metadata",
                detail: error.to_string(),
            })?
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
    };

    Ok(CheckoutEntry {
        path: entry.path.clone(),
        git_path: entry.git_path.clone(),
        mode: entry.mode,
        oid: entry.oid,
        size: index_size(&entry.path, entry.size)?,
        stat: FileStat::try_from_metadata(&path, &metadata)?,
    })
}

fn materialize_regular_file(
    path: &Path,
    entry: &CheckoutManifestEntry,
    object_reader: &dyn ObjectReader,
) -> Result<fs::Metadata, CloneError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(match entry.mode {
        TreeMode::Executable => 0o755,
        _ => 0o644,
    });
    let mut file = options
        .open(path)
        .map_err(|error| CloneError::CheckoutFailed {
            path: path.to_owned(),
            operation: "creating file",
            detail: error.to_string(),
        })?;
    let written = object_reader.stream_blob(entry.oid, &mut file)?;
    if written != entry.size {
        return Err(CloneError::CheckoutFailed {
            path: path.to_owned(),
            operation: "streaming file",
            detail: format!("wrote {written} bytes, expected {}", entry.size),
        });
    }
    #[cfg(unix)]
    if entry.mode == TreeMode::Executable {
        let metadata = file
            .metadata()
            .map_err(|error| CloneError::CheckoutFailed {
                path: path.to_owned(),
                operation: "reading executable file metadata",
                detail: error.to_string(),
            })?;
        if metadata.permissions().mode() & 0o100 == 0 {
            file.set_permissions(fs::Permissions::from_mode(0o755))
                .map_err(|error| CloneError::CheckoutFailed {
                    path: path.to_owned(),
                    operation: "setting executable permissions",
                    detail: error.to_string(),
                })?;
        }
    }
    file.metadata().map_err(|error| CloneError::CheckoutFailed {
        path: path.to_owned(),
        operation: "reading checkout metadata",
        detail: error.to_string(),
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
    entries.sort_by(|left, right| left.git_path.cmp(&right.git_path));
    let mut index = Vec::with_capacity(estimated_index_size(entries)?);
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

    let path_bytes = entry.git_path.as_slice();
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

fn estimated_index_size(entries: &[CheckoutEntry]) -> Result<usize, CloneError> {
    let mut size = 12usize + 20;
    for entry in entries {
        let entry_size = 62usize
            .checked_add(entry.git_path.len())
            .and_then(|size| size.checked_add(1))
            .ok_or_else(|| CloneError::CheckoutFailed {
                path: entry.path.clone(),
                operation: "sizing git index",
                detail: "index entry size overflowed usize".to_owned(),
            })?;
        let padded = entry_size.next_multiple_of(8);
        size = size
            .checked_add(padded)
            .ok_or_else(|| CloneError::CheckoutFailed {
                path: entry.path.clone(),
                operation: "sizing git index",
                detail: "index size overflowed usize".to_owned(),
            })?;
    }
    Ok(size)
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use super::{
        CheckoutDirectory, create_checkout_directories, git_index_path_bytes,
        materialize_default_branch, path_depth,
    };
    use crate::error::CloneError;
    use crate::pack::{ObjectBytes, ObjectId, ObjectMeta, ObjectReader, ObjectType};
    use crate::repo::RepoLayout;

    #[derive(Debug, Default)]
    struct FakeObjectReader {
        objects: HashMap<ObjectId, ObjectBytes>,
        meta: HashMap<ObjectId, ObjectMeta>,
    }

    impl FakeObjectReader {
        fn insert(&mut self, oid: ObjectId, object_type: ObjectType, data: Vec<u8>) {
            self.meta.insert(
                oid,
                ObjectMeta {
                    object_type,
                    size: data.len() as u64,
                    pack_inflated_size: data.len() as u64,
                    pack_offset: 0,
                    compressed_start: 0,
                    compressed_len: 0,
                    crc32: 0,
                    delta_base: None,
                },
            );
            self.objects.insert(
                oid,
                ObjectBytes {
                    object_type,
                    data: Arc::from(data),
                },
            );
        }
    }

    impl ObjectReader for FakeObjectReader {
        fn get_meta(&self, oid: ObjectId) -> Option<&ObjectMeta> {
            self.meta.get(&oid)
        }

        fn read_object(&self, oid: ObjectId) -> Result<ObjectBytes, CloneError> {
            self.objects
                .get(&oid)
                .cloned()
                .ok_or_else(|| CloneError::ObjectLookupFailed {
                    oid: oid.to_hex(),
                    expected_type: "object",
                    detail: "fake reader did not contain object".to_owned(),
                })
        }

        fn stream_blob(&self, oid: ObjectId, out: &mut dyn Write) -> Result<u64, CloneError> {
            let object = self.read_object(oid)?;
            if object.object_type != ObjectType::Blob {
                return Err(CloneError::ObjectLookupFailed {
                    oid: oid.to_hex(),
                    expected_type: "blob",
                    detail: format!("found {}", object.object_type.as_git_name()),
                });
            }
            out.write_all(&object.data)
                .map_err(|error| CloneError::CheckoutFailed {
                    path: PathBuf::from("fake"),
                    operation: "streaming fake blob",
                    detail: error.to_string(),
                })?;
            Ok(object.data.len() as u64)
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct ParsedIndexEntry {
        path: String,
        mode: u32,
        size: u32,
        oid: [u8; 20],
    }

    #[cfg(unix)]
    #[test]
    fn checkout_should_materialize_files_executables_symlinks_and_write_index() {
        let temp = TestDir::new("checkout-all");
        let repo = test_repo(temp.path());
        let commit_oid = oid(1);
        let root_tree_oid = oid(2);
        let bin_tree_oid = oid(3);
        let docs_tree_oid = oid(4);
        let executable_oid = oid(5);
        let readme_oid = oid(6);
        let symlink_oid = oid(7);
        let mut reader = FakeObjectReader::default();
        reader.insert(commit_oid, ObjectType::Commit, commit(root_tree_oid));
        reader.insert(
            root_tree_oid,
            ObjectType::Tree,
            tree(&[
                ("040000", "bin", bin_tree_oid),
                ("040000", "docs", docs_tree_oid),
                ("120000", "link", symlink_oid),
            ]),
        );
        reader.insert(
            bin_tree_oid,
            ObjectType::Tree,
            tree(&[("100755", "run.sh", executable_oid)]),
        );
        reader.insert(
            docs_tree_oid,
            ObjectType::Tree,
            tree(&[("100644", "readme.txt", readme_oid)]),
        );
        reader.insert(executable_oid, ObjectType::Blob, b"#!/bin/sh\n".to_vec());
        reader.insert(readme_oid, ObjectType::Blob, b"hello\n".to_vec());
        reader.insert(symlink_oid, ObjectType::Blob, b"docs/readme.txt".to_vec());

        let report = materialize_default_branch(&repo, &reader, commit_oid)
            .expect("checkout should succeed");

        assert_eq!(report.file_count, 3);
        assert_eq!(report.dir_count, 2);
        assert_eq!(
            fs::read_to_string(temp.path().join("docs/readme.txt"))
                .expect("readme should be readable"),
            "hello\n"
        );
        assert_eq!(
            fs::read_to_string(temp.path().join("bin/run.sh"))
                .expect("executable should be readable"),
            "#!/bin/sh\n"
        );
        assert_ne!(
            fs::metadata(temp.path().join("bin/run.sh"))
                .expect("executable metadata should be readable")
                .permissions()
                .mode()
                & 0o100,
            0
        );
        assert_eq!(
            fs::read_link(temp.path().join("link")).expect("symlink should be readable"),
            Path::new("docs/readme.txt")
        );
        let index = parse_index(&repo.git_index_path());
        assert_eq!(
            index,
            vec![
                ParsedIndexEntry {
                    path: "bin/run.sh".to_owned(),
                    mode: 0o100_755,
                    size: 10,
                    oid: executable_oid.as_bytes(),
                },
                ParsedIndexEntry {
                    path: "docs/readme.txt".to_owned(),
                    mode: 0o100_644,
                    size: 6,
                    oid: readme_oid.as_bytes(),
                },
                ParsedIndexEntry {
                    path: "link".to_owned(),
                    mode: 0o120_000,
                    size: 15,
                    oid: symlink_oid.as_bytes(),
                },
            ]
        );
    }

    #[test]
    fn checkout_should_fail_when_file_path_already_exists() {
        let temp = TestDir::new("checkout-existing-file");
        let repo = test_repo(temp.path());
        fs::write(temp.path().join("readme.txt"), "existing")
            .expect("existing file should be created");
        let commit_oid = oid(11);
        let tree_oid = oid(12);
        let blob_oid = oid(13);
        let mut reader = FakeObjectReader::default();
        reader.insert(commit_oid, ObjectType::Commit, commit(tree_oid));
        reader.insert(
            tree_oid,
            ObjectType::Tree,
            tree(&[("100644", "readme.txt", blob_oid)]),
        );
        reader.insert(blob_oid, ObjectType::Blob, b"new".to_vec());

        let error = materialize_default_branch(&repo, &reader, commit_oid)
            .expect_err("checkout should refuse to replace existing files");

        assert!(error.to_string().contains("creating file"));
    }

    #[test]
    fn create_checkout_directories_should_create_nested_directories_parent_first() {
        let temp = TestDir::new("checkout-dirs");
        fs::create_dir(temp.path()).expect("checkout root should be created");
        let child = PathBuf::from("a/b");
        let parent = PathBuf::from("a");
        create_checkout_directories(
            temp.path(),
            vec![
                CheckoutDirectory {
                    git_path: git_index_path_bytes(&child),
                    depth: path_depth(&child),
                    path: child,
                },
                CheckoutDirectory {
                    git_path: git_index_path_bytes(&parent),
                    depth: path_depth(&parent),
                    path: parent,
                },
            ],
        )
        .expect("directories should be created parent first");

        assert!(temp.path().join("a/b").is_dir());
    }

    fn test_repo(root: &Path) -> RepoLayout {
        RepoLayout::create(root).expect("repo layout should be created")
    }

    fn oid(byte: u8) -> ObjectId {
        ObjectId::from_bytes([byte; 20])
    }

    fn commit(tree_oid: ObjectId) -> Vec<u8> {
        format!("tree {}\nauthor A <a@example.com> 0 +0000\ncommitter A <a@example.com> 0 +0000\n\nmessage\n", tree_oid.to_hex()).into_bytes()
    }

    fn tree(entries: &[(&str, &str, ObjectId)]) -> Vec<u8> {
        let mut data = Vec::new();
        for (mode, name, oid) in entries {
            data.extend_from_slice(mode.as_bytes());
            data.push(b' ');
            data.extend_from_slice(name.as_bytes());
            data.push(0);
            data.extend_from_slice(&oid.as_bytes());
        }
        data
    }

    fn parse_index(path: &Path) -> Vec<ParsedIndexEntry> {
        let data = fs::read(path).expect("git index should be readable");
        assert_eq!(&data[..4], b"DIRC");
        assert_eq!(
            u32::from_be_bytes(data[4..8].try_into().expect("version bytes")),
            2
        );
        let entries = u32::from_be_bytes(data[8..12].try_into().expect("entry count bytes"));
        let mut offset = 12usize;
        let mut parsed = Vec::new();
        for _ in 0..entries {
            let start = offset;
            let mode = u32::from_be_bytes(data[offset + 24..offset + 28].try_into().expect("mode"));
            let size = u32::from_be_bytes(data[offset + 36..offset + 40].try_into().expect("size"));
            let oid: [u8; 20] = data[offset + 40..offset + 60].try_into().expect("oid");
            let flags =
                u16::from_be_bytes(data[offset + 60..offset + 62].try_into().expect("flags"));
            let path_len = usize::from(flags & 0x0fff);
            offset += 62;
            let path = String::from_utf8(data[offset..offset + path_len].to_vec())
                .expect("index path should be UTF-8 in test");
            offset += path_len + 1;
            while !(offset - start).is_multiple_of(8) {
                offset += 1;
            }
            parsed.push(ParsedIndexEntry {
                path,
                mode,
                size,
                oid,
            });
        }
        assert_eq!(data.len() - offset, 20);
        parsed
    }

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(prefix: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time should be after Unix epoch")
                .as_nanos();
            let path =
                std::env::temp_dir().join(format!("fcl-{prefix}-{}-{nanos}", std::process::id()));
            if path.exists() {
                fs::remove_dir_all(&path).expect("stale test directory should be removable");
            }
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            if self.path.exists() {
                let _ = fs::remove_dir_all(&self.path);
            }
        }
    }
}
