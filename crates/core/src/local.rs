use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};

use crate::error::CloneError;

#[derive(Debug)]
pub struct LocalCloneRequest {
    source: PathBuf,
    target: Option<PathBuf>,
}

impl LocalCloneRequest {
    pub const fn new(source: PathBuf, target: Option<PathBuf>) -> Self {
        Self { source, target }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalCloneProgressPhase {
    InspectingSource,
    CopyingFiles,
    Finalizing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalCloneProgressEvent {
    Started,
    PhaseStarted(LocalCloneProgressPhase),
    PhaseCompleted(LocalCloneProgressPhase),
    CopyProgress {
        files: usize,
        dirs: usize,
        symlinks: usize,
        bytes: u64,
    },
    Completed,
}

#[derive(Debug)]
pub struct LocalCloneReport {
    pub strategy: &'static str,
    pub total_ms: u128,
    pub file_count: usize,
    pub dir_count: usize,
    pub symlink_count: usize,
    pub bytes: u64,
}

#[derive(Debug, Default)]
struct CopyStats {
    file_count: usize,
    dir_count: usize,
    symlink_count: usize,
    bytes: u64,
}

#[derive(Debug, Default)]
struct CopyProgressThrottle {
    next_entries: usize,
    next_bytes: u64,
}

pub fn local_clone(request: LocalCloneRequest) -> Result<LocalCloneReport, CloneError> {
    local_clone_inner(request, None)
}

pub fn local_clone_with_progress(
    request: LocalCloneRequest,
    progress: impl Fn(LocalCloneProgressEvent),
) -> Result<LocalCloneReport, CloneError> {
    local_clone_inner(request, Some(&progress))
}

fn local_clone_inner(
    request: LocalCloneRequest,
    progress: Option<&dyn Fn(LocalCloneProgressEvent)>,
) -> Result<LocalCloneReport, CloneError> {
    let start = Instant::now();
    emit_progress(progress, LocalCloneProgressEvent::Started);
    emit_progress(
        progress,
        LocalCloneProgressEvent::PhaseStarted(LocalCloneProgressPhase::InspectingSource),
    );
    let source = request
        .source
        .canonicalize()
        .map_err(|error| CloneError::LocalCloneFailed {
            path: request.source.clone(),
            operation: "canonicalizing source path",
            detail: error.to_string(),
        })?;
    inspect_source_repo(&source)?;
    let target = request.target.unwrap_or_else(|| default_target(&source));
    if target.exists() {
        return Err(CloneError::TargetAlreadyExists { path: target });
    }
    ensure_same_device(&source, target.parent().unwrap_or_else(|| Path::new(".")))?;
    emit_progress(
        progress,
        LocalCloneProgressEvent::PhaseCompleted(LocalCloneProgressPhase::InspectingSource),
    );
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    return Err(CloneError::LocalCloneFailed {
        path: source,
        operation: "selecting copy-on-write strategy",
        detail: "fcl local currently supports macOS APFS clonefile and Linux FICLONE only"
            .to_owned(),
    });

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    let strategy = cow_strategy();
    let mut stats = CopyStats::default();
    let mut progress_throttle = CopyProgressThrottle::default();
    emit_progress(
        progress,
        LocalCloneProgressEvent::PhaseStarted(LocalCloneProgressPhase::CopyingFiles),
    );
    copy_tree_cow(
        &source,
        &target,
        &mut stats,
        progress,
        &mut progress_throttle,
    )?;
    emit_copy_progress(progress, &stats);
    emit_progress(
        progress,
        LocalCloneProgressEvent::PhaseCompleted(LocalCloneProgressPhase::CopyingFiles),
    );
    emit_progress(
        progress,
        LocalCloneProgressEvent::PhaseStarted(LocalCloneProgressPhase::Finalizing),
    );
    emit_progress(
        progress,
        LocalCloneProgressEvent::PhaseCompleted(LocalCloneProgressPhase::Finalizing),
    );
    emit_progress(progress, LocalCloneProgressEvent::Completed);
    Ok(LocalCloneReport {
        strategy,
        total_ms: start.elapsed().as_millis(),
        file_count: stats.file_count,
        dir_count: stats.dir_count,
        symlink_count: stats.symlink_count,
        bytes: stats.bytes,
    })
}

fn emit_progress(
    progress: Option<&dyn Fn(LocalCloneProgressEvent)>,
    event: LocalCloneProgressEvent,
) {
    if let Some(progress) = progress {
        progress(event);
    }
}

fn emit_copy_progress(progress: Option<&dyn Fn(LocalCloneProgressEvent)>, stats: &CopyStats) {
    emit_progress(
        progress,
        LocalCloneProgressEvent::CopyProgress {
            files: stats.file_count,
            dirs: stats.dir_count,
            symlinks: stats.symlink_count,
            bytes: stats.bytes,
        },
    );
}

fn maybe_emit_copy_progress(
    progress: Option<&dyn Fn(LocalCloneProgressEvent)>,
    stats: &CopyStats,
    throttle: &mut CopyProgressThrottle,
) {
    let entries = stats
        .file_count
        .saturating_add(stats.dir_count)
        .saturating_add(stats.symlink_count);
    if entries >= throttle.next_entries || stats.bytes >= throttle.next_bytes {
        emit_copy_progress(progress, stats);
        throttle.next_entries = entries.saturating_add(128);
        throttle.next_bytes = stats.bytes.saturating_add(8 * 1024 * 1024);
    }
}

fn inspect_source_repo(source: &Path) -> Result<(), CloneError> {
    let git = source.join(".git");
    let metadata = fs::symlink_metadata(&git).map_err(|error| CloneError::LocalCloneFailed {
        path: source.to_owned(),
        operation: "reading .git metadata",
        detail: error.to_string(),
    })?;
    if metadata.is_file() {
        return Err(CloneError::LocalCloneFailed {
            path: source.to_owned(),
            operation: "inspecting source repository",
            detail: "linked worktrees with .git files are not supported by fcl local yet"
                .to_owned(),
        });
    }
    if !metadata.is_dir() {
        return Err(CloneError::LocalCloneFailed {
            path: source.to_owned(),
            operation: "inspecting source repository",
            detail: ".git exists but is not a directory".to_owned(),
        });
    }
    for marker in [
        "MERGE_HEAD",
        "CHERRY_PICK_HEAD",
        "REVERT_HEAD",
        "BISECT_LOG",
        "index.lock",
        "HEAD.lock",
        "rebase-apply",
        "rebase-merge",
    ] {
        if git.join(marker).exists() {
            return Err(CloneError::LocalCloneFailed {
                path: source.to_owned(),
                operation: "checking Git operation state",
                detail: format!(
                    "source has in-progress Git state at .git/{marker}; finish or abort it before using fcl local"
                ),
            });
        }
    }
    Ok(())
}

fn default_target(source: &Path) -> PathBuf {
    let name = source
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "repo".to_owned());
    source.with_file_name(format!("{name}-fcl"))
}

#[cfg(unix)]
fn ensure_same_device(source: &Path, target_parent: &Path) -> Result<(), CloneError> {
    let target_parent = if target_parent.exists() {
        target_parent.to_path_buf()
    } else {
        target_parent
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf()
    };
    let source_dev = fs::metadata(source)
        .map_err(|error| CloneError::LocalCloneFailed {
            path: source.to_owned(),
            operation: "reading source filesystem metadata",
            detail: error.to_string(),
        })?
        .dev();
    let target_dev = fs::metadata(&target_parent)
        .map_err(|error| CloneError::LocalCloneFailed {
            path: target_parent.clone(),
            operation: "reading target filesystem metadata",
            detail: error.to_string(),
        })?
        .dev();
    if source_dev != target_dev {
        return Err(CloneError::LocalCloneFailed {
            path: target_parent,
            operation: "checking filesystem locality",
            detail: "source and target must be on the same filesystem for copy-on-write cloning"
                .to_owned(),
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_same_device(_source: &Path, _target_parent: &Path) -> Result<(), CloneError> {
    Err(CloneError::LocalCloneFailed {
        path: _source.to_owned(),
        operation: "checking filesystem locality",
        detail: "fcl local currently requires Unix filesystem metadata".to_owned(),
    })
}

#[cfg(target_os = "macos")]
const fn cow_strategy() -> &'static str {
    "apfs-clonefile"
}

#[cfg(target_os = "linux")]
const fn cow_strategy() -> &'static str {
    "linux-ficlone"
}

fn copy_tree_cow(
    source: &Path,
    target: &Path,
    stats: &mut CopyStats,
    progress: Option<&dyn Fn(LocalCloneProgressEvent)>,
    progress_throttle: &mut CopyProgressThrottle,
) -> Result<(), CloneError> {
    let metadata = fs::symlink_metadata(source).map_err(|error| CloneError::LocalCloneFailed {
        path: source.to_owned(),
        operation: "reading source entry metadata",
        detail: error.to_string(),
    })?;
    let file_type = metadata.file_type();
    if file_type.is_dir() {
        fs::create_dir(target).map_err(|error| CloneError::LocalCloneFailed {
            path: target.to_owned(),
            operation: "creating target directory",
            detail: error.to_string(),
        })?;
        stats.dir_count += 1;
        maybe_emit_copy_progress(progress, stats, progress_throttle);
        for entry in fs::read_dir(source).map_err(|error| CloneError::LocalCloneFailed {
            path: source.to_owned(),
            operation: "reading source directory",
            detail: error.to_string(),
        })? {
            let entry = entry.map_err(|error| CloneError::LocalCloneFailed {
                path: source.to_owned(),
                operation: "reading source directory entry",
                detail: error.to_string(),
            })?;
            copy_tree_cow(
                &entry.path(),
                &target.join(entry.file_name()),
                stats,
                progress,
                progress_throttle,
            )?;
        }
        #[cfg(unix)]
        fs::set_permissions(
            target,
            fs::Permissions::from_mode(metadata.permissions().mode()),
        )
        .map_err(|error| CloneError::LocalCloneFailed {
            path: target.to_owned(),
            operation: "copying directory permissions",
            detail: error.to_string(),
        })?;
        return Ok(());
    }
    if file_type.is_symlink() {
        copy_symlink(source, target)?;
        stats.symlink_count += 1;
        maybe_emit_copy_progress(progress, stats, progress_throttle);
        return Ok(());
    }
    #[cfg(unix)]
    if file_type.is_fifo()
        || file_type.is_socket()
        || file_type.is_block_device()
        || file_type.is_char_device()
    {
        return Err(CloneError::LocalCloneFailed {
            path: source.to_owned(),
            operation: "copying special file",
            detail: "special filesystem entries are not supported by fcl local".to_owned(),
        });
    }
    clone_file_cow(source, target)?;
    stats.file_count += 1;
    stats.bytes = stats.bytes.saturating_add(metadata.len());
    maybe_emit_copy_progress(progress, stats, progress_throttle);
    Ok(())
}

#[cfg(unix)]
fn copy_symlink(source: &Path, target: &Path) -> Result<(), CloneError> {
    let link_target = fs::read_link(source).map_err(|error| CloneError::LocalCloneFailed {
        path: source.to_owned(),
        operation: "reading symlink target",
        detail: error.to_string(),
    })?;
    std::os::unix::fs::symlink(&link_target, target).map_err(|error| CloneError::LocalCloneFailed {
        path: target.to_owned(),
        operation: "creating symlink",
        detail: error.to_string(),
    })
}

#[cfg(not(unix))]
fn copy_symlink(source: &Path, _target: &Path) -> Result<(), CloneError> {
    Err(CloneError::LocalCloneFailed {
        path: source.to_owned(),
        operation: "copying symlink",
        detail: "symlink local clone is not supported on this platform".to_owned(),
    })
}

#[cfg(target_os = "macos")]
fn clone_file_cow(source: &Path, target: &Path) -> Result<(), CloneError> {
    reflink_copy::reflink(source, target).map_err(|error| CloneError::LocalCloneFailed {
        path: source.to_owned(),
        operation: "copying file with APFS clonefile",
        detail: error.to_string(),
    })
}

#[cfg(target_os = "linux")]
fn clone_file_cow(source: &Path, target: &Path) -> Result<(), CloneError> {
    reflink_copy::reflink(source, target).map_err(|error| CloneError::LocalCloneFailed {
        path: source.to_owned(),
        operation: "copying file with Linux FICLONE",
        detail: error.to_string(),
    })
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn clone_file_cow(source: &Path, _target: &Path) -> Result<(), CloneError> {
    Err(CloneError::LocalCloneFailed {
        path: source.to_owned(),
        operation: "copying file with copy-on-write",
        detail: "unsupported platform".to_owned(),
    })
}
