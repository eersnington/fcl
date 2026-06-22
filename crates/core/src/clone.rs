use std::fs;
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;

use url::Url;

use crate::archive::{archive_checkout_enabled, checkout_github_archive};
use crate::checkout::{index_existing_default_branch, materialize_default_branch};
use crate::error::CloneError;
use crate::metrics::{CloneMetrics, CloneReport, measure_ms};
use crate::pack::{ObjectId, ingest_pack};
use crate::protocol::{discover_remote, fetch_full_pack, http_client};
use crate::repo::RepoLayout;

#[derive(Debug)]
pub struct CloneRequest {
    pub url: String,
    pub target: Option<PathBuf>,
}

impl CloneRequest {
    pub const fn new(url: String, target: Option<PathBuf>) -> Self {
        Self { url, target }
    }
}

pub fn clone_repo(request: CloneRequest) -> Result<CloneReport, CloneError> {
    let mut metrics = CloneMetrics::start();
    let client = http_client()?;
    let (remote, discovery_ms) = measure_ms(|| discover_remote(&client, &request.url));
    let remote = remote?;
    metrics.discovery_ms = discovery_ms;
    let refs = remote.refs.select_full_clone_universe();
    metrics.ref_count = refs.len();

    let target = match request.target {
        Some(target) => target,
        None => default_target_dir(&request.url)?,
    };
    let repo = RepoLayout::create(&target)?;
    let checkout_head = checkout_head_oid(remote.refs.default_branch.as_ref(), &refs)?;
    let archive_checkout = spawn_archive_checkout(
        &request.url,
        checkout_head.to_hex(),
        repo.root().to_path_buf(),
    );

    let (pack_bytes, fetch_ms) =
        measure_ms(|| fetch_full_pack(&client, &remote, &refs, &repo.pack_temp_path()));
    metrics.pack_bytes = pack_bytes?;
    metrics.fetch_ms = fetch_ms;
    enforce_max_bytes(
        "FCL_MAX_PACK_BYTES",
        metrics.pack_bytes,
        "checking fetched pack size",
    )?;
    enforce_max_bytes(
        "FCL_MAX_TEMP_BYTES",
        directory_bytes(repo.root())?,
        "checking target size after fetch",
    )?;
    let (pack_index, ingest_ms) =
        measure_ms(|| ingest_pack(&repo.pack_temp_path(), &repo.pack_index_temp_path()));
    let pack_index = pack_index?;
    metrics.ingest_ms = ingest_ms;
    metrics.retained_object_count = pack_index.retained_object_count();
    metrics.retained_object_bytes = pack_index.retained_object_bytes();
    metrics.spilled_object_count = pack_index.spilled_object_count();
    metrics.spilled_object_bytes = pack_index.spilled_object_bytes();
    repo.write_initial_metadata(&remote, &refs)?;
    enforce_max_bytes(
        "FCL_MAX_TEMP_BYTES",
        directory_bytes(repo.root())?,
        "checking target size after ingest",
    )?;
    let (checkout, checkout_ms) = measure_ms(|| {
        if archive_checkout_completed(archive_checkout) {
            index_existing_default_branch(&repo, &pack_index, &remote.refs, &refs)
        } else {
            materialize_default_branch(&repo, &pack_index, &remote.refs, &refs)
        }
    });
    let checkout = checkout?;
    metrics.checkout_ms = checkout_ms;
    metrics.checkout_manifest_ms = checkout.manifest_ms;
    metrics.checkout_dir_create_ms = checkout.dir_create_ms;
    metrics.checkout_file_materialize_ms = checkout.file_materialize_ms;
    metrics.checkout_index_write_ms = checkout.index_write_ms;
    metrics.checkout_file_count = checkout.file_count;
    metrics.checkout_dir_count = checkout.dir_count;
    metrics.checkout_blob_bytes = checkout.blob_bytes;
    metrics.reconstructed_object_count = pack_index.reconstructed_object_count();
    metrics.target_bytes = directory_bytes(repo.root())?;
    metrics.rss_bytes = rss_bytes();
    enforce_max_bytes(
        "FCL_MAX_TEMP_BYTES",
        metrics.target_bytes,
        "checking final target size",
    )?;

    Ok(metrics.into())
}

fn enforce_max_bytes(
    env_name: &'static str,
    actual: u64,
    operation: &'static str,
) -> Result<(), CloneError> {
    let Some(max) = optional_u64_env(env_name)? else {
        return Ok(());
    };
    if actual > max {
        return Err(CloneError::CloneLimitExceeded {
            operation,
            detail: format!("{env_name} is {max} bytes, but current usage is {actual} bytes"),
        });
    }
    Ok(())
}

fn optional_u64_env(name: &'static str) -> Result<Option<u64>, CloneError> {
    let Some(raw) = std::env::var_os(name) else {
        return Ok(None);
    };
    let raw = raw.to_string_lossy();
    let value = raw
        .parse::<u64>()
        .map_err(|error| CloneError::CloneLimitExceeded {
            operation: "parsing clone safety limit",
            detail: format!("{name} must be an unsigned byte count, got `{raw}`: {error}"),
        })?;
    Ok(Some(value))
}

fn directory_bytes(path: &Path) -> Result<u64, CloneError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| CloneError::RepoLayoutFailed {
        path: path.to_owned(),
        operation: "reading target metadata for size accounting",
        source: error,
    })?;
    if metadata.is_file() || metadata.file_type().is_symlink() {
        return Ok(metadata.len());
    }

    let mut total = 0u64;
    for entry in fs::read_dir(path).map_err(|error| CloneError::RepoLayoutFailed {
        path: path.to_owned(),
        operation: "reading target directory for size accounting",
        source: error,
    })? {
        let entry = entry.map_err(|error| CloneError::RepoLayoutFailed {
            path: path.to_owned(),
            operation: "reading target directory entry for size accounting",
            source: error,
        })?;
        total = total.saturating_add(directory_bytes(&entry.path())?);
    }
    Ok(total)
}

fn rss_bytes() -> Option<u64> {
    memory_stats::memory_stats().map(|stats| stats.physical_mem as u64)
}

fn spawn_archive_checkout(
    url: &str,
    head: String,
    target: PathBuf,
) -> Option<JoinHandle<Result<(), CloneError>>> {
    if !archive_checkout_enabled() {
        return None;
    }
    let url = url.to_owned();
    Some(std::thread::spawn(move || {
        checkout_github_archive(&url, &head, &target)
    }))
}

fn archive_checkout_completed(handle: Option<JoinHandle<Result<(), CloneError>>>) -> bool {
    let Some(handle) = handle else {
        return false;
    };
    matches!(handle.join(), Ok(Ok(())))
}

fn checkout_head_oid(
    default_branch: Option<&String>,
    refs: &[crate::protocol::RemoteRef],
) -> Result<ObjectId, CloneError> {
    let Some(default_branch) = default_branch else {
        return Err(CloneError::CheckoutFailed {
            path: PathBuf::from("."),
            operation: "resolving default branch",
            detail: "remote did not advertise a HEAD symref".to_owned(),
        });
    };
    let head = refs
        .iter()
        .find(|remote_ref| remote_ref.name == *default_branch)
        .ok_or_else(|| CloneError::CheckoutFailed {
            path: PathBuf::from("."),
            operation: "resolving default branch",
            detail: format!("HEAD points to `{default_branch}`, but that ref was not fetched"),
        })?;
    ObjectId::parse_hex(&head.oid)
}

fn default_target_dir(raw_url: &str) -> Result<PathBuf, CloneError> {
    let url = Url::parse(raw_url).map_err(|_| CloneError::TargetNameMissing {
        url: raw_url.to_owned(),
    })?;
    let segment = url
        .path_segments()
        .and_then(Iterator::last)
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| CloneError::TargetNameMissing {
            url: raw_url.to_owned(),
        })?;
    let name = segment.strip_suffix(".git").unwrap_or(segment);
    if name.is_empty() {
        return Err(CloneError::TargetNameMissing {
            url: raw_url.to_owned(),
        });
    }
    Ok(PathBuf::from(name))
}
