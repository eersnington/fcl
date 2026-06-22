use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use url::Url;

use crate::checkout::materialize_default_branch;
use crate::error::CloneError;
use crate::metrics::{CloneReport, measure_ms};
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
    let start = Instant::now();
    let client = http_client()?;
    let (remote, discovery_ms) = measure_ms(|| discover_remote(&client, &request.url));
    let remote = remote?;
    let refs = remote.refs.select_full_clone_universe();
    let ref_count = refs.len();
    let default_branch = resolve_default_branch(remote.refs.default_branch.as_deref(), &refs)?;
    let default_commit = ObjectId::parse_hex(&default_branch.oid)?;

    let target = match request.target {
        Some(target) => target,
        None => default_target_dir(&request.url)?,
    };
    let repo = RepoLayout::create(&target)?;

    let (pack_bytes, fetch_ms) =
        measure_ms(|| fetch_full_pack(&client, &remote, &refs, &repo.pack_temp_path()));
    let pack_bytes = pack_bytes?;
    enforce_max_bytes(
        "FCL_MAX_PACK_BYTES",
        pack_bytes,
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
    let retained_object_count = pack_index.retained_object_count();
    let retained_object_bytes = pack_index.retained_object_bytes();
    let spilled_object_count = pack_index.spilled_object_count();
    let spilled_object_bytes = pack_index.spilled_object_bytes();
    repo.write_initial_metadata(&remote, &refs, &default_branch.name)?;
    enforce_max_bytes(
        "FCL_MAX_TEMP_BYTES",
        directory_bytes(repo.root())?,
        "checking target size after ingest",
    )?;
    let (checkout, checkout_ms) =
        measure_ms(|| materialize_default_branch(&repo, &pack_index, default_commit));
    let checkout = checkout?;
    let reconstructed_object_count = pack_index.reconstructed_object_count();
    let target_bytes = directory_bytes(repo.root())?;
    let rss_bytes = rss_bytes();
    enforce_max_bytes(
        "FCL_MAX_TEMP_BYTES",
        target_bytes,
        "checking final target size",
    )?;

    Ok(CloneReport {
        ref_count,
        pack_bytes,
        total_ms: start.elapsed().as_millis(),
        discovery_ms,
        fetch_ms,
        ingest_ms,
        checkout_ms,
        checkout_manifest_ms: checkout.manifest_ms,
        checkout_dir_create_ms: checkout.dir_create_ms,
        checkout_file_materialize_ms: checkout.file_materialize_ms,
        checkout_index_write_ms: checkout.index_write_ms,
        checkout_file_count: checkout.file_count,
        checkout_dir_count: checkout.dir_count,
        checkout_blob_bytes: checkout.blob_bytes,
        retained_object_count,
        retained_object_bytes,
        spilled_object_count,
        spilled_object_bytes,
        reconstructed_object_count,
        target_bytes,
        rss_bytes,
    })
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

fn resolve_default_branch<'a>(
    default_branch: Option<&str>,
    refs: &'a [crate::protocol::RemoteRef],
) -> Result<&'a crate::protocol::RemoteRef, CloneError> {
    let Some(default_branch) = default_branch else {
        return Err(CloneError::CheckoutFailed {
            path: PathBuf::from("."),
            operation: "resolving default branch",
            detail: "remote did not advertise a HEAD symref".to_owned(),
        });
    };
    refs.iter()
        .find(|remote_ref| remote_ref.name == default_branch)
        .ok_or_else(|| CloneError::CheckoutFailed {
            path: PathBuf::from("."),
            operation: "resolving default branch",
            detail: format!("HEAD points to `{default_branch}`, but that ref was not fetched"),
        })
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
