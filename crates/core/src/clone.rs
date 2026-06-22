use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::Instant;

use url::Url;

use crate::checkout::materialize_default_branch;
use crate::error::CloneError;
use crate::metrics::{CloneReport, measure_ms};
use crate::pack::{
    CheckoutHint, ObjectId, PackIngestOptions, PackStorage, PipelineObjectStore,
    ingest_fetched_pack, ingest_pack_pipeline, ingest_scanned_pack,
};
use crate::protocol::{discover_remote, fetch_full_pack, fetch_full_pack_pipelined, http_client};
use crate::repo::{FinalizingRepo, RepoLayout};

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
    if env_bool("FCL_PIPELINE") {
        clone_repo_pipelined(request)
    } else {
        clone_repo_sequential(request)
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "top-level clone orchestration keeps phase timing and report assembly together"
)]
fn clone_repo_sequential(request: CloneRequest) -> Result<CloneReport, CloneError> {
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
    let staged_repo = FinalizingRepo::create(&target)?;
    let repo = staged_repo.layout()?;
    let max_temp_bytes = optional_u64_env("FCL_MAX_TEMP_BYTES")?;

    let (fetched_pack, fetch_ms) =
        measure_ms(|| fetch_full_pack(&client, &remote, &refs, &repo.pack_temp_path()));
    let fetched_pack = fetched_pack?;
    let pack_bytes = fetched_pack.bytes;
    enforce_optional_max_bytes(
        "FCL_MAX_PACK_BYTES",
        optional_u64_env("FCL_MAX_PACK_BYTES")?,
        pack_bytes,
        "checking fetched pack size",
    )?;
    enforce_target_size_limit(max_temp_bytes, repo, "checking target size after fetch")?;
    let streaming_pack_scan = fetched_pack.scan.is_some();
    let ingest_options = PackIngestOptions {
        checkout_hint: Some(CheckoutHint { default_commit }),
    };
    let (ingest_report, ingest_ms) = measure_ms(|| {
        if let Some(scan) = fetched_pack.scan.as_ref() {
            let pack = PackStorage::open_file_backed(&repo.pack_temp_path())?;
            ingest_scanned_pack(
                &repo.pack_temp_path(),
                &repo.pack_index_temp_path(),
                pack,
                scan,
                fetched_pack.scan_ms,
                ingest_options,
            )
        } else {
            ingest_fetched_pack(
                &repo.pack_temp_path(),
                &repo.pack_index_temp_path(),
                fetched_pack.checksum,
                ingest_options,
            )
        }
    });
    let ingest_report = ingest_report?;
    let pack_scan_ms = ingest_report.scan_ms;
    let pack_resolve_ms = ingest_report.resolve_ms;
    let pack_idx_write_ms = ingest_report.idx_write_ms;
    let pack_object_state_ms = ingest_report.object_state_ms;
    let pack_index = ingest_report.index;
    let retained_object_count = pack_index.retained_object_count();
    let retained_object_bytes = pack_index.retained_object_bytes();
    let spilled_object_count = pack_index.spilled_object_count();
    let spilled_object_bytes = pack_index.spilled_object_bytes();
    let checkout_needed_blob_count = ingest_report.checkout_needed_blob_count;
    let checkout_ready_blob_count = ingest_report.checkout_ready_blob_count;
    let checkout_ready_blob_bytes = ingest_report.checkout_ready_blob_bytes;
    let checkout_spilled_blob_count = ingest_report.checkout_spilled_blob_count;
    let checkout_spilled_blob_bytes = ingest_report.checkout_spilled_blob_bytes;
    let checkout_missing_blob_count = ingest_report.checkout_missing_blob_count;
    repo.write_initial_metadata(&remote, &refs, &default_branch.name)?;
    enforce_target_size_limit(max_temp_bytes, repo, "checking target size after ingest")?;
    let (checkout, checkout_ms) =
        measure_ms(|| materialize_default_branch(repo, &pack_index, default_commit));
    let checkout = checkout?;
    let reconstructed_object_count = pack_index.reconstructed_object_count();
    let total_ms = start.elapsed().as_millis();
    let repo = staged_repo.commit()?;
    let target_bytes = directory_bytes(repo.root())?;
    let rss_bytes = rss_bytes();
    enforce_optional_max_bytes(
        "FCL_MAX_TEMP_BYTES",
        max_temp_bytes,
        target_bytes,
        "checking final target size",
    )?;

    Ok(CloneReport {
        ref_count,
        pack_bytes,
        total_ms,
        discovery_ms,
        fetch_ms,
        ingest_ms,
        pack_scan_ms,
        pack_resolve_ms,
        pack_idx_write_ms,
        pack_object_state_ms,
        streaming_pack_scan,
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
        checkout_needed_blob_count,
        checkout_ready_blob_count,
        checkout_ready_blob_bytes,
        checkout_spilled_blob_count,
        checkout_spilled_blob_bytes,
        checkout_missing_blob_count,
        reconstructed_object_count,
        pipeline_enabled: false,
        pipeline_frame_count: None,
        pipeline_checkout_wait_ms: None,
        pipeline_peak_pending_delta_count: None,
        pipeline_arena_spill_bytes: None,
        target_bytes,
        rss_bytes,
    })
}

#[expect(
    clippy::too_many_lines,
    reason = "top-level pipeline orchestration keeps thread joins, timings, and report assembly together"
)]
fn clone_repo_pipelined(request: CloneRequest) -> Result<CloneReport, CloneError> {
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
    let staged_repo = FinalizingRepo::create(&target)?;
    let repo = staged_repo.layout()?;
    let max_temp_bytes = optional_u64_env("FCL_MAX_TEMP_BYTES")?;
    repo.write_initial_metadata(&remote, &refs, &default_branch.name)?;

    let pack_path = repo.pack_temp_path();
    let index_path = repo.pack_index_temp_path();
    let (sender, receiver) = sync_channel(pipeline_queue_capacity());
    let store = PipelineObjectStore::new(&pack_path);

    let fetch_client = client;
    let fetch_remote = remote.clone();
    let fetch_refs = refs.clone();
    let fetch_pack_path = pack_path.clone();
    let fetch_thread = thread::spawn(move || {
        let fetch_start = Instant::now();
        fetch_full_pack_pipelined(
            &fetch_client,
            &fetch_remote,
            &fetch_refs,
            &fetch_pack_path,
            &sender,
        )
        .map(|fetched_pack| (fetched_pack, fetch_start.elapsed().as_millis()))
    });

    let resolver_store = store.clone();
    let resolver_pack_path = pack_path.clone();
    let resolver_index_path = index_path;
    let resolver_thread = thread::spawn(move || {
        let result = ingest_pack_pipeline(
            &resolver_pack_path,
            &resolver_index_path,
            receiver,
            resolver_store.clone(),
        );
        if let Err(error) = &result {
            resolver_store.fail("resolving pipeline pack", error.to_string());
        }
        result
    });

    let checkout_start = Instant::now();
    let checkout_result = materialize_default_branch(repo, &store, default_commit);
    let checkout_ms = checkout_start.elapsed().as_millis();

    let fetch_joined = fetch_thread
        .join()
        .map_err(|_| CloneError::RemoteDiscoveryFailed {
            url: remote.url.clone(),
            operation: "joining pipeline fetch thread",
            detail: "fetch thread panicked".to_owned(),
        })?;
    let (fetched_pack, fetch_ms) = fetch_joined?;
    let pack_bytes = fetched_pack.bytes;
    enforce_optional_max_bytes(
        "FCL_MAX_PACK_BYTES",
        optional_u64_env("FCL_MAX_PACK_BYTES")?,
        pack_bytes,
        "checking fetched pack size",
    )?;

    let ingest_report = resolver_thread
        .join()
        .map_err(|_| CloneError::PackIndexFailed {
            path: pack_path.clone(),
            operation: "joining pipeline resolver thread",
            detail: "resolver thread panicked".to_owned(),
        })??;
    let checkout = checkout_result?;
    let pack_index = ingest_report.index;
    enforce_target_size_limit(
        max_temp_bytes,
        repo,
        "checking target size after pipeline clone",
    )?;
    let reconstructed_object_count = pack_index.reconstructed_object_count();
    let total_ms = start.elapsed().as_millis();
    let repo = staged_repo.commit()?;
    let target_bytes = directory_bytes(repo.root())?;
    let rss_bytes = rss_bytes();
    enforce_optional_max_bytes(
        "FCL_MAX_TEMP_BYTES",
        max_temp_bytes,
        target_bytes,
        "checking final target size",
    )?;

    Ok(CloneReport {
        ref_count,
        pack_bytes,
        total_ms,
        discovery_ms,
        fetch_ms,
        ingest_ms: ingest_report.resolve_ms
            + ingest_report.idx_write_ms
            + ingest_report.object_state_ms,
        pack_scan_ms: ingest_report.scan_ms,
        pack_resolve_ms: ingest_report.resolve_ms,
        pack_idx_write_ms: ingest_report.idx_write_ms,
        pack_object_state_ms: ingest_report.object_state_ms,
        streaming_pack_scan: true,
        checkout_ms,
        checkout_manifest_ms: checkout.manifest_ms,
        checkout_dir_create_ms: checkout.dir_create_ms,
        checkout_file_materialize_ms: checkout.file_materialize_ms,
        checkout_index_write_ms: checkout.index_write_ms,
        checkout_file_count: checkout.file_count,
        checkout_dir_count: checkout.dir_count,
        checkout_blob_bytes: checkout.blob_bytes,
        retained_object_count: pack_index.retained_object_count(),
        retained_object_bytes: pack_index.retained_object_bytes(),
        spilled_object_count: pack_index.spilled_object_count(),
        spilled_object_bytes: pack_index.spilled_object_bytes(),
        checkout_needed_blob_count: ingest_report.checkout_needed_blob_count,
        checkout_ready_blob_count: ingest_report.checkout_ready_blob_count,
        checkout_ready_blob_bytes: ingest_report.checkout_ready_blob_bytes,
        checkout_spilled_blob_count: ingest_report.checkout_spilled_blob_count,
        checkout_spilled_blob_bytes: ingest_report.checkout_spilled_blob_bytes,
        checkout_missing_blob_count: ingest_report.checkout_missing_blob_count,
        reconstructed_object_count,
        pipeline_enabled: true,
        pipeline_frame_count: ingest_report.pipeline_frame_count,
        pipeline_checkout_wait_ms: ingest_report.pipeline_checkout_wait_ms,
        pipeline_peak_pending_delta_count: ingest_report.pipeline_peak_pending_delta_count,
        pipeline_arena_spill_bytes: ingest_report.pipeline_arena_spill_bytes,
        target_bytes,
        rss_bytes,
    })
}

fn pipeline_queue_capacity() -> usize {
    std::env::var("FCL_PIPELINE_FRAME_QUEUE")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1024)
}

fn env_bool(name: &str) -> bool {
    std::env::var(name)
        .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
}

fn enforce_target_size_limit(
    max: Option<u64>,
    repo: &RepoLayout,
    operation: &'static str,
) -> Result<(), CloneError> {
    if max.is_none() {
        return Ok(());
    }
    enforce_optional_max_bytes(
        "FCL_MAX_TEMP_BYTES",
        max,
        directory_bytes(repo.root())?,
        operation,
    )
}

fn enforce_optional_max_bytes(
    env_name: &'static str,
    max: Option<u64>,
    actual: u64,
    operation: &'static str,
) -> Result<(), CloneError> {
    let Some(max) = max else {
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
