use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::Instant;

use url::Url;

use crate::checkout::materialize_default_branch;
use crate::compression::compression_backend;
use crate::error::CloneError;
use crate::metrics::{CloneReport, measure_ms};
use crate::pack::{
    CheckoutHint, ObjectId, PackIngestOptions, PackStorage, PipelineObjectStore,
    ingest_fetched_pack, ingest_pack_pipeline, ingest_scanned_pack,
};
use crate::protocol::{discover_remote, fetch_full_pack, fetch_full_pack_pipelined, http_client};
use crate::repo::{FinalizingRepo, RepoLayout};

type ProgressCallback<'a> = dyn Fn(CloneProgressEvent) + Sync + 'a;

#[derive(Debug)]
pub struct CloneRequest {
    pub url: String,
    pub target: Option<PathBuf>,
    pub pipeline: bool,
}

impl CloneRequest {
    pub const fn new(url: String, target: Option<PathBuf>) -> Self {
        Self {
            url,
            target,
            pipeline: true,
        }
    }

    #[must_use]
    pub const fn with_pipeline(mut self, pipeline: bool) -> Self {
        self.pipeline = pipeline;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloneProgressPhase {
    ResolvingRefs,
    FetchingPack,
    IndexingObjects,
    CheckingOut,
    Finalizing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloneProgressEvent {
    Started,
    PhaseStarted(CloneProgressPhase),
    PhaseCompleted(CloneProgressPhase),
    FetchProgress { bytes: u64 },
    Completed,
}

pub fn clone_repo(request: CloneRequest) -> Result<CloneReport, CloneError> {
    clone_repo_inner(request, None)
}

pub fn clone_repo_with_progress(
    request: CloneRequest,
    progress: impl Fn(CloneProgressEvent) + Sync,
) -> Result<CloneReport, CloneError> {
    clone_repo_inner(request, Some(&progress))
}

fn clone_repo_inner(
    request: CloneRequest,
    progress: Option<&ProgressCallback<'_>>,
) -> Result<CloneReport, CloneError> {
    emit_progress(progress, CloneProgressEvent::Started);
    let use_pipeline = pipeline_enabled_for_request(&request);
    if use_pipeline {
        let result = clone_repo_pipelined(request, progress);
        if result.is_ok() {
            emit_progress(progress, CloneProgressEvent::Completed);
        }
        result
    } else {
        let result = clone_repo_sequential(request, progress);
        if result.is_ok() {
            emit_progress(progress, CloneProgressEvent::Completed);
        }
        result
    }
}

fn pipeline_enabled_for_request(request: &CloneRequest) -> bool {
    pipeline_enabled(
        request.pipeline,
        env_bool("FCL_PIPELINE"),
        env_bool("FCL_DISABLE_PIPELINE"),
    )
}

const fn pipeline_enabled(
    request_pipeline: bool,
    force_pipeline: bool,
    disable_pipeline: bool,
) -> bool {
    (request_pipeline || force_pipeline) && !disable_pipeline
}

fn emit_progress(progress: Option<&ProgressCallback<'_>>, event: CloneProgressEvent) {
    if let Some(progress) = progress {
        progress(event);
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "top-level clone orchestration keeps phase timing and report assembly together"
)]
fn clone_repo_sequential(
    request: CloneRequest,
    progress: Option<&ProgressCallback<'_>>,
) -> Result<CloneReport, CloneError> {
    let start = Instant::now();
    let client = http_client()?;
    emit_progress(
        progress,
        CloneProgressEvent::PhaseStarted(CloneProgressPhase::ResolvingRefs),
    );
    let (remote, discovery_ms) = measure_ms(|| discover_remote(&client, &request.url));
    let remote = remote?;
    let refs = remote.refs.select_full_clone_universe();
    let ref_count = refs.len();
    let default_branch = resolve_default_branch(remote.refs.default_branch.as_deref(), &refs)?;
    let default_commit = ObjectId::parse_hex(&default_branch.oid)?;
    emit_progress(
        progress,
        CloneProgressEvent::PhaseCompleted(CloneProgressPhase::ResolvingRefs),
    );

    let target = match request.target {
        Some(target) => target,
        None => default_target_dir(&request.url)?,
    };
    let staged_repo = FinalizingRepo::create(&target)?;
    let repo = staged_repo.layout()?;
    let max_temp_bytes = optional_u64_env("FCL_MAX_TEMP_BYTES")?;

    emit_progress(
        progress,
        CloneProgressEvent::PhaseStarted(CloneProgressPhase::FetchingPack),
    );
    let fetch_progress = |bytes| {
        emit_progress(progress, CloneProgressEvent::FetchProgress { bytes });
    };
    let (fetched_pack, fetch_ms) = measure_ms(|| {
        fetch_full_pack(
            &client,
            &remote,
            &refs,
            &repo.pack_temp_path(),
            Some(&fetch_progress),
        )
    });
    let fetched_pack = fetched_pack?;
    let pack_bytes = fetched_pack.bytes;
    enforce_optional_max_bytes(
        "FCL_MAX_PACK_BYTES",
        optional_u64_env("FCL_MAX_PACK_BYTES")?,
        pack_bytes,
        "checking fetched pack size",
    )?;
    enforce_target_size_limit(max_temp_bytes, repo, "checking target size after fetch")?;
    emit_progress(
        progress,
        CloneProgressEvent::PhaseCompleted(CloneProgressPhase::FetchingPack),
    );
    let streaming_pack_scan = fetched_pack.scan.is_some();
    let ingest_options = PackIngestOptions {
        checkout_hint: Some(CheckoutHint { default_commit }),
    };
    emit_progress(
        progress,
        CloneProgressEvent::PhaseStarted(CloneProgressPhase::IndexingObjects),
    );
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
    emit_progress(
        progress,
        CloneProgressEvent::PhaseCompleted(CloneProgressPhase::IndexingObjects),
    );
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
    let pack_object_count = ingest_report.object_count;
    let pack_base_object_count = ingest_report.base_object_count;
    let pack_delta_count = ingest_report.delta_count;
    let pack_offset_delta_count = ingest_report.offset_delta_count;
    let pack_ref_delta_count = ingest_report.ref_delta_count;
    let pack_declared_inflated_bytes = ingest_report.declared_inflated_bytes;
    repo.write_initial_metadata(&remote, &refs, &default_branch.name)?;
    enforce_target_size_limit(max_temp_bytes, repo, "checking target size after ingest")?;
    emit_progress(
        progress,
        CloneProgressEvent::PhaseStarted(CloneProgressPhase::CheckingOut),
    );
    let (checkout, checkout_ms) =
        measure_ms(|| materialize_default_branch(repo, &pack_index, default_commit));
    let checkout = checkout?;
    emit_progress(
        progress,
        CloneProgressEvent::PhaseCompleted(CloneProgressPhase::CheckingOut),
    );
    let reconstructed_object_count = pack_index.reconstructed_object_count();
    emit_progress(
        progress,
        CloneProgressEvent::PhaseStarted(CloneProgressPhase::Finalizing),
    );
    let reported_before_finalize_ms = start.elapsed().as_millis();
    let finalize_start = Instant::now();
    let repo = staged_repo.commit()?;
    let publish_ms = finalize_start.elapsed().as_millis();
    let target_size_start = Instant::now();
    let target_bytes = directory_bytes(repo.root())?;
    let target_size_scan_ms = target_size_start.elapsed().as_millis();
    let rss_bytes = rss_bytes();
    enforce_optional_max_bytes(
        "FCL_MAX_TEMP_BYTES",
        max_temp_bytes,
        target_bytes,
        "checking final target size",
    )?;
    emit_progress(
        progress,
        CloneProgressEvent::PhaseCompleted(CloneProgressPhase::Finalizing),
    );
    let finalize_ms = finalize_start.elapsed().as_millis().max(publish_ms);
    let total_ms = start.elapsed().as_millis();
    let clone_wall_ms = total_ms;
    let clone_unreported_ms = clone_wall_ms.saturating_sub(reported_before_finalize_ms);
    let pack_receive_bytes_per_sec = bytes_per_second(pack_bytes, fetch_ms);

    Ok(CloneReport {
        compression_backend: compression_backend(),
        ref_count,
        pack_bytes,
        total_ms,
        clone_wall_ms,
        clone_unreported_ms,
        discovery_ms,
        fetch_ms,
        fetch_request_ms: fetched_pack.timings.request_ms,
        fetch_first_byte_ms: fetched_pack.timings.first_byte_ms,
        fetch_sideband_read_ms: fetched_pack.timings.sideband_read_ms,
        fetch_pack_write_ms: fetched_pack.timings.pack_write_ms,
        fetch_pack_flush_ms: fetched_pack.timings.pack_flush_ms,
        fetch_checksum_ms: fetched_pack.timings.checksum_ms,
        fetch_frame_send_wait_ms: None,
        pack_receive_bytes_per_sec,
        ingest_ms,
        pack_scan_ms,
        pack_resolve_ms,
        pack_idx_write_ms,
        pack_object_state_ms,
        pack_object_count,
        pack_base_object_count,
        pack_delta_count,
        pack_offset_delta_count,
        pack_ref_delta_count,
        pack_declared_inflated_bytes,
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
        pipeline_checkout_wait_count: None,
        pipeline_checkout_wait_max_ms: None,
        pipeline_peak_pending_delta_count: None,
        pipeline_resolver_wall_ms: None,
        pipeline_resolver_wait_for_frame_ms: None,
        pipeline_queue_peak_depth: None,
        pipeline_arena_spill_bytes: None,
        finalize_ms,
        target_size_scan_ms,
        target_bytes,
        rss_bytes,
    })
}

#[expect(
    clippy::too_many_lines,
    reason = "top-level pipeline orchestration keeps thread joins, timings, and report assembly together"
)]
fn clone_repo_pipelined(
    request: CloneRequest,
    progress: Option<&ProgressCallback<'_>>,
) -> Result<CloneReport, CloneError> {
    let start = Instant::now();
    let client = http_client()?;
    emit_progress(
        progress,
        CloneProgressEvent::PhaseStarted(CloneProgressPhase::ResolvingRefs),
    );
    let (remote, discovery_ms) = measure_ms(|| discover_remote(&client, &request.url));
    let remote = remote?;
    let refs = remote.refs.select_full_clone_universe();
    let ref_count = refs.len();
    let default_branch = resolve_default_branch(remote.refs.default_branch.as_deref(), &refs)?;
    let default_commit = ObjectId::parse_hex(&default_branch.oid)?;
    emit_progress(
        progress,
        CloneProgressEvent::PhaseCompleted(CloneProgressPhase::ResolvingRefs),
    );

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
    emit_progress(
        progress,
        CloneProgressEvent::PhaseStarted(CloneProgressPhase::FetchingPack),
    );
    let fetch_progress = |bytes| {
        emit_progress(progress, CloneProgressEvent::FetchProgress { bytes });
    };

    let resolver_store = store.clone();
    let resolver_pack_path = pack_path.clone();
    let resolver_index_path = index_path;
    let (fetched_pack, fetch_ms, ingest_report, checkout, checkout_ms) = thread::scope(|scope| {
        let fetch_progress = &fetch_progress;
        let fetch_thread = scope.spawn(move || {
            let fetch_start = Instant::now();
            fetch_full_pack_pipelined(
                &fetch_client,
                &fetch_remote,
                &fetch_refs,
                &fetch_pack_path,
                &sender,
                Some(fetch_progress),
            )
            .map(|fetched_pack| (fetched_pack, fetch_start.elapsed().as_millis()))
        });

        emit_progress(
            progress,
            CloneProgressEvent::PhaseStarted(CloneProgressPhase::IndexingObjects),
        );
        let resolver_thread = scope.spawn(move || {
            let result = ingest_pack_pipeline(
                &resolver_pack_path,
                &resolver_index_path,
                &receiver,
                resolver_store.clone(),
            );
            if let Err(error) = &result {
                resolver_store.fail("resolving pipeline pack", error.to_string());
            }
            result
        });

        let checkout_start = Instant::now();
        emit_progress(
            progress,
            CloneProgressEvent::PhaseStarted(CloneProgressPhase::CheckingOut),
        );
        let checkout_result = materialize_default_branch(repo, &store, default_commit);
        let checkout_ms = checkout_start.elapsed().as_millis();

        let fetch_result = fetch_thread
            .join()
            .map_err(|_| CloneError::RemoteDiscoveryFailed {
                url: remote.url.clone(),
                operation: "joining pipeline fetch thread",
                detail: "fetch thread panicked".to_owned(),
            })
            .and_then(|result| result);
        let resolver_result = resolver_thread
            .join()
            .map_err(|_| CloneError::PackIndexFailed {
                path: pack_path.clone(),
                operation: "joining pipeline resolver thread",
                detail: "resolver thread panicked".to_owned(),
            })
            .and_then(|result| result);
        let (fetched_pack, fetch_ms, ingest_report) = match (fetch_result, resolver_result) {
            (Ok((fetched_pack, fetch_ms)), Ok(ingest_report)) => {
                (fetched_pack, fetch_ms, ingest_report)
            }
            (Err(_fetch_error), Err(resolver_error)) => return Err(resolver_error),
            (Err(fetch_error), Ok(_)) => return Err(fetch_error),
            (Ok(_), Err(resolver_error)) => return Err(resolver_error),
        };
        emit_progress(
            progress,
            CloneProgressEvent::PhaseCompleted(CloneProgressPhase::FetchingPack),
        );
        let pack_bytes = fetched_pack.bytes;
        enforce_optional_max_bytes(
            "FCL_MAX_PACK_BYTES",
            optional_u64_env("FCL_MAX_PACK_BYTES")?,
            pack_bytes,
            "checking fetched pack size",
        )?;

        emit_progress(
            progress,
            CloneProgressEvent::PhaseCompleted(CloneProgressPhase::IndexingObjects),
        );
        let checkout = checkout_result?;
        emit_progress(
            progress,
            CloneProgressEvent::PhaseCompleted(CloneProgressPhase::CheckingOut),
        );

        Ok::<_, CloneError>((fetched_pack, fetch_ms, ingest_report, checkout, checkout_ms))
    })?;
    let pack_bytes = fetched_pack.bytes;
    let pack_index = ingest_report.index;
    emit_progress(
        progress,
        CloneProgressEvent::PhaseStarted(CloneProgressPhase::Finalizing),
    );
    enforce_target_size_limit(
        max_temp_bytes,
        repo,
        "checking target size after pipeline clone",
    )?;
    let reconstructed_object_count = pack_index.reconstructed_object_count();
    let reported_before_finalize_ms = start.elapsed().as_millis();
    let finalize_start = Instant::now();
    let repo = staged_repo.commit()?;
    let publish_ms = finalize_start.elapsed().as_millis();
    let target_size_start = Instant::now();
    let target_bytes = directory_bytes(repo.root())?;
    let target_size_scan_ms = target_size_start.elapsed().as_millis();
    let rss_bytes = rss_bytes();
    enforce_optional_max_bytes(
        "FCL_MAX_TEMP_BYTES",
        max_temp_bytes,
        target_bytes,
        "checking final target size",
    )?;
    emit_progress(
        progress,
        CloneProgressEvent::PhaseCompleted(CloneProgressPhase::Finalizing),
    );
    let finalize_ms = finalize_start.elapsed().as_millis().max(publish_ms);
    let total_ms = start.elapsed().as_millis();
    let clone_wall_ms = total_ms;
    let clone_unreported_ms = clone_wall_ms.saturating_sub(reported_before_finalize_ms);
    let pack_receive_bytes_per_sec = bytes_per_second(pack_bytes, fetch_ms);

    Ok(CloneReport {
        compression_backend: compression_backend(),
        ref_count,
        pack_bytes,
        total_ms,
        clone_wall_ms,
        clone_unreported_ms,
        discovery_ms,
        fetch_ms,
        fetch_request_ms: fetched_pack.timings.request_ms,
        fetch_first_byte_ms: fetched_pack.timings.first_byte_ms,
        fetch_sideband_read_ms: fetched_pack.timings.sideband_read_ms,
        fetch_pack_write_ms: fetched_pack.timings.pack_write_ms,
        fetch_pack_flush_ms: fetched_pack.timings.pack_flush_ms,
        fetch_checksum_ms: fetched_pack.timings.checksum_ms,
        fetch_frame_send_wait_ms: Some(fetched_pack.timings.frame_send_wait_ms),
        pack_receive_bytes_per_sec,
        ingest_ms: ingest_report.resolve_ms
            + ingest_report.idx_write_ms
            + ingest_report.object_state_ms,
        pack_scan_ms: ingest_report.scan_ms,
        pack_resolve_ms: ingest_report.resolve_ms,
        pack_idx_write_ms: ingest_report.idx_write_ms,
        pack_object_state_ms: ingest_report.object_state_ms,
        pack_object_count: ingest_report.object_count,
        pack_base_object_count: ingest_report.base_object_count,
        pack_delta_count: ingest_report.delta_count,
        pack_offset_delta_count: ingest_report.offset_delta_count,
        pack_ref_delta_count: ingest_report.ref_delta_count,
        pack_declared_inflated_bytes: ingest_report.declared_inflated_bytes,
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
        pipeline_checkout_wait_count: ingest_report.pipeline_checkout_wait_count,
        pipeline_checkout_wait_max_ms: ingest_report.pipeline_checkout_wait_max_ms,
        pipeline_peak_pending_delta_count: ingest_report.pipeline_peak_pending_delta_count,
        pipeline_resolver_wall_ms: ingest_report.pipeline_resolver_wall_ms,
        pipeline_resolver_wait_for_frame_ms: ingest_report.pipeline_resolver_wait_for_frame_ms,
        pipeline_queue_peak_depth: ingest_report.pipeline_queue_peak_depth,
        pipeline_arena_spill_bytes: ingest_report.pipeline_arena_spill_bytes,
        finalize_ms,
        target_size_scan_ms,
        target_bytes,
        rss_bytes,
    })
}

fn bytes_per_second(bytes: u64, ms: u128) -> u64 {
    if ms == 0 {
        return 0;
    }
    let bytes_per_second = u128::from(bytes).saturating_mul(1000) / ms;
    u64::try_from(bytes_per_second).unwrap_or(u64::MAX)
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

#[cfg(test)]
mod tests {
    use super::{CloneRequest, pipeline_enabled};

    #[test]
    fn clone_request_should_enable_pipeline_by_default() {
        let request = CloneRequest::new("https://example.com/repo.git".to_owned(), None);

        assert!(request.pipeline);
    }

    #[test]
    fn clone_request_should_allow_disabling_pipeline() {
        let request =
            CloneRequest::new("https://example.com/repo.git".to_owned(), None).with_pipeline(false);

        assert!(!request.pipeline);
    }

    #[test]
    fn pipeline_dispatch_should_use_pipeline_by_default() {
        assert!(pipeline_enabled(true, false, false));
    }

    #[test]
    fn pipeline_dispatch_should_allow_env_force_and_disable() {
        assert!(pipeline_enabled(false, true, false));
        assert!(!pipeline_enabled(true, true, true));
    }
}
