use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

use clap::{Parser, ValueEnum};
use fcl_core::{
    CloneError, CloneReport, CloneRequest, PackReplayInflatePolicy, PackReplayReport,
    PackReplayRequest, clone_repo, replay_pack,
};

#[derive(Debug, Parser)]
#[command(name = "fcl bench", about = "Benchmark fcl against git clone")]
pub struct BenchCli {
    #[arg(help = "Repository URL to benchmark.")]
    pub url: Option<String>,

    #[arg(
        long,
        help = "Replay a saved pack file locally instead of cloning a URL."
    )]
    pub pack: Option<PathBuf>,

    #[arg(long, default_value_t = 1, help = "Number of runs per tool.")]
    pub runs: usize,

    #[arg(long, help = "Also run stock git clone.")]
    pub compare_git: bool,

    #[arg(
        long,
        help = "In --pack mode, also run git index-pack as an explicit control."
    )]
    pub compare_index_pack: bool,

    #[arg(
        long,
        default_value = "1",
        help = "Comma-separated resolver worker counts for --pack mode."
    )]
    pub resolver_workers: String,

    #[arg(
        long,
        value_enum,
        default_value_t = BenchInflatePolicy::Current,
        help = "Inflate policy for --pack mode."
    )]
    pub inflate_policy: BenchInflatePolicy,

    #[arg(
        long,
        default_value = "1",
        help = "Comma-separated git pack.threads values for --compare-index-pack."
    )]
    pub git_pack_threads: String,

    #[arg(
        long,
        value_enum,
        default_value_t = BenchOrder::Alternate,
        help = "Order to run tools when comparing fcl with git."
    )]
    pub order: BenchOrder,

    #[arg(
        long,
        help = "Capture Git Trace2 perf output during timed git clone runs."
    )]
    pub git_trace2: bool,

    #[arg(
        long,
        help = "Validate each cloned repository with git fsck/status/diff."
    )]
    pub validate: bool,

    #[command(flatten)]
    pub pipeline: BenchPipeline,

    #[command(flatten)]
    pub output: BenchOutput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BenchInflatePolicy {
    Current,
    MetadataOnly,
    Inflate,
}

impl BenchInflatePolicy {
    const fn as_core(self) -> PackReplayInflatePolicy {
        match self {
            Self::Current => PackReplayInflatePolicy::Current,
            Self::MetadataOnly => PackReplayInflatePolicy::MetadataOnly,
            Self::Inflate => PackReplayInflatePolicy::Inflate,
        }
    }
}

#[derive(Debug, Parser)]
pub struct BenchPipeline {
    #[arg(
        long,
        help = "Disable the default streaming pipeline for fcl benchmark runs."
    )]
    pub no_pipeline: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BenchOrder {
    FclFirst,
    GitFirst,
    Alternate,
}

#[derive(Debug, Parser)]
pub struct BenchOutput {
    #[arg(long, help = "Emit CSV rows.")]
    pub csv: bool,

    #[arg(long, help = "Emit JSON lines.")]
    pub json: bool,
}

#[derive(Debug)]
struct BenchResult {
    tool: &'static str,
    compression_backend: &'static str,
    run: usize,
    total_ms: u128,
    clone_wall_ms: Option<u128>,
    clone_unreported_ms: Option<u128>,
    discovery_ms: Option<u128>,
    fetch_ms: Option<u128>,
    fetch_request_ms: Option<u128>,
    fetch_first_byte_ms: Option<u128>,
    fetch_sideband_read_ms: Option<u128>,
    fetch_pack_write_ms: Option<u128>,
    fetch_pack_flush_ms: Option<u128>,
    fetch_checksum_ms: Option<u128>,
    fetch_frame_send_wait_ms: Option<u128>,
    pack_receive_bytes_per_sec: Option<u64>,
    ingest_ms: Option<u128>,
    pack_scan_ms: Option<u128>,
    pack_resolve_ms: Option<u128>,
    pack_idx_write_ms: Option<u128>,
    pack_object_state_ms: Option<u128>,
    pack_object_count: Option<usize>,
    pack_base_object_count: Option<usize>,
    pack_delta_count: Option<usize>,
    pack_offset_delta_count: Option<usize>,
    pack_ref_delta_count: Option<usize>,
    pack_declared_inflated_bytes: Option<u64>,
    streaming_pack_scan: Option<bool>,
    checkout_ms: Option<u128>,
    checkout_manifest_ms: Option<u128>,
    checkout_dir_create_ms: Option<u128>,
    checkout_file_materialize_ms: Option<u128>,
    checkout_index_write_ms: Option<u128>,
    checkout_file_count: Option<usize>,
    checkout_dir_count: Option<usize>,
    checkout_blob_bytes: Option<u64>,
    pack_bytes: Option<u64>,
    ref_count: Option<usize>,
    retained_object_count: Option<usize>,
    retained_object_bytes: Option<usize>,
    spilled_object_count: Option<usize>,
    spilled_object_bytes: Option<usize>,
    checkout_needed_blob_count: Option<usize>,
    checkout_ready_blob_count: Option<usize>,
    checkout_ready_blob_bytes: Option<usize>,
    checkout_spilled_blob_count: Option<usize>,
    checkout_spilled_blob_bytes: Option<usize>,
    checkout_missing_blob_count: Option<usize>,
    reconstructed_object_count: Option<usize>,
    pipeline_enabled: bool,
    pipeline_frame_count: Option<usize>,
    pipeline_checkout_wait_ms: Option<u128>,
    pipeline_checkout_wait_count: Option<usize>,
    pipeline_checkout_wait_max_ms: Option<u128>,
    pipeline_peak_pending_delta_count: Option<usize>,
    pipeline_resolver_wall_ms: Option<u128>,
    pipeline_resolver_wait_for_frame_ms: Option<u128>,
    pipeline_queue_peak_depth: Option<usize>,
    pipeline_arena_spill_bytes: Option<u64>,
    finalize_ms: Option<u128>,
    target_size_scan_ms: Option<u128>,
    target_bytes: Option<u64>,
    rss_bytes: Option<u64>,
    git_trace_path: Option<PathBuf>,
    git_remote_ms: Option<u128>,
    git_index_pack_ms: Option<u128>,
    git_checkout_ms: Option<u128>,
    git_trace_parse_error: Option<String>,
    validated: bool,
}

pub fn run_bench(cli: &BenchCli) -> Result<(), CloneError> {
    if cli.runs == 0 {
        return Err(CloneError::BenchmarkFailed {
            operation: "parsing benchmark arguments",
            detail: "--runs must be greater than 0".to_owned(),
        });
    }

    if cli.pack.is_some() {
        return run_pack_bench(cli);
    }

    let url = clone_bench_url(cli)?;

    if cli.compare_index_pack {
        return Err(CloneError::BenchmarkFailed {
            operation: "parsing benchmark arguments",
            detail: "--compare-index-pack is only valid with --pack".to_owned(),
        });
    }

    if cli.output.csv {
        println!(
            "url,tool,compression_backend,run,total_ms,discovery_ms,fetch_ms,ingest_ms,pack_scan_ms,pack_resolve_ms,pack_idx_write_ms,pack_object_state_ms,streaming_pack_scan,checkout_ms,checkout_manifest_ms,checkout_dir_create_ms,checkout_file_materialize_ms,checkout_index_write_ms,checkout_file_count,checkout_dir_count,checkout_blob_bytes,pack_bytes,ref_count,retained_object_count,retained_object_bytes,spilled_object_count,spilled_object_bytes,checkout_needed_blob_count,checkout_ready_blob_count,checkout_ready_blob_bytes,checkout_spilled_blob_count,checkout_spilled_blob_bytes,checkout_missing_blob_count,reconstructed_object_count,pipeline_enabled,pipeline_frame_count,pipeline_checkout_wait_ms,pipeline_peak_pending_delta_count,pipeline_arena_spill_bytes,target_bytes,rss_bytes,git_trace_path,git_remote_ms,git_index_pack_ms,git_checkout_ms,git_trace_parse_error,clone_wall_ms,clone_unreported_ms,fetch_request_ms,fetch_first_byte_ms,fetch_sideband_read_ms,fetch_pack_write_ms,fetch_pack_flush_ms,fetch_checksum_ms,fetch_frame_send_wait_ms,pack_receive_bytes_per_sec,pack_object_count,pack_base_object_count,pack_delta_count,pack_offset_delta_count,pack_ref_delta_count,pack_declared_inflated_bytes,pipeline_checkout_wait_count,pipeline_checkout_wait_max_ms,pipeline_resolver_wall_ms,pipeline_resolver_wait_for_frame_ms,pipeline_queue_peak_depth,finalize_ms,target_size_scan_ms,validated"
        );
    }

    let mut results = Vec::new();
    for run in 1..=cli.runs {
        if cli.compare_git && run_git_first(cli.order, run) {
            run_git_bench(cli, url, run, &mut results)?;
        }

        run_fcl_bench(cli, url, run, &mut results)?;

        if cli.compare_git && !run_git_first(cli.order, run) {
            run_git_bench(cli, url, run, &mut results)?;
        }
    }

    print_summaries(cli, &results);

    Ok(())
}

fn clone_bench_url(cli: &BenchCli) -> Result<&str, CloneError> {
    cli.url
        .as_deref()
        .ok_or_else(|| CloneError::BenchmarkFailed {
            operation: "parsing benchmark arguments",
            detail: "fcl bench requires a repository URL unless --pack is provided".to_owned(),
        })
}

#[derive(Debug)]
struct PackBenchResult {
    tool: &'static str,
    run: usize,
    pack_path: PathBuf,
    compression_backend: &'static str,
    resolver_workers: Option<usize>,
    inflate_policy: Option<&'static str>,
    git_pack_threads: Option<usize>,
    total_ms: u128,
    scan_ms: Option<u128>,
    resolve_ms: Option<u128>,
    idx_write_ms: Option<u128>,
    eof_to_complete_ms: Option<u128>,
    object_count: Option<usize>,
    base_object_count: Option<usize>,
    delta_count: Option<usize>,
    offset_delta_count: Option<usize>,
    ref_delta_count: Option<usize>,
    declared_inflated_bytes: Option<u64>,
    reconstructed_object_count: Option<usize>,
}

impl PackBenchResult {
    fn from_fcl(run: usize, pack_path: &Path, report: &PackReplayReport) -> Self {
        Self {
            tool: "fcl-pack",
            run,
            pack_path: pack_path.to_owned(),
            compression_backend: report.compression_backend,
            resolver_workers: Some(report.resolver_workers),
            inflate_policy: Some(replay_policy_label(report.inflate_policy)),
            git_pack_threads: None,
            total_ms: report.scan_ms.saturating_add(report.eof_to_complete_ms),
            scan_ms: Some(report.scan_ms),
            resolve_ms: Some(report.resolve_ms),
            idx_write_ms: Some(report.idx_write_ms),
            eof_to_complete_ms: Some(report.eof_to_complete_ms),
            object_count: Some(report.object_count),
            base_object_count: Some(report.base_object_count),
            delta_count: Some(report.delta_count),
            offset_delta_count: Some(report.offset_delta_count),
            ref_delta_count: Some(report.ref_delta_count),
            declared_inflated_bytes: Some(report.declared_inflated_bytes),
            reconstructed_object_count: Some(report.reconstructed_object_count),
        }
    }

    fn from_git(run: usize, pack_path: &Path, git_pack_threads: usize, total_ms: u128) -> Self {
        Self {
            tool: "git-index-pack",
            run,
            pack_path: pack_path.to_owned(),
            compression_backend: "git",
            resolver_workers: None,
            inflate_policy: None,
            git_pack_threads: Some(git_pack_threads),
            total_ms,
            scan_ms: None,
            resolve_ms: None,
            idx_write_ms: None,
            eof_to_complete_ms: None,
            object_count: None,
            base_object_count: None,
            delta_count: None,
            offset_delta_count: None,
            ref_delta_count: None,
            declared_inflated_bytes: None,
            reconstructed_object_count: None,
        }
    }
}

fn run_pack_bench(cli: &BenchCli) -> Result<(), CloneError> {
    let pack_path = cli
        .pack
        .as_deref()
        .ok_or_else(|| CloneError::BenchmarkFailed {
            operation: "parsing benchmark arguments",
            detail: "--pack mode requires a pack path".to_owned(),
        })?;
    if cli.url.is_some() {
        return Err(CloneError::BenchmarkFailed {
            operation: "parsing benchmark arguments",
            detail: "--pack mode does not accept a repository URL".to_owned(),
        });
    }
    if cli.compare_git {
        return Err(CloneError::BenchmarkFailed {
            operation: "parsing benchmark arguments",
            detail: "--compare-git compares clone commands; use --compare-index-pack with --pack"
                .to_owned(),
        });
    }
    if cli.git_trace2 {
        return Err(CloneError::BenchmarkFailed {
            operation: "parsing benchmark arguments",
            detail: "--git-trace2 is only valid for URL clone benchmarks".to_owned(),
        });
    }
    if cli.pipeline.no_pipeline {
        return Err(CloneError::BenchmarkFailed {
            operation: "parsing benchmark arguments",
            detail: "--no-pipeline is only valid for URL clone benchmarks".to_owned(),
        });
    }
    if !pack_path.exists() {
        return Err(CloneError::BenchmarkFailed {
            operation: "opening pack benchmark input",
            detail: format!("pack file `{}` does not exist", pack_path.display()),
        });
    }
    let resolver_workers = parse_usize_list(&cli.resolver_workers, "--resolver-workers")?;
    let git_pack_threads = parse_usize_list(&cli.git_pack_threads, "--git-pack-threads")?;

    if cli.output.csv {
        print_pack_csv_header();
    }

    for run in 1..=cli.runs {
        let reports = replay_pack(&PackReplayRequest {
            pack_path: pack_path.to_owned(),
            resolver_workers: resolver_workers.clone(),
            inflate_policy: cli.inflate_policy.as_core(),
        })?;
        for report in &reports {
            print_pack_result(cli, &PackBenchResult::from_fcl(run, pack_path, report));
        }
        if cli.compare_index_pack {
            for &threads in &git_pack_threads {
                let total_ms = run_git_index_pack(pack_path, run, threads)?;
                print_pack_result(
                    cli,
                    &PackBenchResult::from_git(run, pack_path, threads, total_ms),
                );
            }
        }
    }

    Ok(())
}

fn parse_usize_list(raw: &str, flag: &'static str) -> Result<Vec<usize>, CloneError> {
    let mut values = Vec::new();
    for value in raw.split(',') {
        let value = value.trim();
        if value.is_empty() {
            return Err(CloneError::BenchmarkFailed {
                operation: "parsing benchmark arguments",
                detail: format!("{flag} contains an empty value"),
            });
        }
        let parsed = value
            .parse::<usize>()
            .map_err(|error| CloneError::BenchmarkFailed {
                operation: "parsing benchmark arguments",
                detail: format!("{flag} value `{value}` is not an unsigned integer: {error}"),
            })?;
        if parsed == 0 {
            return Err(CloneError::BenchmarkFailed {
                operation: "parsing benchmark arguments",
                detail: format!("{flag} values must be greater than 0"),
            });
        }
        values.push(parsed);
    }
    Ok(values)
}

fn run_git_index_pack(pack_path: &Path, run: usize, threads: usize) -> Result<u128, CloneError> {
    let input = fs::File::open(pack_path).map_err(|error| CloneError::BenchmarkFailed {
        operation: "opening pack for git index-pack",
        detail: format!("{}: {error}", pack_path.display()),
    })?;
    let idx_path = bench_target("git-index-pack", run).with_extension(format!("{threads}.idx"));
    if let Some(parent) = idx_path.parent() {
        fs::create_dir_all(parent).map_err(|error| CloneError::BenchmarkFailed {
            operation: "creating git index-pack output directory",
            detail: format!("{}: {error}", parent.display()),
        })?;
    }
    let _ = fs::remove_file(&idx_path);
    let start = Instant::now();
    let output = Command::new("git")
        .arg("-c")
        .arg(format!("pack.threads={threads}"))
        .arg("index-pack")
        .arg("--stdin")
        .arg("-o")
        .arg(&idx_path)
        .stdin(Stdio::from(input))
        .output()
        .map_err(|error| CloneError::BenchmarkFailed {
            operation: "running git index-pack",
            detail: error.to_string(),
        })?;
    let total_ms = start.elapsed().as_millis();
    let _ = fs::remove_file(&idx_path);
    if !output.status.success() {
        return Err(CloneError::BenchmarkFailed {
            operation: "running git index-pack",
            detail: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    Ok(total_ms)
}

fn replay_policy_label(policy: PackReplayInflatePolicy) -> &'static str {
    match policy {
        PackReplayInflatePolicy::Current => "current",
        PackReplayInflatePolicy::MetadataOnly => "metadata-only",
        PackReplayInflatePolicy::Inflate => "inflate",
    }
}

fn print_pack_csv_header() {
    println!(
        "mode,pack_path,tool,compression_backend,run,total_ms,resolver_workers,inflate_policy,git_pack_threads,scan_ms,resolve_ms,idx_write_ms,eof_to_complete_ms,object_count,base_object_count,delta_count,offset_delta_count,ref_delta_count,declared_inflated_bytes,reconstructed_object_count"
    );
}

fn print_pack_result(cli: &BenchCli, result: &PackBenchResult) {
    if cli.output.json {
        print_pack_json_result(result);
    } else if cli.output.csv {
        print_pack_csv_result(result);
    } else {
        print_pack_plain_result(result);
    }
}

fn print_pack_json_result(result: &PackBenchResult) {
    println!(
        "{{\"mode\":\"pack\",\"pack_path\":\"{}\",\"tool\":\"{}\",\"compression_backend\":\"{}\",\"run\":{},\"total_ms\":{},\"resolver_workers\":{},\"inflate_policy\":{},\"git_pack_threads\":{},\"scan_ms\":{},\"resolve_ms\":{},\"idx_write_ms\":{},\"eof_to_complete_ms\":{},\"object_count\":{},\"base_object_count\":{},\"delta_count\":{},\"offset_delta_count\":{},\"ref_delta_count\":{},\"declared_inflated_bytes\":{},\"reconstructed_object_count\":{}}}",
        escape_json(&result.pack_path.display().to_string()),
        result.tool,
        escape_json(result.compression_backend),
        result.run,
        result.total_ms,
        option_usize(result.resolver_workers),
        option_string(result.inflate_policy),
        option_usize(result.git_pack_threads),
        option_u128(result.scan_ms),
        option_u128(result.resolve_ms),
        option_u128(result.idx_write_ms),
        option_u128(result.eof_to_complete_ms),
        option_usize(result.object_count),
        option_usize(result.base_object_count),
        option_usize(result.delta_count),
        option_usize(result.offset_delta_count),
        option_usize(result.ref_delta_count),
        option_u64(result.declared_inflated_bytes),
        option_usize(result.reconstructed_object_count)
    );
}

fn print_pack_csv_result(result: &PackBenchResult) {
    let fields = vec![
        "pack".to_owned(),
        csv_string(Some(&result.pack_path.display().to_string())),
        result.tool.to_owned(),
        result.compression_backend.to_owned(),
        result.run.to_string(),
        result.total_ms.to_string(),
        csv_usize(result.resolver_workers),
        csv_string(result.inflate_policy),
        csv_usize(result.git_pack_threads),
        csv_u128(result.scan_ms),
        csv_u128(result.resolve_ms),
        csv_u128(result.idx_write_ms),
        csv_u128(result.eof_to_complete_ms),
        csv_usize(result.object_count),
        csv_usize(result.base_object_count),
        csv_usize(result.delta_count),
        csv_usize(result.offset_delta_count),
        csv_usize(result.ref_delta_count),
        csv_u64(result.declared_inflated_bytes),
        csv_usize(result.reconstructed_object_count),
    ];
    println!("{}", fields.join(","));
}

fn print_pack_plain_result(result: &PackBenchResult) {
    println!(
        "{} run {} backend={}: pack={} total={}ms resolver_workers={} inflate_policy={} git_pack_threads={} scan={} resolve={} idx_write={} eof_to_complete={} objects={} bases={} deltas={} ofs_deltas={} ref_deltas={} inflated_bytes={} reconstructed_objects={}",
        result.tool,
        result.run,
        result.compression_backend,
        result.pack_path.display(),
        result.total_ms,
        usize_or_dash(result.resolver_workers),
        string_or_dash(result.inflate_policy),
        usize_or_dash(result.git_pack_threads),
        ms_or_dash(result.scan_ms),
        ms_or_dash(result.resolve_ms),
        ms_or_dash(result.idx_write_ms),
        ms_or_dash(result.eof_to_complete_ms),
        usize_or_dash(result.object_count),
        usize_or_dash(result.base_object_count),
        usize_or_dash(result.delta_count),
        usize_or_dash(result.offset_delta_count),
        usize_or_dash(result.ref_delta_count),
        u64_or_dash(result.declared_inflated_bytes),
        usize_or_dash(result.reconstructed_object_count)
    );
}

const fn run_git_first(order: BenchOrder, run: usize) -> bool {
    match order {
        BenchOrder::FclFirst => false,
        BenchOrder::GitFirst => true,
        BenchOrder::Alternate => run.is_multiple_of(2),
    }
}

fn run_fcl_bench(
    cli: &BenchCli,
    url: &str,
    run: usize,
    results: &mut Vec<BenchResult>,
) -> Result<(), CloneError> {
    let target = bench_target("fcl", run);
    remove_target(&target)?;
    let report = clone_repo(
        CloneRequest::new(url.to_owned(), Some(target.clone()))
            .with_pipeline(!cli.pipeline.no_pipeline),
    )?;
    if cli.validate {
        validate_repo(&target)?;
    }
    let result = BenchResult::from_fcl(run, &report, cli.validate);
    print_result(cli, url, &result);
    results.push(result);
    Ok(())
}

fn run_git_bench(
    cli: &BenchCli,
    url: &str,
    run: usize,
    results: &mut Vec<BenchResult>,
) -> Result<(), CloneError> {
    let target = bench_target("git", run);
    remove_target(&target)?;
    let trace_path = cli.git_trace2.then(|| git_trace_path(run));
    if let Some(trace_path) = &trace_path
        && trace_path.exists()
    {
        fs::remove_file(trace_path).map_err(|error| CloneError::BenchmarkFailed {
            operation: "removing previous git trace",
            detail: format!("{}: {error}", trace_path.display()),
        })?;
    }
    let start = Instant::now();
    run_git_clone(url, &target, trace_path.as_deref())?;
    let total_ms = start.elapsed().as_millis();
    if cli.validate {
        validate_repo(&target)?;
    }
    let git_trace = match trace_path.as_deref().map(parse_git_trace2).transpose() {
        Ok(Some(summary)) => summary,
        Ok(None) => GitTraceSummary::default(),
        Err(error) => GitTraceSummary::parse_error(error.to_string()),
    };
    let result = BenchResult {
        tool: "git",
        compression_backend: "git",
        run,
        total_ms,
        clone_wall_ms: None,
        clone_unreported_ms: None,
        discovery_ms: None,
        fetch_ms: None,
        fetch_request_ms: None,
        fetch_first_byte_ms: None,
        fetch_sideband_read_ms: None,
        fetch_pack_write_ms: None,
        fetch_pack_flush_ms: None,
        fetch_checksum_ms: None,
        fetch_frame_send_wait_ms: None,
        pack_receive_bytes_per_sec: None,
        ingest_ms: None,
        pack_scan_ms: None,
        pack_resolve_ms: None,
        pack_idx_write_ms: None,
        pack_object_state_ms: None,
        pack_object_count: None,
        pack_base_object_count: None,
        pack_delta_count: None,
        pack_offset_delta_count: None,
        pack_ref_delta_count: None,
        pack_declared_inflated_bytes: None,
        streaming_pack_scan: None,
        checkout_ms: None,
        checkout_manifest_ms: None,
        checkout_dir_create_ms: None,
        checkout_file_materialize_ms: None,
        checkout_index_write_ms: None,
        checkout_file_count: None,
        checkout_dir_count: None,
        checkout_blob_bytes: None,
        pack_bytes: None,
        ref_count: None,
        retained_object_count: None,
        retained_object_bytes: None,
        spilled_object_count: None,
        spilled_object_bytes: None,
        checkout_needed_blob_count: None,
        checkout_ready_blob_count: None,
        checkout_ready_blob_bytes: None,
        checkout_spilled_blob_count: None,
        checkout_spilled_blob_bytes: None,
        checkout_missing_blob_count: None,
        reconstructed_object_count: None,
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
        finalize_ms: None,
        target_size_scan_ms: None,
        target_bytes: target_size(&target).ok(),
        rss_bytes: None,
        git_trace_path: trace_path,
        git_remote_ms: git_trace.remote_ms,
        git_index_pack_ms: git_trace.index_pack_ms,
        git_checkout_ms: git_trace.checkout_ms,
        git_trace_parse_error: git_trace.parse_error,
        validated: cli.validate,
    };
    print_result(cli, url, &result);
    results.push(result);
    Ok(())
}

impl BenchResult {
    const fn from_fcl(run: usize, report: &CloneReport, validated: bool) -> Self {
        Self {
            tool: "fcl",
            compression_backend: report.compression_backend,
            run,
            total_ms: report.total_ms,
            clone_wall_ms: Some(report.clone_wall_ms),
            clone_unreported_ms: Some(report.clone_unreported_ms),
            discovery_ms: Some(report.discovery_ms),
            fetch_ms: Some(report.fetch_ms),
            fetch_request_ms: Some(report.fetch_request_ms),
            fetch_first_byte_ms: Some(report.fetch_first_byte_ms),
            fetch_sideband_read_ms: Some(report.fetch_sideband_read_ms),
            fetch_pack_write_ms: Some(report.fetch_pack_write_ms),
            fetch_pack_flush_ms: Some(report.fetch_pack_flush_ms),
            fetch_checksum_ms: Some(report.fetch_checksum_ms),
            fetch_frame_send_wait_ms: report.fetch_frame_send_wait_ms,
            pack_receive_bytes_per_sec: Some(report.pack_receive_bytes_per_sec),
            ingest_ms: Some(report.ingest_ms),
            pack_scan_ms: Some(report.pack_scan_ms),
            pack_resolve_ms: Some(report.pack_resolve_ms),
            pack_idx_write_ms: Some(report.pack_idx_write_ms),
            pack_object_state_ms: Some(report.pack_object_state_ms),
            pack_object_count: Some(report.pack_object_count),
            pack_base_object_count: Some(report.pack_base_object_count),
            pack_delta_count: Some(report.pack_delta_count),
            pack_offset_delta_count: Some(report.pack_offset_delta_count),
            pack_ref_delta_count: Some(report.pack_ref_delta_count),
            pack_declared_inflated_bytes: Some(report.pack_declared_inflated_bytes),
            streaming_pack_scan: Some(report.streaming_pack_scan),
            checkout_ms: Some(report.checkout_ms),
            checkout_manifest_ms: Some(report.checkout_manifest_ms),
            checkout_dir_create_ms: Some(report.checkout_dir_create_ms),
            checkout_file_materialize_ms: Some(report.checkout_file_materialize_ms),
            checkout_index_write_ms: Some(report.checkout_index_write_ms),
            checkout_file_count: Some(report.checkout_file_count),
            checkout_dir_count: Some(report.checkout_dir_count),
            checkout_blob_bytes: Some(report.checkout_blob_bytes),
            pack_bytes: Some(report.pack_bytes),
            ref_count: Some(report.ref_count),
            retained_object_count: Some(report.retained_object_count),
            retained_object_bytes: Some(report.retained_object_bytes),
            spilled_object_count: Some(report.spilled_object_count),
            spilled_object_bytes: Some(report.spilled_object_bytes),
            checkout_needed_blob_count: Some(report.checkout_needed_blob_count),
            checkout_ready_blob_count: Some(report.checkout_ready_blob_count),
            checkout_ready_blob_bytes: Some(report.checkout_ready_blob_bytes),
            checkout_spilled_blob_count: Some(report.checkout_spilled_blob_count),
            checkout_spilled_blob_bytes: Some(report.checkout_spilled_blob_bytes),
            checkout_missing_blob_count: Some(report.checkout_missing_blob_count),
            reconstructed_object_count: Some(report.reconstructed_object_count),
            pipeline_enabled: report.pipeline_enabled,
            pipeline_frame_count: report.pipeline_frame_count,
            pipeline_checkout_wait_ms: report.pipeline_checkout_wait_ms,
            pipeline_checkout_wait_count: report.pipeline_checkout_wait_count,
            pipeline_checkout_wait_max_ms: report.pipeline_checkout_wait_max_ms,
            pipeline_peak_pending_delta_count: report.pipeline_peak_pending_delta_count,
            pipeline_resolver_wall_ms: report.pipeline_resolver_wall_ms,
            pipeline_resolver_wait_for_frame_ms: report.pipeline_resolver_wait_for_frame_ms,
            pipeline_queue_peak_depth: report.pipeline_queue_peak_depth,
            pipeline_arena_spill_bytes: report.pipeline_arena_spill_bytes,
            finalize_ms: Some(report.finalize_ms),
            target_size_scan_ms: Some(report.target_size_scan_ms),
            target_bytes: Some(report.target_bytes),
            rss_bytes: report.rss_bytes,
            git_trace_path: None,
            git_remote_ms: None,
            git_index_pack_ms: None,
            git_checkout_ms: None,
            git_trace_parse_error: None,
            validated,
        }
    }
}

fn target_size(path: &Path) -> Result<u64, CloneError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| CloneError::BenchmarkFailed {
        operation: "reading benchmark target metadata",
        detail: format!("{}: {error}", path.display()),
    })?;
    if metadata.is_file() || metadata.file_type().is_symlink() {
        return Ok(metadata.len());
    }
    let mut total = 0u64;
    for entry in fs::read_dir(path).map_err(|error| CloneError::BenchmarkFailed {
        operation: "reading benchmark target directory",
        detail: format!("{}: {error}", path.display()),
    })? {
        let entry = entry.map_err(|error| CloneError::BenchmarkFailed {
            operation: "reading benchmark target directory entry",
            detail: format!("{}: {error}", path.display()),
        })?;
        total = total.saturating_add(target_size(&entry.path())?);
    }
    Ok(total)
}

fn bench_target(tool: &str, run: usize) -> PathBuf {
    std::env::temp_dir()
        .join("fcl-bench")
        .join(format!("{tool}-{run}"))
}

fn remove_target(target: &Path) -> Result<(), CloneError> {
    if target.exists() {
        fs::remove_dir_all(target).map_err(|error| CloneError::BenchmarkFailed {
            operation: "removing previous benchmark target",
            detail: format!("{}: {error}", target.display()),
        })?;
    }
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(|error| CloneError::BenchmarkFailed {
            operation: "creating benchmark parent directory",
            detail: format!("{}: {error}", parent.display()),
        })?;
    }
    Ok(())
}

fn git_trace_path(run: usize) -> PathBuf {
    std::env::temp_dir()
        .join("fcl-bench")
        .join(format!("git-trace-{run}.txt"))
}

fn run_git_clone(url: &str, target: &Path, trace_path: Option<&Path>) -> Result<(), CloneError> {
    let mut command = Command::new("git");
    if let Some(trace_path) = trace_path {
        command.env("GIT_TRACE2_PERF", trace_path);
    }
    let output = command
        .arg("clone")
        .arg(url)
        .arg(target)
        .output()
        .map_err(|error| CloneError::BenchmarkFailed {
            operation: "running git clone",
            detail: error.to_string(),
        })?;
    if let Some(trace_path) = trace_path
        && !trace_path.exists()
    {
        fs::write(trace_path, &output.stderr).map_err(|error| CloneError::BenchmarkFailed {
            operation: "writing git performance trace",
            detail: format!("{}: {error}", trace_path.display()),
        })?;
    }
    if !output.status.success() {
        return Err(CloneError::BenchmarkFailed {
            operation: "running git clone",
            detail: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    Ok(())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct GitTraceSummary {
    remote_ms: Option<u128>,
    index_pack_ms: Option<u128>,
    checkout_ms: Option<u128>,
    parse_error: Option<String>,
}

impl GitTraceSummary {
    fn parse_error(error: String) -> Self {
        Self {
            parse_error: Some(error),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GitChildPhase {
    Remote,
    IndexPack,
}

fn parse_git_trace2(path: &Path) -> Result<GitTraceSummary, CloneError> {
    let content = fs::read_to_string(path).map_err(|error| CloneError::BenchmarkFailed {
        operation: "reading git Trace2 output",
        detail: format!("{}: {error}", path.display()),
    })?;
    Ok(parse_git_trace2_content(&content))
}

fn parse_git_trace2_content(content: &str) -> GitTraceSummary {
    let mut children = HashMap::<String, GitChildPhase>::new();
    let mut remote_ms = 0u128;
    let mut index_pack_ms = 0u128;
    let mut checkout_ms = 0u128;
    let mut saw_remote = false;
    let mut saw_index_pack = false;
    let mut saw_checkout = false;
    let mut malformed_lines = 0usize;

    for line in content.lines() {
        let Some(record) = Trace2Record::parse(line) else {
            malformed_lines += 1;
            continue;
        };
        if record.session != "d0" {
            continue;
        }
        match record.event {
            "child_start" => {
                if let Some(child_id) = trace_child_id(record.message)
                    && let Some(phase) = classify_git_child(record.message)
                {
                    children.insert(child_id.to_owned(), phase);
                }
            }
            "child_exit" => {
                if let Some(child_id) = trace_child_id(record.message)
                    && let Some(phase) = children.remove(child_id)
                    && let Some(duration_ms) = trace_seconds_to_ms(record.duration_seconds)
                {
                    match phase {
                        GitChildPhase::Remote => {
                            remote_ms += duration_ms;
                            saw_remote = true;
                        }
                        GitChildPhase::IndexPack => {
                            index_pack_ms += duration_ms;
                            saw_index_pack = true;
                        }
                    }
                }
            }
            "region_leave" => {
                if is_checkout_region(record.category, record.message)
                    && let Some(duration_ms) = trace_seconds_to_ms(record.duration_seconds)
                {
                    checkout_ms += duration_ms;
                    saw_checkout = true;
                }
            }
            _ => {}
        }
    }

    GitTraceSummary {
        remote_ms: saw_remote.then_some(remote_ms),
        index_pack_ms: saw_index_pack.then_some(index_pack_ms),
        checkout_ms: saw_checkout.then_some(checkout_ms),
        parse_error: (malformed_lines > 0)
            .then(|| format!("ignored {malformed_lines} malformed Trace2 lines")),
    }
}

#[derive(Debug)]
struct Trace2Record<'a> {
    session: &'a str,
    event: &'a str,
    duration_seconds: &'a str,
    category: &'a str,
    message: &'a str,
}

impl<'a> Trace2Record<'a> {
    fn parse(line: &'a str) -> Option<Self> {
        let fields = line.split('|').map(str::trim).collect::<Vec<_>>();
        if fields.len() < 9 {
            return None;
        }
        Some(Self {
            session: fields[1],
            event: fields[3],
            duration_seconds: fields[6],
            category: fields[7],
            message: fields[8],
        })
    }
}

fn classify_git_child(message: &str) -> Option<GitChildPhase> {
    if message.contains("remote-https") || message.contains("git-remote-https") {
        Some(GitChildPhase::Remote)
    } else if message.contains("index-pack") {
        Some(GitChildPhase::IndexPack)
    } else {
        None
    }
}

fn trace_child_id(message: &str) -> Option<&str> {
    let start = message.find("[ch")? + 1;
    let end = message[start..].find(']')? + start;
    Some(&message[start..end])
}

fn trace_seconds_to_ms(raw: &str) -> Option<u128> {
    let raw = raw.trim();
    if raw.is_empty() || raw.starts_with('-') {
        return None;
    }
    let (seconds, fraction) = raw.split_once('.').unwrap_or((raw, ""));
    let seconds = seconds.parse::<u128>().ok()?;
    let mut fraction_digits = fraction
        .chars()
        .take(4)
        .map(|character| character.to_digit(10))
        .collect::<Option<Vec<_>>>()?;
    while fraction_digits.len() < 4 {
        fraction_digits.push(0);
    }
    let milliseconds = u128::from(fraction_digits[0]) * 100
        + u128::from(fraction_digits[1]) * 10
        + u128::from(fraction_digits[2]);
    let rounded_milliseconds = milliseconds + u128::from(fraction_digits[3] >= 5);
    seconds.checked_mul(1000)?.checked_add(rounded_milliseconds)
}

fn is_checkout_region(category: &str, message: &str) -> bool {
    let category = category.to_ascii_lowercase();
    let message = message.to_ascii_lowercase();
    category.contains("unpack_trees")
        || category.contains("checkout")
        || message.contains("unpack_trees")
        || message.contains("checkout")
        || message.contains("updating files")
        || message.contains("filtering content")
}

fn validate_repo(target: &Path) -> Result<(), CloneError> {
    run_git_validation(target, &["fsck", "--full"])?;
    run_git_validation(target, &["status", "--short"])?;
    run_git_validation(target, &["diff", "--exit-code", "HEAD"])
}

fn run_git_validation(target: &Path, args: &[&str]) -> Result<(), CloneError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(target)
        .args(args)
        .output()
        .map_err(|error| CloneError::BenchmarkFailed {
            operation: "running git validation",
            detail: error.to_string(),
        })?;
    if !output.status.success() || !output.stdout.is_empty() {
        return Err(CloneError::BenchmarkFailed {
            operation: "validating benchmark clone",
            detail: format!(
                "git {} failed for {}: stdout=`{}` stderr=`{}`",
                args.join(" "),
                target.display(),
                String::from_utf8_lossy(&output.stdout).trim(),
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        });
    }
    Ok(())
}

fn print_result(cli: &BenchCli, url: &str, result: &BenchResult) {
    if cli.output.json {
        print_json_result(url, result);
    } else if cli.output.csv {
        print_csv_result(url, result);
    } else {
        print_plain_result(result);
    }
}

fn print_json_result(url: &str, result: &BenchResult) {
    println!(
        "{{\"url\":\"{}\",\"tool\":\"{}\",\"compression_backend\":\"{}\",\"run\":{},\"total_ms\":{},\"discovery_ms\":{},\"fetch_ms\":{},\"ingest_ms\":{},\"pack_scan_ms\":{},\"pack_resolve_ms\":{},\"pack_idx_write_ms\":{},\"pack_object_state_ms\":{},\"streaming_pack_scan\":{},\"checkout_ms\":{},\"checkout_manifest_ms\":{},\"checkout_dir_create_ms\":{},\"checkout_file_materialize_ms\":{},\"checkout_index_write_ms\":{},\"checkout_file_count\":{},\"checkout_dir_count\":{},\"checkout_blob_bytes\":{},\"pack_bytes\":{},\"ref_count\":{},\"retained_object_count\":{},\"retained_object_bytes\":{},\"spilled_object_count\":{},\"spilled_object_bytes\":{},\"checkout_needed_blob_count\":{},\"checkout_ready_blob_count\":{},\"checkout_ready_blob_bytes\":{},\"checkout_spilled_blob_count\":{},\"checkout_spilled_blob_bytes\":{},\"checkout_missing_blob_count\":{},\"reconstructed_object_count\":{},\"pipeline_enabled\":{},\"pipeline_frame_count\":{},\"pipeline_checkout_wait_ms\":{},\"pipeline_peak_pending_delta_count\":{},\"pipeline_arena_spill_bytes\":{},\"target_bytes\":{},\"rss_bytes\":{},\"git_trace_path\":{},\"git_remote_ms\":{},\"git_index_pack_ms\":{},\"git_checkout_ms\":{},\"git_trace_parse_error\":{},\"clone_wall_ms\":{},\"clone_unreported_ms\":{},\"fetch_request_ms\":{},\"fetch_first_byte_ms\":{},\"fetch_sideband_read_ms\":{},\"fetch_pack_write_ms\":{},\"fetch_pack_flush_ms\":{},\"fetch_checksum_ms\":{},\"fetch_frame_send_wait_ms\":{},\"pack_receive_bytes_per_sec\":{},\"pack_object_count\":{},\"pack_base_object_count\":{},\"pack_delta_count\":{},\"pack_offset_delta_count\":{},\"pack_ref_delta_count\":{},\"pack_declared_inflated_bytes\":{},\"pipeline_checkout_wait_count\":{},\"pipeline_checkout_wait_max_ms\":{},\"pipeline_resolver_wall_ms\":{},\"pipeline_resolver_wait_for_frame_ms\":{},\"pipeline_queue_peak_depth\":{},\"finalize_ms\":{},\"target_size_scan_ms\":{},\"validated\":{}}}",
        escape_json(url),
        result.tool,
        escape_json(result.compression_backend),
        result.run,
        result.total_ms,
        option_u128(result.discovery_ms),
        option_u128(result.fetch_ms),
        option_u128(result.ingest_ms),
        option_u128(result.pack_scan_ms),
        option_u128(result.pack_resolve_ms),
        option_u128(result.pack_idx_write_ms),
        option_u128(result.pack_object_state_ms),
        option_bool(result.streaming_pack_scan),
        option_u128(result.checkout_ms),
        option_u128(result.checkout_manifest_ms),
        option_u128(result.checkout_dir_create_ms),
        option_u128(result.checkout_file_materialize_ms),
        option_u128(result.checkout_index_write_ms),
        option_usize(result.checkout_file_count),
        option_usize(result.checkout_dir_count),
        option_u64(result.checkout_blob_bytes),
        option_u64(result.pack_bytes),
        option_usize(result.ref_count),
        option_usize(result.retained_object_count),
        option_usize(result.retained_object_bytes),
        option_usize(result.spilled_object_count),
        option_usize(result.spilled_object_bytes),
        option_usize(result.checkout_needed_blob_count),
        option_usize(result.checkout_ready_blob_count),
        option_usize(result.checkout_ready_blob_bytes),
        option_usize(result.checkout_spilled_blob_count),
        option_usize(result.checkout_spilled_blob_bytes),
        option_usize(result.checkout_missing_blob_count),
        option_usize(result.reconstructed_object_count),
        result.pipeline_enabled,
        option_usize(result.pipeline_frame_count),
        option_u128(result.pipeline_checkout_wait_ms),
        option_usize(result.pipeline_peak_pending_delta_count),
        option_u64(result.pipeline_arena_spill_bytes),
        option_u64(result.target_bytes),
        option_u64(result.rss_bytes),
        option_path(result.git_trace_path.as_deref()),
        option_u128(result.git_remote_ms),
        option_u128(result.git_index_pack_ms),
        option_u128(result.git_checkout_ms),
        option_string(result.git_trace_parse_error.as_deref()),
        option_u128(result.clone_wall_ms),
        option_u128(result.clone_unreported_ms),
        option_u128(result.fetch_request_ms),
        option_u128(result.fetch_first_byte_ms),
        option_u128(result.fetch_sideband_read_ms),
        option_u128(result.fetch_pack_write_ms),
        option_u128(result.fetch_pack_flush_ms),
        option_u128(result.fetch_checksum_ms),
        option_u128(result.fetch_frame_send_wait_ms),
        option_u64(result.pack_receive_bytes_per_sec),
        option_usize(result.pack_object_count),
        option_usize(result.pack_base_object_count),
        option_usize(result.pack_delta_count),
        option_usize(result.pack_offset_delta_count),
        option_usize(result.pack_ref_delta_count),
        option_u64(result.pack_declared_inflated_bytes),
        option_usize(result.pipeline_checkout_wait_count),
        option_u128(result.pipeline_checkout_wait_max_ms),
        option_u128(result.pipeline_resolver_wall_ms),
        option_u128(result.pipeline_resolver_wait_for_frame_ms),
        option_usize(result.pipeline_queue_peak_depth),
        option_u128(result.finalize_ms),
        option_u128(result.target_size_scan_ms),
        result.validated
    );
}

fn print_csv_result(url: &str, result: &BenchResult) {
    let fields = vec![
        url.to_owned(),
        result.tool.to_owned(),
        result.compression_backend.to_owned(),
        result.run.to_string(),
        result.total_ms.to_string(),
        csv_u128(result.discovery_ms),
        csv_u128(result.fetch_ms),
        csv_u128(result.ingest_ms),
        csv_u128(result.pack_scan_ms),
        csv_u128(result.pack_resolve_ms),
        csv_u128(result.pack_idx_write_ms),
        csv_u128(result.pack_object_state_ms),
        csv_bool(result.streaming_pack_scan),
        csv_u128(result.checkout_ms),
        csv_u128(result.checkout_manifest_ms),
        csv_u128(result.checkout_dir_create_ms),
        csv_u128(result.checkout_file_materialize_ms),
        csv_u128(result.checkout_index_write_ms),
        csv_usize(result.checkout_file_count),
        csv_usize(result.checkout_dir_count),
        csv_u64(result.checkout_blob_bytes),
        csv_u64(result.pack_bytes),
        csv_usize(result.ref_count),
        csv_usize(result.retained_object_count),
        csv_usize(result.retained_object_bytes),
        csv_usize(result.spilled_object_count),
        csv_usize(result.spilled_object_bytes),
        csv_usize(result.checkout_needed_blob_count),
        csv_usize(result.checkout_ready_blob_count),
        csv_usize(result.checkout_ready_blob_bytes),
        csv_usize(result.checkout_spilled_blob_count),
        csv_usize(result.checkout_spilled_blob_bytes),
        csv_usize(result.checkout_missing_blob_count),
        csv_usize(result.reconstructed_object_count),
        result.pipeline_enabled.to_string(),
        csv_usize(result.pipeline_frame_count),
        csv_u128(result.pipeline_checkout_wait_ms),
        csv_usize(result.pipeline_peak_pending_delta_count),
        csv_u64(result.pipeline_arena_spill_bytes),
        csv_u64(result.target_bytes),
        csv_u64(result.rss_bytes),
        csv_path(result.git_trace_path.as_deref()),
        csv_u128(result.git_remote_ms),
        csv_u128(result.git_index_pack_ms),
        csv_u128(result.git_checkout_ms),
        csv_string(result.git_trace_parse_error.as_deref()),
        csv_u128(result.clone_wall_ms),
        csv_u128(result.clone_unreported_ms),
        csv_u128(result.fetch_request_ms),
        csv_u128(result.fetch_first_byte_ms),
        csv_u128(result.fetch_sideband_read_ms),
        csv_u128(result.fetch_pack_write_ms),
        csv_u128(result.fetch_pack_flush_ms),
        csv_u128(result.fetch_checksum_ms),
        csv_u128(result.fetch_frame_send_wait_ms),
        csv_u64(result.pack_receive_bytes_per_sec),
        csv_usize(result.pack_object_count),
        csv_usize(result.pack_base_object_count),
        csv_usize(result.pack_delta_count),
        csv_usize(result.pack_offset_delta_count),
        csv_usize(result.pack_ref_delta_count),
        csv_u64(result.pack_declared_inflated_bytes),
        csv_usize(result.pipeline_checkout_wait_count),
        csv_u128(result.pipeline_checkout_wait_max_ms),
        csv_u128(result.pipeline_resolver_wall_ms),
        csv_u128(result.pipeline_resolver_wait_for_frame_ms),
        csv_usize(result.pipeline_queue_peak_depth),
        csv_u128(result.finalize_ms),
        csv_u128(result.target_size_scan_ms),
        result.validated.to_string(),
    ];
    println!("{}", fields.join(","));
}

fn print_plain_result(result: &BenchResult) {
    println!(
        "{} run {} backend={}: total={}ms discovery={} fetch={} ingest={} pack_scan={} pack_resolve={} pack_idx_write={} pack_object_state={} streaming_pack_scan={} checkout={} checkout_manifest={} checkout_dirs={} checkout_files={} checkout_index={} checkout_file_count={} checkout_dir_count={} checkout_blob_bytes={} pack_bytes={} refs={} retained_objects={} retained_bytes={} spilled_objects={} spilled_bytes={} checkout_needed_blobs={} checkout_ready_blobs={} checkout_ready_blob_bytes={} checkout_spilled_blobs={} checkout_spilled_blob_bytes={} checkout_missing_blobs={} reconstructed_objects={} pipeline_enabled={} pipeline_frames={} pipeline_checkout_wait={} pipeline_peak_pending_deltas={} pipeline_arena_spill_bytes={} target_bytes={} rss={} git_trace={} git_remote={} git_index_pack={} git_checkout={} git_trace_parse_error={} clone_wall={} clone_unreported={} fetch_request={} fetch_first_byte={} fetch_sideband_read={} fetch_pack_write={} fetch_pack_flush={} fetch_checksum={} fetch_frame_send_wait={} pack_receive_bytes_per_sec={} pack_objects={} pack_bases={} pack_deltas={} pack_ofs_deltas={} pack_ref_deltas={} pack_declared_inflated_bytes={} pipeline_checkout_wait_count={} pipeline_checkout_wait_max={} pipeline_resolver_wall={} pipeline_resolver_wait_for_frame={} pipeline_queue_peak={} finalize={} target_size_scan={} validated={}",
        result.tool,
        result.run,
        result.compression_backend,
        result.total_ms,
        ms_or_dash(result.discovery_ms),
        ms_or_dash(result.fetch_ms),
        ms_or_dash(result.ingest_ms),
        ms_or_dash(result.pack_scan_ms),
        ms_or_dash(result.pack_resolve_ms),
        ms_or_dash(result.pack_idx_write_ms),
        ms_or_dash(result.pack_object_state_ms),
        bool_or_dash(result.streaming_pack_scan),
        ms_or_dash(result.checkout_ms),
        ms_or_dash(result.checkout_manifest_ms),
        ms_or_dash(result.checkout_dir_create_ms),
        ms_or_dash(result.checkout_file_materialize_ms),
        ms_or_dash(result.checkout_index_write_ms),
        usize_or_dash(result.checkout_file_count),
        usize_or_dash(result.checkout_dir_count),
        u64_or_dash(result.checkout_blob_bytes),
        u64_or_dash(result.pack_bytes),
        usize_or_dash(result.ref_count),
        usize_or_dash(result.retained_object_count),
        usize_or_dash(result.retained_object_bytes),
        usize_or_dash(result.spilled_object_count),
        usize_or_dash(result.spilled_object_bytes),
        usize_or_dash(result.checkout_needed_blob_count),
        usize_or_dash(result.checkout_ready_blob_count),
        usize_or_dash(result.checkout_ready_blob_bytes),
        usize_or_dash(result.checkout_spilled_blob_count),
        usize_or_dash(result.checkout_spilled_blob_bytes),
        usize_or_dash(result.checkout_missing_blob_count),
        usize_or_dash(result.reconstructed_object_count),
        result.pipeline_enabled,
        usize_or_dash(result.pipeline_frame_count),
        ms_or_dash(result.pipeline_checkout_wait_ms),
        usize_or_dash(result.pipeline_peak_pending_delta_count),
        u64_or_dash(result.pipeline_arena_spill_bytes),
        u64_or_dash(result.target_bytes),
        u64_or_dash(result.rss_bytes),
        path_or_dash(result.git_trace_path.as_deref()),
        ms_or_dash(result.git_remote_ms),
        ms_or_dash(result.git_index_pack_ms),
        ms_or_dash(result.git_checkout_ms),
        string_or_dash(result.git_trace_parse_error.as_deref()),
        ms_or_dash(result.clone_wall_ms),
        ms_or_dash(result.clone_unreported_ms),
        ms_or_dash(result.fetch_request_ms),
        ms_or_dash(result.fetch_first_byte_ms),
        ms_or_dash(result.fetch_sideband_read_ms),
        ms_or_dash(result.fetch_pack_write_ms),
        ms_or_dash(result.fetch_pack_flush_ms),
        ms_or_dash(result.fetch_checksum_ms),
        ms_or_dash(result.fetch_frame_send_wait_ms),
        u64_or_dash(result.pack_receive_bytes_per_sec),
        usize_or_dash(result.pack_object_count),
        usize_or_dash(result.pack_base_object_count),
        usize_or_dash(result.pack_delta_count),
        usize_or_dash(result.pack_offset_delta_count),
        usize_or_dash(result.pack_ref_delta_count),
        u64_or_dash(result.pack_declared_inflated_bytes),
        usize_or_dash(result.pipeline_checkout_wait_count),
        ms_or_dash(result.pipeline_checkout_wait_max_ms),
        ms_or_dash(result.pipeline_resolver_wall_ms),
        ms_or_dash(result.pipeline_resolver_wait_for_frame_ms),
        usize_or_dash(result.pipeline_queue_peak_depth),
        ms_or_dash(result.finalize_ms),
        ms_or_dash(result.target_size_scan_ms),
        result.validated
    );
}

fn print_summaries(cli: &BenchCli, results: &[BenchResult]) {
    if cli.output.csv || cli.output.json {
        return;
    }
    for tool in ["fcl", "git"] {
        let totals = results
            .iter()
            .filter(|result| result.tool == tool)
            .map(|result| result.total_ms)
            .collect::<Vec<_>>();
        let Some(summary) = TimingSummary::from_samples(&totals) else {
            continue;
        };
        println!(
            "{} summary: total median={}ms min={}ms max={}ms samples={}",
            tool, summary.median_ms, summary.min_ms, summary.max_ms, summary.samples
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimingSummary {
    samples: usize,
    median_ms: u128,
    min_ms: u128,
    max_ms: u128,
}

impl TimingSummary {
    fn from_samples(samples: &[u128]) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        let len = sorted.len();
        let median_ms = if len.is_multiple_of(2) {
            u128::midpoint(sorted[len / 2 - 1], sorted[len / 2])
        } else {
            sorted[len / 2]
        };
        Some(Self {
            samples: len,
            median_ms,
            min_ms: sorted[0],
            max_ms: sorted[len - 1],
        })
    }
}

fn option_u128(value: Option<u128>) -> String {
    value.map_or_else(|| "null".to_owned(), |value| value.to_string())
}

fn option_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "null".to_owned(), |value| value.to_string())
}

fn option_usize(value: Option<usize>) -> String {
    value.map_or_else(|| "null".to_owned(), |value| value.to_string())
}

fn option_bool(value: Option<bool>) -> String {
    value.map_or_else(|| "null".to_owned(), |value| value.to_string())
}

fn option_path(value: Option<&Path>) -> String {
    value.map_or_else(
        || "null".to_owned(),
        |value| format!("\"{}\"", escape_json(&value.display().to_string())),
    )
}

fn option_string(value: Option<&str>) -> String {
    value.map_or_else(
        || "null".to_owned(),
        |value| format!("\"{}\"", escape_json(value)),
    )
}

fn csv_u128(value: Option<u128>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn csv_u64(value: Option<u64>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn csv_usize(value: Option<usize>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn csv_bool(value: Option<bool>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn csv_path(value: Option<&Path>) -> String {
    value.map_or_else(String::new, |value| value.display().to_string())
}

fn csv_string(value: Option<&str>) -> String {
    value.map_or_else(String::new, ToOwned::to_owned)
}

fn ms_or_dash(value: Option<u128>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| format!("{value}ms"))
}

fn u64_or_dash(value: Option<u64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

fn usize_or_dash(value: Option<usize>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

fn bool_or_dash(value: Option<bool>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

fn path_or_dash(value: Option<&Path>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.display().to_string())
}

fn string_or_dash(value: Option<&str>) -> String {
    value.map_or_else(|| "-".to_owned(), ToOwned::to_owned)
}

fn escape_json(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::{
        BenchOrder, GitTraceSummary, TimingSummary, csv_path, option_path,
        parse_git_trace2_content, run_git_first,
    };
    use std::path::Path;

    const TRACE_PREFIX: &str =
        "00:00:00.000000 file.c:1                 | d0 | main                     |";

    #[test]
    fn timing_summary_should_report_median_min_and_max_for_odd_samples() {
        let summary = TimingSummary::from_samples(&[30, 10, 20]).expect("summary should exist");

        assert_eq!(
            summary,
            TimingSummary {
                samples: 3,
                median_ms: 20,
                min_ms: 10,
                max_ms: 30,
            }
        );
    }

    #[test]
    fn timing_summary_should_report_integer_median_for_even_samples() {
        let summary = TimingSummary::from_samples(&[10, 40, 20, 30]).expect("summary should exist");

        assert_eq!(summary.median_ms, 25);
    }

    #[test]
    fn timing_summary_should_reject_empty_samples() {
        assert_eq!(TimingSummary::from_samples(&[]), None);
    }

    #[test]
    fn benchmark_order_should_support_fixed_and_alternating_git_position() {
        assert!(!run_git_first(BenchOrder::FclFirst, 1));
        assert!(run_git_first(BenchOrder::GitFirst, 1));
        assert!(!run_git_first(BenchOrder::Alternate, 1));
        assert!(run_git_first(BenchOrder::Alternate, 2));
    }

    #[test]
    fn path_output_should_render_json_null_for_missing_path() {
        assert_eq!(option_path(None), "null");
    }

    #[test]
    fn path_output_should_render_csv_empty_for_missing_path() {
        assert_eq!(csv_path(None), "");
    }

    #[test]
    fn path_output_should_escape_json_path() {
        assert_eq!(option_path(Some(Path::new("a\"b"))), "\"a\\\"b\"");
    }

    #[test]
    fn trace2_parser_should_pair_child_start_and_exit() {
        let trace = format!(
            "{TRACE_PREFIX} child_start  |     |  0.010000 |           |              | [ch0] class:remote-https argv:[git remote-https origin https://example.com/repo.git]\n\
             {TRACE_PREFIX} child_exit   |     |  0.110000 |  0.100000 |              | [ch0] pid:1 code:0"
        );

        let summary = parse_git_trace2_content(&trace);

        assert_eq!(summary.remote_ms, Some(100));
        assert_eq!(summary.index_pack_ms, None);
        assert_eq!(summary.parse_error, None);
    }

    #[test]
    fn trace2_parser_should_classify_remote_https_and_index_pack() {
        let trace = format!(
            "{TRACE_PREFIX} child_start  |     |  0.010000 |           |              | [ch0] class:remote-https argv:[git remote-https origin https://example.com/repo.git]\n\
             {TRACE_PREFIX} child_exit   |     |  0.110000 |  0.100000 |              | [ch0] pid:1 code:0\n\
             {TRACE_PREFIX} child_start  |     |  0.120000 |           |              | [ch1] class:? argv:[git index-pack --stdin]\n\
             {TRACE_PREFIX} child_exit   |     |  0.320000 |  0.200000 |              | [ch1] pid:2 code:0"
        );

        let summary = parse_git_trace2_content(&trace);

        assert_eq!(summary.remote_ms, Some(100));
        assert_eq!(summary.index_pack_ms, Some(200));
    }

    #[test]
    fn trace2_parser_should_sum_checkout_regions_when_present() {
        let trace = format!(
            "{TRACE_PREFIX} region_leave | r1  |  0.200000 |  0.050000 | unpack_trees | label:unpack_trees\n\
             {TRACE_PREFIX} region_leave | r1  |  0.300000 |  0.025000 | checkout     | label:writing-files"
        );

        let summary = parse_git_trace2_content(&trace);

        assert_eq!(summary.checkout_ms, Some(75));
    }

    #[test]
    fn trace2_parser_should_ignore_unknown_or_malformed_lines() {
        let trace = format!(
            "not trace2\n\
             {TRACE_PREFIX} child_start  |     |  0.010000 |           |              | [ch0] class:unknown argv:[git status]\n\
             {TRACE_PREFIX} child_exit   |     |  0.110000 |  0.100000 |              | [ch0] pid:1 code:0"
        );

        let summary = parse_git_trace2_content(&trace);

        assert_eq!(summary.remote_ms, None);
        assert_eq!(summary.index_pack_ms, None);
        assert_eq!(
            summary.parse_error,
            Some("ignored 1 malformed Trace2 lines".to_owned())
        );
    }

    #[test]
    fn trace2_columns_should_be_empty_without_git_trace2() {
        assert_eq!(
            GitTraceSummary::default(),
            GitTraceSummary {
                remote_ms: None,
                index_pack_ms: None,
                checkout_ms: None,
                parse_error: None,
            }
        );
    }
}
