use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use clap::Parser;
use fcl_core::{CloneError, CloneReport, CloneRequest, clone_repo};

#[derive(Debug, Parser)]
#[command(name = "fcl bench", about = "Benchmark fcl against git clone")]
pub struct BenchCli {
    /// Repository URL to benchmark.
    pub url: String,

    /// Number of runs per tool.
    #[arg(long, default_value_t = 1)]
    pub runs: usize,

    /// Also run stock git clone.
    #[arg(long)]
    pub compare_git: bool,

    /// Validate each cloned repository with git fsck/status/diff.
    #[arg(long)]
    pub validate: bool,

    #[command(flatten)]
    pub output: BenchOutput,
}

#[derive(Debug, Parser)]
pub struct BenchOutput {
    /// Emit CSV rows.
    #[arg(long)]
    pub csv: bool,

    /// Emit JSON lines.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug)]
struct BenchResult {
    tool: &'static str,
    run: usize,
    total_ms: u128,
    discovery_ms: Option<u128>,
    fetch_ms: Option<u128>,
    ingest_ms: Option<u128>,
    pack_scan_ms: Option<u128>,
    pack_resolve_ms: Option<u128>,
    pack_idx_write_ms: Option<u128>,
    pack_object_state_ms: Option<u128>,
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
    reconstructed_object_count: Option<usize>,
    target_bytes: Option<u64>,
    rss_bytes: Option<u64>,
    git_trace_path: Option<PathBuf>,
    validated: bool,
}

pub fn run_bench(cli: &BenchCli) -> Result<(), CloneError> {
    if cli.runs == 0 {
        return Err(CloneError::BenchmarkFailed {
            operation: "parsing benchmark arguments",
            detail: "--runs must be greater than 0".to_owned(),
        });
    }

    if cli.output.csv {
        println!(
            "url,tool,run,total_ms,discovery_ms,fetch_ms,ingest_ms,pack_scan_ms,pack_resolve_ms,pack_idx_write_ms,pack_object_state_ms,checkout_ms,checkout_manifest_ms,checkout_dir_create_ms,checkout_file_materialize_ms,checkout_index_write_ms,checkout_file_count,checkout_dir_count,checkout_blob_bytes,pack_bytes,ref_count,retained_object_count,retained_object_bytes,spilled_object_count,spilled_object_bytes,reconstructed_object_count,target_bytes,rss_bytes,git_trace_path,validated"
        );
    }

    let mut results = Vec::new();
    for run in 1..=cli.runs {
        let target = bench_target("fcl", run);
        remove_target(&target)?;
        let report = clone_repo(CloneRequest::new(cli.url.clone(), Some(target.clone())))?;
        if cli.validate {
            validate_repo(&target)?;
        }
        let result = BenchResult::from_fcl(run, &report, cli.validate);
        print_result(cli, &result);
        results.push(result);

        if cli.compare_git {
            let target = bench_target("git", run);
            remove_target(&target)?;
            let start = Instant::now();
            let trace_path = git_trace_path(run);
            run_git_clone(&cli.url, &target, &trace_path)?;
            let total_ms = start.elapsed().as_millis();
            if cli.validate {
                validate_repo(&target)?;
            }
            let result = BenchResult {
                tool: "git",
                run,
                total_ms,
                discovery_ms: None,
                fetch_ms: None,
                ingest_ms: None,
                pack_scan_ms: None,
                pack_resolve_ms: None,
                pack_idx_write_ms: None,
                pack_object_state_ms: None,
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
                reconstructed_object_count: None,
                target_bytes: target_size(&target).ok(),
                rss_bytes: None,
                git_trace_path: Some(trace_path),
                validated: cli.validate,
            };
            print_result(cli, &result);
            results.push(result);
        }
    }

    print_summaries(cli, &results);

    Ok(())
}

impl BenchResult {
    const fn from_fcl(run: usize, report: &CloneReport, validated: bool) -> Self {
        Self {
            tool: "fcl",
            run,
            total_ms: report.total_ms,
            discovery_ms: Some(report.discovery_ms),
            fetch_ms: Some(report.fetch_ms),
            ingest_ms: Some(report.ingest_ms),
            pack_scan_ms: Some(report.pack_scan_ms),
            pack_resolve_ms: Some(report.pack_resolve_ms),
            pack_idx_write_ms: Some(report.pack_idx_write_ms),
            pack_object_state_ms: Some(report.pack_object_state_ms),
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
            reconstructed_object_count: Some(report.reconstructed_object_count),
            target_bytes: Some(report.target_bytes),
            rss_bytes: report.rss_bytes,
            git_trace_path: None,
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

fn run_git_clone(url: &str, target: &Path, trace_path: &Path) -> Result<(), CloneError> {
    let output = Command::new("git")
        .env("GIT_TRACE_PERFORMANCE", "1")
        .arg("clone")
        .arg(url)
        .arg(target)
        .output()
        .map_err(|error| CloneError::BenchmarkFailed {
            operation: "running git clone",
            detail: error.to_string(),
        })?;
    fs::write(trace_path, &output.stderr).map_err(|error| CloneError::BenchmarkFailed {
        operation: "writing git performance trace",
        detail: format!("{}: {error}", trace_path.display()),
    })?;
    if !output.status.success() {
        return Err(CloneError::BenchmarkFailed {
            operation: "running git clone",
            detail: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    Ok(())
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

fn print_result(cli: &BenchCli, result: &BenchResult) {
    if cli.output.json {
        print_json_result(&cli.url, result);
    } else if cli.output.csv {
        print_csv_result(&cli.url, result);
    } else {
        print_plain_result(result);
    }
}

fn print_json_result(url: &str, result: &BenchResult) {
    println!(
        "{{\"url\":\"{}\",\"tool\":\"{}\",\"run\":{},\"total_ms\":{},\"discovery_ms\":{},\"fetch_ms\":{},\"ingest_ms\":{},\"pack_scan_ms\":{},\"pack_resolve_ms\":{},\"pack_idx_write_ms\":{},\"pack_object_state_ms\":{},\"checkout_ms\":{},\"checkout_manifest_ms\":{},\"checkout_dir_create_ms\":{},\"checkout_file_materialize_ms\":{},\"checkout_index_write_ms\":{},\"checkout_file_count\":{},\"checkout_dir_count\":{},\"checkout_blob_bytes\":{},\"pack_bytes\":{},\"ref_count\":{},\"retained_object_count\":{},\"retained_object_bytes\":{},\"spilled_object_count\":{},\"spilled_object_bytes\":{},\"reconstructed_object_count\":{},\"target_bytes\":{},\"rss_bytes\":{},\"git_trace_path\":{},\"validated\":{}}}",
        escape_json(url),
        result.tool,
        result.run,
        result.total_ms,
        option_u128(result.discovery_ms),
        option_u128(result.fetch_ms),
        option_u128(result.ingest_ms),
        option_u128(result.pack_scan_ms),
        option_u128(result.pack_resolve_ms),
        option_u128(result.pack_idx_write_ms),
        option_u128(result.pack_object_state_ms),
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
        option_usize(result.reconstructed_object_count),
        option_u64(result.target_bytes),
        option_u64(result.rss_bytes),
        option_path(result.git_trace_path.as_deref()),
        result.validated
    );
}

fn print_csv_result(url: &str, result: &BenchResult) {
    println!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        url,
        result.tool,
        result.run,
        result.total_ms,
        csv_u128(result.discovery_ms),
        csv_u128(result.fetch_ms),
        csv_u128(result.ingest_ms),
        csv_u128(result.pack_scan_ms),
        csv_u128(result.pack_resolve_ms),
        csv_u128(result.pack_idx_write_ms),
        csv_u128(result.pack_object_state_ms),
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
        csv_usize(result.reconstructed_object_count),
        csv_u64(result.target_bytes),
        csv_u64(result.rss_bytes),
        csv_path(result.git_trace_path.as_deref()),
        result.validated
    );
}

fn print_plain_result(result: &BenchResult) {
    println!(
        "{} run {}: total={}ms discovery={} fetch={} ingest={} pack_scan={} pack_resolve={} pack_idx_write={} pack_object_state={} checkout={} checkout_manifest={} checkout_dirs={} checkout_files={} checkout_index={} checkout_file_count={} checkout_dir_count={} checkout_blob_bytes={} pack_bytes={} refs={} retained_objects={} retained_bytes={} spilled_objects={} spilled_bytes={} reconstructed_objects={} target_bytes={} rss={} git_trace={} validated={}",
        result.tool,
        result.run,
        result.total_ms,
        ms_or_dash(result.discovery_ms),
        ms_or_dash(result.fetch_ms),
        ms_or_dash(result.ingest_ms),
        ms_or_dash(result.pack_scan_ms),
        ms_or_dash(result.pack_resolve_ms),
        ms_or_dash(result.pack_idx_write_ms),
        ms_or_dash(result.pack_object_state_ms),
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
        usize_or_dash(result.reconstructed_object_count),
        u64_or_dash(result.target_bytes),
        u64_or_dash(result.rss_bytes),
        path_or_dash(result.git_trace_path.as_deref()),
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

fn option_path(value: Option<&Path>) -> String {
    value.map_or_else(
        || "null".to_owned(),
        |value| format!("\"{}\"", escape_json(&value.display().to_string())),
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

fn csv_path(value: Option<&Path>) -> String {
    value.map_or_else(String::new, |value| value.display().to_string())
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

fn path_or_dash(value: Option<&Path>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.display().to_string())
}

fn escape_json(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::{TimingSummary, csv_path, option_path};
    use std::path::Path;

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
}
