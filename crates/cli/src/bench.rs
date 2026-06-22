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
            "url,tool,run,total_ms,discovery_ms,fetch_ms,ingest_ms,checkout_ms,checkout_manifest_ms,checkout_dir_create_ms,checkout_file_materialize_ms,checkout_index_write_ms,checkout_file_count,checkout_dir_count,checkout_blob_bytes,pack_bytes,ref_count,retained_object_count,retained_object_bytes,spilled_object_count,spilled_object_bytes,reconstructed_object_count,target_bytes,rss_bytes,validated"
        );
    }

    for run in 1..=cli.runs {
        let target = bench_target("fcl", run);
        remove_target(&target)?;
        let report = clone_repo(CloneRequest::new(cli.url.clone(), Some(target.clone())))?;
        if cli.validate {
            validate_repo(&target)?;
        }
        let result = BenchResult::from_fcl(run, &report, cli.validate);
        print_result(cli, &result);

        if cli.compare_git {
            let target = bench_target("git", run);
            remove_target(&target)?;
            let start = Instant::now();
            run_git_clone(&cli.url, &target)?;
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
                validated: cli.validate,
            };
            print_result(cli, &result);
        }
    }

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

fn run_git_clone(url: &str, target: &Path) -> Result<(), CloneError> {
    let output = Command::new("git")
        .arg("clone")
        .arg(url)
        .arg(target)
        .output()
        .map_err(|error| CloneError::BenchmarkFailed {
            operation: "running git clone",
            detail: error.to_string(),
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
        "{{\"url\":\"{}\",\"tool\":\"{}\",\"run\":{},\"total_ms\":{},\"discovery_ms\":{},\"fetch_ms\":{},\"ingest_ms\":{},\"checkout_ms\":{},\"checkout_manifest_ms\":{},\"checkout_dir_create_ms\":{},\"checkout_file_materialize_ms\":{},\"checkout_index_write_ms\":{},\"checkout_file_count\":{},\"checkout_dir_count\":{},\"checkout_blob_bytes\":{},\"pack_bytes\":{},\"ref_count\":{},\"retained_object_count\":{},\"retained_object_bytes\":{},\"spilled_object_count\":{},\"spilled_object_bytes\":{},\"reconstructed_object_count\":{},\"target_bytes\":{},\"rss_bytes\":{},\"validated\":{}}}",
        escape_json(url),
        result.tool,
        result.run,
        result.total_ms,
        option_u128(result.discovery_ms),
        option_u128(result.fetch_ms),
        option_u128(result.ingest_ms),
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
        result.validated
    );
}

fn print_csv_result(url: &str, result: &BenchResult) {
    println!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        url,
        result.tool,
        result.run,
        result.total_ms,
        csv_u128(result.discovery_ms),
        csv_u128(result.fetch_ms),
        csv_u128(result.ingest_ms),
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
        result.validated
    );
}

fn print_plain_result(result: &BenchResult) {
    println!(
        "{} run {}: total={}ms discovery={} fetch={} ingest={} checkout={} checkout_manifest={} checkout_dirs={} checkout_files={} checkout_index={} checkout_file_count={} checkout_dir_count={} checkout_blob_bytes={} pack_bytes={} refs={} retained_objects={} retained_bytes={} spilled_objects={} spilled_bytes={} reconstructed_objects={} target_bytes={} rss={} validated={}",
        result.tool,
        result.run,
        result.total_ms,
        ms_or_dash(result.discovery_ms),
        ms_or_dash(result.fetch_ms),
        ms_or_dash(result.ingest_ms),
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
        result.validated
    );
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

fn csv_u128(value: Option<u128>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn csv_u64(value: Option<u64>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn csv_usize(value: Option<usize>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
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

fn escape_json(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}
