mod bench;

use std::path::PathBuf;

use bench::{BenchCli, run_bench};
use clap::Parser;
use fcl_core::{CloneRequest, LocalCloneRequest, clone_repo, local_clone};

#[derive(Debug, Parser)]
#[command(
    name = "fcl",
    version,
    about = "Fast full Git clone from first principles"
)]
struct CloneCli {
    /// Repository URL to clone.
    url: String,

    /// Target directory. Defaults to the repository name.
    target: Option<PathBuf>,
}

fn main() {
    let mut args = std::env::args().collect::<Vec<_>>();
    if args.get(1).is_some_and(|arg| arg == "bench") {
        args.remove(1);
        let cli = BenchCli::parse_from(args);
        if let Err(error) = run_bench(&cli) {
            eprintln!("fcl: {error}");
            std::process::exit(1);
        }
        return;
    }
    if args.get(1).is_some_and(|arg| arg == "local") {
        args.remove(1);
        let cli = LocalCli::parse_from(args);
        match local_clone(LocalCloneRequest::new(cli.source, cli.target)) {
            Ok(report) => {
                eprintln!(
                    "fcl: local clone strategy={} files={} dirs={} symlinks={} bytes={} total={}ms",
                    report.strategy,
                    report.file_count,
                    report.dir_count,
                    report.symlink_count,
                    report.bytes,
                    report.total_ms
                );
            }
            Err(error) => {
                eprintln!("fcl: {error}");
                std::process::exit(1);
            }
        }
        return;
    }

    let cli = CloneCli::parse();
    let request = CloneRequest::new(cli.url, cli.target);

    match clone_repo(request) {
        Ok(report) => {
            eprintln!("fcl: fetched {} refs", report.ref_count);
            eprintln!("fcl: wrote {} bytes of pack data", report.pack_bytes);
            eprintln!(
                "fcl: discovery={}ms fetch={}ms ingest={}ms checkout={}ms",
                report.discovery_ms, report.fetch_ms, report.ingest_ms, report.checkout_ms
            );
            eprintln!(
                "fcl: pack scan={}ms resolve={}ms idx_write={}ms object_state={}ms",
                report.pack_scan_ms,
                report.pack_resolve_ms,
                report.pack_idx_write_ms,
                report.pack_object_state_ms
            );
            eprintln!(
                "fcl: checkout manifest={}ms dirs={}ms files={}ms index={}ms files={} dirs={} blob_bytes={}",
                report.checkout_manifest_ms,
                report.checkout_dir_create_ms,
                report.checkout_file_materialize_ms,
                report.checkout_index_write_ms,
                report.checkout_file_count,
                report.checkout_dir_count,
                report.checkout_blob_bytes
            );
            eprintln!(
                "fcl: retained {} objects ({} bytes) for checkout",
                report.retained_object_count, report.retained_object_bytes
            );
            eprintln!(
                "fcl: spilled {} objects ({} bytes), reconstructed {} objects",
                report.spilled_object_count,
                report.spilled_object_bytes,
                report.reconstructed_object_count
            );
            eprintln!("fcl: target uses {} bytes", report.target_bytes);
            if let Some(rss_bytes) = report.rss_bytes {
                eprintln!("fcl: rss {rss_bytes} bytes");
            }
            eprintln!("fcl: completed in {} ms", report.total_ms);
        }
        Err(error) => {
            eprintln!("fcl: {error}");
            std::process::exit(1);
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "fcl local",
    about = "Fast local clone using filesystem copy-on-write"
)]
struct LocalCli {
    /// Local source repository path.
    source: PathBuf,

    /// Target directory. Defaults to '<source>-fcl'.
    target: Option<PathBuf>,
}
