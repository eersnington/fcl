mod bench;

use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use bench::{BenchCli, run_bench};
use clap::Parser;
use fcl_core::{
    CloneError, CloneProgressEvent, CloneProgressPhase, CloneReport, CloneRequest,
    LocalCloneProgressEvent, LocalCloneProgressPhase, LocalCloneReport, LocalCloneRequest,
    clone_repo_with_progress as core_clone_repo_with_progress,
    local_clone_with_progress as core_local_clone_with_progress,
};

#[derive(Debug, Parser)]
#[command(
    name = "fcl",
    version,
    about = "Fast full Git clone from first principles"
)]
struct CloneCli {
    #[arg(
        long,
        help = "Print detailed clone timings and cache metrics after completion."
    )]
    stats: bool,

    #[arg(
        long,
        help = "Disable the default streaming pipeline and use the sequential clone path."
    )]
    no_pipeline: bool,

    #[arg(help = "Repository URL to clone.")]
    url: String,

    #[arg(help = "Target directory. Defaults to the repository name.")]
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
        let print_stats = cli.stats;
        let source_label = cli.source.display().to_string();
        let target_label = cli.target.as_ref().map_or_else(
            || "<default>".to_owned(),
            |target| target.display().to_string(),
        );
        match local_clone_with_progress_ui(
            LocalCloneRequest::new(cli.source, cli.target),
            source_label,
            target_label,
        ) {
            Ok(report) => {
                print_local_summary(&report);
                if print_stats {
                    print_local_stats(&report);
                }
            }
            Err(error) => {
                eprintln!("fcl: {error}");
                std::process::exit(1);
            }
        }
        return;
    }

    let cli = CloneCli::parse();
    let print_stats = cli.stats;
    let source_label = cli.url.clone();
    let target_label = cli.target.as_ref().map_or_else(
        || "<default>".to_owned(),
        |target| target.display().to_string(),
    );
    let request = CloneRequest::new(cli.url, cli.target).with_pipeline(!cli.no_pipeline);

    match clone_repo_with_progress_ui(request, source_label, target_label) {
        Ok(report) => {
            print_clone_summary(&report);
            if print_stats {
                print_clone_stats(&report);
            }
        }
        Err(error) => {
            eprintln!("fcl: {error}");
            std::process::exit(1);
        }
    }
}

fn print_clone_summary(report: &CloneReport) {
    eprintln!(
        "fcl: cloned {} refs, {} files, {} pack in {}",
        report.ref_count,
        report.checkout_file_count,
        format_bytes(report.pack_bytes),
        format_elapsed(Duration::from_millis(u64_saturating_from_u128(
            report.total_ms
        )))
    );
}

fn print_clone_stats(report: &CloneReport) {
    eprintln!("fcl: compression backend {}", report.compression_backend);
    if !report.remote_features.is_empty() {
        eprintln!("fcl: remote features {}", report.remote_features.join(","));
    }
    eprintln!("fcl: fetched {} refs", report.ref_count);
    eprintln!("fcl: wrote {} bytes of pack data", report.pack_bytes);
    eprintln!(
        "fcl: discovery={}ms fetch={}ms ingest={}ms checkout={}ms finalize={}ms hidden_before={}ms",
        report.discovery_ms,
        report.fetch_ms,
        report.ingest_ms,
        report.checkout_ms,
        report.finalize_ms,
        report.clone_unreported_ms
    );
    eprintln!(
        "fcl: fetch request={}ms first_byte={}ms sideband_read={}ms pack_write={}ms flush={}ms checksum={}ms throughput={}/s",
        report.fetch_request_ms,
        report.fetch_first_byte_ms,
        report.fetch_sideband_read_ms,
        report.fetch_pack_write_ms,
        report.fetch_pack_flush_ms,
        report.fetch_checksum_ms,
        format_bytes(report.pack_receive_bytes_per_sec)
    );
    eprintln!(
        "fcl: pack scan={}ms resolve={}ms idx_write={}ms object_state={}ms streaming_scan={} objects={} bases={} deltas={} ofs_deltas={} ref_deltas={} inflated_bytes={}",
        report.pack_scan_ms,
        report.pack_resolve_ms,
        report.pack_idx_write_ms,
        report.pack_object_state_ms,
        report.streaming_pack_scan,
        report.pack_object_count,
        report.pack_base_object_count,
        report.pack_delta_count,
        report.pack_offset_delta_count,
        report.pack_ref_delta_count,
        report.pack_declared_inflated_bytes
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
        report.spilled_object_count, report.spilled_object_bytes, report.reconstructed_object_count
    );
    eprintln!(
        "fcl: checkout blobs needed={} ready={} ready_bytes={} spilled={} spilled_bytes={} missing={}",
        report.checkout_needed_blob_count,
        report.checkout_ready_blob_count,
        report.checkout_ready_blob_bytes,
        report.checkout_spilled_blob_count,
        report.checkout_spilled_blob_bytes,
        report.checkout_missing_blob_count
    );
    if report.pipeline_enabled {
        eprintln!(
            "fcl: pipeline frames={} checkout_wait={}ms wait_count={} wait_max={}ms peak_pending_deltas={} resolver={}ms resolver_wait={}ms queue_peak={} frame_send_wait={}ms arena_spill_bytes={}",
            report.pipeline_frame_count.unwrap_or_default(),
            report.pipeline_checkout_wait_ms.unwrap_or_default(),
            report.pipeline_checkout_wait_count.unwrap_or_default(),
            report.pipeline_checkout_wait_max_ms.unwrap_or_default(),
            report.pipeline_peak_pending_delta_count.unwrap_or_default(),
            report.pipeline_resolver_wall_ms.unwrap_or_default(),
            report
                .pipeline_resolver_wait_for_frame_ms
                .unwrap_or_default(),
            report.pipeline_queue_peak_depth.unwrap_or_default(),
            report.fetch_frame_send_wait_ms.unwrap_or_default(),
            report.pipeline_arena_spill_bytes.unwrap_or_default()
        );
    }
    eprintln!(
        "fcl: target uses {} bytes, target_size_scan={}ms",
        report.target_bytes, report.target_size_scan_ms
    );
    if let Some(rss_bytes) = report.rss_bytes {
        eprintln!("fcl: rss {rss_bytes} bytes");
    }
    eprintln!("fcl: completed in {} ms", report.total_ms);
}

fn print_local_summary(report: &LocalCloneReport) {
    eprintln!(
        "fcl: local cloned {} files, {} dirs, {} symlinks, {} in {}",
        report.file_count,
        report.dir_count,
        report.symlink_count,
        format_bytes(report.bytes),
        format_elapsed(Duration::from_millis(u64_saturating_from_u128(
            report.total_ms
        )))
    );
}

fn print_local_stats(report: &LocalCloneReport) {
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

fn clone_repo_with_progress_ui(
    request: CloneRequest,
    source_label: String,
    target_label: String,
) -> Result<CloneReport, CloneError> {
    let (events, receiver) = mpsc::channel();
    let progress = TaskProgress::start(ProgressSpec::remote(source_label, target_label), receiver);
    let result = core_clone_repo_with_progress(request, |event| {
        let _ = events.send(map_remote_progress_event(event));
    });
    drop(events);
    progress.finish(result.is_ok());
    result
}

fn local_clone_with_progress_ui(
    request: LocalCloneRequest,
    source_label: String,
    target_label: String,
) -> Result<LocalCloneReport, CloneError> {
    let (events, receiver) = mpsc::channel();
    let progress = TaskProgress::start(ProgressSpec::local(source_label, target_label), receiver);
    let result = core_local_clone_with_progress(request, |event| {
        let _ = events.send(map_local_progress_event(event));
    });
    drop(events);
    progress.finish(result.is_ok());
    result
}

struct TaskProgress {
    finish: Option<mpsc::Sender<bool>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl TaskProgress {
    fn start(spec: ProgressSpec, events: mpsc::Receiver<ProgressEvent>) -> Self {
        let terminal = io::stderr().is_terminal();
        let (finish, finished) = mpsc::channel();
        let handle = thread::spawn(move || {
            run_progress_renderer(spec, &events, &finished, terminal);
        });

        Self {
            finish: Some(finish),
            handle: Some(handle),
        }
    }

    fn finish(mut self, success: bool) {
        self.stop(success);
    }

    fn stop(&mut self, success: bool) {
        if let Some(finish) = self.finish.take() {
            let _ = finish.send(success);
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for TaskProgress {
    fn drop(&mut self) {
        self.stop(false);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProgressEvent {
    Started,
    PhaseStarted(usize),
    PhaseCompleted(usize),
    FetchProgress {
        bytes: u64,
    },
    LocalCopyProgress {
        files: usize,
        dirs: usize,
        symlinks: usize,
        bytes: u64,
    },
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProgressMetric {
    None,
    FetchBytes,
    LocalCopy,
}

#[derive(Debug, Clone, Copy)]
struct ProgressPhase {
    label: &'static str,
    metric: ProgressMetric,
}

#[derive(Debug)]
struct ProgressSpec {
    title: String,
    source_label: String,
    target_label: String,
    source_word: &'static str,
    target_word: &'static str,
    phases: Vec<ProgressPhase>,
}

impl ProgressSpec {
    fn remote(source_label: String, target_label: String) -> Self {
        let title = format!("fcl clone {}", clone_title(&source_label));
        Self {
            title,
            source_label,
            target_label,
            source_word: "from",
            target_word: "to",
            phases: vec![
                ProgressPhase {
                    label: "resolving refs",
                    metric: ProgressMetric::None,
                },
                ProgressPhase {
                    label: "fetching pack",
                    metric: ProgressMetric::FetchBytes,
                },
                ProgressPhase {
                    label: "indexing objects",
                    metric: ProgressMetric::None,
                },
                ProgressPhase {
                    label: "checking out files",
                    metric: ProgressMetric::None,
                },
                ProgressPhase {
                    label: "finalizing repo",
                    metric: ProgressMetric::None,
                },
            ],
        }
    }

    fn local(source_label: String, target_label: String) -> Self {
        let title = format!("fcl local {}", clone_title(&source_label));
        Self {
            title,
            source_label,
            target_label,
            source_word: "from",
            target_word: "to",
            phases: vec![
                ProgressPhase {
                    label: "checking source",
                    metric: ProgressMetric::None,
                },
                ProgressPhase {
                    label: "cloning files",
                    metric: ProgressMetric::LocalCopy,
                },
                ProgressPhase {
                    label: "finalizing clone",
                    metric: ProgressMetric::None,
                },
            ],
        }
    }
}

const fn map_remote_progress_event(event: CloneProgressEvent) -> ProgressEvent {
    match event {
        CloneProgressEvent::Started => ProgressEvent::Started,
        CloneProgressEvent::PhaseStarted(phase) => {
            ProgressEvent::PhaseStarted(remote_phase_index(phase))
        }
        CloneProgressEvent::PhaseCompleted(phase) => {
            ProgressEvent::PhaseCompleted(remote_phase_index(phase))
        }
        CloneProgressEvent::FetchProgress { bytes } => ProgressEvent::FetchProgress { bytes },
        CloneProgressEvent::Completed => ProgressEvent::Completed,
    }
}

const fn map_local_progress_event(event: LocalCloneProgressEvent) -> ProgressEvent {
    match event {
        LocalCloneProgressEvent::Started => ProgressEvent::Started,
        LocalCloneProgressEvent::PhaseStarted(phase) => {
            ProgressEvent::PhaseStarted(local_phase_index(phase))
        }
        LocalCloneProgressEvent::PhaseCompleted(phase) => {
            ProgressEvent::PhaseCompleted(local_phase_index(phase))
        }
        LocalCloneProgressEvent::CopyProgress {
            files,
            dirs,
            symlinks,
            bytes,
        } => ProgressEvent::LocalCopyProgress {
            files,
            dirs,
            symlinks,
            bytes,
        },
        LocalCloneProgressEvent::Completed => ProgressEvent::Completed,
    }
}

const fn remote_phase_index(phase: CloneProgressPhase) -> usize {
    match phase {
        CloneProgressPhase::ResolvingRefs => 0,
        CloneProgressPhase::FetchingPack => 1,
        CloneProgressPhase::IndexingObjects => 2,
        CloneProgressPhase::CheckingOut => 3,
        CloneProgressPhase::Finalizing => 4,
    }
}

const fn local_phase_index(phase: LocalCloneProgressPhase) -> usize {
    match phase {
        LocalCloneProgressPhase::InspectingSource => 0,
        LocalCloneProgressPhase::CopyingFiles => 1,
        LocalCloneProgressPhase::Finalizing => 2,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PhaseStatus {
    Waiting,
    Running,
    Done,
}

struct ProgressState {
    spec: ProgressSpec,
    started: Instant,
    phases: Vec<PhaseStatus>,
    phase_started_at: Vec<Option<Instant>>,
    phase_finished_at: Vec<Option<Instant>>,
    fetch_bytes: u64,
    local_files: usize,
    local_dirs: usize,
    local_symlinks: usize,
    local_bytes: u64,
    finished: Option<bool>,
}

impl ProgressState {
    fn new(spec: ProgressSpec) -> Self {
        let phase_count = spec.phases.len();
        Self {
            spec,
            started: Instant::now(),
            phases: vec![PhaseStatus::Waiting; phase_count],
            phase_started_at: vec![None; phase_count],
            phase_finished_at: vec![None; phase_count],
            fetch_bytes: 0,
            local_files: 0,
            local_dirs: 0,
            local_symlinks: 0,
            local_bytes: 0,
            finished: None,
        }
    }

    fn apply(&mut self, event: ProgressEvent) {
        match event {
            ProgressEvent::Started => {}
            ProgressEvent::PhaseStarted(index) => {
                if let Some(phase) = self.phases.get_mut(index) {
                    *phase = PhaseStatus::Running;
                    self.phase_started_at[index] = Some(Instant::now());
                }
            }
            ProgressEvent::PhaseCompleted(index) => {
                if let Some(phase) = self.phases.get_mut(index) {
                    *phase = PhaseStatus::Done;
                    self.phase_finished_at[index] = Some(Instant::now());
                }
            }
            ProgressEvent::FetchProgress { bytes } => {
                self.fetch_bytes = bytes;
            }
            ProgressEvent::LocalCopyProgress {
                files,
                dirs,
                symlinks,
                bytes,
            } => {
                self.local_files = files;
                self.local_dirs = dirs;
                self.local_symlinks = symlinks;
                self.local_bytes = bytes;
            }
            ProgressEvent::Completed => {
                self.finished = Some(true);
                for phase in &mut self.phases {
                    if *phase != PhaseStatus::Done {
                        *phase = PhaseStatus::Done;
                    }
                }
            }
        }
    }
}

fn run_progress_renderer(
    spec: ProgressSpec,
    events: &mpsc::Receiver<ProgressEvent>,
    finished: &mpsc::Receiver<bool>,
    terminal: bool,
) {
    let mut state = ProgressState::new(spec);
    let mut rendered_lines = 0usize;
    let mut tick = 0usize;
    let colors = terminal && std::env::var_os("NO_COLOR").is_none();

    if terminal {
        render_terminal_progress(&state, tick, colors, &mut rendered_lines);
    }

    loop {
        while let Ok(event) = events.try_recv() {
            if !terminal {
                print_progress_event(&state, event);
            }
            state.apply(event);
        }

        if let Ok(success) = finished.try_recv() {
            state.finished = Some(success);
            if terminal {
                clear_terminal_progress(rendered_lines);
            }
            return;
        }

        if terminal {
            render_terminal_progress(&state, tick, colors, &mut rendered_lines);
            tick = tick.wrapping_add(1);
        }

        match events.recv_timeout(Duration::from_millis(120)) {
            Ok(event) => {
                if !terminal {
                    print_progress_event(&state, event);
                }
                state.apply(event);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                if let Ok(success) = finished.recv_timeout(Duration::from_millis(120)) {
                    state.finished = Some(success);
                    if terminal {
                        clear_terminal_progress(rendered_lines);
                    }
                }
                return;
            }
        }
    }
}

fn render_terminal_progress(
    state: &ProgressState,
    tick: usize,
    colors: bool,
    rendered_lines: &mut usize,
) {
    let lines = progress_lines(state, tick, colors);
    let mut stderr = io::stderr();
    if *rendered_lines > 0 && write!(stderr, "\x1b[{}A", *rendered_lines).is_err() {
        return;
    }
    for line in &lines {
        if writeln!(stderr, "\r\x1b[2K{line}").is_err() {
            return;
        }
    }
    let _ = stderr.flush();
    *rendered_lines = lines.len();
}

fn clear_terminal_progress(rendered_lines: usize) {
    if rendered_lines == 0 {
        return;
    }
    let mut stderr = io::stderr();
    let _ = write!(stderr, "\x1b[{rendered_lines}A\r\x1b[J").and_then(|()| stderr.flush());
}

fn progress_lines(state: &ProgressState, tick: usize, colors: bool) -> Vec<String> {
    let _ = tick;
    let active_index = active_phase_index(state).unwrap_or_else(|| completed_phase_count(state));
    let active_index = active_index.min(state.spec.phases.len().saturating_sub(1));
    let active_phase = state.spec.phases[active_index];
    let completed = completed_phase_count(state);
    let elapsed = format_elapsed(state.started.elapsed());
    let next = next_phase_label(state).unwrap_or("final output");
    let active_detail = active_phase_detail(state, active_index);

    vec![
        format!(
            "{} {}  {}",
            paint(colors, Color::Cyan, "◆"),
            paint(colors, Color::Cyan, &state.spec.title),
            paint(colors, Color::Dim, "Ctrl-C to cancel"),
        ),
        format!(
            "  {} {}",
            paint(colors, Color::Dim, state.spec.source_word),
            truncate_middle(&state.spec.source_label, 86),
        ),
        format!(
            "  {}   {}    {} {}",
            paint(colors, Color::Dim, state.spec.target_word),
            truncate_middle(&state.spec.target_label, 44),
            paint(colors, Color::Dim, "elapsed"),
            paint(colors, Color::Dim, &elapsed),
        ),
        format!(
            "  {} {}/{} complete    {} {}",
            paint(colors, Color::Green, "✓"),
            completed,
            state.spec.phases.len(),
            paint(colors, Color::Dim, "next"),
            paint(colors, Color::Dim, next,)
        ),
        format!(
            "  {} [{}/{}] {:<19} {}",
            paint(colors, Color::Magenta, "▶"),
            active_index + 1,
            state.spec.phases.len(),
            active_phase.label,
            paint(colors, Color::Dim, &active_detail),
        ),
    ]
}

fn active_phase_index(state: &ProgressState) -> Option<usize> {
    state
        .phases
        .iter()
        .position(|status| *status == PhaseStatus::Running)
}

fn completed_phase_count(state: &ProgressState) -> usize {
    state
        .phases
        .iter()
        .filter(|status| **status == PhaseStatus::Done)
        .count()
}

fn next_phase_label(state: &ProgressState) -> Option<&'static str> {
    state
        .spec
        .phases
        .iter()
        .enumerate()
        .find(|(index, _)| state.phases[*index] == PhaseStatus::Waiting)
        .map(|(_, phase)| phase.label)
}

fn phase_elapsed_label(state: &ProgressState, index: usize) -> String {
    let Some(started) = state.phase_started_at[index] else {
        return "00:00".to_owned();
    };
    let ended = state.phase_finished_at[index].unwrap_or_else(Instant::now);
    format_clock(ended.duration_since(started))
}

fn active_phase_detail(state: &ProgressState, index: usize) -> String {
    match state.spec.phases[index].metric {
        ProgressMetric::FetchBytes if state.fetch_bytes > 0 => format_fetch_progress(state, index),
        ProgressMetric::LocalCopy if local_copy_has_progress(state) => {
            format_local_copy_progress(state, index)
        }
        ProgressMetric::None | ProgressMetric::FetchBytes | ProgressMetric::LocalCopy => {
            phase_elapsed_label(state, index)
        }
    }
}

fn format_fetch_progress(state: &ProgressState, index: usize) -> String {
    let bytes = format_bytes(state.fetch_bytes);
    let Some(started) = state.phase_started_at[index] else {
        return bytes;
    };
    let elapsed_ms = started.elapsed().as_millis().max(1);
    let bytes_per_second = (u128::from(state.fetch_bytes) * 1000) / elapsed_ms;
    let bytes_per_second = u64_saturating_from_u128(bytes_per_second);
    format!("{} @ {}/s", bytes, format_bytes(bytes_per_second))
}

const fn local_copy_has_progress(state: &ProgressState) -> bool {
    state.local_files > 0
        || state.local_dirs > 0
        || state.local_symlinks > 0
        || state.local_bytes > 0
}

fn format_local_copy_progress(state: &ProgressState, index: usize) -> String {
    let Some(started) = state.phase_started_at[index] else {
        return format!(
            "{} files, {}",
            state.local_files,
            format_bytes(state.local_bytes)
        );
    };
    let elapsed_ms = started.elapsed().as_millis().max(1);
    let bytes_per_second = (u128::from(state.local_bytes) * 1000) / elapsed_ms;
    let bytes_per_second = u64_saturating_from_u128(bytes_per_second);
    format!(
        "{} files, {} dirs, {} symlinks, {} @ {}/s",
        state.local_files,
        state.local_dirs,
        state.local_symlinks,
        format_bytes(state.local_bytes),
        format_bytes(bytes_per_second)
    )
}

fn print_progress_event(state: &ProgressState, event: ProgressEvent) {
    match event {
        ProgressEvent::Started => {
            eprintln!(
                "{}: {} -> {}",
                state.spec.title,
                truncate_middle(&state.spec.source_label, 92),
                truncate_middle(&state.spec.target_label, 92)
            );
        }
        ProgressEvent::PhaseStarted(index) => {
            eprintln!(
                "[{}/{}] {}",
                index + 1,
                state.spec.phases.len(),
                state.spec.phases[index].label
            );
        }
        ProgressEvent::FetchProgress { .. }
        | ProgressEvent::LocalCopyProgress { .. }
        | ProgressEvent::PhaseCompleted(_)
        | ProgressEvent::Completed => {}
    }
}

#[derive(Debug, Clone, Copy)]
enum Color {
    Cyan,
    Green,
    Magenta,
    Dim,
}

fn paint(enabled: bool, color: Color, value: &str) -> String {
    if !enabled {
        return value.to_owned();
    }
    let code = match color {
        Color::Cyan => "36",
        Color::Green => "32",
        Color::Magenta => "35",
        Color::Dim => "2",
    };
    format!("\x1b[{code}m{value}\x1b[0m")
}

fn clone_title(source: &str) -> String {
    let tail = source
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(source)
        .trim_end_matches(".git");
    truncate_middle(tail, 32)
}

fn truncate_middle(value: &str, max_len: usize) -> String {
    let char_count = value.chars().count();
    if char_count <= max_len {
        return value.to_owned();
    }
    if max_len <= 3 {
        return ".".repeat(max_len);
    }
    let prefix_len = (max_len - 3) / 2;
    let suffix_len = max_len - 3 - prefix_len;
    let prefix = value.chars().take(prefix_len).collect::<String>();
    let suffix = value
        .chars()
        .skip(char_count - suffix_len)
        .collect::<String>();
    format!("{prefix}...{suffix}")
}

fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;

    if bytes >= GIB {
        format_scaled_bytes(bytes, GIB, "GiB")
    } else if bytes >= MIB {
        format_scaled_bytes(bytes, MIB, "MiB")
    } else if bytes >= KIB {
        format_scaled_bytes(bytes, KIB, "KiB")
    } else {
        format!("{bytes} B")
    }
}

fn format_scaled_bytes(bytes: u64, unit: u64, suffix: &str) -> String {
    let whole = bytes / unit;
    let tenth = (bytes % unit) * 10 / unit;
    format!("{whole}.{tenth} {suffix}")
}

fn u64_saturating_from_u128(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn format_elapsed(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs();
    if seconds < 60 {
        format!("{seconds}s")
    } else {
        format!("{}m{}s", seconds / 60, seconds % 60)
    }
}

fn format_clock(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs();
    format!("{:02}:{:02}", seconds / 60, seconds % 60)
}

#[derive(Debug, Parser)]
#[command(
    name = "fcl local",
    about = "Fast local clone using filesystem copy-on-write"
)]
struct LocalCli {
    #[arg(long, help = "Print detailed local clone metrics after completion.")]
    stats: bool,

    #[arg(help = "Local source repository path.")]
    source: PathBuf,

    #[arg(help = "Target directory. Defaults to '<source>-fcl'.")]
    target: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{clone_title, format_bytes, format_clock, format_elapsed, truncate_middle};

    #[test]
    fn progress_elapsed_should_render_seconds() {
        assert_eq!(format_elapsed(Duration::from_secs(12)), "12s");
    }

    #[test]
    fn progress_elapsed_should_render_minutes_and_seconds() {
        assert_eq!(format_elapsed(Duration::from_secs(125)), "2m5s");
    }

    #[test]
    fn progress_clock_should_render_minutes_and_seconds() {
        assert_eq!(format_clock(Duration::from_secs(65)), "01:05");
    }

    #[test]
    fn summary_bytes_should_render_scaled_units() {
        assert_eq!(format_bytes(420_512_037), "401.0 MiB");
    }

    #[test]
    fn progress_truncation_should_preserve_ends() {
        assert_eq!(truncate_middle("abcdefghij", 7), "ab...ij");
    }

    #[test]
    fn clone_title_should_use_repo_name() {
        assert_eq!(
            clone_title("https://github.com/rivet-dev/rivet.git"),
            "rivet"
        );
    }
}
