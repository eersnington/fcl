use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::mpsc::SyncSender;
use std::time::{Duration, Instant};

use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use sha1::{Digest, Sha1};
use url::Url;

use crate::error::CloneError;
use crate::pack::{PackScan, PipelineEvent, ScanPayload, StreamingPackScanner};
use crate::protocol::pkt_line::{
    Packet, encode_data, encode_delimiter, encode_flush, parse_packets,
};

const DEFAULT_PACK_WRITE_BUFFER: usize = 1024 * 1024;
const PACK_PROGRESS_STEP_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct RemoteRef {
    pub oid: String,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct RemoteRefs {
    pub default_branch: Option<String>,
    pub refs: Vec<RemoteRef>,
}

impl RemoteRefs {
    pub fn select_full_clone_universe(&self) -> Vec<RemoteRef> {
        self.refs
            .iter()
            .filter(|remote_ref| {
                remote_ref.name.starts_with("refs/heads/")
                    || remote_ref.name.starts_with("refs/tags/")
            })
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct Remote {
    pub url: String,
    pub upload_pack_url: String,
    pub refs: RemoteRefs,
    pub capabilities: Vec<String>,
}

pub struct FetchedPack {
    pub bytes: u64,
    pub checksum: [u8; 20],
    pub scan: Option<PackScan>,
    pub scan_ms: u128,
    pub timings: FetchTimings,
}

#[derive(Debug, Clone, Copy, Default)]
#[expect(
    clippy::struct_field_names,
    reason = "fetch timing fields keep the ms suffix to match CloneReport and CLI columns"
)]
pub struct FetchTimings {
    pub request_ms: u128,
    pub first_byte_ms: u128,
    pub sideband_read_ms: u128,
    pub pack_write_ms: u128,
    pub pack_flush_ms: u128,
    pub checksum_ms: u128,
    pub frame_send_wait_ms: u128,
}

pub fn http_client() -> Result<Client, CloneError> {
    let mut builder = Client::builder()
        .connect_timeout(Duration::from_secs(env_u64("FCL_CONNECT_TIMEOUT_SECS", 10)))
        .timeout(Duration::from_secs(env_u64(
            "FCL_REQUEST_TIMEOUT_SECS",
            300,
        )))
        .pool_idle_timeout(Duration::from_secs(env_u64(
            "FCL_POOL_IDLE_TIMEOUT_SECS",
            4,
        )));

    if env_bool("FCL_HTTP1_ONLY") {
        builder = builder.http1_only();
    }
    if env_bool("FCL_HICKORY_DNS") {
        builder = builder.hickory_dns(true);
    }

    builder
        .build()
        .map_err(|error| CloneError::RemoteDiscoveryFailed {
            url: "<client>".to_owned(),
            operation: "building HTTP client",
            detail: error.to_string(),
        })
}

pub fn discover_remote(client: &Client, raw_url: &str) -> Result<Remote, CloneError> {
    let url = parse_https_url(raw_url)?;
    let endpoints = smart_http_endpoints(&url);
    let capabilities =
        with_retries(|| discover_capabilities(client, raw_url, &endpoints.info_refs_url))?;

    if !capabilities
        .iter()
        .any(|line| line.trim_end() == "version 2")
    {
        return Err(CloneError::UnsupportedRemoteCapability {
            url: raw_url.to_owned(),
            capability: "Git protocol v2",
            remediation: "The first native fcl milestone only supports Smart HTTP protocol v2.",
        });
    }

    let refs = with_retries(|| ls_refs(client, raw_url, &endpoints.upload_pack_url))?;

    Ok(Remote {
        url: raw_url.to_owned(),
        upload_pack_url: endpoints.upload_pack_url,
        refs,
        capabilities,
    })
}

pub fn fetch_full_pack(
    client: &Client,
    remote: &Remote,
    refs: &[RemoteRef],
    pack_path: &Path,
    progress: Option<&dyn Fn(u64)>,
) -> Result<FetchedPack, CloneError> {
    let body = fetch_body(remote, refs);

    let request_start = Instant::now();
    let mut response = with_retries(|| {
        client
            .post(&remote.upload_pack_url)
            .headers(upload_pack_headers())
            .body(body.clone())
            .send()
            .map_err(|error| CloneError::RemoteDiscoveryFailed {
                url: remote.url.clone(),
                operation: "fetching full pack",
                detail: error.to_string(),
            })
    })?;
    let request_ms = request_start.elapsed().as_millis();

    if !response.status().is_success() {
        return Err(CloneError::RemoteDiscoveryFailed {
            url: remote.url.clone(),
            operation: "fetching full pack",
            detail: format!("server returned HTTP {}", response.status()),
        });
    }

    let file = File::create(pack_path).map_err(|source| CloneError::PackWriteFailed {
        path: pack_path.to_owned(),
        source,
    })?;
    let mut file = BufWriter::with_capacity(
        env_usize("FCL_PACK_WRITE_BUFFER", DEFAULT_PACK_WRITE_BUFFER),
        file,
    );
    let mut fetched_pack =
        write_sideband_pack(&remote.url, pack_path, &mut response, &mut file, progress)?;
    fetched_pack.timings.request_ms = request_ms;
    let flush_start = Instant::now();
    file.flush().map_err(|source| CloneError::PackWriteFailed {
        path: pack_path.to_owned(),
        source,
    })?;
    fetched_pack.timings.pack_flush_ms += flush_start.elapsed().as_millis();
    Ok(fetched_pack)
}

pub fn fetch_full_pack_pipelined(
    client: &Client,
    remote: &Remote,
    refs: &[RemoteRef],
    pack_path: &Path,
    sender: &SyncSender<PipelineEvent>,
    progress: Option<&dyn Fn(u64)>,
) -> Result<FetchedPack, CloneError> {
    let body = fetch_body(remote, refs);
    let request_start = Instant::now();
    let mut response = with_retries(|| {
        client
            .post(&remote.upload_pack_url)
            .headers(upload_pack_headers())
            .body(body.clone())
            .send()
            .map_err(|error| CloneError::RemoteDiscoveryFailed {
                url: remote.url.clone(),
                operation: "fetching full pack",
                detail: error.to_string(),
            })
    })?;
    let request_ms = request_start.elapsed().as_millis();

    if !response.status().is_success() {
        return Err(CloneError::RemoteDiscoveryFailed {
            url: remote.url.clone(),
            operation: "fetching full pack",
            detail: format!("server returned HTTP {}", response.status()),
        });
    }

    let file = File::create(pack_path).map_err(|source| CloneError::PackWriteFailed {
        path: pack_path.to_owned(),
        source,
    })?;
    let mut file = BufWriter::with_capacity(
        env_usize("FCL_PACK_WRITE_BUFFER", DEFAULT_PACK_WRITE_BUFFER),
        file,
    );
    let mut fetched_pack = write_sideband_pack_pipelined(
        &remote.url,
        pack_path,
        &mut response,
        &mut file,
        sender,
        progress,
    )?;
    fetched_pack.timings.request_ms = request_ms;
    let flush_start = Instant::now();
    file.flush().map_err(|source| CloneError::PackWriteFailed {
        path: pack_path.to_owned(),
        source,
    })?;
    fetched_pack.timings.pack_flush_ms += flush_start.elapsed().as_millis();
    Ok(fetched_pack)
}

fn fetch_body(remote: &Remote, refs: &[RemoteRef]) -> Vec<u8> {
    fetch_body_with_remote_progress(
        refs,
        env_bool("FCL_REMOTE_PROGRESS") && remote.advertises_fetch_command(),
    )
}

impl Remote {
    fn advertises_fetch_command(&self) -> bool {
        self.capabilities
            .iter()
            .any(|capability| capability.trim_end().starts_with("fetch"))
    }
}

fn fetch_body_with_remote_progress(refs: &[RemoteRef], remote_progress: bool) -> Vec<u8> {
    let mut body = Vec::new();
    encode_data("command=fetch\n", &mut body);
    encode_data("agent=fcl/0.1\n", &mut body);
    encode_delimiter(&mut body);
    if !remote_progress {
        encode_data("no-progress\n", &mut body);
    }
    encode_data("thin-pack\n", &mut body);
    encode_data("ofs-delta\n", &mut body);

    for oid in unique_oids(refs) {
        encode_data(&format!("want {oid}\n"), &mut body);
    }

    encode_data("done\n", &mut body);
    encode_flush(&mut body);
    body
}

fn parse_https_url(raw_url: &str) -> Result<Url, CloneError> {
    let url = Url::parse(raw_url).map_err(|error| CloneError::RemoteDiscoveryFailed {
        url: raw_url.to_owned(),
        operation: "parsing URL",
        detail: error.to_string(),
    })?;

    if url.scheme() != "https" {
        return Err(CloneError::UnsupportedUrlScheme {
            url: raw_url.to_owned(),
            supported: "https".to_owned(),
        });
    }

    Ok(url)
}

#[derive(Debug)]
struct SmartHttpEndpoints {
    info_refs_url: String,
    upload_pack_url: String,
}

fn smart_http_endpoints(url: &Url) -> SmartHttpEndpoints {
    let mut info_refs = url.clone();
    let base_path = info_refs.path().trim_end_matches('/').to_owned();
    info_refs.set_path(&format!("{base_path}/info/refs"));
    info_refs.set_query(Some("service=git-upload-pack"));

    let mut upload_pack = url.clone();
    upload_pack.set_path(&format!("{base_path}/git-upload-pack"));
    upload_pack.set_query(None);

    SmartHttpEndpoints {
        info_refs_url: info_refs.to_string(),
        upload_pack_url: upload_pack.to_string(),
    }
}

fn discover_capabilities(
    client: &Client,
    raw_url: &str,
    endpoint: &str,
) -> Result<Vec<String>, CloneError> {
    let response = client
        .get(endpoint)
        .headers(discovery_headers())
        .send()
        .map_err(|error| CloneError::RemoteDiscoveryFailed {
            url: raw_url.to_owned(),
            operation: "discovering git-upload-pack capabilities",
            detail: error.to_string(),
        })?;

    if !response.status().is_success() {
        return Err(CloneError::RemoteDiscoveryFailed {
            url: raw_url.to_owned(),
            operation: "discovering git-upload-pack capabilities",
            detail: format!("server returned HTTP {}", response.status()),
        });
    }

    let bytes = response
        .bytes()
        .map_err(|error| CloneError::RemoteDiscoveryFailed {
            url: raw_url.to_owned(),
            operation: "reading capability response",
            detail: error.to_string(),
        })?;
    let packets = parse_packets(raw_url, "parsing capability response", &bytes)?;
    let mut capabilities = Vec::new();

    for packet in packets {
        if let Packet::Data(data) = packet {
            capabilities.push(String::from_utf8_lossy(&data).into_owned());
        }
    }

    Ok(capabilities)
}

fn ls_refs(client: &Client, raw_url: &str, endpoint: &str) -> Result<RemoteRefs, CloneError> {
    let mut body = Vec::new();
    encode_data("command=ls-refs\n", &mut body);
    encode_data("agent=fcl/0.1\n", &mut body);
    encode_delimiter(&mut body);
    encode_data("peel\n", &mut body);
    encode_data("symrefs\n", &mut body);
    encode_data("ref-prefix HEAD\n", &mut body);
    encode_data("ref-prefix refs/heads/\n", &mut body);
    encode_data("ref-prefix refs/tags/\n", &mut body);
    encode_flush(&mut body);

    let response = client
        .post(endpoint)
        .headers(upload_pack_headers())
        .body(body)
        .send()
        .map_err(|error| CloneError::RemoteDiscoveryFailed {
            url: raw_url.to_owned(),
            operation: "listing refs",
            detail: error.to_string(),
        })?;

    if !response.status().is_success() {
        return Err(CloneError::RemoteDiscoveryFailed {
            url: raw_url.to_owned(),
            operation: "listing refs",
            detail: format!("server returned HTTP {}", response.status()),
        });
    }

    let bytes = response
        .bytes()
        .map_err(|error| CloneError::RemoteDiscoveryFailed {
            url: raw_url.to_owned(),
            operation: "reading refs response",
            detail: error.to_string(),
        })?;
    let packets = parse_packets(raw_url, "parsing refs response", &bytes)?;
    let mut default_branch = None;
    let mut refs = Vec::new();

    for packet in packets {
        let Packet::Data(data) = packet else {
            continue;
        };
        let line = String::from_utf8_lossy(&data);
        let line = line.trim_end_matches('\n');
        let mut parts = line.split(' ');
        let oid = parts.next().unwrap_or_default();
        let name = parts.next().unwrap_or_default();
        if oid.is_empty() || name.is_empty() {
            return Err(CloneError::MalformedRemoteResponse {
                url: raw_url.to_owned(),
                operation: "parsing refs response",
                detail: format!("invalid ref line `{line}`"),
            });
        }

        for attribute in parts {
            if name == "HEAD"
                && let Some(target) = attribute.strip_prefix("symref-target:")
            {
                default_branch = Some(target.to_owned());
            }
        }

        if name != "HEAD" {
            refs.push(RemoteRef {
                oid: oid.to_owned(),
                name: name.to_owned(),
            });
        }
    }

    Ok(RemoteRefs {
        default_branch,
        refs,
    })
}

fn write_sideband_pack(
    raw_url: &str,
    pack_path: &Path,
    response: &mut impl Read,
    file: &mut impl Write,
    progress: Option<&dyn Fn(u64)>,
) -> Result<FetchedPack, CloneError> {
    let mut pack_bytes = 0u64;
    let mut next_progress_bytes = 0u64;
    let mut hasher = PackTrailerHasher::new();
    let max_pack_bytes = optional_u64_env("FCL_MAX_PACK_BYTES")?;
    let mut scanner = env_bool("FCL_STREAM_SCAN").then(|| StreamingPackScanner::new(pack_path));
    let mut scan_duration = Duration::ZERO;
    let mut timings = FetchTimings::default();
    let mut packet_reader = PacketReader::new();
    let receive_start = Instant::now();
    let mut saw_pack_data = false;

    loop {
        let read_start = Instant::now();
        let packet = packet_reader.read_packet(raw_url, response)?;
        timings.sideband_read_ms += read_start.elapsed().as_millis();
        let Some(data) = packet else {
            break;
        };
        if data == b"packfile\n" || data == b"shallow-info\n" || data == b"acknowledgments\n" {
            continue;
        }
        let Some((band, payload)) = data.split_first() else {
            continue;
        };
        match *band {
            1 => {
                if !saw_pack_data {
                    timings.first_byte_ms = receive_start.elapsed().as_millis();
                    saw_pack_data = true;
                }
                let write_start = Instant::now();
                file.write_all(payload)
                    .map_err(|source| CloneError::PackWriteFailed {
                        path: pack_path.to_owned(),
                        source,
                    })?;
                timings.pack_write_ms += write_start.elapsed().as_millis();
                pack_bytes += payload.len() as u64;
                emit_pack_progress(progress, pack_bytes, &mut next_progress_bytes);
                enforce_max_pack_bytes(max_pack_bytes, pack_bytes)?;
                hasher.update(payload);
                if let Some(scanner) = scanner.as_mut() {
                    let scan_start = Instant::now();
                    scanner.feed(payload)?;
                    scan_duration += scan_start.elapsed();
                }
            }
            2 => {}
            3 => {
                return Err(CloneError::RemoteDiscoveryFailed {
                    url: raw_url.to_owned(),
                    operation: "fetching full pack",
                    detail: String::from_utf8_lossy(payload).trim().to_owned(),
                });
            }
            other => {
                return Err(CloneError::MalformedRemoteResponse {
                    url: raw_url.to_owned(),
                    operation: "parsing pack sideband response",
                    detail: format!("unknown sideband channel {other}"),
                });
            }
        }
    }

    let checksum_start = Instant::now();
    let checksum = validate_streaming_pack_checksum(raw_url, pack_path, pack_bytes, hasher)?;
    timings.checksum_ms = checksum_start.elapsed().as_millis();
    if let Some(progress) = progress {
        progress(pack_bytes);
    }
    let scan = scanner
        .map(|scanner| scanner.finish(checksum))
        .transpose()?;

    Ok(FetchedPack {
        bytes: pack_bytes,
        checksum,
        scan,
        scan_ms: scan_duration.as_millis(),
        timings,
    })
}

#[expect(
    clippy::too_many_lines,
    reason = "sideband receive, pack write, scan, and pipeline event emission share one tight loop"
)]
fn write_sideband_pack_pipelined(
    raw_url: &str,
    pack_path: &Path,
    response: &mut impl Read,
    file: &mut impl Write,
    sender: &SyncSender<PipelineEvent>,
    progress: Option<&dyn Fn(u64)>,
) -> Result<FetchedPack, CloneError> {
    let mut pack_bytes = 0u64;
    let mut next_progress_bytes = 0u64;
    let mut hasher = PackTrailerHasher::new();
    let max_pack_bytes = optional_u64_env("FCL_MAX_PACK_BYTES")?;
    let mut scanner = StreamingPackScanner::with_payload(pack_path, ScanPayload::Inflate);
    let mut scan_duration = Duration::ZERO;
    let mut timings = FetchTimings::default();
    let mut packet_reader = PacketReader::new();
    let receive_start = Instant::now();
    let mut saw_pack_data = false;

    loop {
        let read_start = Instant::now();
        let packet = packet_reader.read_packet(raw_url, response)?;
        timings.sideband_read_ms += read_start.elapsed().as_millis();
        let Some(data) = packet else {
            break;
        };
        if data == b"packfile\n" || data == b"shallow-info\n" || data == b"acknowledgments\n" {
            continue;
        }
        let Some((band, payload)) = data.split_first() else {
            continue;
        };
        match *band {
            1 => {
                if !saw_pack_data {
                    timings.first_byte_ms = receive_start.elapsed().as_millis();
                    saw_pack_data = true;
                }
                let write_start = Instant::now();
                file.write_all(payload)
                    .map_err(|source| CloneError::PackWriteFailed {
                        path: pack_path.to_owned(),
                        source,
                    })?;
                timings.pack_write_ms += write_start.elapsed().as_millis();
                pack_bytes += payload.len() as u64;
                emit_pack_progress(progress, pack_bytes, &mut next_progress_bytes);
                enforce_max_pack_bytes(max_pack_bytes, pack_bytes)?;
                hasher.update(payload);
                let scan_start = Instant::now();
                let frames = scanner.feed_collect(payload)?;
                scan_duration += scan_start.elapsed();
                if !frames.is_empty() {
                    let send_start = Instant::now();
                    sender
                        .send(PipelineEvent::Frames(frames))
                        .map_err(|error| CloneError::PackIndexFailed {
                            path: pack_path.to_owned(),
                            operation: "sending pipeline object frames",
                            detail: error.to_string(),
                        })?;
                    timings.frame_send_wait_ms += send_start.elapsed().as_millis();
                }
            }
            2 => {}
            3 => {
                return Err(CloneError::RemoteDiscoveryFailed {
                    url: raw_url.to_owned(),
                    operation: "fetching full pack",
                    detail: String::from_utf8_lossy(payload).trim().to_owned(),
                });
            }
            other => {
                return Err(CloneError::MalformedRemoteResponse {
                    url: raw_url.to_owned(),
                    operation: "parsing pack sideband response",
                    detail: format!("unknown sideband channel {other}"),
                });
            }
        }
    }

    let checksum_start = Instant::now();
    let checksum = validate_streaming_pack_checksum(raw_url, pack_path, pack_bytes, hasher)?;
    timings.checksum_ms = checksum_start.elapsed().as_millis();
    if let Some(progress) = progress {
        progress(pack_bytes);
    }
    scanner.finish(checksum)?;
    let scan_ms = scan_duration.as_millis();
    let flush_start = Instant::now();
    file.flush().map_err(|source| CloneError::PackWriteFailed {
        path: pack_path.to_owned(),
        source,
    })?;
    timings.pack_flush_ms += flush_start.elapsed().as_millis();
    let send_start = Instant::now();
    sender
        .send(PipelineEvent::Finished {
            checksum,
            pack_bytes,
            scan_ms,
        })
        .map_err(|error| CloneError::PackIndexFailed {
            path: pack_path.to_owned(),
            operation: "sending pipeline completion",
            detail: error.to_string(),
        })?;
    timings.frame_send_wait_ms += send_start.elapsed().as_millis();

    Ok(FetchedPack {
        bytes: pack_bytes,
        checksum,
        scan: None,
        scan_ms,
        timings,
    })
}

fn emit_pack_progress(
    progress: Option<&dyn Fn(u64)>,
    pack_bytes: u64,
    next_progress_bytes: &mut u64,
) {
    if let Some(progress) = progress
        && pack_bytes >= *next_progress_bytes
    {
        progress(pack_bytes);
        *next_progress_bytes = pack_bytes.saturating_add(PACK_PROGRESS_STEP_BYTES);
    }
}

struct PacketReader {
    data: Vec<u8>,
}

impl PacketReader {
    const fn new() -> Self {
        Self { data: Vec::new() }
    }

    fn read_packet<'a>(
        &'a mut self,
        raw_url: &str,
        reader: &mut impl Read,
    ) -> Result<Option<&'a [u8]>, CloneError> {
        let mut header = [0u8; 4];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => {
                return Err(CloneError::RemoteDiscoveryFailed {
                    url: raw_url.to_owned(),
                    operation: "reading pack response pkt-line header",
                    detail: error.to_string(),
                });
            }
        }
        let header =
            std::str::from_utf8(&header).map_err(|error| CloneError::MalformedRemoteResponse {
                url: raw_url.to_owned(),
                operation: "parsing pack sideband response",
                detail: format!("pkt-line header was not UTF-8 hex: {error}"),
            })?;
        match header {
            "0000" | "0001" | "0002" => return Ok(None),
            _ => {}
        }
        let len = usize::from_str_radix(header, 16).map_err(|error| {
            CloneError::MalformedRemoteResponse {
                url: raw_url.to_owned(),
                operation: "parsing pack sideband response",
                detail: format!("pkt-line length `{header}` was invalid: {error}"),
            }
        })?;
        if len < 4 {
            return Err(CloneError::MalformedRemoteResponse {
                url: raw_url.to_owned(),
                operation: "parsing pack sideband response",
                detail: format!("pkt-line length `{len}` is smaller than its header"),
            });
        }
        self.data.clear();
        self.data.resize(len - 4, 0);
        reader
            .read_exact(&mut self.data)
            .map_err(|error| CloneError::RemoteDiscoveryFailed {
                url: raw_url.to_owned(),
                operation: "reading pack response pkt-line payload",
                detail: error.to_string(),
            })?;
        Ok(Some(&self.data))
    }
}

struct PackTrailerHasher {
    hasher: Sha1,
    trailer: [u8; 20],
    len: usize,
    cursor: usize,
}

impl PackTrailerHasher {
    fn new() -> Self {
        Self {
            hasher: Sha1::new(),
            trailer: [0u8; 20],
            len: 0,
            cursor: 0,
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        if self.len + bytes.len() <= 20 {
            self.trailer[self.len..self.len + bytes.len()].copy_from_slice(bytes);
            self.len += bytes.len();
            return;
        }

        let mut bytes = bytes;
        if self.len < 20 {
            let needed = 20 - self.len;
            self.trailer[self.len..20].copy_from_slice(&bytes[..needed]);
            self.len = 20;
            bytes = &bytes[needed..];
        }

        if bytes.len() >= 20 {
            let hash_len = bytes.len() - 20;
            self.flush_trailer();
            self.hasher.update(&bytes[..hash_len]);
            self.trailer.copy_from_slice(&bytes[hash_len..]);
            self.cursor = 0;
            return;
        }

        for &byte in bytes {
            if self.len < 20 {
                self.trailer[self.len] = byte;
                self.len += 1;
            } else {
                self.hasher.update([self.trailer[self.cursor]]);
                self.trailer[self.cursor] = byte;
                self.cursor = (self.cursor + 1) % 20;
            }
        }
    }

    fn flush_trailer(&mut self) {
        if self.cursor == 0 {
            self.hasher.update(self.trailer);
        } else {
            self.hasher.update(&self.trailer[self.cursor..]);
            self.hasher.update(&self.trailer[..self.cursor]);
        }
    }

    fn finish(self) -> Option<([u8; 20], [u8; 20])> {
        if self.len != 20 {
            return None;
        }
        let mut trailer = [0u8; 20];
        for (index, byte) in trailer.iter_mut().enumerate() {
            *byte = self.trailer[(self.cursor + index) % 20];
        }
        let actual = self.hasher.finalize();
        let mut actual_bytes = [0u8; 20];
        actual_bytes.copy_from_slice(&actual);
        Some((trailer, actual_bytes))
    }
}

fn validate_streaming_pack_checksum(
    raw_url: &str,
    pack_path: &Path,
    pack_bytes: u64,
    hasher: PackTrailerHasher,
) -> Result<[u8; 20], CloneError> {
    let Some((trailer, actual)) = hasher.finish() else {
        return Err(CloneError::MalformedRemoteResponse {
            url: raw_url.to_owned(),
            operation: "validating pack",
            detail: "response did not contain a complete PACK file".to_owned(),
        });
    };
    if pack_bytes < 32 {
        return Err(CloneError::MalformedRemoteResponse {
            url: raw_url.to_owned(),
            operation: "validating pack",
            detail: "response did not contain a complete PACK file".to_owned(),
        });
    }
    if trailer != actual {
        return Err(CloneError::PackChecksumMismatch {
            path: pack_path.to_owned(),
            expected: hex::encode(trailer),
            actual: hex::encode(actual),
        });
    }
    Ok(trailer)
}

fn enforce_max_pack_bytes(max_pack_bytes: Option<u64>, actual: u64) -> Result<(), CloneError> {
    if let Some(max_pack_bytes) = max_pack_bytes
        && actual > max_pack_bytes
    {
        return Err(CloneError::CloneLimitExceeded {
            operation: "receiving pack data",
            detail: format!(
                "FCL_MAX_PACK_BYTES is {max_pack_bytes} bytes, but received pack data reached {actual} bytes"
            ),
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

fn unique_oids(refs: &[RemoteRef]) -> Vec<&str> {
    let mut oids = refs
        .iter()
        .map(|remote_ref| remote_ref.oid.as_str())
        .collect::<Vec<_>>();
    oids.sort_unstable();
    oids.dedup();
    oids
}

fn discovery_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("fcl/0.1"));
    headers.insert("Git-Protocol", HeaderValue::from_static("version=2"));
    headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
    headers
}

fn upload_pack_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    let user_agent = std::env::var("FCL_USER_AGENT").unwrap_or_else(|_| "git/2.45.0".to_owned());
    headers.insert(
        USER_AGENT,
        HeaderValue::from_str(&user_agent).unwrap_or_else(|_| HeaderValue::from_static("fcl/0.1")),
    );
    headers.insert("Git-Protocol", HeaderValue::from_static("version=2"));
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/x-git-upload-pack-request"),
    );
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/x-git-upload-pack-result"),
    );
    headers
}

fn with_retries<T>(mut f: impl FnMut() -> Result<T, CloneError>) -> Result<T, CloneError> {
    let retries = env_usize("FCL_FETCH_RETRIES", 1);
    let mut attempt = 0usize;
    loop {
        match f() {
            Ok(value) => return Ok(value),
            Err(error) if attempt < retries => {
                attempt += 1;
                std::thread::sleep(Duration::from_millis(100 * attempt as u64));
                drop(error);
            }
            Err(error) => return Err(error),
        }
    }
}

fn env_bool(name: &str) -> bool {
    std::env::var(name)
        .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::{PacketReader, RemoteRef, fetch_body_with_remote_progress};

    #[test]
    fn fetch_body_should_request_no_progress_by_default() {
        let refs = [RemoteRef {
            oid: "0123456789012345678901234567890123456789".to_owned(),
            name: "refs/heads/main".to_owned(),
        }];

        let body = fetch_body_with_remote_progress(&refs, false);
        let body = String::from_utf8_lossy(&body);

        assert!(body.contains("no-progress\n"));
    }

    #[test]
    fn fetch_body_should_allow_remote_progress_for_debugging() {
        let refs = [RemoteRef {
            oid: "0123456789012345678901234567890123456789".to_owned(),
            name: "refs/heads/main".to_owned(),
        }];

        let body = fetch_body_with_remote_progress(&refs, true);
        let body = String::from_utf8_lossy(&body);

        assert!(!body.contains("no-progress\n"));
    }

    #[test]
    fn packet_reader_should_reuse_scratch_buffer() {
        let mut reader = PacketReader::new();
        let mut input = b"0009abcd\n0008efg\n0000".as_slice();

        let first = reader
            .read_packet("https://example.com/repo.git", &mut input)
            .expect("first packet should parse")
            .expect("first packet should exist")
            .as_ptr();
        let second = reader
            .read_packet("https://example.com/repo.git", &mut input)
            .expect("second packet should parse")
            .expect("second packet should exist")
            .as_ptr();
        let end = reader
            .read_packet("https://example.com/repo.git", &mut input)
            .expect("flush should parse");

        assert_eq!(first, second);
        assert!(end.is_none());
    }
}
