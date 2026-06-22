use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct CloneMetrics {
    start: Instant,
    pub ref_count: usize,
    pub pack_bytes: u64,
    pub discovery_ms: u128,
    pub fetch_ms: u128,
    pub ingest_ms: u128,
    pub checkout_ms: u128,
    pub checkout_manifest_ms: u128,
    pub checkout_dir_create_ms: u128,
    pub checkout_file_materialize_ms: u128,
    pub checkout_index_write_ms: u128,
    pub checkout_file_count: usize,
    pub checkout_dir_count: usize,
    pub checkout_blob_bytes: u64,
    pub retained_object_count: usize,
    pub retained_object_bytes: usize,
    pub spilled_object_count: usize,
    pub spilled_object_bytes: usize,
    pub reconstructed_object_count: usize,
    pub target_bytes: u64,
    pub rss_bytes: Option<u64>,
}

impl CloneMetrics {
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
            ref_count: 0,
            pack_bytes: 0,
            discovery_ms: 0,
            fetch_ms: 0,
            ingest_ms: 0,
            checkout_ms: 0,
            checkout_manifest_ms: 0,
            checkout_dir_create_ms: 0,
            checkout_file_materialize_ms: 0,
            checkout_index_write_ms: 0,
            checkout_file_count: 0,
            checkout_dir_count: 0,
            checkout_blob_bytes: 0,
            retained_object_count: 0,
            retained_object_bytes: 0,
            spilled_object_count: 0,
            spilled_object_bytes: 0,
            reconstructed_object_count: 0,
            target_bytes: 0,
            rss_bytes: None,
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

#[derive(Debug)]
pub struct CloneReport {
    pub ref_count: usize,
    pub pack_bytes: u64,
    pub total_ms: u128,
    pub discovery_ms: u128,
    pub fetch_ms: u128,
    pub ingest_ms: u128,
    pub checkout_ms: u128,
    pub checkout_manifest_ms: u128,
    pub checkout_dir_create_ms: u128,
    pub checkout_file_materialize_ms: u128,
    pub checkout_index_write_ms: u128,
    pub checkout_file_count: usize,
    pub checkout_dir_count: usize,
    pub checkout_blob_bytes: u64,
    pub retained_object_count: usize,
    pub retained_object_bytes: usize,
    pub spilled_object_count: usize,
    pub spilled_object_bytes: usize,
    pub reconstructed_object_count: usize,
    pub target_bytes: u64,
    pub rss_bytes: Option<u64>,
}

impl From<CloneMetrics> for CloneReport {
    fn from(metrics: CloneMetrics) -> Self {
        Self {
            ref_count: metrics.ref_count,
            pack_bytes: metrics.pack_bytes,
            total_ms: metrics.elapsed().as_millis(),
            discovery_ms: metrics.discovery_ms,
            fetch_ms: metrics.fetch_ms,
            ingest_ms: metrics.ingest_ms,
            checkout_ms: metrics.checkout_ms,
            checkout_manifest_ms: metrics.checkout_manifest_ms,
            checkout_dir_create_ms: metrics.checkout_dir_create_ms,
            checkout_file_materialize_ms: metrics.checkout_file_materialize_ms,
            checkout_index_write_ms: metrics.checkout_index_write_ms,
            checkout_file_count: metrics.checkout_file_count,
            checkout_dir_count: metrics.checkout_dir_count,
            checkout_blob_bytes: metrics.checkout_blob_bytes,
            retained_object_count: metrics.retained_object_count,
            retained_object_bytes: metrics.retained_object_bytes,
            spilled_object_count: metrics.spilled_object_count,
            spilled_object_bytes: metrics.spilled_object_bytes,
            reconstructed_object_count: metrics.reconstructed_object_count,
            target_bytes: metrics.target_bytes,
            rss_bytes: metrics.rss_bytes,
        }
    }
}

pub fn measure_ms<T>(operation: impl FnOnce() -> T) -> (T, u128) {
    let start = Instant::now();
    let result = operation();
    (result, start.elapsed().as_millis())
}
