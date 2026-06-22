use std::time::Instant;

#[derive(Debug)]
pub struct CloneReport {
    pub ref_count: usize,
    pub pack_bytes: u64,
    pub total_ms: u128,
    pub discovery_ms: u128,
    pub fetch_ms: u128,
    pub ingest_ms: u128,
    pub pack_scan_ms: u128,
    pub pack_resolve_ms: u128,
    pub pack_idx_write_ms: u128,
    pub pack_object_state_ms: u128,
    pub streaming_pack_scan: bool,
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

pub fn measure_ms<T>(operation: impl FnOnce() -> T) -> (T, u128) {
    let start = Instant::now();
    let result = operation();
    (result, start.elapsed().as_millis())
}
