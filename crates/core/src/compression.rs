pub const fn compression_backend() -> &'static str {
    if cfg!(feature = "flate2-zlib-rs") {
        "zlib-rs"
    } else if cfg!(feature = "flate2-zlib-ng") {
        "zlib-ng"
    } else if cfg!(feature = "flate2-zlib-ng-compat") {
        "zlib-ng-compat"
    } else if cfg!(feature = "flate2-zlib-default") {
        "zlib-default"
    } else if cfg!(feature = "flate2-zlib") {
        "zlib"
    } else if cfg!(feature = "flate2-miniz-oxide") {
        "miniz_oxide"
    } else if cfg!(feature = "flate2-rust") {
        "rust_backend"
    } else {
        "unknown"
    }
}

#[cfg(test)]
mod tests {
    use super::compression_backend;

    #[test]
    fn compression_backend_should_match_enabled_feature() {
        assert_ne!(compression_backend(), "unknown");
    }
}
