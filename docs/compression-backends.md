# Compression Backends

`fcl` uses `flate2` for Git pack zlib streams. Backend selection is compile-time and exposed in clone and benchmark output as `compression_backend`.

## Backends

Configured features:

| Feature | `flate2` backend |
|---|---|
| `flate2-rust` | `rust_backend` |
| `flate2-miniz-oxide` | `miniz_oxide` |
| `flate2-zlib-rs` | `zlib-rs` |
| `flate2-zlib-ng` | `zlib-ng` |
| `flate2-zlib-ng-compat` | `zlib-ng-compat` |
| `flate2-zlib-default` | `zlib-default` |
| `flate2-zlib` | `zlib` |

`cloudflare_zlib` is not exposed by the currently resolved `flate2 1.1.9`, so it is excluded from the current benchmark matrix unless a compatible `flate2` release adds that feature.

## Benchmarking

Run the backend matrix with:

```bash
scripts/bench-flate2-backends.sh \
  --runs 2 \
  --out target/fcl-backend-bench \
  https://github.com/pnpm/pacquet.git
```

The script builds each backend with `--no-default-features --features <backend>`, runs sequential and `FCL_PIPELINE=1` clone benchmarks, validates each clone, and writes `combined.csv`.

Recommended matrix:

```bash
scripts/bench-flate2-backends.sh \
  --runs 1 \
  --out target/fcl-backend-bench-workerd \
  https://github.com/cloudflare/workerd.git
```

## Selection Rules

Choose a default by this order:

1. All clone validation must pass.
2. Prefer lower median end-to-end `total_ms` on medium and large repositories.
3. Use `pack_scan_ms` and `pack_resolve_ms` to confirm the win is decompression-related.
4. Reject backends with unacceptable `rss_bytes` regressions.
5. Prefer portable defaults when results are close.

## Initial Results

Measured on macOS with release builds and clone validation enabled.

`pnpm/pacquet`, one run per backend:

| Backend | Sequential `total_ms` | Pipeline `total_ms` | Sequential scan/resolve | Pipeline checkout wait |
|---|---:|---:|---:|---:|
| `rust_backend` | 1648 | 1639 | 44 / 14 | 1010 |
| `miniz_oxide` | 1678 | 2329 | 44 / 13 | 1781 |
| `zlib-rs` | 1631 | 1383 | 25 / 35 | 882 |
| `zlib-ng` | 1509 | 1573 | 28 / 13 | 926 |
| `zlib-ng-compat` | 2098 | 1398 | 28 / 14 | 886 |
| `zlib` | 1513 | 1448 | 19 / 13 | 943 |

`cloudflare/workerd`, one run for top candidates:

| Backend | Sequential `total_ms` | Pipeline `total_ms` | Sequential scan/resolve | Pipeline checkout wait |
|---|---:|---:|---:|---:|
| `rust_backend` | 9502 | 6780 | 542 / 534 | 6369 |
| `zlib-rs` | 7797 | 7183 | 319 / 531 | 6674 |
| `zlib` | 7970 | 6849 | 249 / 378 | 6201 |

## Decision

The default backend is `flate2-zlib` for now.

Rationale:

- It was the fastest sequential backend on both measured repositories.
- It had the best large-repo `pack_scan_ms` and `pack_resolve_ms` among top candidates.
- Its pipeline result on `workerd` was close to `rust_backend` while using less RSS in that run.
- It is widely available on supported Unix-like platforms.

This choice should be revisited after the Milestone E dedicated resolve pool lands, because current pipeline checkout wait time is still dominated by resolver availability.
