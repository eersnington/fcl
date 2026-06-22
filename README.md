# fcl

> **Warning: experimental repository**
>
> `fcl` is not ready for general use. The clone format it writes is a normal Git repository, but the implementation, CLI output, benchmark numbers, and supported platforms may change without notice.

`fcl` is a Rust implementation of full Git clone.

- native Smart HTTP Git protocol v2
- full branch and tag fetches
- native pack receive, indexing, and delta resolution
- streaming checkout from pack-backed object storage
- local copy-on-write clone mode for existing repos
- benchmark harness against `git clone`

No shallow clone. No default blob filter. No archive-only checkout.

## Install

```bash
cargo build -p fcl --release
```

The binary is written to:

```bash
target/release/fcl
```

## Platforms

| Platform | Remote clone | `fcl local` backend | Notes |
| --- | --- | --- | --- |
| macOS | HTTPS Git protocol v2 | APFS clonefile via `reflink-copy` | Tested during development. |
| Linux | HTTPS Git protocol v2 | `FICLONE` via `reflink-copy` | Requires reflink-capable filesystem for `fcl local`. |
| Windows | Not supported | Not supported | No Windows path yet. |

## CLI

### Remote clone

```bash
fcl https://github.com/octocat/Hello-World.git hello
```

The target is a regular Git repository:

```bash
git -C hello fsck --full
git -C hello status --short
git -C hello diff --exit-code HEAD
```

Remote clone currently supports public HTTPS remotes that speak Git protocol v2. SSH URLs and private repositories are not supported yet.

### Local copy-on-write clone

```bash
fcl local ./source-repo ./target-repo
```

`fcl local` copies an existing repository using filesystem copy-on-write. It is for cases where the repo is already on disk and another self-contained copy is needed quickly.

Current behavior:

- requires a `.git` directory in the source
- rejects linked worktrees with `.git` files
- rejects merge, rebase, cherry-pick, revert, bisect, and Git lock states
- requires source and target on the same filesystem
- rejects special filesystem entries
- does not fall back to slow recursive copy

### Benchmark

```bash
fcl bench https://github.com/cloudflare/workerd.git --runs 1 --compare-git --validate --csv
```

`--validate` runs:

```bash
git fsck --full
git status --short
git diff --exit-code HEAD
```

## Early benchmark

Single macOS sample against `cloudflare/workerd`:

| Tool | Time |
| --- | ---: |
| `fcl` | 9.42s |
| `git clone` | 9.37s |

`fcl` phase breakdown from the same run:

| Phase | Time |
| --- | ---: |
| discovery | 0.70s |
| fetch | 5.99s |
| ingest | 2.47s |
| checkout | 0.13s |

Workload:

```text
repo: cloudflare/workerd
pack: 51.3 MB
files checked out: 2365
directories created: 259
```

Remote clone timings vary a lot. Run multiple samples before comparing changes.

## Storage model

Remote clone path:

```text
discover refs
  -> fetch pack
  -> validate pack checksum
  -> parse pack frames
  -> resolve deltas
  -> write .idx
  -> write refs/config/HEAD
  -> build checkout manifest
  -> stream blobs into working tree
  -> write .git/index
```

Checkout does not carry regular file blob bytes in the manifest. Manifest entries store path, mode, object id, and size. File contents are streamed from the object reader.

Object data states:

```text
Resident
Spilled
Reconstructable
```

Blobs are usually reconstructable from pack metadata. `FCL_SPILL_BLOBS=1` can be used to exercise spill storage.

## Environment

Safety caps:

```bash
FCL_MAX_PACK_BYTES=200000000
FCL_MAX_OBJECTS=500000
FCL_MAX_TEMP_BYTES=1000000000
FCL_MAX_SPILL_BYTES=1000000000
```

Checkout and object storage:

```bash
FCL_CHECKOUT_JOBS=8
FCL_OBJECT_CACHE_BYTES=536870912
FCL_SPILL_BLOBS=1
FCL_SPILL_DIR=/tmp/fcl-spill
FCL_LOW_MEMORY=1
```

HTTP:

```bash
FCL_HTTP1_ONLY=1
FCL_HICKORY_DNS=1
FCL_NO_PROGRESS=1
FCL_FETCH_RETRIES=2
FCL_PACK_WRITE_BUFFER=1048576
FCL_USER_AGENT=git/2.45.0
FCL_CONNECT_TIMEOUT_SECS=10
FCL_REQUEST_TIMEOUT_SECS=300
FCL_POOL_IDLE_TIMEOUT_SECS=4
```

## Repository layout

- `crates/core`: protocol, pack ingest, object reader, checkout, local CoW clone.
- `crates/cli`: CLI and benchmark runner.

## Development

```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

See [`specs.md`](./specs.md) for requirements and call graphs.
