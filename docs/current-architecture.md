# Current Remote Clone Architecture

This document describes the current `fcl` remote clone architecture after removing the ref-based sharding experiment. The current implementation is close to `git clone` in shape and benchmark behavior: one full Git protocol fetch, one server-generated pack, streaming receive, streaming pack scan, overlapped resolve/index, and overlapped checkout.

## Product Semantics

The remote clone path creates a normal full Git repository:

```text
no shallow clone
no partial/promisor clone
no blob filter
no sparse checkout
no archive checkout
```

The resulting repository is expected to pass:

```bash
git fsck --full
git status --short
git diff --exit-code HEAD
```

## Top-Level Dispatch

Current dispatch in `crates/core/src/clone.rs`:

```text
clone_repo
  -> clone_repo_inner
    -> clone_repo_pipelined      default
    -> clone_repo_sequential     only with --no-pipeline or env override
```

There is no current sharded path in the code.

Pipeline selection:

```text
CloneRequest::new(...)
  -> pipeline: true

--no-pipeline
  -> disables pipeline for CLI clone

FCL_PIPELINE=1
  -> forces pipeline

FCL_DISABLE_PIPELINE=1
  -> disables pipeline
```

## Default Pipeline Call Graph

```text
clone_repo_pipelined
  -> http_client
  -> discover_remote
     -> GET info/refs?service=git-upload-pack
     -> POST command=ls-refs
  -> select_full_clone_universe
     -> refs/heads/*
     -> refs/tags/*
  -> resolve_default_branch
  -> FinalizingRepo::create
  -> RepoLayout::write_initial_metadata
  -> create PipelineObjectStore
  -> create sync_channel

  -> thread::scope
     -> fetch thread
        -> fetch_full_pack_pipelined
           -> POST command=fetch
           -> receive sideband pack stream
           -> write pack bytes to .git/objects/pack/fcl.pack
           -> scan object frames while bytes arrive
           -> send PipelineEvent::Frames

     -> resolver thread
        -> ingest_pack_pipeline
           -> receive object frames
           -> resolve base objects
           -> resolve offset deltas
           -> resolve ref deltas
           -> publish objects to PipelineObjectStore
           -> write .git/objects/pack/fcl.idx

     -> main thread
        -> materialize_default_branch
           -> read commit/tree/blob objects from PipelineObjectStore
           -> wait when required objects are not ready
           -> write working tree
           -> write .git/index

  -> finalize staged repo
  -> measure target size and RSS
  -> return CloneReport
```

## Pipeline Dataflow

```text
GitHub upload-pack
  -> sideband pkt-line response
    -> pack bytes written to disk
    -> StreamingPackScanner
      -> ObjectFrame batches
        -> channel
          -> PipelineResolver
            -> PipelineObjectStore
              -> checkout
```

The important property is overlap:

```text
fetch
pack scan
delta resolution/indexing
checkout
```

These run concurrently in the default path. The total wall time is mostly the longest overlapped critical path, not the sum of the phase timings.

## Protocol Shape

Current Smart HTTP behavior in `crates/core/src/protocol/smart_http.rs`:

```text
GET  /info/refs?service=git-upload-pack
POST /git-upload-pack command=ls-refs
POST /git-upload-pack command=fetch
```

Fetch request shape:

```text
command=fetch
agent=fcl/0.1
no-progress              default unless FCL_REMOTE_PROGRESS is set
thin-pack
ofs-delta
want <oid>
want <oid>
...
done
```

The selected wants are unique OIDs from:

```text
refs/heads/*
refs/tags/*
```

This keeps `fcl` close to normal full `git clone` semantics and lets the server produce one globally optimized pack.

## Pack Pipeline

Current pack components in `crates/core/src/pack/mod.rs`:

```text
StreamingPackScanner
  -> parses pack header
  -> parses object headers
  -> tracks pack offsets
  -> tracks compressed ranges
  -> calculates CRCs
  -> optionally carries inflated payloads
  -> emits ObjectFrame values

PipelineResolver
  -> receives ObjectFrame batches
  -> resolves base objects
  -> resolves offset deltas
  -> resolves ref deltas
  -> tracks pending deltas by offset/OID
  -> publishes resolved objects
  -> writes idx v2
```

Object state model:

```text
Resident
Spilled
Reconstructable
```

The pipeline object store exposes:

```text
read_object
stream_blob
```

Checkout can ask for objects before the pack is fully resolved. If an object is not ready, checkout waits on the pipeline store.

## Checkout

Current checkout path in `crates/core/src/checkout/mod.rs`:

```text
materialize_default_branch
  -> read commit object
  -> parse root tree OID
  -> recursively collect tree manifest
  -> create directories
  -> materialize files in parallel with rayon
  -> stream regular blob contents from ObjectReader
  -> create symlinks from blob contents
  -> stat materialized files
  -> write .git/index v2
```

The checkout manifest does not store regular file blob bytes. It stores:

```text
path
git path
mode
object id
```

File contents are streamed from the object reader.

## Repository Layout And Finalization

Current repo writer in `crates/core/src/repo/mod.rs` stages the clone first:

```text
.<target>.fcl-staging-<pid>-<timestamp>/
  .git/
    config
    HEAD
    packed-refs
    refs/heads/<default>
    objects/pack/fcl.pack
    objects/pack/fcl.idx
    index
  working tree files
```

The staging directory is renamed to the final target only after successful clone finalization. If clone fails before commit, the staging directory is cleaned up by `FinalizingRepo` drop behavior.

## Metrics Emitted

`CloneReport` records:

```text
total_ms
discovery_ms
fetch_ms
ingest_ms
checkout_ms
finalize_ms
fetch_request_ms
fetch_first_byte_ms
fetch_sideband_read_ms
fetch_pack_write_ms
fetch_pack_flush_ms
fetch_checksum_ms
fetch_frame_send_wait_ms
pack_receive_bytes_per_sec
pack_scan_ms
pack_resolve_ms
pack_idx_write_ms
pack_object_state_ms
pack_object_count
pack_base_object_count
pack_delta_count
pack_offset_delta_count
pack_ref_delta_count
pack_declared_inflated_bytes
checkout_manifest_ms
checkout_dir_create_ms
checkout_file_materialize_ms
checkout_index_write_ms
pipeline_frame_count
pipeline_checkout_wait_ms
pipeline_checkout_wait_count
pipeline_checkout_wait_max_ms
pipeline_peak_pending_delta_count
pipeline_resolver_wall_ms
pipeline_resolver_wait_for_frame_ms
pipeline_queue_peak_depth
pipeline_arena_spill_bytes
rss_bytes
```

The CLI prints these with `--stats`.

The benchmark command can compare against `git clone`:

```bash
fcl bench <url> --runs N --compare-git --validate --csv
```

With `--git-trace2`, the benchmark runner captures Git Trace2 timings for:

```text
git remote phase
git index-pack phase
git checkout phase
```

## Current Benchmark Interpretation

The current code has been observed at roughly `1:1` with normal `git clone`, sometimes around `1%` better.

That result matches the current architecture:

```text
one full clone request
one optimized remote pack
streamed pack receive
overlapped local resolve/index
overlapped checkout
normal final Git repo
```

`fcl` is not changing clone semantics to win. It is close to Git's shape and competes mostly on local pipeline scheduling and implementation overhead.

## Workerd Trace Context

From the previous Workerd default pipeline run:

```text
total: 7.454s
fetch: 6.500s
resolve/index pipeline: 6.590s
checkout: 6.724s
actual checkout file materialization: 0.216s
finalize: 0.011s
pack bytes: 48.7 MiB
objects: 109,622
deltas: 84,676
RSS: 1.00 GiB
validated: yes
```

Trace shape:

```text
discover        [0.712s]
fetch           [================ 6.500s ================]
resolve/index   [================ 6.590s ================]
checkout        [================= 6.724s =================]
finalize                                                 [0.011s]
```

The large checkout time is mostly waiting for objects to become available from the pipeline. The actual file writes are small.

## Architectural Summary

Current `fcl` is competitive with `git clone` because it follows the same full-clone server interaction while overlapping local work:

```text
GitHub produces one pack
fcl streams that pack once
scanner emits frames as bytes arrive
resolver publishes objects as they resolve
checkout consumes objects as soon as possible
```

This is the current baseline architecture.
