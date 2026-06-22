# fcl specs

## Product goal

`fcl` should create a normal, self-contained Git repository faster than `git clone` for the cases it supports.

The clone must keep full Git semantics. If a repository is cloned remotely, the resulting repo needs the full selected branch and tag history, a valid object database, normal refs, a normal index, and a checked out default branch.

## Non-goals

- No hosted mirror service.
- No daemon requirement for the default clone path.
- No shallow clone as the default behavior.
- No partial clone or blob filter as the default behavior.
- No archive-only clone pretending to be Git.
- No cross-sandbox cache requirement.
- No Git LFS support for now.

## Required commands

Remote clone:

```txt
fcl <https-url> [target]
```

Local copy-on-write clone:

```txt
fcl local <source-path> [target]
```

Benchmark:

```txt
fcl bench <https-url> --runs N [--compare-git] [--validate] [--csv | --json]
```

## Remote clone requirements

- Accept HTTPS Git URLs.
- Reject unsupported URL schemes with a clear error.
- Discover refs using Smart HTTP Git protocol v2.
- Fetch all branch and tag refs selected by `refs/heads/*` and `refs/tags/*`.
- Write pack data to disk while receiving it.
- Validate the pack checksum.
- Resolve base objects, offset deltas, and ref deltas.
- Write a Git `.idx` v2 file.
- Write `.git/config`, `HEAD`, local branch refs, remote-tracking refs, and tags.
- Check out the remote default branch.
- Write a Git `.git/index` file.
- Leave the target as a normal Git repo that passes `git fsck --full`.

Remote clone call graph:

```txt
fcl <url> [target]
  -> clone_repo
     -> http_client
     -> discover_remote
        -> parse HTTPS URL
        -> GET info/refs?service=git-upload-pack
        -> ls-refs
     -> select_full_clone_universe
     -> RepoLayout::create
     -> fetch_full_pack
        -> POST git-upload-pack fetch request
        -> parse sideband response
        -> stream pack bytes to .git/objects/pack/fcl.pack
     -> ingest_pack
     -> write_initial_metadata
     -> materialize_default_branch
     -> return CloneReport
```

## Pack ingest requirements

- Parse pack headers and object frames.
- Track compressed ranges and pack offsets for every object.
- Compute Git object IDs from object type and content.
- Write sorted `.idx` entries with CRCs and offsets.
- Keep metadata separately from object bytes.
- Keep structural objects resident when useful.
- Treat regular blob bytes as reconstructable from pack metadata unless explicitly spilled.
- Release resolved blob payloads when their known delta children are done.
- Support safety limits for pack bytes, object count, temp bytes, cache bytes, and spill bytes.

Pack ingest call graph:

```txt
ingest_pack(pack_path, index_path)
  -> read pack bytes
  -> scan_and_inflate_pack | scan_pack_metadata
     -> validate_pack
     -> parse_object_header
     -> parse delta base information
     -> record compressed ranges
  -> resolve_inflated_frames
     -> build_delta_adjacency
     -> resolve base objects
     -> apply deltas in dependency order
     -> release blob payloads when liveness reaches zero
  -> write_idx_v2
  -> build_object_states
  -> PackIndex/ObjectReader
```

Object reading contract:

```rust
trait ObjectReader {
    fn get_meta(&self, oid: ObjectId) -> Option<&ObjectMeta>;
    fn read_object(&self, oid: ObjectId) -> Result<ObjectBytes, CloneError>;
    fn stream_blob(&self, oid: ObjectId, out: &mut dyn Write) -> Result<u64, CloneError>;
}
```

Object state model:

```txt
ObjectDataState
  -> Resident(bytes)
  -> Spilled(path, len)
  -> Reconstructable
```

## Checkout requirements

- Build the checkout manifest from commit and tree objects.
- Do not store regular file blob bytes in manifest entries.
- Store path, mode, object ID, and size for each checkout entry.
- Create directories before file writes.
- Stream regular file contents from `ObjectReader::stream_blob`.
- Read symlink blobs into memory only for symlink target creation.
- Write executable permissions on Unix.
- Stat files once after materialization for index entries.
- Write `.git/index` v2.

Checkout call graph:

```txt
materialize_default_branch(repo, object_reader, remote_refs, selected_refs)
  -> parse_commit_tree_oid
  -> collect_tree_manifest
     -> read tree objects
     -> append directories
     -> append file entries { path, mode, oid, size }
  -> create directories
  -> materialize_manifest
     -> materialize_manifest_entry
        -> create file
        -> stream_blob
        -> chmod executable when needed
        -> create symlink when needed
        -> stat path
  -> write_git_index
```

## Local clone requirements

`fcl local` is a separate command. It optimizes the case where the source repository already exists on disk and the bottleneck is filesystem work.

Requirements:

- Require a source `.git` directory.
- Reject linked worktrees that use a `.git` file.
- Reject merge, rebase, cherry-pick, revert, bisect, and Git lock states.
- Require source and target to be on the same filesystem.
- Use filesystem copy-on-write where available.
- Preserve a self-contained `.git` directory in the target.
- Reject unsupported special files.
- Do not silently fall back to slow recursive copy.

Local clone call graph:

```txt
fcl local <source> [target]
  -> local_clone
     -> canonicalize source
     -> inspect_source_repo
        -> require .git directory
        -> reject in-progress Git state
     -> choose target
     -> ensure_same_device
     -> copy_tree_cow
        -> create directories
        -> clone files with reflink-copy
        -> recreate symlinks
        -> reject special files
     -> return LocalCloneReport
```

Platform behavior:

```txt
macOS: reflink-copy uses APFS clonefile when supported
Linux: reflink-copy uses FICLONE when supported
other: unsupported for now
```

## Benchmark requirements

- Run `fcl` one or more times.
- Optionally run `git clone` after each `fcl` run.
- Optionally validate each output repo with Git.
- Emit human-readable output by default.
- Emit CSV or JSON when requested.
- Include clone phase timing and checkout sub-phase timing for `fcl`.

Benchmark call graph:

```txt
fcl bench <url>
  -> run_bench
     -> remove_target(fcl)
     -> clone_repo
     -> validate_repo when requested
     -> print fcl result
     -> run git clone when requested
     -> validate_repo when requested
     -> print git result
```

Validation commands:

```txt
git fsck --full
git status --short
git diff --exit-code HEAD
```

## Metrics

Remote clone reports:

```txt
total_ms
discovery_ms
fetch_ms
ingest_ms
checkout_ms
checkout_manifest_ms
checkout_dir_create_ms
checkout_file_materialize_ms
checkout_index_write_ms
checkout_file_count
checkout_dir_count
checkout_blob_bytes
pack_bytes
ref_count
retained_object_count
retained_object_bytes
spilled_object_count
spilled_object_bytes
reconstructed_object_count
target_bytes
rss_bytes
```

Local clone reports:

```txt
strategy
total_ms
file_count
dir_count
symlink_count
bytes
```

## Safety requirements

- Refuse to overwrite an existing target directory.
- Fail early when configured byte or object limits are exceeded.
- Make cleanup guidance explicit when a target contains only temporary clone data.
- Preserve enough stage metrics to know whether time went to network, pack ingest, checkout, or local filesystem work.

Safety environment variables:

```txt
FCL_MAX_PACK_BYTES
FCL_MAX_OBJECTS
FCL_MAX_TEMP_BYTES
FCL_OBJECT_CACHE_BYTES
FCL_MAX_SPILL_BYTES
FCL_SPILL_DIR
```

## Performance direction

Remote clones are often fetch-bound. `fcl` should still optimize network behavior, but only after the storage path is solid enough to make local work visible and measurable.

The main performance work should focus on:

- bounded pack ingest memory;
- fewer redundant inflations;
- better delta-base lifetime tracking;
- checkout write scheduling by filesystem behavior;
- APFS, ext4, btrfs, tmpfs, and Firecracker filesystem measurements;
- local CoW clone reliability across supported filesystems.

## Correctness bar

A supported clone is not done until these pass:

```txt
git -C <target> fsck --full
git -C <target> status --short
git -C <target> diff --exit-code HEAD
```

The final repository must not depend on `fcl` to be usable by Git.
