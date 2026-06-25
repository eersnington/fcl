# Ref-Based Sharding Findings

This document records the deleted ref-based sharding experiment and the problems it validated. The experiment preserved normal full-clone semantics, but it was architecturally worse than the current one-pack streaming pipeline.

## Approach Tested

The sharded path split the full clone by refs:

```text
clone_repo_sharded
  -> discover refs
  -> select full branch/tag universe
  -> resolve default branch
  -> fetch default branch as seed pack
  -> ingest seed pack
  -> remove refs already pointing at seed OID
  -> partition residual refs by unique OID
  -> fetch residual shard packs in parallel
  -> ingest each residual shard pack
  -> build PackSet from all pack indexes
  -> checkout default branch after all packs are ready
  -> finalize normal Git repo
```

The important shape was:

```text
seed first
residual shards second
checkout last
```

This differs from the current default pipeline, which overlaps fetch, resolve/index, and checkout against one server-generated pack.

## Semantics Validated

The sharded clones remained normal full Git repositories. The experiment did not rely on shallow, partial, promisor, sparse, archive-only, or hosted-mirror semantics.

Validation used:

```bash
git fsck --full
git status --short
git diff --exit-code HEAD
```

Repos validated during the experiment:

- `octocat/Hello-World`
- `cloudflare/workerd`
- `rivet-dev/rivet`

## Hello-World Results

`octocat/Hello-World` only acted as a correctness and protocol smoke test. It was too small to provide useful performance signal.

| Mode | Total | Pack Bytes | Pack Files | Duplicates | Validated |
|---|---:|---:|---:|---:|---|
| default | `0.860s` | `1,586 B` | `1` | `0` | yes |
| sharded `N=4` | `1.068s` | `1,650 B` | `3` | `0` | yes |

Finding:

```text
Hello-World proved the basic multi-pack/protocol path could be correct.
It did not prove anything meaningful about performance.
```

## Workerd Results

Fresh `cloudflare/workerd` matrix from the sharded experiment:

| Mode | Total | Fetch Wall | Seed | Residual | Pack Bytes | Pack Files | Duplicates | Dup Ratio | Objects | Deltas | RSS | Validated |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| default | `7.454s` | `6.500s` | n/a | n/a | `48.7 MiB` | `1` | `0` | n/a | `109,622` | `84,676` | `1.00 GiB` | yes |
| sharded `N=2` | `10.713s` | `10.029s` | `6.351s` | `3.677s` | `54.7 MiB` | `3` | `2,404` | `2.14%` | `112,026` | `84,287` | `1.06 GiB` | yes |
| sharded `N=4` | `11.720s` | `11.085s` | `7.227s` | `3.858s` | `59.5 MiB` | `5` | `3,657` | `3.22%` | `113,279` | `84,330` | `0.93 GiB` | yes |
| sharded `N=8` | `10.587s` | `9.876s` | `6.328s` | `3.548s` | `65.5 MiB` | `9` | `4,227` | `3.71%` | `113,849` | `83,655` | `1.06 GiB` | yes |

Shard spread:

| Mode | Min Pack | Max Pack | Min Objects | Max Objects |
|---|---:|---:|---:|---:|
| sharded `N=2` | `6.5 MiB` | `40.9 MiB` | `11,054` | `88,739` |
| sharded `N=4` | `3.8 MiB` | `41.0 MiB` | `4,628` | `88,739` |
| sharded `N=8` | `2.5 MiB` | `40.8 MiB` | `2,339` | `88,739` |

Workerd trace shape:

```text
default total: 7.454s

discover        [0.712s]
fetch           [================ 6.500s ================]
resolve/index   [================ 6.590s ================]
checkout        [================= 6.724s =================]
finalize                                                 [0.011s]
```

Those default phases overlapped.

```text
sharded N=8 total: 10.587s

discover        [0.558s]
seed            [============= 6.328s =============]
residuals                                           [======= 3.548s =======]
checkout                                                                    [0.134s]
finalize                                                                            [0.011s]
```

Finding:

```text
The seed alone was almost as expensive as the default clone's critical path.
Residual shards then added another serial phase.
More shards increased bytes and duplicate objects.
```

## Rivet Results

Historical `rivet-dev/rivet` sharded result:

| Mode | Total | Pack Bytes | Pack Files | Objects | Duplicates | RSS | Validated |
|---|---:|---:|---:|---:|---:|---:|---|
| default pipeline | `62.540s` | `401.0 MiB` | `1` | `220,157` | `0` | `1.21 GiB` | yes |
| seeded sharded `N=4` | `98.017s` | `680.7 MiB` | `5` | `500,249` | `280,092` | `1.47 GiB` | yes |

Rivet showed the failure mode strongly:

```text
+279.7 MiB pack bytes
+280,092 duplicate objects
+35.477s wall time
+0.26 GiB RSS
```

## Architectural Findings

The performance assumption behind ref-based sharding was incorrect:

```text
incorrect assumption:
  refs can be split into independent chunks and fetched faster in parallel

observed behavior:
  refs share object graph history, trees, blobs, and delta bases
```

A normal GitHub full clone receives one globally optimized pack for the selected reachable object graph. Ref-based sharding asks GitHub for several locally optimized packs over overlapping graph regions.

That created:

- more pack bytes
- duplicate objects
- duplicate scan/resolve/hash work
- extra memory pressure
- seed/residual serialization
- loss of streaming fetch/resolve/checkout overlap

## What Did Not Work

Ref-based sharding did not work as a performance strategy.

Increasing shard count did not fix the shape:

```text
Workerd N=2: 54.7 MiB, 2,404 duplicates, 10.713s
Workerd N=4: 59.5 MiB, 3,657 duplicates, 11.720s
Workerd N=8: 65.5 MiB, 4,227 duplicates, 10.587s
```

Post-fetch compaction did not explain or fix the slowdown. By the time compaction can run, the expensive work has already happened:

```text
server pack generation
network transfer
pack writes
checksum work
pack scan
delta resolution
object indexing
```

Final pack count was not the root problem. The problem was asking the server for multiple overlapping packs.

Checkout file materialization was not the bottleneck. In the Workerd traces, actual file writes were small compared to fetch and resolve timing.

## Conclusion

The ref-based sharding experiment validated correctness but invalidated the performance assumption. It preserved full-clone semantics, but it fought Git's object graph and GitHub's global pack optimization.

The measured bottleneck was not final repository layout. It was earlier:

```text
repeated server pack generation
duplicate object transfer
duplicate local object processing
serialized seed/residual phases
lost streaming overlap
```
