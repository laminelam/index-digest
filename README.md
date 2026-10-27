# Hierarchical Index Digest (HID) for OpenSearch

Hierarchical Index Digest (HID) provides incremental integrity verification for
OpenSearch indices. It builds a hierarchical hash tree (Merkle-style) over
documents ordered by an externally supplied monotonic version field (default
`version_ts`), seals immutable external-version windows as data ages, and lets
a comparison API detect and **localize** divergence between two indices — down
to the exact leaf bucket — in O(log n) probes instead of a full scan.

RFC: [opensearch-project/OpenSearch#20889](https://github.com/opensearch-project/OpenSearch/issues/20889)

## How it works

- Every document carries an external version field: a **monotonically
  increasing long** assigned by the producer (never by OpenSearch). The
  default field name is `version_ts`. Updates get a fresh, higher external
  version.
- Documents are bucketed by `bucketId = (externalVersion >> level) << level`.
  Leaves live at `min_level` (default 10 → 1024 versions per leaf), parents
  reduce up to `max_level` (default 20 → ~1M versions per top window).
- A background scanner **seals** whole top windows once the observed version
  frontier has moved `guard_windows` (default 1) windows past them. Sealed
  digests are immutable: new data only ever lands in newer windows, so
  steady-state work is proportional to *new* data, not index size.
- Trees are sparse (only non-empty buckets exist) and are stored per shard
  copy in the hidden system index `.index_digest`.
- **Deletes** are captured from the engine's tombstones as durable *delete
  events* on the same version axis, folded into leaf digests, so a delete
  missed by one side surfaces as ordinary, localizable divergence.

## Producer contract

1. The configured external version field, default `version_ts`, is a
   non-negative long, monotonically increasing across all writes the producer
   emits. Generating it from a **hybrid logical clock** is strongly
   recommended: it keeps the sequence monotonic across producer failover and
   clock skew.
2. **Deletes must carry the next version** on the same axis, via external
   versioning — this is the only channel a delete can carry the ordering
   value:

   ```
   DELETE /my-index/_doc/X?version=<new external version>&version_type=external
   ```

3. The field must be mapped as `long` (indexed + doc values, the defaults).

Contract violations are detected, not silently absorbed: a write or delete
below the sealed watermark triggers a logged reseal of only the affected
window(s) of that shard copy's tree (`reason=late_write` /
`reason=late_delete_event`); the rest of the tree is never rebuilt.

## Supported comparison modes

- **Primary vs primary** (`scan_mode: primary_only`, the default): fully
  supported under live traffic. This is the production configuration.
- **Replica comparisons** (document-replication indices only): supported when
  **all writes — including deletes — are quiesced** and one scan tick has
  completed per copy after quiescing. Once the global checkpoint catches up,
  no operation can roll back and replica trees converge to the primary's.
  Comparison covers data **up to the sealed watermark**; the final
  `guard_windows` + open window of version-space remain unverified while the
  frontier is stalled.
  Under live traffic, replica comparisons are **advisory only**: transient
  divergence near the frontier (replication lag vs. seal timing) is expected.
- **Segment-replication and remote-store replicas are never scanned**: their
  replication engine does not satisfy the plugin's checkpoint contract.
  Primaries of such indices are scanned normally.

## Self-healing behavior

The tool's rule is: *report honestly, heal automatically, never lie silently.*

- **Late writes / late deletes** (below the sealed watermark): detected the
  tick they arrive; only the window(s) they fall in are resealed, folding
  them at their proper buckets. Repair cost tracks the span of the damage,
  never the size or age of the index.
- **Seal-timing shadows**: two clusters seal the same window at slightly
  different moments around a replicated delete; the resulting artifact is
  detected by `_compare`, which enqueues an automatic **reseal** of just that
  window on both sides (non-destructive request docs; the owning scanners
  execute them next tick). Timing artifacts dissolve; real divergence
  survives the reseal and is reported without re-request loops (10-minute
  cooldown). Set `plugins.index_digest.auto_reseal: false` to make `_compare`
  strictly read-only.
- **Config/format changes, history gaps, system-index loss**: the affected
  copy resets and rebuilds. Delete events are retained permanently and are
  re-folded on rebuild.
- Scan ticks fire at **UTC wall-clock boundaries** on every node, so
  independent clusters seal within NTP skew of each other, shrinking
  seal-timing exposure from minutes to seconds.

## REST API

| Endpoint | Purpose |
|---|---|
| `GET /_plugins/_index_digest/{index}/_tree?shard_id=&allocation_id=&root_level=&root_bucket=&depth=` | Inspect a subtree of one copy's hash tree |
| `GET /_plugins/_index_digest/_compare?left_index=&left_shard_id=&left_allocation_id=&right_...&root_level=&max_mismatches=` | Compare two copies' trees up to the common sealed watermark; drills down to the differing leaf buckets |
| `POST /_plugins/_index_digest/_run_once` | Trigger one synchronous scan of all allowlisted local shard copies |
| `GET /_plugins/_index_digest/_status` | Scheduler status |

`_compare` reports `comparable`, `equal`, per-bucket `mismatches` with
reasons (`missing_left` / `missing_right` / `digest_mismatch` /
`metadata_mismatch`), each side's watermark, levels, format version and
`last_seen_epoch_millis`, plus `reseal_requested_windows` when auto-heal
was triggered.

## Settings (`plugins.index_digest.*`)

| Setting | Default | Notes |
|---|---|---|
| `enabled` | `true` | dynamic |
| `indices` | `[]` | allowlist of index name patterns; empty = scan nothing |
| `scan_interval` | `30s` | dynamic, min `1s`; ticks align to UTC boundaries |
| `scan_mode` | `primary_only` | `all_copies` / `selected_copies` log a replica-semantics warning |
| `min_level` / `max_level` | `10` / `20` | fixed node settings; changes trigger rebuild |
| `guard_windows` | `1` | extra windows held back before sealing (reorder tolerance) |
| `max_top_buckets_per_run` | `1` | windows sealed per tick per copy |
| `auto_reseal` | `true` | dynamic; `false` makes `_compare` strictly read-only |

## Known limitations

- Content is not hashed: two documents with the same `(_id, externalVersion)`
  but different bodies are indistinguishable. The contract is that the
  external version changes whenever content changes.
- Reseals and rebuilds recompute from live data plus retained delete events;
  resets should be paired across mirrors being compared.
- The scanner never forces a refresh. It harvests and seals only up to what
  the shard's last refresh made visible, so with `refresh_interval: -1` the
  tree lags until the operator refreshes or a flush occurs.
- Comparison requires both trees in the same cluster's `.index_digest`
  (cross-cluster comparison needs an external fetch step — future work).
- Indices with document-level security filtering (DLS/FLS) must not be
  scanned: reader wrappers would distort digests.
- Stale trees of relocated-away allocations are not yet cleaned up
  automatically.

## Build & test

```
./gradlew build          # compile + unit tests
./gradlew integTest      # end-to-end suite against a test cluster
./gradlew yamlRestTest   # REST-spec tests
./gradlew run            # local cluster with the plugin, for manual testing
```

The integration suite doubles as a demo script: deterministic digests across
segment layouts, divergence localization, missed-delete detection via
tombstone events, late-write rebaselining, and shadow auto-reseal
convergence all run against a real cluster.

## License

Apache License, Version 2.0.
