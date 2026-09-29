# S4 Architecture Documentation

## Overview

S4 is built as a single-node, high-performance object storage server with a focus on simplicity, reliability, and solving the inode exhaustion problem.

## System Layers

```
┌─────────────────────────────────────────┐
│  LAYER 1: Network & Protocol (Axum)    │
│  HTTP/1.1, HTTP/2, HTTP/3 QUIC         │
│  AWS Signature V4 Auth                 │
└──────────────────────────┬──────────────┘
                           ⬇
┌─────────────────────────────────────────┐
│  LAYER 2: Business Logic               │
│  • Deduplication Engine (CAS)           │
│  • Atomic Directory Operations          │
│  • ACL / Policy Check                   │
└──────────────────────────┬──────────────┘
                           ⬇
┌─────────────────────────────────────────┐
│  LAYER 3: Storage Engine (Bitcask)      │
│  • Hot Index: fjall (LSM, MVCC, LZ4)  │
│  • Cold Data: Append-only Log Volumes  │
│  • Compactor (Background GC)            │
└─────────────────────────────────────────┘
```

## Storage Engine

### Design Principles

1. **Append-Only Logs**: All writes are sequential (maximum disk performance)
2. **Metadata Separation**: Fast metadata in ACID database (fjall)
3. **Content Deduplication**: Same content stored only once
4. **Crash Recovery**: Metadata can be rebuilt from volume headers

### Storage Strategy

- **All Objects**: Stored in append-only volume files (volumes are the single source of truth for object data)
- **Metadata**: Stored in fjall database with separate keyspaces (objects, versions, buckets, IAM, dedup)

### Volume Structure

Volume files are named `volume_XXXXXX.dat` where XXXXXX is a zero-padded volume ID.

Each blob in a volume has the structure:
```
[BlobHeader (serialized)] + [Key (UTF-8 bytes)] + [Data (raw bytes)]
```

The `BlobHeader` contains:
- CRC32 checksum
- Timestamp
- Key length
- Blob length
- Flags (deleted marker, multipart, etc.)

A volume is one blob after another, and the offset `VolumeWriter` reports for a blob is where its
header starts. That holds even when a write is dropped halfway — an RPC whose client hung up, an
HTTP request whose client timed out: whatever the dropped write already handed to the file is cut
off before the next write, which starts exactly where the last whole blob ends. No index record can
point into those bytes, because `IndexRecord` is committed only after its blob is whole.

### Index Database

The fjall database stores `IndexRecord` entries across multiple keyspaces:
- **objects**: bucket/key → Volume ID, Offset, Size, Metadata, Content Hash
- **versions**: bucket/key → Version list
- **buckets**: bucket_name → Bucket configuration
- **iam**: user/key → IAM records
- **dedup**: content_hash → DedupEntry (ref count, volume location)

This allows:
- Fast lookups (O(log n) with LSM-tree)
- Lock-free concurrent reads (MVCC)
- Atomic cross-keyspace batch writes
- Native prefix scans (O(log n) seek, not full table scan)
- Built-in LZ4 compression

## Data Flow

### Write Path (PUT Object)

1. Client sends PUT request with data
2. Compute SHA-256 hash of data
3. Check deduplicator: does this content already exist?
   - If yes: Create new index entry pointing to existing blob (reference counting)
   - If no: Write blob to current volume, register in deduplicator
4. Create IndexRecord with metadata
5. Store IndexRecord in fjall (atomic batch write)
6. fsync volume file (if strict_sync enabled)
7. Return 200 OK to client

### Read Path (GET Object)

1. Client sends GET request
2. Lookup IndexRecord in fjall by key (lock-free MVCC read)
3. If not found, return 404
4. Read blob from volume file at (volume_id, offset)
5. Verify CRC32 checksum
6. Return data to client

### Delete Path (DELETE Object)

1. Client sends DELETE request
2. Lookup IndexRecord in fjall
3. Mark blob as deleted (tombstone in volume or index)
4. Remove IndexRecord from fjall (atomic batch)
5. Update deduplicator (decrement reference count)
6. Return 204 No Content

## Deduplication

Content-Addressable Storage (CAS) is implemented using SHA-256 hashes:

- Before writing, compute hash of content
- Check if hash exists in deduplicator map
- If exists: Create new index entry pointing to existing blob
- If not: Write new blob and register hash

This saves 30-50% storage space for typical workloads with duplicate content.

## Atomic Operations

Directory operations are atomic because "directories" are virtual - they're just key prefixes in the index database.

To rename a directory:
1. Start atomic batch in fjall
2. Find all keys with old prefix
3. Update all keys to new prefix (in-memory operation)
4. Commit transaction

This takes milliseconds even for millions of files because we're only updating metadata, not moving data.

## Crash Recovery

If the process crashes:

1. On startup, scan all volume files
2. Read BlobHeader from each blob
3. Reconstruct index database from headers
4. Rebuild deduplicator map

This ensures data integrity even if the metadata database is lost. The fjall WAL provides instant crash recovery in the normal case.

## Volume Compaction (s4-compactor)

S4 uses append-only volume files — when objects are deleted or overwritten, the old
blob data remains as dead space. The compactor reclaims this space:

### How It Works

1. **Discover** all volume files (skipping the currently active one)
2. **Build dedup index**: `(volume_id, offset) → content_hash` from the dedup keyspace
3. **Analyze** each volume — scan all blobs, classify as live or dead based on dedup index
4. **Compact** volumes where fragmentation exceeds the threshold (default: 30% dead space):
   - Copy live blobs to a new volume via `VolumeWriter`
   - Atomically update all `IndexRecord` and `DedupEntry` locations in a single fjall batch
   - Rename old volume to `.dat.compacted`, then delete (crash-safe two-phase removal)
5. **Report** statistics: volumes compacted, bytes reclaimed, errors

### Two Compaction Modes

- **Regular** (every N hours, default 6): processes up to 10 volumes per cycle — lightweight, minimal I/O impact
- **Full** (once daily at configured time, default 02:00 local): processes all volumes with no limits — reclaims all dead space

The daily full compaction time is configurable via `S4_COMPACTION_FULL_TIME` (format: "HH:MM"). Set to empty string to disable.

### Key Invariants

- **Never lose confirmed data** — old volume is deleted only after all live blobs are verified relocated
- **Crash-safe** — incomplete compaction is recoverable (idempotent restart)
- **Non-blocking** — runs in background while normal reads/writes continue
- **Dedup-aware** — updates DedupEntry locations, not just IndexRecords

### Architecture

- **s4-compactor** crate provides `VolumeCompactor` (compaction) and `VolumeScrubber` (CRC32 integrity verification)
- **s4-server** runs a `CompactionWorker` background task (same pattern as `LifecycleWorker`)
- Compaction interval, threshold, and dry-run mode are configurable via environment variables

### Volume Scrubber

The scrubber verifies CRC32 checksums of all blobs in all volumes, detecting bit rot
or data corruption. It reports the number of healthy and corrupted blobs.

## Performance Characteristics

- **Write Latency**: ~1-2ms for small objects (limited by fsync)
- **Read Latency**: ~0.5-1ms (metadata lookup + disk read)
- **Throughput**: Limited by disk sequential write speed (~500MB/s on SSD)
- **Concurrency**: High (async I/O with tokio)

## Scalability & Federation

S4 supports two deployment modes:

### Single-Node Mode (Default)

Optimized for maximum performance on one machine. No distributed overhead. All data is stored locally.

### Distributed Mode (Federation)

S4 uses **leaderless quorum replication** for high availability. Any node can accept any request — there is no single leader.

```
                    ┌─────────────────────┐
                    │   Load Balancer     │
                    │   (HAProxy/Nginx)   │
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
        ┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼─────┐
        │  Node 1   │   │  Node 2   │   │  Node 3   │
        │           │◄─►│           │◄─►│           │
        │ S3 API +  │   │ S3 API +  │   │ S3 API +  │
        │ Storage   │   │ Storage   │   │ Storage   │
        ├───────────┤   ├───────────┤   ├───────────┤
        │ fjall     │   │ fjall     │   │ fjall     │
        │ volumes   │   │ volumes   │   │ volumes   │
        ├───────────┤   ├───────────┤   ├───────────┤
        │ Gossip    │◄─►│ Gossip    │◄─►│ Gossip    │
        │ (SWIM)    │   │ (SWIM)    │   │ (SWIM)    │
        └───────────┘   └───────────┘   └───────────┘
              Pool 1 (immutable after creation)
```

**Key properties:**
- **Server Pools**: Cluster = union of immutable pools. Each pool is a fixed set of nodes (recommended: 3). Horizontal scaling = adding new pools.
- **Stable Member Identity**: Bootstrap passes each pool's resolved NodeId roster directly to placement; addresses are transport hints and may be reused without merging two identities. Duplicate identities are rejected. The remembered peer record is persisted with file fsync, atomic rename and directory fsync.
- **Quorum (N=3, W=2, R=2)**: Reads and writes tolerate 1 node failure. W+R > N guarantees read-your-writes.
- **Read Failure Semantics**: Only an explicit `ObjectNotFound` counts as an absence response; storage failures remain errors in local digest collection and replica HEAD/read handlers. The digest collector owns its replica futures directly, so cancellation aborts pending async reads rather than detaching them. The brief late-response drain cannot extend the digest quorum deadline; this is not a deadline for the subsequent object-body transfer or already-started blocking disk I/O.
- **SWIM Gossip**: Failure detection via the `foca` crate (suspect: 5s, dead: 30s). foca tracks members by address, so one node id appears twice across a restart that changes address — at the address the node left and at the one it answers on. Membership is therefore kept per identity: a death notice applies only to the run a node is currently known by, a rename records whoever took over an address and forgets where the displaced node is rather than calling it dead, and a changed address drops the metadata that described the old endpoint. Node metadata is ordered by process start time and then broadcast counter, so a restarted node's broadcasts are not mistaken for stale ones by peers that stayed up. The gRPC pool re-checks a peer's address on every call and drops a cached connection to an address the node has left. A clock that moves backwards across a restart still makes the returning run look older, in gossip as in foca itself.
- **gRPC Inter-Node**: Data and metadata replication over gRPC (port 9100), optional TLS. A blob body at or above 8 MB is replicated through `WriteBlobStream` in 256 KB chunks instead of one message, so an object larger than the 16 MB message limit still reaches quorum; the replica reassembles it and commits it through the same durable write path as a unary `WriteBlob`. A peer that does not serve the streaming method answers `Unimplemented` and the write falls back to unary, so a mixed-version pool keeps working.
- **Multipart Quorum State**: Multipart sessions, uploaded part records, and completion are stored through the replica quorum, so load balancers do not need sticky sessions. `UploadPart`, `UploadPartCopy`, and aws-chunked multipart requests stream through bounded buffers instead of materializing full parts in RAM.
- **Bucket-to-Pool Pinning**: Each bucket belongs to one pool. All objects in a bucket are on the same set of nodes.
- **Hinted Handoff**: Writes to offline replicas are stored as hints (TTL: 3h) and delivered on recovery.
- **EC Small-Placement Hints**: A separate durable continuation covers the whole placement-debt queue. Retry updates use snapshot comparison; acknowledgements and deletes clear only the placement generation they cover. These local hints are not a substitute for quorum-durable placement facts in the original data write.
- **Read Repair**: Stale replicas are asynchronously updated when detected during reads.
- **HLC Ordering**: Hybrid Logical Clocks provide causal ordering; conflicts resolved by Last-Writer-Wins (LWW).
- **Anti-Entropy**: Background Merkle tree exchange every 10 minutes detects and repairs divergences between replicas.
- **Distributed Tombstones**: Tombstones live for `gc_grace` (7 days) and are only purged after all replicas confirm synchronization via repair frontier.
- **EC Tombstone Sweeps**: Each metadata page fits both the remaining manifest scan budget and the candidate limit. The shared EC metadata pager seeks directly past its cursor with a native fjall range instead of enumerating the prefix again; it retains one lookahead to detect continuation. This bounds record enumeration, not physical I/O inside the LSM. Background continuation is durable; manual GC has independent opaque cursors and explicit round completion. Owner probes and their corrective handoffs share a ten-second network budget per owner-poll invocation. Only `Unimplemented` enables legacy probes; unknown replies never authorize shard release or handoff to that owner.
- **Per-Node Dedup**: Deduplication remains local to each node (no cross-node dedup coordination). Mark-sweep GC replaces ref-count decrement.
- **Bit Rot Protection**: Background scrubber verifies CRC32 checksums across all volumes (full scan every 30 days). Corrupted blobs are auto-healed from replicas.
- **Deployment Modes**: `single` (default), `cluster` (storage + replication), `gateway` (stateless router).
- **Graceful Operations**: Graceful shutdown with drain, rolling upgrades with protocol versioning, admin API for cluster health and operations.

## Editions (CE / EE)

S4 follows an Open-Core model with two editions built from the same codebase:

- **Community Edition (CE)**: Apache 2.0. Full single-node and 3-node cluster (1 pool, RF=3, W=2, R=2). All clustering primitives (gossip, gRPC, quorum, anti-entropy, tombstone GC, CRC scrubber) are included.
- **Enterprise Edition (EE)**: Elastic License 2.0. Removes scaling limits (unlimited pools and nodes) and adds operational features (deep SHA-256 scrubber, rolling upgrades, automated node replacement, LDAP/SAML/OIDC, WebDAV, audit log).

### CE/EE Boundary

The boundary is enforced at two levels:

1. **Quantitative limits** (`ClusterLimits`): CE = 1 pool, 3 nodes. EE limits come from the Ed25519-signed license.
2. **Trait-based DI** (`ClusterServices`): EE behavioral features (deep scrubber, node replacer, etc.) are injected via trait objects. CE uses noop implementations.

`#[cfg(feature = "enterprise")]` appears **only** in `s4-server/src/edition.rs`. No feature gates in domain code.

### Docker Images

| Tag | Edition |
|-----|---------|
| `s4core:latest` | CE (default) |
| `s4core:ce` | CE (explicit alias) |
| `s4core:ee` | EE (requires `S4_LICENSE_KEY`) |

Build locally: `docker build .` (CE) or `docker build --build-arg EDITION=ee .` (EE).

## Manual Test Scripts

The `scripts/` directory holds numbered scenarios for manual validation. They start real servers,
drive them through a real S3 client and assert on observable behaviour, so they cover what unit and
integration tests cannot: clustering, crash recovery, third-party client compatibility and the
enterprise erasure-coding pipeline.

Run a community scenario directly; an EE scenario needs a licence key:

```bash
./scripts/1-simple-test.sh
S4_LICENSE_KEY="$(cat ee/license-key.txt)" ./scripts/42-ec-tiering-test.sh ./target/release/s4-server
```

`./scripts/check.sh` is the full CI check (format, clippy, the whole test suite) and is not one of
these scenarios.

| Script | Description |
|--------|-------------|
| `start-server.sh` | Server startup commands (various auth modes) |
| `1-simple-test.sh` | Basic S3 operations (mb, cp, ls, sync, mv, rm, rb) |
| `2-object-lock-test.sh` | Object Lock and WORM compliance testing |
| `3-versioning-test.sh` | Object versioning operations |
| `4-lifecycle-policies-test.sh` | Lifecycle policy configuration and execution |
| `5-core-iam-s3-admin-api-test.sh` | IAM and Admin API testing |
| `6-basic-metrics-test.sh` | Prometheus metrics endpoint testing |
| `7-deduplication-test.sh` | Content deduplication verification |
| `8-rclone-restic-test.sh` | Third-party tool compatibility (rclone, restic) |
| `9-warp-minio-test.sh` | Warp (MinIO) benchmark testing |
| `10-mc-minio-test.sh` | MinIO client (mc) compatibility testing |
| `11-s3-select-test.sh` | S3 Select SQL and multi-object query testing |
| `13-compaction-test.sh` | Volume compaction via Admin API |
| `14-oom-multipart-test.sh` | Large file test: Debian ISO 3.7GB upload/download with SHA-256 verification |
| `15-iam-compaction-bug-test.sh` | IAM + compaction bug reproduction (users disappear after compaction) |
| `16-abandoned-multipart-test.sh` | Abandoned multipart upload cleanup by compactor (requires `S4_COMPACTION_MULTIPART_TTL_SECS=1`) |
| `17-crash-recovery-cluster.sh` | Cluster crash-recovery and failover validation |
| `18-ghost-objects-test.sh` | Ghost object regression validation |
| `19-ec-staged-transcode-hot-purge-test.sh` | EE EC staged transcode, manifest commit, and hot-copy purge E2E |
| `20-ec-safety-gate-failed-reconstruction-test.sh` | EE EC reconstruction-failure safety gate and orphan GC E2E |
| `21-ec-manifest-authority-bitrot-test.sh` | EE EC manifest-authoritative bit-rot detection and repair E2E, including a shard that rots twice: each repair must put the shard's bytes back on its owner's disk, because a repair is neither a duplicate write nor a deduplication hit (decision EC2-164) |
| `22-ec-rs-degraded-threshold-test.sh` | EE EC RS degraded read threshold and fail-closed E2E |
| `23-ec-local-metadata-miss-not-404-test.sh` | EE EC quorum metadata fallback when local metadata is missing |
| `24-ec-multipart-haproxy-no-sticky-test.sh` | EE EC multipart load-balancer no-sticky E2E |
| `25-ec-transcode-crash-state-machine-test.sh` | EE EC staged transcode crash-state-machine E2E |
| `26-ec-delete-tombstone-zombie-test.sh` | EE EC delete, tombstone, and zombie-prevention E2E |
| `27-ec-concurrent-put-orphan-gc-test.sh` | EE EC concurrent PUT conflict and orphan GC E2E |
| `28-ec-small-object-fallback-test.sh` | EE EC explicit replicated-small fallback E2E |
| `29-ec-range-read-chunk-boundary-test.sh` | EE EC range-read chunk-boundary E2E |
| `30-ec-orphan-gc-lease-safety-test.sh` | EE EC orphan-GC lease safety E2E |
| `31-ec-admin-verify-repair-test.sh` | EE EC admin verify and repair E2E |
| `32-ec-topology-policy-test.sh` | EE EC topology policy E2E |
| `33-ec-backpressure-cpu-budget-test.sh` | EE EC codec backpressure and CPU budget E2E |
| `34-ec-anti-entropy-scrub-pagination-test.sh` | EE EC anti-entropy and scrub pagination E2E |
| `35-ec-lrc-failure-matrix-test.sh` | EE EC LRC failure matrix E2E |
| `36-ec-full-cluster-restart-large-object-test.sh` | EE EC full-cluster restart durability E2E, plus a rolling restart with one node stopped for good: the remaining five are restarted one at a time, each has to come up while its neighbour is gone, the object reads byte for byte after every step, and the pool keeps calling the stopped node the owner of its slots |
| `37-ec-ce-license-policy-test.sh` | EE EC CE/license policy E2E |
| `38-ec-standard-client-smoke-test.sh` | EE EC standard client smoke test (AWS CLI, signed curl, rclone, restic) |
| `39-ec-v1-production-readiness-suite.sh` | EE EC v1 readiness suite runner (tiers: `part2`, `compat`, `chaos`, `gate`, `docker`, `client`, `full`). `gate` is what CI runs on a feature branch: `part2` + `compat` + `chaos`, each script once, minus the ones the runner records as known red. The runner refuses to start if an EC v1 script on disk belongs to no tier, or if a tier names a script that is gone |
| `40-ec-v2-client-invariance-test.sh` | EE EC v2 shared guard rail: client-visible state unchanged across EC operations — encoding on write, auto-tiering, a set added to a running pool, packing into a segment, replacing the machine that held one of an object's shards, checked both while the replacement holds none of them and after repair has rebuilt them, and switching a node between reading the `k` data shards of a chunk and reading all `n` of them and back. The range fingerprint covers the shard boundary as well as the chunk boundary, which is the boundary a narrowed read now serves from |
| `41-ec-isal-mixed-cluster-test.sh` | EE EC v2 ISA-L mixed-cluster byte-equality E2E (skips until the `isal` feature is built) |
| `42-ec-tiering-test.sh` | EE EC v2 auto-tiering: activity tracking, bucket policies, the dry run, the RF=3 → EC migration worker, and the EC → RF=3 reverse migration with its manual rollback — including namespace reconciliation run on every node while a promotion is in flight, and a rollback left to the background workers at their default intervals (EC2-026) |
| `43-ec-multi-set-test.sh` | EE EC v2 multi-set pools: per-set metrics, degraded reads, repair and GC staying inside their erasure set, adding a set to a running pool under erasure-coded load, writes spreading over two sets, failure isolation between sets, deactivation, objects written before the expansion staying untouched, and the rolling-upgrade gate |
| `44-ec-packed-segments-test.sh` | EE EC v2 packed segments, end to end: small objects packed into segments, the segment lifecycle running unattended, the direct read out of a data shard, client invariance, range reads, delete inside a segment, the space saving against RF=3, repacking a fragmented segment and retiring what it emptied, surviving a node dying while a segment is open and while it is being encoded, unpacking back to RF=3, and GET latency against a `ReplicatedSmall` baseline |
| `45-ec-topology-lrc-test.sh` | EE EC v2 cross-zone LRC groups: every set's slot layout gets recorded, a restart with unchanged labels changes nothing, relabeling a running LRC pool moves no slot and leaves every object written before it readable byte for byte through every node, with read passes that, once the cluster has settled, neither degrade nor queue a repair; new objects follow the recorded layout, and an unknown `S4_EC_LRC_GROUP_DOMAIN` stops the start; after ten nodes are appended, no node writes a set's layout record again and every node takes the recorded layout, a set whose nodes sit in too few racks for a local group is refused under `strict` with the numbers and counted in `s4_ec_set_layout_unsatisfied_total`, and a set on five racks is laid out by the LRC group rule, every group in distinct racks, reported identically by all twenty nodes, written into by the placement exactly as recorded, read byte for byte through every node and rebuilt unchanged from its record after a restart; the owner of a shard of that set repairs a data shard and a local parity from the three other members of their group and a global parity shard from six, `s4_ec_repair_bytes_total` counts exactly those bytes and none across zones, and a shard repaired once is repaired again on the next request; pool health reports where every slot sits by failure domain beside the fields it always carried, collapsing the racks under a set is found as drift while moving no slot and leaving its objects readable byte for byte, the EC v1 set never becomes a violator, restoring the labels clears the violation, an apply the labels cannot satisfy is refused with `400` and the numbers, a dry run writes nothing, and applying the rule to a set that already meets it keeps every local group in distinct racks and reaches all twenty nodes |
| `46-ec-lrc-solver-test.sh` | EE EC v2 unified LRC solver: a loss only the unified solver reaches — two data shards of one local group, its local parity alive — read byte for byte through S3 and counted as a solver recovery, while a node held to the EC v1 fast paths refuses the same loss and the same repair; the shard rebuilt by the solver is the encoder's own shard, and the pre-solver node then serves the object and verifies it; one shard further — the same data shards plus the group's local parity — still fails closed, and the two editions of the decoder publish failure matrix version 2 and version 1 side by side |
| `47-ec-dedup-test.sh` | EE EC v2 duplicate-content measurement (plan 07, phase 0): the same bytes uploaded under three names across two buckets produce three erasure-coded manifests with one content hash, and the report counts two of them as repeats and names the group; those three objects encode to byte-identical shards in identical slots on identical nodes, while write-path dedup shares the blobs, which is why the repeat exists in metadata and not on disk; the walk covers the pool rather than the node it was asked on, a node that was not read is named with its reason and withholds the verdict, a walk stopped by `S4_EC_ADMIN_SCAN_LIMIT` withholds it too, a sample below the EC2-121 floor is refused as noise, the measurement leaves listing, HEAD and object bytes untouched, and client GET latency stays within budget while every node walks its manifests |
| `48-ec-v2-acceptance-suite.sh` | EE EC v2 acceptance suite runner (tiers: `shared`, `isal`, `tiering`, `multiset`, `gate`, `full`). `full` runs every plan's script; `gate` is `full` minus the ones the runner records as known red, and is what CI runs on a feature branch. Every script a tier names must exist, since all ten EC v2 plans have landed. Same coverage gate as runner 39 |
| `49-ec-node-replacement-test.sh` | EE EC v2 node replacement (plan 08): the data directory of one node is destroyed five times, and the phases differ from each other by its identity and nothing else — with the identity kept the pool still assigns it the same slots, the node reports every one of its shards missing, ordinary admin repair filtered by `node_id` rebuilds them all, and the node then verifies each rebuilt shard against the manifest and reads each object back whole through erasure coding; with a fresh identity the same machine gets no slot, refuses the pool's set layout record and cannot serve a single object. The whole replacement then runs off the admin API and one variable: a blank machine adopts the identity from `S4_EXPECTED_NODE_IDENTITY` and is rebuilt in place, while a data directory belonging to another node — and a malformed value — stop the start with both identities named and change nothing. Between taking the identity and finishing the rebuild that machine is a slot owner the pool sees alive and holding nothing: it answers `503` with its verdict on every erasure-coded read, publishes `s4_ec_node_rejoin_serving 0`, reports its slots against zero shards and a `degraded` pool in its own health while the rest of the pool serves those objects byte for byte, and returns to the read path only on an operator's approval — which is recorded as the operator's decision and refused when addressed to another identity. The rebuild itself is one command and it has an end: before anything reads an object the pool counts every shard of that identity missing, a single repair request queues them all and reports nothing left behind, repeating it leaves the queue at one task per shard, and completion is decided by counting the shards the pool still misses rather than by a drained queue. Beside it the cost to clients is measured — two samples of client reads through one node, quiet and during a whole-node rebuild, compared at the 95th percentile against a budget, with a sample counted only when `s4_ec_repair_bytes_total` moved on both sides of it, so a run that could not measure fails differently from one whose reads were slow. A blank machine handed the identity of a node that is still running does not start, names the address that already answers for it, and leaves behind not even the identity file it had adopted. While the node is down the pool names it as a slot owner it cannot see, apart from topology risk. All of it is readable from outside as well: the shards the pool still owes that identity and the verdict on its rebuild are gauges, published when that owner's rebuild status is asked for and absent, rather than zero, for an owner nobody has asked about. Objects stay identical in body, `ETag`, size, content type and `Last-Modified` at every step |
| `50-ec-repair-priority-test.sh` | EE EC v2 repair priority (plan 09): a queue built by hand on twelve nodes and two erasure sets, with every repair worker switched off, so that the order the tasks are carried out in is the product's decision and not the timing of the requests that queued them. Before anything is repaired the queue is readable from outside: `s4_ec_repair_queue_depth_by_losses` splits the same tasks the depth gauge counts by how many shards each task's own chunk is missing, publishes all four buckets for every set so an empty one reads as zero rather than as a gap, adds up to that set's depth, and stays at zero on a node that holds no queue. Then the chunk missing two of the two shards `ec-rs-small` can spare is repaired first, ahead of two tasks queued before it — and once its first shard is back it takes its place by queue time again, because the measure is the damage a chunk has now. Turns alternate between the sets, so the neighbouring set's repairs are carried out while the damaged set still has work. The starvation guard is then shown as a lever with two positions over the same queue shape: with the age band off the oldest task waits for one repair of each of the three worse damaged chunks, and with the band on every second turn it is served second, ahead of chunks missing more than it is. Every loss is a real loss — taken by flipping a byte of a shard, confirmed by a degraded read on a third node — and every repair is checked where it can be seen: the rebuilt shard's bytes are back in the volume of the node that owns the slot. At the end the queue reads zero in every bucket of both sets and all five objects read byte for byte, with the ETag, size, content type and Last-Modified they had before a single shard was taken away. **55 checks, 0 failures** |
| `51-ec-minimal-read-test.sh` | EE EC v2 minimal read (plan 10): on six nodes, each in a zone of its own, what a read pulls off the pool is counted rather than argued about. Both read counters exist on every node at zero before a single object is written, so an empty graph reads as "nothing was read" and not as "no data". A healthy `GET` then pulls exactly the `k` data shards of every chunk and a short range exactly the shards it falls in — one inside a shard, two across a shard boundary, two across a chunk boundary — with the expected fetches, bytes and cross-zone split computed from the manifest and the topology labels, so the metric is checked against the pool rather than against itself. The same node with `S4_EC_MINIMAL_READ_ENABLED=false` is the EC v1 read again, all `n` shards of every chunk it touches, answering byte for byte the same bodies with the same `ETag`, size, type and `Last-Modified`, and saying which read it is doing in its own pool health. A shard the range falls in going missing costs the client nothing but time: the range still answers with the object's bytes, the read lifts the whole chunk exactly once without asking for anything twice, counts itself degraded, counts the shard that failed its hash and queues its repair, which puts the bytes back on the owner's disk. Then the price of the saving, in the open: a corrupt parity shard is invisible to reads — right bytes, not degraded, nothing counted, nothing queued — and it is the local scrub pass that finds it, reports it as corruption rather than as absence, quarantines it so the node stops claiming a shard it cannot produce, and queues the ordinary repair that names that very slot. The first run of this scenario found that last part wrong — a shard whose bytes its owner refuses was reported as missing, skipping quarantine and every corruption counter — which is decision EC2-185 |
| `52-bitrot-durability-test.sh` | Bit rot on a three-node RF=3 cluster: a blob whose bytes were flipped is never handed to another object by deduplication — the next writer of that content gets a copy of its own — and the scrubber repairs rather than reports, fetching a healthy copy from a replica and writing it back, checked in the volume file of the node whose disk was damaged and in `s4_scrubber_corruptions_healed_total` (decision EC2-165) |
| `53-ec-stale-layout-record-test.sh` | EE EC regression on six nodes `ec-rs-small`: a node's cached layout record that fell behind the quorum. The hot-copy purge commits the manifest again as `hot_replicas_purged`, and an overwrite through another node clears the record only on the node that took it, so the node that ran the transcode, a node that read the object between cutover and purge, and the node that transcoded the previous generation all hold a reference the quorum no longer matches. Through exactly those nodes, with no read in between to refresh them: `DELETE` after the purge and `DELETE` after an overwrite answer 204, the object is gone on every node, the quorum holds a tombstone, and it stays gone after every node runs the namespace reconcile — the defect answered 500 there after the hot delete had committed, and the reconcile brought the object back; admin verify after the purge and after an overwrite reads the whole object and matches on every node. Which node runs each transcode is chosen by where the `PUT` goes and confirmed in that node's log |

This section is kept last on purpose: `AGENTS.md` points at it by line number, and appending
elsewhere in this file would move it.
