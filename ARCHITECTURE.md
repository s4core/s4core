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
- **Quorum (N=3, W=2, R=2)**: Reads and writes tolerate 1 node failure. W+R > N guarantees read-your-writes.
- **SWIM Gossip**: Failure detection via the `foca` crate (suspect: 5s, dead: 30s).
- **gRPC Inter-Node**: Data and metadata replication over gRPC (port 9100), optional TLS.
- **Multipart Quorum State**: Multipart sessions, uploaded part records, and completion are stored through the replica quorum, so load balancers do not need sticky sessions. `UploadPart`, `UploadPartCopy`, and aws-chunked multipart requests stream through bounded buffers instead of materializing full parts in RAM.
- **Bucket-to-Pool Pinning**: Each bucket belongs to one pool. All objects in a bucket are on the same set of nodes.
- **Hinted Handoff**: Writes to offline replicas are stored as hints (TTL: 3h) and delivered on recovery.
- **Read Repair**: Stale replicas are asynchronously updated when detected during reads.
- **HLC Ordering**: Hybrid Logical Clocks provide causal ordering; conflicts resolved by Last-Writer-Wins (LWW).
- **Anti-Entropy**: Background Merkle tree exchange every 10 minutes detects and repairs divergences between replicas.
- **Distributed Tombstones**: Tombstones live for `gc_grace` (7 days) and are only purged after all replicas confirm synchronization via repair frontier.
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
