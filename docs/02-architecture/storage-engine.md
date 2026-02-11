# Storage Engine

The storage engine is the core of S4. It uses a hybrid approach to minimize inode usage while maintaining high performance.

## The Inode Problem

Traditional file-based object storage creates one file per object on disk. At scale, this exhausts the filesystem's inode limit. For example, storing 1 billion objects requires 1 billion inodes — far exceeding typical filesystem limits.

S4 solves this completely:

| Approach | 1 Billion Objects | Files on Disk |
|----------|-------------------|---------------|
| Traditional (1 file per object) | 1,000,000,000 inodes | 1,000,000,000 |
| **S4 (append-only volumes)** | **~1,000 inodes** | **~1,000** |

## Storage Strategy

S4 stores objects differently based on their size:

### Tiny Objects (< 4KB)

Stored **inline** in the redb metadata database. This avoids the overhead of volume I/O for very small objects.

### Large Objects (>= 4KB)

Stored in **append-only volume files**. Each volume is approximately 1GB. When a volume fills up, a new one is created.

## Data Layout on Disk

```
/data/
  +-- redb/                    # Metadata (1 file)
  |   +-- data.redb
  +-- volumes/                 # Object data (minimal files)
  |   +-- volume_000001.dat    # ~1GB append-only log
  |   +-- volume_000002.dat
  |   +-- ...
  +-- temp/                    # Multipart uploads (temporary)
```

## Key Data Structures

### BlobHeader

Each object in a volume file is preceded by a header:

```rust
struct BlobHeader {
    crc: u32,          // CRC32 checksum (bit-rot protection)
    timestamp: u64,    // Write timestamp
    key_len: u32,      // Length of the object key
    blob_len: u64,     // Length of the object data
    is_deleted: bool,  // Tombstone marker
}
```

### IndexRecord

Metadata for each object is stored in redb:

```rust
struct IndexRecord {
    file_id: u32,      // Volume number (volume_000042.dat)
    offset: u64,       // Byte offset within the volume
    size: u64,         // Object size in bytes

    // S3 metadata
    etag: String,
    content_type: String,
    metadata: HashMap<String, String>,

    // Deduplication
    content_hash: [u8; 32],  // SHA-256 hash

    // Versioning
    version_id: Option<String>,

    // Object Lock
    retention_mode: Option<RetentionMode>,
    retain_until_timestamp: Option<u64>,
    legal_hold: bool,
}
```

## Crash Recovery

S4 guarantees data integrity through:

1. **fsync on every write** — data is durable before returning HTTP 200
2. **CRC32 checksums** — every blob is verified on read
3. **ACID transactions** — metadata updates in redb are atomic
4. **Recovery on startup** — the engine scans volumes and rebuilds the index if needed

## Volume Compaction

Over time, deleted objects leave gaps in volume files. The background compactor reclaims this space by:

1. Scanning volumes for tombstoned or unreferenced blobs
2. Copying live data to new volumes
3. Removing old volumes

This process runs in the background without blocking reads or writes.
