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
│  • Hot Index: redb (Metadata)          │
│  • Cold Data: Append-only Log Volumes  │
│  • Compactor (Background GC)            │
└─────────────────────────────────────────┘
```

## Storage Engine

### Design Principles

1. **Append-Only Logs**: All writes are sequential (maximum disk performance)
2. **Metadata Separation**: Fast metadata in ACID database (redb)
3. **Content Deduplication**: Same content stored only once
4. **Crash Recovery**: Metadata can be rebuilt from volume headers

### Storage Strategy

- **Tiny Objects (< 4KB)**: Stored inline in redb database
- **All Other Objects (> 4KB)**: Stored in append-only volume files

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

The redb database stores `IndexRecord` entries mapping:
- Key (bucket/path) → Volume ID, Offset, Size, Metadata, Content Hash

This allows:
- Fast lookups (O(log n))
- Atomic operations (ACID transactions)
- Efficient listing with prefix filtering

## Data Flow

### Write Path (PUT Object)

1. Client sends PUT request with data
2. Compute SHA-256 hash of data
3. Check deduplicator: does this content already exist?
   - If yes: Create new index entry pointing to existing blob (reference counting)
   - If no: Write blob to current volume, register in deduplicator
4. Create IndexRecord with metadata
5. Store IndexRecord in redb (ACID transaction)
6. fsync volume file (if strict_sync enabled)
7. Return 200 OK to client

### Read Path (GET Object)

1. Client sends GET request
2. Lookup IndexRecord in redb by key
3. If not found, return 404
4. Read blob from volume file at (volume_id, offset)
5. Verify CRC32 checksum
6. Return data to client

### Delete Path (DELETE Object)

1. Client sends DELETE request
2. Lookup IndexRecord in redb
3. Mark blob as deleted (tombstone in volume or index)
4. Remove IndexRecord from redb
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
1. Start ACID transaction in redb
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

This ensures data integrity even if the metadata database is lost.

## Compaction

Background compaction process:

1. Scan old volume files
2. Check which blobs are still referenced in index
3. Copy live blobs to new volume
4. Delete old volume file

This reclaims space from deleted objects.

## Performance Characteristics

- **Write Latency**: ~1-2ms for small objects (limited by fsync)
- **Read Latency**: ~0.5-1ms (metadata lookup + disk read)
- **Throughput**: Limited by disk sequential write speed (~500MB/s on SSD)
- **Concurrency**: High (async I/O with tokio)

## Scalability

S4 is designed as a single-node system. For high availability:
- Use Active-Passive HA with external replication (DRBD, filesystem-level)
- Use load balancer for multiple instances (each with separate storage)

Distributed mode (Raft, sharding) is not implemented to keep the codebase simple and maintainable.
