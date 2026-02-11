# Content Deduplication

S4 automatically deduplicates stored data using content-addressable storage (CAS). When two or more objects have identical content, only one copy is stored on disk.

## How It Works

1. When an object is uploaded, S4 computes its **SHA-256 hash**
2. The hash is checked against the deduplication index
3. If the hash already exists, the object data is **not written again** — only a new metadata reference is created
4. A reference counter tracks how many objects point to each unique blob
5. When all references to a blob are deleted, the space becomes reclaimable by the compactor

## Storage Savings

Deduplication provides **30-50% space savings** on typical workloads. Savings depend on how much duplicate content exists in your data.

**Examples of high-dedup workloads:**
- Backup storage (incremental backups share most data)
- Container image registries (layers are often shared)
- Log storage (repeated patterns)
- File sync services (multiple users uploading the same files)

## Dedup Statistics

Check deduplication effectiveness via the stats API:

```bash
curl http://localhost:9000/api/stats
```

Response includes:

```json
{
  "dedup_unique_blobs": 1500,
  "dedup_total_references": 3200,
  "dedup_ratio": 2.13
}
```

| Field | Description |
|-------|-------------|
| `dedup_unique_blobs` | Number of unique data blobs on disk |
| `dedup_total_references` | Total number of object references |
| `dedup_ratio` | Ratio of references to unique blobs (higher = more savings) |

A ratio of 2.13 means that on average, each unique blob is referenced by 2.13 objects — roughly 53% storage savings.

## Interaction with Other Features

- **Versioning**: Each version is deduplicated independently. If version 1 and version 3 have identical content, only one copy is stored.
- **Object Lock**: Deduplication is transparent to Object Lock. Locked objects are protected regardless of dedup status.
- **Lifecycle Policies**: When lifecycle rules delete objects, dedup reference counts are decremented. The actual data is removed only when no references remain.

## Design Details

- Hashing algorithm: **SHA-256** (cryptographically strong, no collisions in practice)
- Dedup granularity: **whole-object** (not block-level)
- Only objects >= 4KB are deduplicated (inline objects in redb are not deduplicated)
- Dedup index is stored in redb alongside other metadata
