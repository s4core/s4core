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

## Repairs are never deduplicated

Deduplication answers a write with "those bytes are already here". That is the
right answer for a client storing a file twice, and the wrong one for the write
that exists because the bytes on this disk are damaged — a repair rebuilding an
erasure-coded shard after bit rot, for example. Pointing the repaired record at
the blob that already holds that content would point it back at the damage.

Such a write therefore carries a reserved metadata key,
`_s4_force_physical_write`, and the engine treats it as a **healing write**:

- the bytes go into a volume of their own instead of reusing an existing blob;
- the dedup entry for that content is moved to the copy just written, so the
  next writer of the same content lands on the healthy one;
- a replica applies it even if it has already applied a write with the same
  operation id, because a heal is not a retry.

The key describes the write, not the object: it is stripped before the record is
persisted and never appears in object metadata. Ordinary writes are unaffected
and keep the savings above.

## A damaged blob leaves the index

Deduplication saves space by having many objects point at one copy of the
bytes. That is also the one thing that must never happen to a copy the disk has
damaged: a client storing a file whose content happens to match a rotted blob
would get an object that was corrupt before it was ever read.

So the moment a read finds that a blob does not hash to what its record claims
— bit rot, in one word — that blob's dedup entry loses its location. The entry
itself stays, because the objects that point at those bytes still exist and
still have to be able to release them; what goes is the engine's willingness to
hand those bytes to anybody new. The next write of that content stores a copy of
its own and the entry moves onto it, so the pool goes back to sharing one
healthy copy.

Nothing about this slows an ordinary write down: a blob nobody has found
damaged is shared exactly as before. And the object that was damaged is not
forgotten — on a cluster the scrubber fetches a healthy copy from a replica and
writes it back (see [Bit rot](#bit-rot) below).

## Bit rot

A blob can rot on disk without anything announcing it: the index still names the
object, the size and metadata are right, and only the bytes have changed. S4
answers that in three places:

- **Reads never serve it.** Every read verifies the blob against the SHA-256 the
  record holds, and a mismatch is an error, not a response body. In a cluster
  the read then comes from another replica, so the client sees nothing.
- **Writes never adopt it.** The section above.
- **The scrubber repairs it.** In cluster mode the background scrubber walks the
  volumes, verifies each blob against its CRC32, fetches a healthy copy from a
  replica for the ones that fail, and writes it back to this node's disk. Only
  the bytes change: the object keeps its ETag, size, metadata, HLC and version.
  A cycle that could not heal something comes back in
  `S4_SCRUBBER_UNHEALED_RETRY_SECS` rather than waiting out the full scan
  period, because the peer it could not reach may be one restart away.

What an operator watches:

```promql
# Damage found, and damage actually repaired, per node.
s4_scrubber_corruptions_found_total
s4_scrubber_corruptions_healed_total
s4_scrubber_blobs_scanned_total
```

The gap between the first two is the number of blobs this node is still
carrying damaged — on a single node, where there is no replica to fetch from,
that is all of them.

## Design Details

- Hashing algorithm: **SHA-256** (cryptographically strong, no collisions in practice)
- Dedup granularity: **whole-object** (not block-level)
- All objects are deduplicated regardless of size
- Dedup index is stored in a dedicated fjall keyspace alongside other metadata
