# Volume Compaction

S4 uses append-only volume files for data storage. When objects are deleted or overwritten, the old blob data remains as dead space. The compactor reclaims this space by rewriting volumes that contain a high proportion of dead blobs.

## How It Works

Each compaction cycle runs through two phases: **orphan purge** and **volume compaction**.

### Phase 1 — Orphan Purge (Mark-and-Sweep GC)

Before analyzing volumes, the compactor scans ground-truth metadata to find and remove orphaned references that could prevent space reclamation. This handles cases where objects or multipart uploads were deleted but their metadata entries survived (e.g., due to ref-count bugs or incomplete deletes).

1. **Scan live references** — iterate all objects + IAM records to collect live content hashes and composite manifest blob IDs.
2. **Resolve composite manifests** — for each live manifest, read its segment blob IDs from volume storage to build the complete set of live blob IDs.
3. **Purge orphaned dedup entries** — delete dedup entries whose content hash is not referenced by any live object.
4. **Expire multipart sessions** — check each `MultipartUploadSession` in fjall. Sessions where `max(created_at, updated_at)` exceeds the TTL (default: 24 hours, controlled by `S4_MULTIPART_UPLOAD_TTL_HOURS`) are expired and deleted. This protects sessions actively being completed (`updated_at` is refreshed by `CompleteMultipartUpload`).
5. **Purge orphaned multipart parts** — parts whose session no longer exists are deleted. Parts belonging to live sessions have their blob IDs protected.
6. **Purge orphaned blob_ref entries** — blob_ref entries not referenced by any committed object or active multipart part are deleted.

### Phase 2 — Volume Compaction

1. **Discover** all volume files (skipping the currently active one).
2. **Build indices**:
   - **Dedup index**: map `(volume_id, offset) → content_hash` for O(1) blob liveness checks
   - **Blob-ref index**: map `(volume_id, offset) → Vec<BlobId>` for composite/multipart blob tracking
3. **Fast-path**: if both indices are empty (no live data exists anywhere), delete all non-active volumes outright without blob-level scanning.
4. **Analyze** each volume — scan all blobs, classify as live or dead based on the indices.
5. **Compact** volumes where fragmentation exceeds the threshold:
   - Copy live blobs to a new volume via `VolumeWriter`
   - Atomically update all `IndexRecord`, `DedupEntry`, and `BlobRefEntry` locations in a single fjall batch
   - Rename old volume to `.dat.compacted`, then delete (crash-safe two-phase removal)
6. **Handle unreadable volumes** — volumes with non-zero file size but 0 readable blobs and no live index references are purged as orphans. Volumes with live index references are flagged for manual inspection (possible bit-rot).
7. **Report** statistics: volumes compacted, bytes reclaimed, errors.

## Crash Safety

Compaction is crash-safe by design:

- Old volumes are deleted **only after** all live blobs are verified relocated
- Two-phase removal: rename to `.dat.compacted` first, then delete
- If a crash occurs mid-compaction, the next startup recovers cleanly — incomplete compaction is idempotent

## Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `S4_COMPACTION_ENABLED` | `true` | Enable/disable the background compaction worker |
| `S4_COMPACTION_INTERVAL_HOURS` | `1` | How often the compaction worker checks for fragmented volumes |
| `S4_COMPACTION_THRESHOLD` | `0.3` | Minimum fragmentation ratio (dead bytes / total bytes) to trigger compaction |
| `S4_COMPACTION_DRY_RUN` | `false` | When `true`, analyze fragmentation without actually compacting |
| `S4_COMPACTION_FULL_TIME` | `02:00` | Daily full compaction time in HH:MM (local time). Set to empty string to disable |
| `S4_MULTIPART_UPLOAD_TTL_HOURS` | `24` | TTL for multipart sessions — used by both the cleanup worker and the compactor's orphan purge |
| `S4_COMPACTION_MULTIPART_TTL_SECS` | None | *Dev/testing only.* Overrides multipart session TTL for the compactor in seconds (takes priority over `S4_MULTIPART_UPLOAD_TTL_HOURS`) |

## Background Worker

Compaction runs automatically as a background task in `s4-server`. It operates in two modes:

- **Regular cycle** (every N hours, default 1): processes up to 10 volumes per run — lightweight, minimal I/O impact on production traffic
- **Daily full compaction** (once per day at configured time, default 02:00 local): processes all volumes with no limits, reclaiming all dead space

The worker:

- Starts on server boot (if enabled)
- On each interval tick, checks whether a daily full run is due (current time ≥ `S4_COMPACTION_FULL_TIME` and no full run done today)
- If due, runs full compaction; otherwise runs a regular cycle
- Does not block normal reads or writes
- Logs statistics after each run (prefixed with "Full compaction" or "Compaction")

## Dedup Awareness

The compactor is fully aware of S4's content deduplication:

- When a live blob is relocated, both its `IndexRecord` **and** `DedupEntry` are updated atomically
- Multiple objects referencing the same deduplicated blob are all updated correctly
- Reference counts are preserved during relocation

## Multipart Upload Awareness

The compactor correctly handles in-progress multipart uploads:

- **Active sessions** (younger than TTL) have their parts and blob_refs protected from purge
- **Completing sessions** are protected even if `created_at` is old, because `CompleteMultipartUpload` refreshes `updated_at`
- **Expired sessions** (no activity for longer than TTL) are purged along with their parts and blob_refs
- **Orphaned parts** (parts whose session was already deleted) are purged unconditionally

## Volume Scrubber

S4 also includes a volume scrubber that verifies CRC32 checksums of all blobs across all volumes, detecting bit rot or silent data corruption. The scrubber reports the number of healthy and corrupted blobs found.

## On-Demand Compaction (Admin API)

Compaction can be triggered manually via the Admin API. This requires SuperUser authentication (JWT token).

```bash
# Login to get JWT token
TOKEN=$(curl -s -X POST http://127.0.0.1:9000/api/admin/login \
    -H "Content-Type: application/json" \
    -d '{"username": "root", "password": "YOUR_PASSWORD"}' | jq -r '.token')

# Run compaction (compact all fragmented volumes)
curl -s -X POST "http://127.0.0.1:9000/api/admin/compaction/run" \
    -H "Authorization: Bearer $TOKEN"

# Dry run (analyze without compacting)
curl -s -X POST "http://127.0.0.1:9000/api/admin/compaction/run?dry_run=true" \
    -H "Authorization: Bearer $TOKEN"

# Custom threshold (only compact volumes with >50% dead space)
curl -s -X POST "http://127.0.0.1:9000/api/admin/compaction/run?threshold=0.5" \
    -H "Authorization: Bearer $TOKEN"
```

Response:

```json
{
    "volumes_scanned": 5,
    "volumes_compacted": 2,
    "bytes_reclaimed": 163840000,
    "errors": 0,
    "dry_run": false
}
```

## Monitoring

Check compaction activity in the server logs:

```
INFO s4_compactor: Compactor: live refs scan: 4 content hashes, 0 composite manifests
INFO s4_compactor: Compactor: purged 1 expired multipart sessions (TTL: 86400s)
INFO s4_compactor: Compactor: purged 7480 orphaned multipart part records (no live session)
INFO s4_compactor: Compactor: purged 7446 orphaned blob_ref entries
INFO s4_compactor: Compactor: orphan purge complete — 4 dedup + 7446 blob_ref entries removed
INFO s4_compactor: Compactor: compacted volume 20 -> 0: 0 live blobs copied, 63 dead skipped, 1056969333 bytes reclaimed
```

Enable dry-run mode to preview compaction without making changes:

```bash
export S4_COMPACTION_DRY_RUN=true
```
