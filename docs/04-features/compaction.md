# Volume Compaction

S4 uses append-only volume files for data storage. When objects are deleted or overwritten, the old blob data remains as dead space. The compactor reclaims this space by rewriting volumes that contain a high proportion of dead blobs.

## How It Works

1. **Discover** all volume files (skipping the currently active one)
2. **Build dedup index**: map `(volume_id, offset) → content_hash` from the dedup keyspace for O(1) blob liveness checks
3. **Analyze** each volume — scan all blobs, classify as live or dead based on the dedup index
4. **Compact** volumes where fragmentation exceeds the threshold (default: 30% dead space):
   - Copy live blobs to a new volume via `VolumeWriter`
   - Atomically update all `IndexRecord` and `DedupEntry` locations in a single fjall batch
   - Rename old volume to `.dat.compacted`, then delete (crash-safe two-phase removal)
5. **Report** statistics: volumes compacted, bytes reclaimed, errors

## Crash Safety

Compaction is crash-safe by design:

- Old volumes are deleted **only after** all live blobs are verified relocated
- Two-phase removal: rename to `.dat.compacted` first, then delete
- If a crash occurs mid-compaction, the next startup recovers cleanly — incomplete compaction is idempotent

## Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `S4_COMPACTION_ENABLED` | `true` | Enable/disable the background compaction worker |
| `S4_COMPACTION_INTERVAL_HOURS` | `6` | How often the compaction worker checks for fragmented volumes |
| `S4_COMPACTION_THRESHOLD` | `0.3` | Minimum fragmentation ratio (dead bytes / total bytes) to trigger compaction |
| `S4_COMPACTION_DRY_RUN` | `false` | When `true`, analyze fragmentation without actually compacting |

## Background Worker

Compaction runs automatically as a background task in `s4-server`. It:

- Starts on server boot (if enabled)
- Runs periodically at the configured interval
- Does not block normal reads or writes
- Logs statistics after each run

## Dedup Awareness

The compactor is fully aware of S4's content deduplication:

- When a live blob is relocated, both its `IndexRecord` **and** `DedupEntry` are updated atomically
- Multiple objects referencing the same deduplicated blob are all updated correctly
- Reference counts are preserved during relocation

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
INFO s4_compactor: Compaction complete: 3 volumes compacted, 156.2 MB reclaimed, 0 errors
```

Enable dry-run mode to preview compaction without making changes:

```bash
export S4_COMPACTION_DRY_RUN=true
```
