# Stats API

S4 provides a lightweight JSON stats endpoint designed for dashboard widgets and quick health checks.

## Endpoint

**GET** `/api/stats`

```bash
curl http://localhost:9000/api/stats
```

No authentication required.

## Response

```json
{
  "uptime_seconds": 86400,
  "buckets_count": 5,
  "objects_count": 12345,
  "storage_used_bytes": 1073741824,
  "dedup_unique_blobs": 8000,
  "dedup_total_references": 12345,
  "dedup_ratio": 0.352,
  "complete": true
}
```

## Fields

| Field | Type | Description |
|-------|------|-------------|
| `uptime_seconds` | integer | Server uptime in seconds |
| `buckets_count` | integer | Total number of buckets |
| `objects_count` | integer | Total number of objects across all buckets |
| `storage_used_bytes` | integer | Total size of the objects in bytes, before deduplication and replication |
| `dedup_unique_blobs` | integer | Number of unique data blobs on this node's disk |
| `dedup_total_references` | integer | Object references to them on this node (includes dedup) |
| `dedup_ratio` | float | Share of references served by a blob already stored: `1 - unique_blobs / total_references`, from `0.0` (no deduplication) towards `1.0` |
| `complete` | boolean | `false` on a cluster node when a node of the pool did not answer, so that the bucket and object numbers miss what only it holds; always `true` on a single node |

### On a cluster node

`buckets_count`, `objects_count` and `storage_used_bytes` are the pool's, whichever node is asked:
buckets and objects are listed from every node of the pool and merged by key, so an object counts
once, at the size of its newest version. Counting the pool lists every object in it, so a node
reuses the count for up to 10 seconds and computes it once for all the requests that arrive
meanwhile; creating or deleting a bucket through the node counts again at once. The deduplication
fields stay the node's own: each node deduplicates what it stores.

## Use Cases

- **Web Console Dashboard** — the S4 Console uses this endpoint for its dashboard widgets
- **Health Checks** — use `/health` instead: it answers at once, while this endpoint counts every
  object (on a cluster node, of the whole pool)
- **Custom Dashboards** — build your own monitoring UI using this JSON data
- **Alerts** — trigger alerts based on storage usage or object counts

## Difference from /metrics

| | `/api/stats` | `/metrics` |
|---|---|---|
| Format | JSON | Prometheus text |
| Audience | Web UI, custom scripts | Prometheus/Grafana |
| Content | Aggregate stats | Per-request counters and histograms |
| Auth required | No | No |
