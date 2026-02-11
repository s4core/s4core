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
  "dedup_ratio": 1.54
}
```

## Fields

| Field | Type | Description |
|-------|------|-------------|
| `uptime_seconds` | integer | Server uptime in seconds |
| `buckets_count` | integer | Total number of buckets |
| `objects_count` | integer | Total number of objects across all buckets |
| `storage_used_bytes` | integer | Total storage used in bytes |
| `dedup_unique_blobs` | integer | Number of unique data blobs on disk |
| `dedup_total_references` | integer | Total object references (includes dedup) |
| `dedup_ratio` | float | Deduplication ratio (`total_references / unique_blobs`) |

## Use Cases

- **Web Console Dashboard** — the S4 Console uses this endpoint for its dashboard widgets
- **Health Checks** — monitoring systems can poll this endpoint to verify S4 is running
- **Custom Dashboards** — build your own monitoring UI using this JSON data
- **Alerts** — trigger alerts based on storage usage or object counts

## Difference from /metrics

| | `/api/stats` | `/metrics` |
|---|---|---|
| Format | JSON | Prometheus text |
| Audience | Web UI, custom scripts | Prometheus/Grafana |
| Content | Aggregate stats | Per-request counters and histograms |
| Auth required | No | No |
