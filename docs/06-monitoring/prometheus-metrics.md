# Prometheus Metrics

S4 exposes metrics in Prometheus text format on the main HTTP port at `/metrics`.

## Enable / Disable

Metrics are enabled by default. To disable:

```bash
export S4_METRICS_ENABLED=false
```

When disabled, the `/metrics` endpoint returns `503 Service Unavailable`.

## Endpoint

```bash
curl http://localhost:9000/metrics
```

**Response:** Prometheus text format (`text/plain; version=0.0.4`)

## Available Metrics

### http_requests_total

**Type:** Counter

Total number of HTTP requests, labeled by method, status code, and normalized path.

```
http_requests_total{method="GET",status="200",path="/{bucket}/{key}"} 1523
http_requests_total{method="PUT",status="200",path="/{bucket}/{key}"} 847
http_requests_total{method="DELETE",status="204",path="/{bucket}/{key}"} 32
```

### http_request_duration_seconds

**Type:** Histogram

Request duration in seconds, labeled by method and normalized path.

```
http_request_duration_seconds_bucket{method="GET",path="/{bucket}/{key}",le="0.001"} 1200
http_request_duration_seconds_bucket{method="GET",path="/{bucket}/{key}",le="0.01"} 1480
http_request_duration_seconds_bucket{method="GET",path="/{bucket}/{key}",le="0.1"} 1520
http_request_duration_seconds_bucket{method="GET",path="/{bucket}/{key}",le="+Inf"} 1523
http_request_duration_seconds_count{method="GET",path="/{bucket}/{key}"} 1523
http_request_duration_seconds_sum{method="GET",path="/{bucket}/{key}"} 3.142
```

## Path Normalization

To prevent high cardinality, request paths are normalized:

| Actual Path | Normalized Path |
|-------------|----------------|
| `/my-bucket/my-key.txt` | `/{bucket}/{key}` |
| `/my-bucket` | `/{bucket}` |
| `/api/admin/users` | `/api/admin` |
| `/api/stats` | `/api/stats` |
| `/metrics` | `/metrics` |

## Grafana Integration

Add S4 as a Prometheus data source in Grafana. Example queries:

```promql
# Request rate (requests per second)
rate(http_requests_total[5m])

# Average latency
rate(http_request_duration_seconds_sum[5m]) / rate(http_request_duration_seconds_count[5m])

# Error rate (4xx + 5xx)
sum(rate(http_requests_total{status=~"[45].."}[5m]))

# P99 latency
histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))
```

## Prometheus Configuration

Add S4 to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 's4'
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost:9000']
    metrics_path: '/metrics'
```
