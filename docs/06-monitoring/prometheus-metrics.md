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

### Bit rot and the scrubber (cluster mode)

The background scrubber verifies every blob on this node against its CRC32 and,
when one fails, fetches a healthy copy from a replica and writes it back:

```promql
# Blobs verified, damage found, and damage actually repaired — per node.
s4_scrubber_blobs_scanned_total
s4_scrubber_corruptions_found_total
s4_scrubber_corruptions_healed_total

# How far the current full scan has got, 0 .. 1.
s4_scrubber_scan_progress
```

`found` minus `healed` is what this node is still carrying damaged — on a
single node, where there is no replica to fetch from, that is all of it. A
counter that stays at zero while `blobs_scanned` grows is the healthy case; a
`blobs_scanned` that never grows at all means the scrubber is finding no
volumes to walk, which the node also says in its log at startup. What happens
around these numbers is described in
[Deduplication → Bit rot](../04-features/deduplication.md#bit-rot).

### Erasure coding (Enterprise)

An Enterprise build with an erasure-coded pool exports an `s4_ec_*` family
alongside the two above: transcoding, repair, tiering, packed segments, set
layout and node rejoin. Every one of them is defined, with its labels and what a
moving value means, in
[Erasure Coding → Metrics](../04-features/erasure-coding.md#metrics). That page
is the reference; this one does not repeat it.

Two of those series exist for one job — watching a replaced node come back — and
they behave differently from the rest, which is worth knowing before an empty
graph is read as good news:

```promql
# Shards the pool still misses from a replaced node. Zero ends the rebuild.
s4_ec_node_shards_missing{node_id="0d3bb4d8-876d-4b6a-a27f-49a69d11d3ae"}

# Whether that zero can be trusted: complete, incomplete or unknown.
s4_ec_node_rebuild_verdict{node_id="0d3bb4d8-876d-4b6a-a27f-49a69d11d3ae"} == 1

# Bytes the rebuild has written, and which limit is pacing it.
rate(s4_ec_repair_bytes_total{leg="write"}[5m])
rate(s4_ec_repair_throttled_total[5m])
```

The first two are written when the node's rebuild status is asked for through
the admin API, because the count behind them is a walk over the pool's
manifests rather than something a scrape can do. A node nobody has asked about
has no series at all — absent, not zero. The full procedure, including the
request that refreshes them, is in
[Erasure Coding → Replacing a node](../04-features/erasure-coding.md#replacing-a-node).

Read traffic is the other pair worth having on a dashboard, because it is what
says whether a pool is paying for its redundancy on every read:

```promql
# Bytes reads pulled out of shards, and how much of it crossed a zone.
rate(s4_ec_read_shard_bytes_total[5m])
rate(s4_ec_read_shard_bytes_total{scope="cross_zone"}[5m])

# Round trips those reads made. Shards are fetched one after another, so this
# is the part of read latency that narrowing the read takes away.
rate(s4_ec_read_shard_fetches_total[5m])
```

Unlike the two above these are written as reads happen, and all eight series
exist from startup at zero. A node reporting everything under
`scope="unknown_zone"` has no `S4_EC_NODE_TOPOLOGY` and cannot place itself in
the pool; the totals are still right. What the switch behind these numbers does
is in
[Erasure Coding → Reading only what is needed](../04-features/erasure-coding.md#reading-only-what-is-needed).

Small objects in an EC pool are not erasure coded; they are placed as RF=3 with
an explicit record saying so. Two counters say whether that record is where it
belongs:

```promql
# Where reads found the record. Steady pool_fanout means it is not on the nodes
# that hold the objects, and every such read costs the whole pool instead of RF.
rate(s4_ec_small_placement_resolves_total{source="replicas"}[5m])
rate(s4_ec_small_placement_resolves_total{source="pool_fanout"}[5m])

# Records a write could not get onto a majority of an object's replicas, and
# the sweep settling them afterwards. `recorded` rising while `cleared` stays
# flat is a pool that is not finishing the job.
rate(s4_ec_small_placement_hints_total{outcome="recorded"}[5m])
rate(s4_ec_small_placement_hints_total{outcome="cleared"}[5m])
```

One thing those reads no longer do is notice a parity shard going bad, because
they no longer fetch one. The local scrub pass does that instead, and it has no
series of its own: what it finds is watched through the repairs it queues —
`s4_ec_repair_jobs_total`, `s4_ec_repair_queue_depth` and the repair status of
the pool. A pool where nothing is ever repaired is a pool whose scrub worker may
simply not be running.

The repair queue gauges are refreshed the same way — by a request for pool
health — and they are the pair to watch during an incident:

```promql
# How deep the repair queue of one set is.
s4_ec_repair_queue_depth{erasure_set="1"}

# And what it is made of: chunks by the number of shards they are missing.
s4_ec_repair_queue_depth_by_losses{erasure_set="1"}
```

The second is the one that says whether a backlog is serious. Its buckets add up
to the first, and a bucket above `1` means chunks whose redundancy is partly
gone — read against the profile of that set, because the same count means
different things to `ec-rs-small` and `ec-rs-dense`. Which of those tasks is
carried out first is decided by the same number, and is described in
[Erasure Coding → Which repair runs next](../04-features/erasure-coding.md#which-repair-runs-next).

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
