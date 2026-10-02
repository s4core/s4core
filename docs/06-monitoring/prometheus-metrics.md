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

Total number of HTTP requests, labeled by method, status code, and normalized path. A request is
counted once it has been answered, so a scrape of `/metrics` shows up in the next scrape rather
than in its own.

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
http_request_duration_seconds_bucket{method="GET",path="/{bucket}/{key}",le="0.0025"} 1391
...
http_request_duration_seconds_bucket{method="GET",path="/{bucket}/{key}",le="30"} 1523
http_request_duration_seconds_bucket{method="GET",path="/{bucket}/{key}",le="+Inf"} 1523
http_request_duration_seconds_count{method="GET",path="/{bucket}/{key}"} 1523
http_request_duration_seconds_sum{method="GET",path="/{bucket}/{key}"} 3.142
```

Every histogram S4 exports has the same bucket bounds, in seconds: `0.001`, `0.0025`, `0.005`,
`0.01`, `0.025`, `0.05`, `0.1`, `0.25`, `0.5`, `1`, `2.5`, `5`, `10`, `30`. Buckets of different
nodes add up, so a cluster-wide percentile is one query (see below).

### s4_build_info

**Type:** Gauge, always `1`

The version of the node and the edition it runs as. An Enterprise build without a licence runs,
and reports, as `ce`.

```
s4_build_info{version="1.1.1",edition="ee"} 1
```

### s4_license_expiry_timestamp_seconds

**Type:** Gauge

Only on a node running under an Enterprise licence: when the licence expires, in Unix seconds.

```promql
# Days left on the licence.
(s4_license_expiry_timestamp_seconds - time()) / 86400
```

### Cluster

A cluster node exports the metrics below from its start, at zero: every value of a closed label,
every other member of `S4_POOL_NODES` as a `peer`, and the pool. An empty graph means nothing
happened, not that the data is missing.

Every number is the view of the node that exports it. A quorum operation is counted by the node
that coordinated it, a call to a peer by the node that made it, and a member's status is the one
this node holds. So scrape every node. No metric carries a node label, because Prometheus'
`instance` already tells the nodes apart. `peer` is a member's name in `S4_POOL_NODES`. Buckets,
keys and request ids never become labels.

| Metric | Type | Labels | What it counts, and what a change means |
|---|---|---|---|
| `s4_cluster_nodes` | gauge | `pool`, `status` = `alive`, `suspect`, `dead`, `left` | The members of `S4_POOL_NODES` by status as this node sees them, itself included. A member it has not heard from since it started is `dead`, so the four add up to the pool size. `suspect` and `left` stay at 0: a suspected member is reported straight as dead, and a stopping node does not announce that it leaves |
| `s4_cluster_peer_up` | gauge | `peer` | 1 while this node holds the peer alive. A peer at 0 on every node is down; at 0 on one node only, the network between the two is |
| `s4_cluster_epoch` | gauge | — | The topology epoch of this node. The nodes of a pool agree on it |
| `s4_quorum_operations_total` | counter | `operation` = `write`, `read`, `head`, `delete`, `list`, `multipart`; `outcome` = `ok`, `not_found`, `quorum_not_met`, `error` | Quorum operations this node coordinated, one per operation. A request into a bucket costs one more: the `head` of the bucket's marker that tells whether the bucket exists (a GET asks only when the object is not found). `quorum_not_met` means fewer replicas answered than the operation needs and the client got an error: the one to alert on |
| `s4_quorum_duration_seconds` | histogram | `operation` | How long those operations took |
| `s4_peer_requests_total` | counter | `peer`, `outcome` = `ok`, `error`, `timeout`, `skipped`, `cancelled` | Calls to the peer, one per call after its retries: quorum traffic, hints, anti-entropy and erasure coding alike. `skipped`: gossip holds the peer dead and no call was made. `cancelled`: the caller stopped waiting, usually because the quorum was already reached. Failures that pile up on one peer point at the node that holds the cluster back |
| `s4_peer_request_duration_seconds` | histogram | `peer` | How long the calls that got an answer or an error took (`ok`, `error`, `timeout`) |
| `s4_read_repairs_total` | counter | `outcome` = `ok`, `failed` | Writes of the latest version to a stale replica after a quorum read. A GET repairs nothing, so this moves with CopyObject, not with read traffic (see [Read Repair](../04-features/federation.md#read-repair)) |
| `s4_hints_pending` | gauge | `peer` | Hints this node holds on disk for the peer, counted from disk at start. It rises while the peer is away and falls to 0 once the peer is back and the hints are delivered |
| `s4_hints_stored_total` | counter | — | Hints written: a write or delete that did not reach a replica |
| `s4_hints_delivered_total` | counter | — | Hints after which their replica was at the current state of the key |
| `s4_hints_dropped_total` | counter | `reason` = `full`, `expired`, `rejected`, `superseded`, `ambiguous` | Hints that will not be delivered. `full`: the 1 GiB limit per peer was reached. `expired`: the hint outlived `S4_HINT_TTL_HOURS`. `rejected`: the replica refused what delivery sent it or is too old to delete one exact version, a copy came with no HLC at all, or the replica's record without an HLC (data written before the node joined a cluster, ordered by its modification time) is newer than the delete it missed. `superseded`: the key left replication, for erasure coding or another placement. `ambiguous`: the newest copy is on fewer replicas than a confirmed write leaves while another replica has no key, so it may be a copy that missed a delete, and it is not spread. A key behind any of these but `superseded` stays behind on that replica until a quorum read repairs it |
| `s4_anti_entropy_rounds_total` | counter | — | Anti-entropy rounds this node finished |
| `s4_anti_entropy_divergent_keys_total` | counter | — | Keys a round found behind on this node: the peer's copy is newer, or the two cannot be compared |
| `s4_anti_entropy_repairs_total` | counter | `outcome` = `ok`, `failed` | What became of each of those keys; `ok` + `failed` adds up to the divergent keys |
| `s4_repair_frontier_lag_seconds` | gauge | — | Age of the oldest repair frontier boundary over the peers of the pool, as of the last round; 0 before the first |
| `s4_tombstones_active` | gauge | — | Tombstones left after the last tombstone GC pass; 0 before the first |
| `s4_tombstones_purged_total` | counter | — | Tombstones GC removed |
| `s4_node_disk_total_bytes` | gauge | — | Size of the filesystem under the data directory, measured every 30 seconds for the peers; 0 when it cannot be measured |
| `s4_node_disk_used_bytes` | gauge | — | Space in use on that filesystem |
| `s4_scrubber_blobs_scanned_total` | counter | — | Blobs the background scrubber verified against their CRC32 |
| `s4_scrubber_corruptions_found_total` | counter | — | Corrupt blobs it found |
| `s4_scrubber_corruptions_healed_total` | counter | — | Corrupt blobs it rewrote from a healthy replica |
| `s4_scrubber_scan_progress` | gauge | — | How far the current full scan has got, from 0 to 1 |

Both histograms have the bucket bounds above. For each node the hint counters add up: pending =
pending at start + stored − delivered − dropped.

The anti-entropy rounds of a replicated pool are not started yet, so the anti-entropy and
tombstone metrics stay at 0 (see
[Federation → Anti-Entropy](../04-features/federation.md#anti-entropy-merkle-trees)).

What is worth an alert:

```promql
# A member some node holds not alive.
sum by (instance) (s4_cluster_nodes{status!="alive"}) > 0

# Quorum operations failing, by operation.
sum by (operation) (rate(s4_quorum_operations_total{outcome="quorum_not_met"}[5m])) > 0

# Calls failing, by peer: the node the cluster is waiting for.
sum by (peer) (rate(s4_peer_requests_total{outcome=~"error|timeout"}[5m])) > 0

# Writes a replica will not get from hinted handoff.
sum by (instance, reason) (increase(s4_hints_dropped_total[1h])) > 0

# Hints still waiting although their peer is up again. Delivery runs every 30
# seconds, so give it `for: 5m`. A hint also waits while another replica of its
# key is down, so this fires during a second outage too.
s4_hints_pending > 0 and on (instance, peer) s4_cluster_peer_up == 1

# A filesystem under a data directory over 90%.
s4_node_disk_used_bytes / (s4_node_disk_total_bytes > 0) > 0.9
```

And the percentiles of the cluster:

```promql
# p99 of quorum writes.
histogram_quantile(0.99, sum by (le) (rate(s4_quorum_duration_seconds_bucket{operation="write"}[5m])))

# p99 of each peer.
histogram_quantile(0.99, sum by (le, peer) (rate(s4_peer_request_duration_seconds_bucket[5m])))
```

The cluster routes of the admin API answer from the same state these metrics are written from —
see [Clustering → Health Check](../08-deployment/clustering.md#health-check).

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
alongside the metrics above: transcoding, repair, tiering, packed segments, set
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
| `/health` | `/health` |

## Grafana Integration

A dashboard for a cluster ships with the documentation:
[`grafana/s4-cluster.json`](grafana/s4-cluster.json). Import it in Grafana (**Dashboards → New →
Import**) and pick the Prometheus data source that scrapes the nodes. It shows membership as each
node sees it, quorum operations and their failures, calls to each peer, hinted handoff, read
repair, anti-entropy, tombstones, disks, the scrubber, versions and the licence, with a node
picker over every instance that exports `s4_cluster_epoch`. A test holds it to the metric list
above: every query names a metric, label and label value a node exports, and every cluster
metric is on a panel.

Example queries for any node, single or in a cluster:

```promql
# Request rate (requests per second)
rate(http_requests_total[5m])

# Average latency
rate(http_request_duration_seconds_sum[5m]) / rate(http_request_duration_seconds_count[5m])

# Error rate (4xx + 5xx)
sum(rate(http_requests_total{status=~"[45].."}[5m]))

# P99 latency of the whole cluster (drop `sum by (le)` for each node on its own)
histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))
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
