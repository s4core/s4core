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
| `s4_cluster_migration_pending_records` | gauge | — | This node's single-mode objects and bucket markers still waiting to reach every owner after a switch to cluster mode; zero on an empty peer. Wait for zero on every node before switching clients. A stalled value means an owner is unavailable, runs another S4 version, or cannot accept the copy; the source log names why |
| `s4_cluster_migration_records_total` | counter | `outcome` = `migrated`, `superseded` | Single-mode records whose migration finished during this process. `migrated`: every owner accepted the earlier copy. `superseded`: a newer cluster write or delete took precedence. Completed records are not counted again after restart |
| `s4_quorum_operations_total` | counter | `operation` = `write`, `read`, `head`, `delete`, `list`, `multipart`; `outcome` = `ok`, `not_found`, `quorum_not_met`, `error`; `origin` = `client`, `system` | Quorum operations this node coordinated, one per operation. `origin="client"` counts the operations that served a request to this node, S3 or admin; `origin="system"` counts the node's own work, such as publishing the pool's records when it starts, moving a grown pool's keys and background workers. A request into a bucket costs one more: the `head` of the bucket's marker that tells whether the bucket exists (a GET asks only when the object is not found). `quorum_not_met` means fewer replicas answered than the operation needs and the client got an error: the one to alert on |
| `s4_quorum_duration_seconds` | histogram | `operation`, `origin` | How long those operations took. The latency clients see is `origin="client"` |
| `s4_peer_requests_total` | counter | `peer`, `outcome` = `ok`, `error`, `timeout`, `skipped`, `cancelled` | Calls to the peer, one per call after its retries: quorum traffic, hints, anti-entropy and erasure coding alike. `skipped`: gossip holds the peer dead and no call was made. `cancelled`: the caller stopped waiting, usually because the quorum was already reached. Failures that pile up on one peer point at the node that holds the cluster back |
| `s4_peer_request_duration_seconds` | histogram | `peer` | How long the calls that got an answer or an error took (`ok`, `error`, `timeout`) |
| `s4_read_repairs_total` | counter | `outcome` = `ok`, `failed` | Writes of the latest version to a stale replica after a quorum read. A GET repairs nothing, so this moves with CopyObject, not with read traffic (see [Read Repair](../04-features/federation.md#read-repair)) |
| `s4_hints_pending` | gauge | `peer` | Hints this node holds on disk for the peer, counted from disk at start. It rises while the peer is away and falls to 0 once the peer is back and the hints are delivered |
| `s4_hints_stored_total` | counter | — | Hints written: a write or delete that did not reach a replica |
| `s4_hints_delivered_total` | counter | — | Hints after which their replica was at the current state of the key |
| `s4_hints_dropped_total` | counter | `reason` = `full`, `expired`, `rejected`, `superseded`, `ambiguous` | Hints that will not be delivered. `full`: the 1 GiB limit per peer was reached. `expired`: the hint outlived `S4_HINT_TTL_HOURS`. `rejected`: the replica refused what delivery sent it or is too old to delete one exact version, a copy came with no HLC at all, or the replica's record without an HLC (data written before the node joined a cluster, ordered by its modification time) is newer than the delete it missed. `superseded`: the key left replication, for erasure coding or another placement. `ambiguous`: the newest copy is on fewer replicas than a confirmed write leaves while another replica has no key, so it may be a copy that missed a delete, and it is not spread. A key behind any of these but `superseded` stays behind on that replica until a quorum read repairs it |
| `s4_anti_entropy_rounds_total` | counter | `outcome` = `complete`, `incomplete`, `skipped` | Anti-entropy rounds this node started. `complete`: it compared every key it shares with the peer and settled every one that differed. `incomplete`: the peer stopped answering, a key could not be settled, or more keys differed than one round settles (`S4_ANTI_ENTROPY_MAX_KEYS_PER_ROUND`); the rest waits for the next rounds. `skipped`: a live node of the pool announces protocol below 8, the live peer whose turn it is has not announced its protocol yet, or no peer is alive. An unknown peer retains its turn. Current protocol: 9. A node with `S4_ANTI_ENTROPY_ENABLED=false` counts none |
| `s4_anti_entropy_round_duration_seconds` | histogram | — | How long the rounds that ran took |
| `s4_anti_entropy_divergent_keys_total` | counter | — | Keys a round found in different versions on this node and its peer, or on one of them only |
| `s4_anti_entropy_repairs_total` | counter | `outcome` = `ok`, `failed`, `ambiguous` | What became of each of those keys; the three add up to the divergent keys. `ok`: every replica that answered holds the newest version now. `failed`: a replica did not answer or refused it. `ambiguous`: too few owners confirm the newest copy and an absent owner cannot exclude an older delete, including one whose tombstone was purged; the copy is not spread |
| `s4_repair_frontier_lag_seconds` | gauge | — | Age of the oldest repair frontier boundary over the peers of the pool, as of the last round: how long ago the oldest last complete round with a peer started, plus 60.5 s (a boundary stands that far before its round, the time a delete may still take to reach a replica), counted from the node's start for a peer without one; 0 before the first round. Only complete rounds advance cuts; cuts are held in memory and start over after a restart |
| `s4_tombstones_active` | gauge | — | Tombstones left after the last tombstone GC pass; 0 before the first |
| `s4_tombstones_purged_total` | counter | — | Tombstones GC removed |
| `s4_bucket_unenforced_configs` | gauge | `kind` = `versioning`, `object_lock` | Buckets whose versioning or Object Lock this node stores from an earlier release, which accepted them in cluster mode; cluster mode does not enforce either. Counted once the node has handed its earlier configurations over at start (within a minute), again after each such pass and every 10 minutes; a bucket leaves the count when it is deleted. Above 0: see [Object Lock → Cluster Mode](../04-features/object-lock.md#cluster-mode) |
| `s4_bucket_config_adoption_keys_total` | counter | `outcome` = `adopted`, `superseded`, `orphaned`, `deferred` | Bucket configurations an earlier release kept on this node alone, by what handing them to the nodes that keep them now came to. `adopted`: the nodes that keep the configuration hold it. `superseded`: they held a version changed later, or deleted it later. `orphaned`: the bucket was deleted or created again since, and the configuration does not count. `deferred`: a node that keeps it did not answer, and the next pass, 30 s later, counts it again. Moves only after an upgrade from such a release (see [Federation](../04-features/federation.md)) |
| `s4_bucket_config_quorum_reads` | gauge | — | 1 when this node reads bucket configurations through the quorums and disables local fallback; 0 during its initial legacy adoption. Quorum reads persist after adoption or uncertain configuration history, including while another node is down |
| `s4_bucket_metadata_stage` | gauge | `pool` | Where this node is in the move of the pool's bucket records and settings to more replicas: `0` three replicas, `1` writing to both layouts while bringing the older records over, `2` reading from the new layout, `3` settled. A pool with nothing to move is `3` from the start. A value below `3` for longer than a few minutes means a node of the pool is down or runs an earlier release; `/api/admin/cluster/health` names it (see [Federation → Buckets](../04-features/federation.md#buckets)) |
| `s4_bucket_metadata_replication_factor` | gauge | `pool` | Replicas the pool keeps each bucket's records and settings on once the move settled: the data's in a replicated pool, `5` for RS 4+2 on six nodes, or `S4_BUCKET_METADATA_REPLICATION_FACTOR` |
| `s4_bucket_metadata_backfill_keys_total` | counter | `outcome` = `copied`, `already`, `unconfirmed`, `failed`, `dropped` | Bucket records and settings this node brought to the new replicas during the move. `copied`: a new replica got them. `already`: every replica held them. `unconfirmed`: a write that was refused left them on too few nodes, and they stay as they were — they read as absent before the move and after. `failed`: a replica did not answer, and the next pass, 10 s later, counts them again. `dropped`: a record an earlier release made up on a node that did not keep it, removed before that node keeps it |
| `s4_bucket_metadata_backfill_passes_total` | counter | `outcome` = `complete`, `incomplete` | Passes of that backfill. The move goes on once a pass is complete on every node |
| `s4_pool_generation` | gauge | `pool` | Epoch of the pool generation this node places keys by. The first generation is `1`, and each growth of the pool adds one. While the pool grows it is the epoch the keys move from; once the growth settled, the grown one. Nodes that disagree for longer than a growth takes point at a node that has not settled (see [Clustering → Growing a pool](../08-deployment/clustering.md#growing-a-pool)) |
| `s4_rebalance_ranges` | gauge | `state` = `pending`, `streaming`, `verified`, `cut_over` | Ranges of the ring of a growing pool by state on this node. `pending`: not begun; `streaming`: being moved; `verified`: the new owners hold what the old owners confirmed; `cut_over`: read through the grown generation. All `0` while the pool does not grow |
| `s4_rebalance_keys_total` | counter | — | Keys this node brought to the new owners of a growing pool: a new owner received a version it lacked |
| `s4_rebalance_bytes_total` | counter | — | Bytes of the copies this node sent to the new owners |
| `s4_rebalance_remaining_bytes` | gauge | — | Estimate of what this node still has to go through before it handed its keys over: the bytes it stores, in the share of its ranges not handed over yet. It counts erasure-coded shards too, which do not move, so in an erasure-coded pool it is an upper bound. `0` once the node handed over everything, on a new node, and while the pool does not grow |
| `s4_rebalance_paused` | gauge | `reason` = `operator`, `no_room` | `1` while the move of this node's keys is paused. `operator`: paused through the admin API, until it is resumed. `no_room`: a new owner has less free space than `S4_REBALANCE_MIN_FREE_RATIO`, and the keys meant for it wait until space is freed |
| `s4_rebalance_errors_total` | counter | `reason` = `checksum`, `unavailable`, `ambiguous`, `storage`, `placement` | Keys that failed a try to reach their new owners. `checksum`: a copy did not match its CRC32 or SHA-256, and it is taken from another replica. `unavailable`: a replica did not answer. `ambiguous`: no write quorum of the old owners holds the newest version. `storage` and `placement`: this node could not read the key or place it. Every such key is tried again; the move of its range does not end until it got through |
| `s4_pool_generation_fenced_writes_total` | counter | — | Writes this node refused because their coordinator placed them by an earlier pool generation than the one the pool moves to: a node of an earlier release, or one restarted with the pool's earlier `S4_POOL_NODES`. The client of that node gets `503`. Restart it with the current release and configuration. Copies that repair brings, ordered by their HLC, are still accepted |
| `s4_bucket_metadata_fenced_writes_total` | counter | — | Writes of bucket records and settings this node refused because they came from a node of an earlier release, or one behind the pool, after the move began. Any increase means such a node is in the pool; it cannot create or delete buckets until it runs this release |
| `s4_iam_fenced_writes_total` | counter | — | Writes of users and root's state this node refused because they were made outside the current lease of IAM changes: by a node whose lease ran out or was taken over by a later one while it stalled or was cut off. Such a write lands nowhere, and the change it belonged to was answered `503`. A short burst after a node stalled is fencing at work; a steady rate means a node keeps writing with a lease it no longer holds |
| `s4_iam_epoch_applied` | gauge | — | Wall time, in Unix milliseconds, of the newest change of users or root's state this node holds, deletes included; `0` while it holds none. Nodes of one pool show the same value a moment after a change; one that stays behind missed changes |
| `s4_iam_replica_lag_seconds` | gauge | — | How long ago a majority of the pool last agreed with this node's copy of users and keys — since the node started, while none has yet. It stays below a second in a healthy pool. Past `S4_IAM_MAX_STALENESS_SECS` the node refuses IAM credentials with `503` |
| `s4_iam_auth_refused_total` | counter | `reason` = `starting`, `stale` | Requests with the password, token or S3 key of an IAM user this node answered `503`, because no majority of the pool had agreed with its copy of users and keys. `starting`: not yet since the node started — it is catching up. `stale`: not for longer than `S4_IAM_MAX_STALENESS_SECS` — the node is cut off from most of its pool. Root and the S3 credentials of the environment are never refused this way |
| `s4_iam_config_mismatch` | gauge | `kind` = `root`, `jwt_secret`, `encryption_key` | `1` while the live nodes of the pool are configured differently: root's name or password (`root`), `S4_JWT_SECRET` (`jwt_secret`), or the key the secrets of S3 credentials are encrypted with (`encryption_key`). The cluster health is then `critical` and names the node that differs. Nodes compare fingerprints carried by gossip, never the values |
| `s4_iam_records_by_key_id` | gauge | `key_id` | How many IAM users this node holds whose S3 secret is encrypted with the key of this id; `key_id="clear"` counts secrets still in clear text, as an earlier release kept them. The series of the current key is always there, `0` while no user has S3 credentials. After a new `S4_IAM_ENCRYPTION_KEY`, the series of the old key falls to `0` once every secret is encrypted again: then the old key can be removed. Present on single nodes too |
| `s4_iam_adoption_pending` | gauge | — | `1` while some node of the pool keeps IAM users of its own that the pool has not taken over — an earlier release, or the node as a single node, stored them on it alone — and IAM changes answer `409 IamAdoptionPending`; `0` once every node announced it keeps none. A new pool shows `1` for a moment after it starts. After an upgrade it stays `1` until the operator decides through `/api/admin/iam/adoption` (see [Cluster Mode](../05-iam/roles-and-permissions.md#cluster-mode)) |
| `s4_node_disk_total_bytes` | gauge | — | Size of the filesystem under the data directory, measured every 30 seconds for the peers; 0 when it cannot be measured |
| `s4_node_disk_used_bytes` | gauge | — | Space in use on that filesystem |
| `s4_scrubber_blobs_scanned_total` | counter | — | Blobs the background scrubber verified against their CRC32 |
| `s4_scrubber_corruptions_found_total` | counter | — | Corrupt blobs it found |
| `s4_scrubber_corruptions_healed_total` | counter | — | Corrupt blobs it rewrote from a healthy replica |
| `s4_scrubber_scan_progress` | gauge | — | How far the current full scan has got, from 0 to 1 |

Every histogram has the bucket bounds above. For each node the hint counters add up: pending =
pending at start + stored − delivered − dropped.

The repair frontier lag of a node grows while one of its peers is down or its rounds with a peer
do not complete: tombstones of the keys it shares with that peer are not removed meanwhile, and
`s4_tombstones_active` grows with the deletes. Once every peer has had a complete round, the lag
falls back below 60.5 s plus a turn over the peers, `S4_ANTI_ENTROPY_INTERVAL_SECS` × peers.
A complete round records a cut at its start minus the 60.5 s arrival window; incomplete and
skipped rounds record none. The node itself needs no cut. After a restart tombstone GC waits
for fresh complete rounds with every other owner of each key. A node with rounds disabled
still answers its peers, but advances no cuts of its own and purges no tombstones.

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

# A growing pool whose move waits for space on a new node.
max by (instance) (s4_rebalance_paused{reason="no_room"}) > 0

# A node that keeps writing by an earlier pool generation.
sum by (instance) (rate(s4_pool_generation_fenced_writes_total[5m])) > 0
```

And the percentiles of the cluster:

```promql
# p99 of the quorum writes of client requests.
histogram_quantile(0.99, sum by (le) (rate(s4_quorum_duration_seconds_bucket{operation="write", origin="client"}[5m])))

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
