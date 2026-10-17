# Federation (Distributed Mode)

> **Status:** Gossip membership, quorum replication, conflict resolution (HLC+LWW), read repair on quorum reads, hinted handoff, anti-entropy, bit rot protection and cluster operations work. Tombstone GC removes tombstones after the grace period and completed repair rounds — see [Distributed Tombstones & GC](#distributed-tombstones--gc).

S4 supports a **leaderless distributed mode** where multiple nodes form a cluster and replicate data using quorum protocols. This provides high availability and horizontal scaling while maintaining S3 API compatibility.

## Key Concepts

### Leaderless Replication

Every node in an S4 cluster is equal — there is no single leader or master. Any node can accept any S3 request and act as the **coordinator** for that operation. The coordinator fans out reads and writes to the appropriate replicas and collects quorum responses.

### Server Pools

A cluster is composed of one or more **server pools**. Each pool is a fixed set of nodes that replicate data among themselves.

```
Cluster
  +-- Pool 1: [Node A, Node B, Node C]   (RF=3, all 3 nodes store all data)
  +-- Pool 2: [Node D, Node E, Node F]   (added later for capacity)
```

Standard pools use quorum replication. Enterprise Edition also has an
erasure-coded pool type (`S4_POOL_TYPE=ec`) with fixed-width erasure sets; see
[Federation Erasure Coding](erasure-coding.md) for status, setup, and
configuration.

**Rules:**
- A pool grows by **appending** nodes (Enterprise Edition); nodes are never removed from a pool or
  reordered. The keys whose replicas change move to the new nodes in the background — see
  [Clustering → Growing a pool](../08-deployment/clustering.md#growing-a-pool)
- Each **bucket** is pinned to one pool at creation time; all objects in that bucket live in that pool
- New buckets are created in the pool with the most free space

### Quorum Parameters

Default configuration: **N=3, W=2, R=2** (replication factor 3, write quorum 2, read quorum 2).

| Parameter | Default | Meaning |
|-----------|---------|---------|
| N (RF) | 3 | Each object is stored on 3 nodes |
| W | 2 | A write succeeds when 2 of 3 replicas confirm |
| R | 2 | A read succeeds when 2 of 3 replicas respond |

This guarantees:
- **Read-your-writes:** W + R = 4 > N = 3, so at least one replica in every read quorum has the latest write
- **Split-brain safety:** 2W = 4 > N = 3, so two partitions cannot both accept writes
- **Fault tolerance:** Cluster continues to serve reads and writes with 1 node down

### Node Identity

Each node gets a unique UUID (persisted in `data_dir/node_id`) on first startup. This ID never changes across restarts, ensuring the node retains ownership of its data.

### Failure Detection (SWIM Gossip)

Nodes discover each other via seed addresses and use the **SWIM protocol** (via the `foca` crate) for failure detection:

- **Alive → Dead:** a node that stops answering pings, directly and through other nodes, is
  suspected by gossip and reported down 5 s later; S4 then marks it dead
- **Dead → Alive:** the node is alive again as soon as it is heard from. A node declared dead
  while it was paused or cut off by the network is told so by the first peer it reaches and
  rejoins under its next incarnation; every node keeps announcing itself to the seeds it does not
  see alive, so a healed network split closes on its own within seconds
- **Metadata propagation:** Each node broadcasts its capacity, version, and pool membership via gossip

### Inter-Node Communication (gRPC)

Nodes communicate over gRPC (default port: 9100) for data and metadata replication:

- **WriteBlob / ReadBlob** — replicate object data between nodes
- **WriteMetadata / ReadMetadata** — replicate metadata records
- **HeadObject** — lightweight digest for quorum comparison
- **Streaming** — large objects (>8 MB) use gRPC streaming
- **TLS** — optional TLS for inter-node traffic

### Data Placement

For the recommended deployment (pool size = RF = 3), every node in the pool stores 100% of the pool's data. No hash ring is needed — all operations broadcast to all pool nodes.

For larger pools (pool size > RF), a consistent hash ring with 128 virtual nodes per physical node routes keys to the correct RF replicas.

A listing asks every node of the pool and merges what they hold, deletes included. It answers
only while every key still has a read quorum of replicas answering — no more than `RF − R` nodes
of the pool silent, one with RF = 3 and R = 2 — and `503` otherwise: with more nodes down some
object's last write or delete may sit only on the silent ones, and a listing without them could
leave out an object or show a deleted one. The check that a bucket is empty before DeleteBucket
is such a listing too.

### Buckets

A bucket exists while its marker does, and the marker is written and read through the same quorums
as objects. CreateBucket and DeleteBucket change it through a write quorum, from the S3 API and
the admin API alike, and every request that writes to, lists or deletes from a bucket first reads
it through a read quorum: PUT, CopyObject, multipart uploads, DeleteObject, DeleteObjects,
listings, tagging and bucket configuration. Since R + W > N, a bucket created through any node
takes writes through every other at once, and one deleted through any node answers `NoSuchBucket`
everywhere. A read of one object asks for its bucket only when the object is not found, so GET
tells `NoSuchBucket` from `NoSuchKey` without slowing the reads that find their object; HEAD,
whose 404 has no body, does not ask. When too few replicas answer to tell whether the bucket
exists, the request answers `503` and writes nothing.

The marker and the bucket's configurations survive as many lost nodes as the pool does. In a
replicated pool they are placed like its objects, with the same replicas and quorums. In an
erasure-coded pool, which survives more lost nodes than three replicas do, they are kept on
`2f + 1` replicas read and written through a majority, `f` being the nodes the profile survives:
five replicas with quorums of three for RS 4+2 on six nodes (see
[Two Nodes Down](erasure-coding.md#two-nodes-down)). `S4_BUCKET_METADATA_REPLICATION_FACTOR` raises
the number up to the pool's nodes; it must be the same on every node.

An EC pool reaches that number by itself, in four stages, once every node runs a release that
knows them: `legacy` (three replicas), `dual_write` (writes to both layouts, while each node brings
the records it holds to the new replicas), `read_new` (reads from the new layout) and `settled`.
A node takes the next stage only once every node of the pool announced the current one, so a
stage waits while any node of the pool is down, and a stage reached is never left: a restarted
node goes on from it. `GET /api/admin/cluster/health` answers where the node is:

```json
"bucket_metadata": {
  "stage": 1, "stage_name": "dual_write",
  "from": {"replication_factor": 3, "write_quorum": 2, "read_quorum": 2},
  "to": {"replication_factor": 5, "write_quorum": 3, "read_quorum": 3},
  "backfill_complete": false,
  "held_by": [{"node": "node-4", "reason": "not alive"}],
  "held_for_secs": 42
}
```

`held_by` names the nodes the next stage waits for and why; a stage that waits longer than ten
minutes is also logged as a warning every ten minutes, with the same list. Once the move began, a
node of an earlier release that rejoins the pool cannot create or delete buckets: the other nodes
refuse its writes of bucket records, and count them in `s4_bucket_metadata_fenced_writes_total`.

As with S3's own bucket operations, deleting a bucket is not atomic with requests already past
their check: an object written in the same instant as its bucket is deleted can outlive it.

A bucket's CORS, lifecycle, policy and encryption configurations are kept the same way: each is
written and deleted through a write quorum and read through a read quorum, so a configuration set
through any node is seen through every other at once and survives the loss of the node that took
it. Each configuration changes on its own, and when two writes of the same one race, the later
wins. A bucket deleted and created again under the same name starts without the configurations of
the first one, even when a configuration write raced the delete. When too few replicas answer, the
request answers `503` and may be retried. Lifecycle rules apply on every node of the pool to the
copies of the objects it keeps, whichever node they were set through.

Of these four, S4 applies only lifecycle rules. A bucket policy and an encryption configuration
are stored and returned, but a policy does not restrict access and objects are not encrypted; the
CORS rules of a bucket are stored and returned too, while the server answers browsers from every
origin (see [CORS](cors.md#what-s4-applies)). This holds in single mode as well.

An earlier release kept each of these configurations only on the node that took the request. After
the upgrade every node hands the configurations it holds to the nodes that now keep them; where one
configuration got different values through different nodes, the one changed last wins, and one
left from a bucket deleted since is dropped. Until every node of the pool has done so, a node also
weighs the configuration it holds itself: it answers with it unless the pool holds a newer value,
or has deleted it, or the configuration belongs to an earlier bucket of the same name. A node that
stays down holds this back until it returns. `s4_bucket_config_quorum_reads` turns 1 on a node once
it reads every configuration from the pool, and stays 1 after the node restarts, including after a
crash or while a peer is down. Uncertain deletion history never reopens local fallback; a lone older
configuration waits for confirmation instead of overwriting a delete.
`s4_bucket_config_adoption_keys_total` counts what was handed over
([metrics](../06-monitoring/prometheus-metrics.md)).

[Versioning](versioning.md#cluster-mode) and [Object Lock](object-lock.md#cluster-mode) are not
supported in cluster mode: objects are written and deleted without versions or retention checks,
and every request that would turn either on answers `501 NotImplemented`.

### Hinted Handoff

When a write or a delete does not land on a replica — the replica refused it, did not answer
within the replica timeout (60 s), or gossip holds it dead — the coordinator stores a **hint** for
it: this key on this replica may be behind. A replica that takes the write, even after the client
already has its answer, gets no hint, so a healthy cluster stores none. A node never hints itself.

A hint holds the bucket, the key and the HLC of the missed operation, not the object. The hints for
one replica are appended to one log on the coordinator's disk, up to 1 GB; a hint that does not fit
is dropped and counted in `s4_hints_dropped_total{reason="full"}`. Hints older than
`S4_HINT_TTL_HOURS` (3 hours) are removed — at most a quarter of that later — and counted as
`expired`; `0` keeps no hints at all.

Every 30 seconds the coordinator delivers its hints to the replicas gossip holds alive. A hint
does not replay the operation: the coordinator reads the key from every other replica and brings
the returned one to the newest version it finds.

- **The key exists.** Its copy is written to the replica with that version's HLC, so a replica that
  already has something newer keeps it.
- **No other replica has the key.** The replica's record is removed only if it is no newer than
  the operation the replica missed, and only by that record's exact HLC.
- **The key is erasure coded now.** No copy is written; the hint counts as
  `dropped{reason="superseded"}`.
- **The other replicas disagree.** A replica that took a delete simply has no key, which cannot be
  told from one that never got the write. So a copy is written only when no other replica lacks
  the key, or when as many of them hold it as a confirmed write leaves behind (with `W=2` and three
  replicas, both others). Otherwise the copy may be one that missed a delete, and writing it would
  bring the deleted key back: nothing is written, and the hint counts as
  `dropped{reason="ambiguous"}` — unless the replica already holds that version or a newer one.

Hints for a key may therefore be delivered in any order and more than once. A hint whose key cannot
be settled now waits for a later pass without holding up the others: another replica did not
answer or gossip holds it down, or the write the hint names may still be landing on the other
replicas (for up to the 60-second replica write timeout after the hint was taken). Only a replica
that stops answering stops the delivery of its own hints until the next pass. A replica that refuses
what it is sent is counted as `rejected`. So is a node too old to delete one exact version (before
v1.1.0), which is not sent such a delete at all, and a copy a replica reported with no HLC at all,
which the replica's LWW could not weigh. After a restart, a node delivers the hints it left on disk.

One window stays open, the same one read repair has: a key deleted in the moment between the read
and the write to the replica comes back on that replica. With `W=1` the rule above cannot tell a
missed delete from a missed write either. Anti-entropy is the backstop for these, for hints that
expired or were dropped, and for writes a coordinator crashed before hinting (see
[Anti-Entropy](#anti-entropy)).

### Read Repair

When a quorum read detects that replicas have different versions of an object, the coordinator asynchronously sends the latest version to stale replicas. This is transparent to the client and does not add latency to reads.

Only the full quorum read does this — the one a CopyObject uses to read its source. A GET streams the object from the replica with the latest version and repairs nothing.

## Write Path (Distributed)

```
Client PUT /bucket/key
       |
       v
  Coordinator (any node)
       |
       +-- 1. Determine pool (from bucket → pool mapping)
       +-- 2. Determine replicas (all nodes in pool for RF=pool_size)
       +-- 3. Generate HLC timestamp + operation_id
       +-- 4. Send WriteBlob to ALL replicas in parallel
       +-- 5. Wait for W=2 successful ACKs
       |       (each ACK means: blob written + metadata stored + fsync done)
       +-- 6. Return 200 OK to client
       |
       +-- If <W ACKs: return 503
       +-- A replica whose write failed or timed out gets a hint
```

## Read Path (Distributed)

```
Client GET /bucket/key
       |
       v
  Coordinator (any node)
       |
       +-- 1. Send HeadObject (digest) to ALL replicas in parallel
       +-- 2. Wait for R=2 digest responses
       +-- 3. Compare HLC timestamps
       |
       +-- If versions match: fetch data from closest replica, return to client
       +-- If versions differ: fetch data from newest replica, return to client
       |                       async: trigger read repair on stale replica(s)
       |
       +-- If <R responses: return 503
```

## Conflict Resolution (HLC + LWW)

S4 uses **Hybrid Logical Clocks (HLC)** for causal ordering and **Last-Writer-Wins (LWW)** for conflict resolution.

- Each write generates an HLC timestamp: `(wall_time, logical_counter, node_id)`
- Concurrent writes to the same key are resolved deterministically: highest HLC wins, with `node_id` as tiebreaker
- All nodes arrive at the same winner for any conflict — no coordination needed
- Clock skew > 500ms is logged as a warning
- A record written without an HLC (stored in `single` mode, before the node joined a cluster) is
  ordered by its modification time, with node 0 in place of a node ID, on every node and every read
  path. Read repair and hinted handoff write that timestamp with the copies they make, so the copies
  stay recognizable as made up. A replica's LWW does not weigh its own record without an HLC, so
  hinted handoff writes such a copy only to a replica that holds nothing newer. A delete hint
  removes a replica's record without an HLC that is no newer than the delete; a newer one is left
  in place (`rejected`)
- Clients can use `If-Match` / `If-None-Match` for conditional writes

## Anti-Entropy

Every node of a replicated pool keeps, beside each object and each delete, a short digest of the
version it holds: its HLC, its ETag and whether it is an object or a tombstone. The digest is
written in the same atomic step as the record, so it never disagrees with it. Bucket markers and
bucket configurations (CORS, lifecycle, policy, encryption) have digests and tombstones too.
They follow the bucket metadata placement, while IAM control records follow a separate
`ClusterControl` class replicated across the pool; each class is compared on its own shared replicas.

Every node orders the versions of a key the same way: the later HLC wins. Two versions with the same
HLC are settled the same way on every node too: a delete wins over an object, and an ETag of a
multipart upload wins over one computed from the bytes, so a copy that lost its multipart ETag gets
it back. GET, HEAD and listings weigh tombstones in the same order, so a newer delete hides
an older live copy. A live object is served or listed only when a read quorum holds a version
of its key; absence alone cannot confirm a copy left by an old release's delete. Otherwise the
request returns 503. Repair carries a tombstone to stale replicas with its original HLC.

Every `S4_ANTI_ENTROPY_INTERVAL_SECS` (600 s) a node runs a **round** with one live peer of its
pool, taking the peers in turn:

1. Both nodes build a tree of 16 384 leaves in one streaming pass over their version digests.
   Each leaf is the XOR of `blake3(full key ‖ digest)` for its keys. Only keys the pair both
   replicate according to their placement class are compared; a key outside that shared
   placement is left out.
2. Behind each hash that differs, they compare keys and digests. A key held in different versions
   by the two, or by one of them only, is divergent (`s4_anti_entropy_divergent_keys_total`).
3. Each divergent key is read from all its replicas and brought to its newest version on every
   replica that answered: the newest copy is written with its own HLC, or the newest tombstone is,
   so a deleted object does not come back. Up to `S4_ANTI_ENTROPY_MAX_KEYS_PER_ROUND` (1000) keys
   a round; the next round with the same peer goes on from where this one stopped, so keys that
   cannot be settled never hold back the rest. A replica that does not answer in time is skipped,
   and the others are settled without it.

So a replica that missed writes and deletes — its hints expired, were dropped or were never
stored — catches up without a client, in both directions: keys the node lacks and keys its peer
lacks are settled alike. `s4_anti_entropy_repairs_total{outcome}` counts each divergent key once:
`ok`, `failed` (a replica did not answer or refused), or `ambiguous`. A key is ambiguous when its
newest copy is held by fewer replicas than a confirmed write leaves while another replica, which
may have deleted it before the pool kept tombstones (data written before the upgrade to this
release), has no key and cannot vouch for its deletion history: the copy is not spread. An
acknowledged write has quorum confirmation; a lone legacy copy with unknown history on the
other owners remains ambiguous.

A round is `complete` when every key the pair compares was settled, `incomplete` when the peer
stopped answering, a key was not settled, or the key limit was reached. Rounds are `skipped` while
any live node of the pool announces protocol below 8, or no peer is alive. The current protocol is 9.
The threshold includes IAM control records: protocol 5 leaves out bucket configuration digests,
and protocol 7 leaves out IAM digests. A live peer whose protocol is not known keeps its turn
until it announces it. A node that is down does not stop the rounds of the others. Requests
with an incompatible digest format are refused, so an older initiator cannot compare a different
set of keys with an upgraded responder.

`S4_ANTI_ENTROPY_ENABLED=false` (`0`, `no` and `off` work too; a value that is neither on nor off
stops the node from starting) stops a node from starting rounds — it logs a warning at start and
`/api/admin/cluster/repair-status` reports `"anti_entropy_enabled": false` — but the node still
answers the rounds of its peers, so the pool keeps converging. Its own repair frontier does not
advance, and its tombstone GC waits. To stop anti-entropy in the whole pool, set it on every node.

## Distributed Tombstones & GC

Deletes create tombstones that must live long enough for all replicas to see them.

- **GC grace period:** 7 days (`S4_GC_GRACE_DAYS`, or `S4_GC_GRACE_SECS` when set). A tombstone is
  kept at least this long
- **Tombstone purge:** a node removes its tombstone once it is older than the grace period and, for
  every other replica of its key, an anti-entropy round of this node with that replica *started*
  more than 60.5 seconds after the delete and completed — so every replica has had the delete. The
  60.5 seconds are the time a delete may still take to reach a replica (the 60-second replica write
  timeout) and the clock skew the cluster tolerates (0.5 seconds). While a replica is down, the
  other nodes keep the tombstones of its keys, however long it stays down. A node with
  `S4_ANTI_ENTROPY_ENABLED=false` completes no rounds of its own and removes no tombstone. GC looks
  for old tombstones every `S4_TOMBSTONE_GC_INTERVAL_SECS` (6 hours by default). Only a complete
  round advances that peer's repair frontier cut, to its start time minus 60.5 seconds; incomplete
  and skipped rounds advance nothing. The node itself needs no cut. These cuts live in memory:
  after a restart GC waits for new complete rounds with every other owner of the key
- **After a purge:** a node that removed tombstones no longer vouches for a key it lacks: an old copy
  of such a key held by a single replica is not spread — anti-entropy counts it as `ambiguous` —
  since it may be one that missed a delete whose tombstone is gone. A copy two replicas hold is
  spread as before
- **Safety margin:** the grace period must be longer than `S4_MAX_REJOIN_DOWNTIME_DAYS` plus a turn
  of anti-entropy rounds over the other nodes of the pool (`S4_ANTI_ENTROPY_INTERVAL_SECS` × their
  number) plus the 60-second replica write timeout and 0.5 seconds of clock skew; a node of a
  replicated pool configured otherwise, or whose safety-margin calculation overflows, does not
  start. The nodes' clocks must stay within 0.5 seconds of each other (NTP): last-writer-wins
  and tombstone GC both rely on it
- **Deduplication:** Remains per-node (no cross-node dedup coordination). Mark-sweep GC runs every 24 hours instead of ref-count decrement

The node keeps monotone deletion-history and purge horizons, so forgetting a tombstone never
makes an older lone copy safe to spread. Existing digests are trusted only with a valid clean
shutdown seal. After a crash or an upgrade from a release without that seal, the node rebuilds
them before opening network handlers and sets its deletion-history boundary to the later of
the retained floor and the current time plus tolerated clock skew. A clean restart preserves
that boundary; it still needs new repair frontier cuts before GC can resume.

## Bit Rot Protection (Data Scrubber)

Background scrubber verifies data integrity and auto-heals from replicas.

- **CRC32 scan:** All volumes scanned over a configurable period (default: 30 days)
- **Auto-heal:** Corrupted blobs are fetched from a healthy replica and replaced locally
- **Deep verify:** SHA-256 content hash verification available via admin API (runs every 90 days or on demand)
- **I/O throttling:** Scrubber uses low-priority I/O to avoid impacting normal operations
- **Metrics:** `blobs_scanned_total`, `corruptions_found_total`, `corruptions_healed_total`, `scan_progress`

## Cluster Operations

### Deployment Modes

| Mode | `S4_MODE` | Description |
|------|-----------|-------------|
| Single-node | `single` (default) | Standard standalone server, no cluster overhead |
| Cluster | `cluster` | Storage node with full replication |
| Gateway | `gateway` | Stateless router — no local storage, forwards to cluster nodes |

### Moving Existing Single-Node Data

A node restarted in cluster mode on its single-mode data directory copies its bucket markers and
objects to every owner in the background, preserving bytes, ETags, content types and user metadata.
Migration starts once every pool member is alive and runs the same S4 version, and resumes after an
interruption. Newer cluster writes and deletes take precedence over the earlier copies.

Wait for `migration_pending_records` in the health response, or
`s4_cluster_migration_pending_records`, to reach zero on every node before switching clients.
Objects still waiting for replicas can answer `503`. This transition supports one populated source
in a replicated pool; versioning in `Enabled` or `Suspended` and an erasure-coded destination refuse
the transition. See [Moving from Single Mode](../08-deployment/clustering.md#moving-from-single-mode)
for the procedure, limitations and an S3 copy alternative for earlier releases.

### Graceful Shutdown

On `SIGTERM`:

1. Stop accepting connections and finish the HTTP requests in flight
2. Sync fjall, close volumes, exit

The node does not announce that it is leaving: peers mark it dead through gossip, as after a
crash, and store a hint for each write it misses.

### Rolling Upgrades

1. Pre-check: every node answers `/health`
2. Stop one node with `SIGTERM` → upgrade binary → restart → wait for `/health`
3. Validate: PUT/GET/DELETE through the restarted node
4. Repeat for remaining nodes one at a time
5. Protocol versioning ensures backward compatibility within minor versions

### Admin API (Cluster)

Every route needs a SuperUser token from `POST /api/admin/login`, sent as
`Authorization: Bearer <token>`.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/admin/cluster/health` | GET | Members of the pool by status, `healthy`/`degraded`/`critical` by the quorum every key can still reach, epoch, versioning and Object Lock this node stores and does not enforce |
| `/api/admin/cluster/topology` | GET | The pool, its quorum and every configured member with status, addresses and version |
| `/api/admin/node/health` | GET | The node asked: identity, name, pool, addresses, version, uptime |
| `/api/admin/node/drain` | POST | Not implemented: answers `501`. Stop the node with `SIGTERM` instead |
| `/api/admin/cluster/repair-status` | GET | Anti-entropy rounds, divergent and repaired keys, last repair time, frontier lag, tombstones held and purged |
| `/api/admin/scrubber/status` | GET | Background scrubber: blobs checked, damage found and healed, scan progress |
| `/api/admin/scrubber/run` | POST | On-demand CRC32 check of this node's volumes; reports damage, does not repair it |

Every answer is the view of the node asked and matches its `/metrics`; what each one means is in
[Clustering → Health Check](../08-deployment/clustering.md#health-check).

### Cluster Configuration (Environment Variables)

| Variable | Description | Default |
|----------|-------------|---------|
| `S4_MODE` | Operating mode: `single`, `cluster`, `gateway` | `single` |
| `S4_CLUSTER_NAME` | Cluster name for network isolation | `default` |
| `S4_NODE_ID` | Human-readable node name (UUID auto-generated internally) | Auto |
| `S4_NODE_GRPC_ADDR` | gRPC listen address for inter-node communication | — |
| `S4_NODE_HTTP_ADDR` | HTTP address advertised to other nodes | — |
| `S4_SEEDS` | Comma-separated seed node gRPC addresses | — |
| `S4_POOL_NAME` | Pool this node belongs to | — |
| `S4_POOL_NODES` | Pool members: `id:addr,id:addr,...` | — |
| `S4_POOL_TYPE` | Pool storage type: `standard` or Enterprise `ec` | `standard` |
| `S4_REPLICATION_FACTOR` | Replication factor (N) | `3` |
| `S4_WRITE_QUORUM` | Write quorum (W) | `2` |
| `S4_READ_QUORUM` | Read quorum (R) | `2` |
| `S4_GC_GRACE_DAYS` | Tombstone GC grace period (days) | `7` |
| `S4_MAX_REJOIN_DOWNTIME_DAYS` | Max offline days before full bootstrap required | `3` |
| `S4_ANTI_ENTROPY_INTERVAL_SECS` | Seconds between two anti-entropy rounds of a node | `600` |
| `S4_ANTI_ENTROPY_MAX_KEYS_PER_ROUND` | Most divergent keys one round settles | `1000` |
| `S4_ANTI_ENTROPY_ENABLED` | `false`: the node starts no rounds, but answers its peers' | `true` |
| `S4_SCRUBBER_FULL_SCAN_DAYS` | Full CRC32 scrub cycle (days) | `30` |
| `S4_HINT_TTL_HOURS` | How long a hint is kept; `0` keeps none | `3` |

### Docker Compose (3-Node Cluster)

```yaml
services:
  s4-node1:
    image: s4core:latest
    environment:
      S4_MODE: cluster
      S4_CLUSTER_NAME: dev
      S4_NODE_ID: node-1
      S4_NODE_GRPC_ADDR: s4-node1:9100
      S4_NODE_HTTP_ADDR: s4-node1:9000
      S4_SEEDS: s4-node1:9100,s4-node2:9100,s4-node3:9100
      S4_POOL_NAME: pool-1
      S4_POOL_NODES: "node-1:s4-node1:9100,node-2:s4-node2:9100,node-3:s4-node3:9100"
      S4_DATA_DIR: /data
      S4_ACCESS_KEY_ID: minioadmin
      S4_SECRET_ACCESS_KEY: minioadmin
    volumes:
      - s4-data-1:/data
    ports:
      - "9001:9000"

  s4-node2:
    image: s4core:latest
    environment:
      S4_MODE: cluster
      S4_NODE_ID: node-2
      S4_NODE_GRPC_ADDR: s4-node2:9100
      S4_NODE_HTTP_ADDR: s4-node2:9000
      S4_SEEDS: s4-node1:9100,s4-node2:9100,s4-node3:9100
      S4_POOL_NAME: pool-1
      S4_POOL_NODES: "node-1:s4-node1:9100,node-2:s4-node2:9100,node-3:s4-node3:9100"
      S4_DATA_DIR: /data
      S4_ACCESS_KEY_ID: minioadmin
      S4_SECRET_ACCESS_KEY: minioadmin
    volumes:
      - s4-data-2:/data

  s4-node3:
    image: s4core:latest
    environment:
      S4_MODE: cluster
      S4_NODE_ID: node-3
      S4_NODE_GRPC_ADDR: s4-node3:9100
      S4_NODE_HTTP_ADDR: s4-node3:9000
      S4_SEEDS: s4-node1:9100,s4-node2:9100,s4-node3:9100
      S4_POOL_NAME: pool-1
      S4_POOL_NODES: "node-1:s4-node1:9100,node-2:s4-node2:9100,node-3:s4-node3:9100"
      S4_DATA_DIR: /data
      S4_ACCESS_KEY_ID: minioadmin
      S4_SECRET_ACCESS_KEY: minioadmin
    volumes:
      - s4-data-3:/data

volumes:
  s4-data-1:
  s4-data-2:
  s4-data-3:
```

## CE vs EE

| Feature | Community Edition | Enterprise Edition |
|---------|------------------|--------------------|
| Pools | 1 pool | Unlimited |
| Nodes per pool | 3 max | Unlimited |
| Gossip & quorum | Full | Full |
| Audit logging | No | Yes |
| Rolling upgrades | No | Yes |
| Deep scrub | No | Yes |
| Dead node replacement | No | Yes |
