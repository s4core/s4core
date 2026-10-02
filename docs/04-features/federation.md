# Federation (Distributed Mode)

> **Status:** Gossip membership, quorum replication, conflict resolution (HLC+LWW), read repair on quorum reads, hinted handoff, bit rot protection and cluster operations work. The background anti-entropy of a replicated pool is built but not started, and so tombstone GC purges nothing yet — see [Anti-Entropy](#anti-entropy-merkle-trees).

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
- Pool membership is **immutable** after creation — nodes are never added or removed from existing pools
- Horizontal scaling = adding new pools (not new nodes to existing pools)
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

As with S3's own bucket operations, deleting a bucket is not atomic with requests already past
their check: an object written in the same instant as its bucket is deleted can outlive it.

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
missed delete from a missed write either. Anti-entropy is meant to be the backstop for these, for
hints that expired or were dropped, and for writes a coordinator crashed before hinting; on a
replicated pool it does not run yet (see [Anti-Entropy](#anti-entropy-merkle-trees)).

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

## Anti-Entropy (Merkle Trees)

Each node builds a **Merkle tree** over its keys at start, keeps it in memory, updates it on every
replica write and answers a peer that asks to compare trees. The background loop that would run
the comparisons is **not started** on a replicated pool: no node runs a round, whatever
`S4_ANTI_ENTROPY_INTERVAL_SECS` says, and `s4_anti_entropy_rounds_total` stays at 0. A replica
that missed writes its hints did not cover stays behind until a quorum read repairs it.

What a round does, as the tests run it:
1. Compares its Merkle root with a random live replica's
2. If the roots differ, asks that replica for its copy of each key this node holds, up to 1000
3. Takes every copy newer than its own; each such key counts in
   `s4_anti_entropy_divergent_keys_total` and in `s4_anti_entropy_repairs_total{outcome}`
4. Moves the **repair frontier** for that replica — only when it repaired something

## Distributed Tombstones & GC

Deletes create tombstones that must live long enough for all replicas to see them.

- **GC grace period:** 7 days (configurable via `S4_GC_GRACE_DAYS`)
- **Tombstone purge** requires: `age > gc_grace` AND repair frontier confirms all replicas have synced.
  The frontier moves only in anti-entropy rounds and never for the node itself, so no tombstone is
  purged yet: `s4_tombstones_purged_total` stays at 0, and `s4_tombstones_active` counts what each
  GC pass leaves
- **Zombie resurrection protection:** If a node is offline > `max_rejoin_downtime` (3 days), it must full-bootstrap instead of incremental repair
- **Deduplication:** Remains per-node (no cross-node dedup coordination). Mark-sweep GC runs every 24 hours instead of ref-count decrement

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
| `/api/admin/cluster/health` | GET | Members of the pool by status, `healthy`/`degraded`/`critical` by the quorum every key can still reach, epoch |
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
| `S4_ANTI_ENTROPY_INTERVAL_SECS` | Anti-entropy Merkle exchange interval; no effect yet, the rounds are not started | `600` |
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
