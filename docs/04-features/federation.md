# Federation (Distributed Mode)

> **Status:** All phases (1–10) implemented. Conflict resolution (HLC+LWW), anti-entropy (Merkle trees), distributed tombstone GC, bit rot protection, and cluster operations are complete.

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

- **Alive → Suspect:** Node fails to respond to ping (5 s timeout)
- **Suspect → Dead:** Node fails indirect probes through K=3 other nodes (30 s timeout)
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

### Hinted Handoff

When a replica is temporarily unreachable during a write:
1. The coordinator stores a **hint** locally (persisted to disk)
2. When the replica comes back online (detected via gossip), hints are delivered
3. Hints have a TTL (default: 3 hours) — after expiry, anti-entropy handles catch-up

### Read Repair

When a quorum read detects that replicas have different versions of an object, the coordinator asynchronously sends the latest version to stale replicas. This is transparent to the client and does not add latency to reads.

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
       +-- If <W ACKs: return 503, store hints for offline replicas
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
- Clients can use `If-Match` / `If-None-Match` for conditional writes

## Anti-Entropy (Merkle Trees)

Background service that detects and repairs divergences between replicas every 10 minutes.

**How it works:**
1. Each node maintains a persisted **Merkle tree** (BLAKE3 hashes, stored in fjall) over its key ranges
2. Every 10 minutes, each node exchanges Merkle root hashes with a random replica
3. If roots differ, the tree is traversed to find divergent keys in O(log K) comparisons
4. Divergent keys are synced to the latest version
5. **Repair frontier** tracks per-replica reconciliation progress — used for safe tombstone purge

## Distributed Tombstones & GC

Deletes create tombstones that must live long enough for all replicas to see them.

- **GC grace period:** 7 days (configurable via `S4_GC_GRACE_DAYS`)
- **Tombstone purge** requires: `age > gc_grace` AND repair frontier confirms all replicas have synced
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

1. Stop accepting new coordinated requests
2. Wait for in-flight operations (timeout: 30s)
3. Flush pending hints
4. Broadcast `NodeStatus::Left` via gossip
5. Sync fjall, close volumes, exit

### Rolling Upgrades

1. Pre-check: all nodes alive, anti-entropy caught up, no pending hints
2. Drain one node → upgrade binary → restart → health check
3. Validate: PUT/GET/DELETE test, inter-node communication, anti-entropy
4. Repeat for remaining nodes one at a time
5. Protocol versioning ensures backward compatibility within minor versions

### Admin API (Cluster)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/admin/cluster/health` | GET | Cluster health, node statuses, epoch |
| `/admin/cluster/topology` | GET | Pools, hash rings, node assignments |
| `/admin/node/health` | GET | Individual node status and version |
| `/admin/cluster/pool` | POST | Add a new pool (no downtime) |
| `/admin/node/drain` | POST | Graceful drain for rolling upgrade |
| `/admin/node/bootstrap` | POST | Full bootstrap for nodes offline > max_rejoin_downtime |
| `/admin/cluster/scrub` | POST | Trigger manual data scrub |
| `/admin/cluster/repair-status` | GET | Repair frontier, divergent keys, last repair time |

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
| `S4_REPLICATION_FACTOR` | Replication factor (N) | `3` |
| `S4_WRITE_QUORUM` | Write quorum (W) | `2` |
| `S4_READ_QUORUM` | Read quorum (R) | `2` |
| `S4_GC_GRACE_DAYS` | Tombstone GC grace period (days) | `7` |
| `S4_MAX_REJOIN_DOWNTIME_DAYS` | Max offline days before full bootstrap required | `3` |
| `S4_ANTI_ENTROPY_INTERVAL_SECS` | Anti-entropy Merkle exchange interval | `600` |
| `S4_SCRUBBER_FULL_SCAN_DAYS` | Full CRC32 scrub cycle (days) | `30` |
| `S4_HINT_TTL_HOURS` | Hinted handoff TTL | `3` |

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
