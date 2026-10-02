# Cluster Deployment

> Last updated: 2026-10-01

This guide covers deploying S4 in distributed (cluster) mode. For architecture and internals, see [Federation](../04-features/federation.md).

## Overview

S4 supports three operating modes controlled by `S4_MODE`:

| Mode | Description |
|------|-------------|
| `single` (default) | Standalone server, no cluster overhead |
| `cluster` | Storage node with quorum replication |
| `gateway` | Stateless router — no local storage, forwards requests to cluster nodes |

In `single` mode, no cluster code runs. Switching to `cluster` starts gossip, gRPC, quorum coordinators, and all background cluster workers automatically.

**A node that ran in `single` mode does not bring its data into a cluster.** Restarted with `S4_MODE=cluster` on the same data directory, it keeps its buckets and objects, but does not copy them to the other nodes: with the default read quorum of 2 every read of them answers `503`, and without that node they are not there at all. To move to a cluster, start the cluster empty and copy the data into it through S3, for example with `mc mirror` or `rclone sync`, then check the copy before switching clients.

## Prerequisites

- All nodes must be able to reach each other on the gRPC port (default: 9100)
- All nodes must be able to reach each other on the HTTP port (default: 9000)
- Clocks should be roughly synchronized (NTP recommended; skew > 500ms triggers warnings)
- All nodes in a pool must run the same S4 version

## Minimal 3-Node Cluster

### Environment Variables

Each node needs these variables:

| Variable | Description |
|----------|-------------|
| `S4_MODE=cluster` | Enable cluster mode |
| `S4_CLUSTER_NAME` | Cluster name (all nodes must match) |
| `S4_NODE_ID` | Human-readable name for this node |
| `S4_NODE_GRPC_ADDR` | This node's gRPC address (host:port) |
| `S4_NODE_HTTP_ADDR` | This node's HTTP address (host:port) |
| `S4_SEEDS` | Comma-separated gRPC addresses of all seed nodes |
| `S4_POOL_NAME` | Pool name (all pool members must match) |
| `S4_POOL_NODES` | Pool members: `name:host:port,name:host:port,...` |
| `S4_ACCESS_KEY_ID` | S3 access key (must match across all nodes) |
| `S4_SECRET_ACCESS_KEY` | S3 secret key (must match across all nodes) |

With IAM (`S4_ROOT_PASSWORD` set, or IAM users left from an earlier start), every node also needs
`S4_JWT_SECRET`, at least 32 bytes and the same value on all of them: a token issued by one node is
then accepted by the others, whichever one the load balancer picks. A cluster or gateway node
without it refuses to start and names the variable. Generate the value once
(`openssl rand -hex 32`) and give every node that value. Any credential can come from a file
instead, through its `_FILE` form — see
[Secrets from Files](../01-getting-started/configuration.md#secrets-from-files-_file).

Start order does not matter. A node announces itself to its seeds over UDP, and
keeps announcing every second to each seed it does not see alive, so a node that
comes up before its seeds still joins once they are listening. This makes
starting a whole cluster at once, and rolling restarts, safe to do without
staggering. The same announcements end a network split: once the network heals,
the two sides find each other again within seconds, without a restart.

### Bare Metal / VM

```bash
# Node 1 (10.0.1.1)
S4_MODE=cluster \
S4_CLUSTER_NAME=production \
S4_NODE_ID=node-1 \
S4_NODE_GRPC_ADDR=10.0.1.1:9100 \
S4_NODE_HTTP_ADDR=10.0.1.1:9000 \
S4_SEEDS=10.0.1.1:9100,10.0.1.2:9100,10.0.1.3:9100 \
S4_POOL_NAME=pool-1 \
S4_POOL_NODES=node-1:10.0.1.1:9100,node-2:10.0.1.2:9100,node-3:10.0.1.3:9100 \
S4_DATA_DIR=/var/lib/s4 \
S4_ACCESS_KEY_ID=myaccesskey \
S4_SECRET_ACCESS_KEY=mysecretkey \
./s4-server

# Node 2 (10.0.1.2) — same config, different S4_NODE_ID and addresses
S4_MODE=cluster \
S4_CLUSTER_NAME=production \
S4_NODE_ID=node-2 \
S4_NODE_GRPC_ADDR=10.0.1.2:9100 \
S4_NODE_HTTP_ADDR=10.0.1.2:9000 \
S4_SEEDS=10.0.1.1:9100,10.0.1.2:9100,10.0.1.3:9100 \
S4_POOL_NAME=pool-1 \
S4_POOL_NODES=node-1:10.0.1.1:9100,node-2:10.0.1.2:9100,node-3:10.0.1.3:9100 \
S4_DATA_DIR=/var/lib/s4 \
S4_ACCESS_KEY_ID=myaccesskey \
S4_SECRET_ACCESS_KEY=mysecretkey \
./s4-server

# Node 3 (10.0.1.3) — same pattern
S4_MODE=cluster \
S4_CLUSTER_NAME=production \
S4_NODE_ID=node-3 \
S4_NODE_GRPC_ADDR=10.0.1.3:9100 \
S4_NODE_HTTP_ADDR=10.0.1.3:9000 \
S4_SEEDS=10.0.1.1:9100,10.0.1.2:9100,10.0.1.3:9100 \
S4_POOL_NAME=pool-1 \
S4_POOL_NODES=node-1:10.0.1.1:9100,node-2:10.0.1.2:9100,node-3:10.0.1.3:9100 \
S4_DATA_DIR=/var/lib/s4 \
S4_ACCESS_KEY_ID=myaccesskey \
S4_SECRET_ACCESS_KEY=mysecretkey \
./s4-server
```

### Docker Compose

The repository ships two compose files for a three-node Community Edition
cluster. Both put HAProxy (`haproxy.cfg`, round-robin, no sticky sessions) in
front of the nodes and keep each node's data in a named volume:

| File | Image | Published ports |
|------|-------|-----------------|
| `docker-compose-cluster.yml` | `s4core/s4core:latest`, the released CE image | `9000` S3 API through HAProxy, `8404` HAProxy stats |
| `docker-compose-cluster-dev.yml` | built from the local sources (`build: .`) | the same, plus every node directly on `9001`, `9002`, `9003` |

The HAProxy service and the first node of `docker-compose-cluster.yml`;
`s4-node2` and `s4-node3` differ only in `S4_NODE_ID`, their addresses and
their volume:

```yaml
services:
  haproxy:
    image: haproxy:3.1-alpine
    volumes:
      - ./haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro
    ports:
      - "9000:9000"   # S3 API, round-robin over the nodes
      - "8404:8404"   # HAProxy stats page
    depends_on:       # HAProxy starts once every node reports healthy
      s4-node1:
        condition: service_healthy
      s4-node2:
        condition: service_healthy
      s4-node3:
        condition: service_healthy

  s4-node1:
    image: s4core/s4core:latest
    environment:
      S4_MODE: cluster
      S4_BIND: "0.0.0.0:9000"
      S4_CLUSTER_NAME: production
      S4_NODE_ID: node-1
      S4_NODE_GRPC_ADDR: s4-node1:9100
      S4_NODE_HTTP_ADDR: s4-node1:9000
      S4_SEEDS: s4-node1:9100,s4-node2:9100,s4-node3:9100
      S4_POOL_NAME: pool-1
      S4_POOL_NODES: "node-1:s4-node1:9100,node-2:s4-node2:9100,node-3:s4-node3:9100"
      S4_DATA_DIR: /data
      S4_ACCESS_KEY_ID: ${S4_ACCESS_KEY_ID:-my-access-key-one}
      S4_SECRET_ACCESS_KEY: ${S4_SECRET_ACCESS_KEY:-my-secret-key-one}
    volumes:
      - s4-data-1:/data
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://127.0.0.1:9000/health"]
      interval: 5s
      timeout: 3s
      start_period: 10s
      retries: 3

volumes:
  s4-data-1:
  s4-data-2:
  s4-data-3:
```

```bash
# Released image
docker compose -f docker-compose-cluster.yml up -d

# Or build the image from the local sources
docker compose -f docker-compose-cluster-dev.yml up -d --build

curl http://localhost:9000/health     # S3 endpoint, through HAProxy
curl http://localhost:8404/           # HAProxy stats

# Stop and delete the data volumes
docker compose -f docker-compose-cluster.yml down -v
```

S3 clients connect to `http://localhost:9000` with the access key
`my-access-key-one` and the secret key `my-secret-key-one`; set
`S4_ACCESS_KEY_ID` and `S4_SECRET_ACCESS_KEY` in the shell before `up` to use
others. `docker-compose-cluster.yml` does not publish the nodes' own ports, so
every request goes through HAProxy; to reach a node directly, use
`docker-compose-cluster-dev.yml` and ports `9001`–`9003`.

Enterprise Edition has its own development clusters, which need an Enterprise
license: `ee/docker-compose-dev.yml` runs six nodes in one erasure-coded pool and
`ee/docker-compose-dev-no-ec.yml` six nodes in one replicated pool. See
[Erasure Coding → Docker Compose Dev Cluster](../04-features/erasure-coding.md#docker-compose-dev-cluster).

## Load Balancer

In production, place all cluster nodes behind a load balancer. Any node can handle any request.

### HAProxy Example

```
frontend s4
    bind *:9000
    default_backend s4_nodes

backend s4_nodes
    balance roundrobin
    option httpchk GET /health
    timeout http-request 60s
    timeout http-keep-alive 60s
    timeout client 10m
    timeout server 10m
    server node1 10.0.1.1:9000 check
    server node2 10.0.1.2:9000 check
    server node3 10.0.1.3:9000 check
```

Round-robin is supported for multipart uploads: S4 replicates multipart session
state and streams part data through the quorum path, so `CreateMultipartUpload`,
`UploadPart`, `UploadPartCopy`, `CompleteMultipartUpload`, and abort do not need
to hit the same HTTP node. `CompleteMultipartUpload` performs a replica-set
preflight and only publishes the composite object on replicas that have the
selected parts locally. Keep client/server timeouts comfortably above the
expected duration of large part transfers.

### Nginx Example

```nginx
upstream s4_cluster {
    server 10.0.1.1:9000;
    server 10.0.1.2:9000;
    server 10.0.1.3:9000;
}

server {
    listen 9000;
    client_max_body_size 10G;

    location / {
        proxy_pass http://s4_cluster;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## Tuning Parameters

| Variable | Default | Description |
|----------|---------|-------------|
| `S4_REPLICATION_FACTOR` | `3` | Number of replicas per object |
| `S4_WRITE_QUORUM` | `2` | Minimum write acknowledgements |
| `S4_READ_QUORUM` | `2` | Minimum read acknowledgements |
| `S4_GC_GRACE_DAYS` | `7` | How long tombstones are kept before purge |
| `S4_MAX_REJOIN_DOWNTIME_DAYS` | `3` | Lower bound on `S4_GC_GRACE_DAYS`; see [Long Downtime](#long-downtime--s4_max_rejoin_downtime_days) |
| `S4_EXPECTED_NODE_IDENTITY` | unset | Identity (UUID) this data directory must belong to; node replacement tool |
| `S4_NODE_IDENTITY_CONFLICT_CHECK` | `true` | Refuse the start when a live pool member already answers for this node's identity |
| `S4_ANTI_ENTROPY_INTERVAL_SECS` | `600` | Merkle tree sync interval (seconds); no effect yet, the rounds are not started |
| `S4_SCRUBBER_FULL_SCAN_DAYS` | `30` | Full CRC32 integrity scan cycle (days) |
| `S4_HINT_TTL_HOURS` | `3` | How long a hint for a replica that missed a write is kept; `0` keeps none |

## Gateway Mode

Gateway nodes (`S4_MODE=gateway`) act as stateless routers. They do not store data locally — they forward all requests to cluster nodes via the quorum coordinators.

Use gateways for:
- Edge locations that need low-latency routing
- Separating client-facing HTTP from storage nodes
- Scaling read throughput without adding storage

```bash
S4_MODE=gateway \
S4_CLUSTER_NAME=production \
S4_SEEDS=10.0.1.1:9100,10.0.1.2:9100,10.0.1.3:9100 \
S4_ACCESS_KEY_ID=myaccesskey \
S4_SECRET_ACCESS_KEY=mysecretkey \
./s4-server
```

Gateway nodes discover cluster topology via gossip and route requests to the appropriate pool.

## Horizontal Scaling

S4 scales horizontally by adding **new pools**, not by adding nodes to existing pools. Pool membership is immutable.

```
Before:
  Pool 1: [Node A, Node B, Node C]  — all buckets here

After:
  Pool 1: [Node A, Node B, Node C]  — existing buckets stay here
  Pool 2: [Node D, Node E, Node F]  — new buckets created here
```

New buckets are automatically created in the pool with the most free space. Existing buckets remain in their original pool.

## Monitoring

### Health Check

The admin API needs a SuperUser token:

```bash
TOKEN=$(curl -s -X POST http://any-node:9000/api/admin/login \
    -H "Content-Type: application/json" \
    -d '{"username": "root", "password": "YOUR_PASSWORD"}' | jq -r '.token')

# Cluster-wide health
curl -H "Authorization: Bearer $TOKEN" http://any-node:9000/api/admin/cluster/health

# Individual node health
curl -H "Authorization: Bearer $TOKEN" http://any-node:9000/api/admin/node/health

# Cluster topology (pools, nodes, assignments)
curl -H "Authorization: Bearer $TOKEN" http://any-node:9000/api/admin/cluster/topology

# Repair status (anti-entropy and tombstone GC)
curl -H "Authorization: Bearer $TOKEN" http://any-node:9000/api/admin/cluster/repair-status

# Background scrubber of this node
curl -H "Authorization: Bearer $TOKEN" http://any-node:9000/api/admin/scrubber/status
```

Each answer is the view of the node asked, read from the same state its metrics are written
from, so it matches a scrape of that node's `/metrics` (see [Key Metrics](#key-metrics)). The
answers below are node-2's in the three-node example above with node-3 stopped, and its scrubber's
after the first full scan. A single node answers these routes with fixed values: one healthy node,
no pools, `"cluster_mode": false`.

`cluster/health` counts the members of `S4_POOL_NODES` by status — the node asked is alive, a
member it has never heard from is dead — and sets `status`. `healthy`: every member is alive.
`degraded`: some are not, but no more than RF − max(W, R) of them, so every key still has enough
live replicas for both quorums. `critical`: more are down, and writes or reads of some keys fail.
In a pool larger than RF this is the worst case, because the members that are down may hold
replicas of the same keys. An erasure-coded pool (Enterprise) is counted by the same rule: its
objects below `S4_EC_MIN_OBJECT_SIZE` are replicated with RF, W and R, and with the default
quorums every built-in profile survives the one loss the rule allows. A custom profile or quorum
under which the erasure set survives fewer losses than RF − max(W, R) is not reflected in `status`;
see the losses each profile survives in
[Erasure Coding](../04-features/erasure-coding.md).

```json
{
  "status": "degraded",
  "nodes_total": 3, "nodes_alive": 2, "nodes_suspect": 0, "nodes_dead": 1, "nodes_left": 0,
  "epoch": 1, "protocol_version": 3, "s4_version": "1.1.1",
  "pool": "pool-1", "replication_factor": 3, "write_quorum": 2, "read_quorum": 2
}
```

`cluster/topology` lists the pool and every configured member with its status and the addresses
and version it last announced. A member the node has never heard from has only its name, its
identity and `dead`.

```json
{
  "mode": "cluster", "epoch": 1,
  "pools": [{
    "pool_id": 0, "name": "pool-1", "storage_class": "standard",
    "replication_factor": 3, "write_quorum": 2, "read_quorum": 2,
    "nodes": [
      {"name": "node-1", "node_id": "ea526217-3b53-4b73-9037-742e4bd8d024", "status": "alive",
       "local": false, "grpc_addr": "10.0.1.1:9100", "http_addr": "10.0.1.1:9000",
       "protocol_version": 3, "s4_version": "1.1.1"},
      {"name": "node-2", "node_id": "1c4f41b1-1d79-4c32-b70c-248d7d338635", "status": "alive",
       "local": true, "grpc_addr": "10.0.1.2:9100", "http_addr": "10.0.1.2:9000",
       "protocol_version": 3, "s4_version": "1.1.1"},
      {"name": "node-3", "node_id": "0299dbf8-bd57-43f7-91c3-6fc5bb458476", "status": "dead",
       "local": false, "grpc_addr": "10.0.1.3:9100", "http_addr": "10.0.1.3:9000",
       "protocol_version": 3, "s4_version": "1.1.1"}
    ]
  }]
}
```

`node/health` is the node asked: its identity (the `node_id` file in its data directory), the name
it announces, its pool, addresses, version and uptime.

```json
{
  "cluster_mode": true, "uptime_secs": 26,
  "node": {
    "node_id": "1c4f41b1-1d79-4c32-b70c-248d7d338635", "node_name": "node-2", "status": "alive",
    "pool_id": 0, "pool": "pool-1", "grpc_addr": "10.0.1.2:9100", "http_addr": "10.0.1.2:9000",
    "protocol_version": 3, "s4_version": "1.1.1"
  }
}
```

`cluster/repair-status` counts, since the node started, anti-entropy rounds, the keys they found
behind on this node (`divergent_keys`), repaired (`total_repairs`) and failed to repair, the time
of the last repair, the repair frontier lag, and the tombstones GC holds and purged. The rounds of
a replicated pool are not started yet, so on a replicated pool these stay at zero.

```json
{
  "rounds_completed": 0, "divergent_keys": 0, "total_repairs": 0, "repairs_failed": 0,
  "last_repair": null, "repair_frontier_lag_secs": 0.0,
  "tombstones_active": 0, "tombstones_purged": 0
}
```

`scrubber/status` counts the blobs the background scrubber checked, the damage it found and
healed, and how far its current scan has got. Objects under 4 KiB are stored in the index rather
than in a volume, and the scrubber does not check them.

```json
{
  "mode": "cluster", "background_scrubber": true,
  "message": "The background scrubber verifies every blob of this node against its CRC32 and heals a corrupted one from a replica. POST /api/admin/scrubber/run checks every volume now.",
  "scan_progress": 1.0, "scan_progress_pct": 100, "cycles_completed": 1,
  "last_full_scan_timestamp": 1790848867, "volumes_scanned_total": 1,
  "blobs_scanned_total": 12, "bytes_scanned_total": 66120,
  "corruptions_found_total": 0, "corruptions_healed_total": 0, "corruptions_unrecoverable_total": 0
}
```

### Key Metrics

Scrape every node: each exports its own `/metrics`, and every number there is that node's view.
The metrics, their labels and what a change in each means are listed in
[Prometheus Metrics → Cluster](../06-monitoring/prometheus-metrics.md#cluster), with the queries
worth an alert. A Grafana dashboard for a cluster ships with the documentation:
[`s4-cluster.json`](../06-monitoring/grafana/s4-cluster.json).

The ones to watch first:

- `s4_cluster_nodes{status!="alive"}` and `s4_cluster_peer_up` — who each node holds down. A peer
  down on every node is down; down on one node only, the network between the two is.
- `s4_quorum_operations_total{outcome="quorum_not_met"}` — operations that failed because too
  few replicas answered.
- `s4_peer_requests_total` by `peer` — failures and timeouts that pile up on one peer point at the
  node that holds the cluster back.
- `s4_hints_pending` and `s4_hints_dropped_total` — writes a replica missed, waiting for it or
  never to reach it through hinted handoff.
- `s4_node_disk_used_bytes` and `s4_node_disk_total_bytes` — the filesystem under each data
  directory.

## Node Recovery

### Short Downtime (< 3 days)

When a node comes back online after a short outage:
1. Gossip automatically detects the node is alive
2. The other nodes deliver the hints they stored for it within 30 seconds; a hint whose key has
   another replica down waits until that replica is back too (see
   [Hinted Handoff](../04-features/federation.md#hinted-handoff))
3. What the hints did not cover — hints that expired or were dropped, including the `ambiguous`
   ones whose copy may have missed a delete — stays behind until a quorum read repairs it:
   anti-entropy of a replicated pool is not started yet

No manual intervention needed.

### Long Downtime (> `S4_MAX_REJOIN_DOWNTIME_DAYS`)

A node offline longer than the max rejoin downtime (default: 3 days) may hold
objects the cluster deleted while it was away, and whose tombstones have since
been purged. Bringing its old data back could resurrect them.

`S4_MAX_REJOIN_DOWNTIME_DAYS` does two things. It constrains the tombstone GC
invariant — `gc_grace` must exceed it — on every pool, and on an
erasure-coded pool it is also the limit a returning node's own downtime is
measured against: a node back after longer than that does not serve
erasure-coded reads until its shards are rebuilt and an operator says its return
is acceptable. `S4_EC_LONG_OFFLINE_REJOIN_POLICY` decides which of the two forms
that takes. See
[Erasure Coding → Coming back to the pool](../04-features/erasure-coding.md#coming-back-to-the-pool).

On a replicated pool nothing is enforced automatically, so bring such a node
back the same way you replace a dead one: clear its data directory and let it
rebuild from its peers.

### Replacing a node that will not come back

A node's identity is a UUID in `<S4_DATA_DIR>/volumes/node_id`, not its hostname
and not `S4_NODE_ID`. Replacement means giving a machine an **empty** data
directory and the identity of the node it replaces:

```bash
S4_EXPECTED_NODE_IDENTITY=<uuid of the dead node> \
S4_DATA_DIR=/var/lib/s4 \
  s4-server
```

An empty data directory adopts that identity; a directory holding a *different*
identity stops the start and names both. Leaving the variable unset keeps the
old behaviour exactly.

On an erasure-coded pool the identity of every node and the slots it holds are
readable from any surviving node, and the rebuild is driven from the admin API —
see [Erasure Coding → Replacing a node](../04-features/erasure-coding.md#replacing-a-node).

Do **not** bring the replacement up with a fresh identity. It joins the cluster
and looks healthy, but the pool goes on assigning the dead node's data to an
identity that no longer exists.

Bringing it up as a *second* copy of a node that is still running is stopped for
you: before joining gossip, a starting node asks every address in
`S4_POOL_NODES` who it is, and a peer answering with this node's identity
refuses the start and names that peer's address. An address that does not answer
proves nothing and never refuses a start, so a pool whose peers are down comes
up as it always did. `S4_NODE_IDENTITY_CONFLICT_CHECK=false` switches the check
off.

## Graceful Shutdown

On `SIGTERM` or `Ctrl+C` a node:

1. Stops accepting connections and finishes the HTTP requests in flight
2. Syncs metadata, closes volumes and exits

It does not announce that it is leaving. Its peers find out through gossip, as they would after
a crash: they mark it dead once gossip has suspected it for 5 seconds, and store a hint for each
write it misses.

`POST /api/admin/node/drain` is not implemented and answers `501`: stopping the node with
`SIGTERM` is the drain.

## CE vs EE Limits

| Feature | Community Edition | Enterprise Edition |
|---------|------------------|--------------------|
| Pools | 1 pool | Unlimited |
| Nodes per pool | 3 max | Unlimited |
| Gossip & quorum | Full | Full |
| Audit logging | No | Yes |
| Rolling upgrades | No | Yes |
| Deep scrub (SHA-256) | No | Yes |
| Dead node replacement | No | Yes |
