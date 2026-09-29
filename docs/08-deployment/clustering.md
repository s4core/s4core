# Cluster Deployment

> Last updated: 2026-09-26

This guide covers deploying S4 in distributed (cluster) mode. For architecture and internals, see [Federation](../04-features/federation.md).

## Overview

S4 supports three operating modes controlled by `S4_MODE`:

| Mode | Description |
|------|-------------|
| `single` (default) | Standalone server, no cluster overhead |
| `cluster` | Storage node with quorum replication |
| `gateway` | Stateless router — no local storage, forwards requests to cluster nodes |

In `single` mode, no cluster code runs. Switching to `cluster` starts gossip, gRPC, quorum coordinators, and all background cluster workers automatically.

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

Start order does not matter. A node announces itself to its seeds over UDP, and
keeps announcing for as long as it knows no peer, so a node that comes up before
its seeds still joins once they are listening. This makes starting a whole
cluster at once, and rolling restarts, safe to do without staggering.

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
| `S4_ANTI_ENTROPY_INTERVAL_SECS` | `600` | Merkle tree sync interval (seconds) |
| `S4_SCRUBBER_FULL_SCAN_DAYS` | `30` | Full CRC32 integrity scan cycle (days) |
| `S4_HINT_TTL_HOURS` | `3` | Hinted handoff TTL for offline replicas |

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

```bash
# Cluster-wide health
curl http://any-node:9000/admin/cluster/health

# Individual node health
curl http://any-node:9000/admin/node/health

# Cluster topology (pools, nodes, assignments)
curl http://any-node:9000/admin/cluster/topology

# Repair status (anti-entropy progress)
curl http://any-node:9000/admin/cluster/repair-status
```

### Key Metrics

In cluster mode, S4 exposes additional Prometheus metrics:

- `s4_cluster_nodes_alive` — number of alive nodes
- `s4_cluster_quorum_writes_total` — total quorum write operations
- `s4_cluster_quorum_reads_total` — total quorum read operations
- `s4_cluster_hints_pending` — pending hinted handoff entries
- `s4_cluster_blobs_scanned_total` — scrubber progress
- `s4_cluster_corruptions_found_total` — bit rot detections
- `s4_cluster_corruptions_healed_total` — auto-healed corruptions

## Node Recovery

### Short Downtime (< 3 days)

When a node comes back online after a short outage:
1. Gossip automatically detects the node is alive
2. Pending hints are delivered from other nodes
3. Anti-entropy repairs any remaining divergences

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

S4 performs a graceful shutdown sequence in cluster mode:

1. Stops accepting new coordinated requests
2. Waits for in-flight operations (timeout: 30s)
3. Flushes pending hints to disk
4. Broadcasts `Left` status via gossip
5. Shuts down gRPC server
6. Syncs metadata, closes volumes, exits

Use `SIGTERM` or `Ctrl+C` to trigger graceful shutdown.

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
