# Cluster Deployment

> Last updated: 2026-04-25

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
      S4_CLUSTER_NAME: dev
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
    ports:
      - "9002:9000"

  s4-node3:
    image: s4core:latest
    environment:
      S4_MODE: cluster
      S4_CLUSTER_NAME: dev
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
    ports:
      - "9003:9000"

volumes:
  s4-data-1:
  s4-data-2:
  s4-data-3:
```

```bash
docker compose -f docker-compose-cluster.yml up -d
```

After startup, any node's HTTP port accepts S3 requests. Place a load balancer in front for production use.

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
| `S4_MAX_REJOIN_DOWNTIME_DAYS` | `3` | Max days a node can be offline and rejoin incrementally |
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

If a node was offline longer than the max rejoin downtime (default: 3 days), it must perform a full bootstrap to avoid zombie resurrection:

```bash
curl -X POST http://node-address:9000/admin/node/bootstrap
```

This triggers a full data sync from healthy replicas.

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
