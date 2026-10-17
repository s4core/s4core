# Cluster Deployment

> Last updated: 2026-10-15

This guide covers deploying S4 in distributed (cluster) mode. For architecture and internals, see [Federation](../04-features/federation.md).

## Overview

S4 supports three operating modes controlled by `S4_MODE`:

| Mode | Description |
|------|-------------|
| `single` (default) | Standalone server, no cluster overhead |
| `cluster` | Storage node with quorum replication |
| `gateway` | Stateless router — no local storage, forwards requests to cluster nodes |

In `single` mode, no cluster code runs. Switching to `cluster` starts gossip, gRPC, quorum coordinators, and all background cluster workers automatically.

Restarting a single node with `S4_MODE=cluster` on the same data directory copies its existing
buckets and objects to their replicas in the background. Follow [Moving from single mode](#moving-from-single-mode)
and wait for the copy to finish before switching clients.

## Moving from Single Mode

Automatic migration supports one node with existing single-mode data and empty peers in a replicated
pool. All pool members must be alive and run the same S4 version before migration starts. The
Community Edition uses three nodes with RF=3, W=R=2; the Enterprise Edition also supports larger
replicated pools.

1. Finish or abort unfinished multipart uploads: open uploads are not migrated. Stop writes,
   shut down the single node cleanly, and back up its data and metadata directories.
2. Configure the cluster as described below. Keep the original node's data and metadata paths;
   give each new node empty directories. Use the same pool roster and S4 version on every node. Existing IAM
   accounts may require the separate [IAM adoption procedure](../05-iam/roles-and-permissions.md#cluster-mode).
3. Start all members with `S4_MODE=cluster`. The original node first copies bucket markers, then
   objects, preserving their bytes, ETags (including multipart ETags), content types and user metadata.
   Until an object has enough replicas its reads can answer `503`.
4. On every node, wait for `migration_pending_records` in `GET /api/admin/cluster/health` and
   `s4_cluster_migration_pending_records` at `/metrics` to reach `0`. The source logs
   `Single-to-cluster migration complete`. Pending records on a new empty node start at zero.
   Also wait for `GET /minio/health/ready` to return `200` on every node. This includes
   required local bucket configuration and IAM recovery; `503` with `Retry-After: 1`
   means the node is still waiting. For a pending IAM adoption decision, use the
   Admin API directly on a node, independently of the client load balancer.
5. Verify bucket listings, configurations and sample object contents through each node, then switch clients to
   the cluster endpoint. Every owner has a copy once migration finishes; the source is no longer
   required for reads within the pool's failure tolerance.

An interrupted migration resumes after the source restarts. A replica that becomes unavailable delays the keys
it owns; other keys continue. Writes and deletes accepted by the cluster while migration runs take
precedence over the earlier single-mode copy. `S4_CLUSTER_MIGRATION_CONCURRENCY` controls how many
records are migrated at once (default `4`, must be positive). Plan space for a full copy on each
replica and network traffic for all those copies. Receiving nodes buffer incoming objects. If a
remote replica holds a newer client version, settling that version also buffers its body on the
migrating node through ordinary repair.
Budget memory for concurrent object sizes and lower concurrency for very large objects.

If an absent owner's delete history is uncertain, an unconfirmed old copy remains pending rather
than risk bringing back a deleted object. This can happen after restoring a peer or restarting peers
without trustworthy shutdown state. The source log explains the wait. Do not switch clients to the
new pool until every pending record is resolved; a restart alone cannot reconstruct lost history.

A configuration acknowledged in single mode becomes available through the cluster only
after its adoption finishes. Keep clients off the new result until the source and the
other members meet the readiness and migration conditions above. The source starts
serving inter-node RPC before readiness, so its peers can receive the data while it
is excluded from client traffic.

A single node with a bucket whose versioning is `Enabled` or `Suspended` refuses this transition,
because cluster mode does not preserve version history. Moving existing single-mode data directly
into an erasure-coded pool is also refused. The error identifies the bucket or pool; reopen the
original directory in single mode to access the data. Combining several populated single nodes into
one pool is not supported.

For an earlier S4 release, or when automatic migration cannot be used, start a separate empty
cluster and copy the required objects through S3, for example with `mc mirror`:

```bash
mc alias set old http://single-node:9000 OLD_ACCESS_KEY OLD_SECRET_KEY
mc alias set new http://cluster-endpoint:9000 NEW_ACCESS_KEY NEW_SECRET_KEY
mc mb new/photos
mc mirror old/photos new/photos
```

Repeat for each bucket, creating empty buckets too, and check the destination before switching
clients. An ordinary mirror copies current objects; it does not preserve their version history or
bucket and IAM settings. Preserve any required history separately and apply supported settings to
the new cluster.

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
(`openssl rand -hex 32`) and give every node that value. The same holds for
`S4_IAM_ENCRYPTION_KEY` when you set it: the secrets of IAM users' S3 credentials are encrypted
with it (without it, with a key derived from `S4_JWT_SECRET`). Every node announces a fingerprint
of root's password, the JWT secret and the encryption key — never the values — and a node that
differs from the others turns the cluster health `critical`, naming it, with
`s4_iam_config_mismatch` at `1`. Any credential can come from a file
instead, through its `_FILE` form — see
[Secrets from Files](../01-getting-started/configuration.md#secrets-from-files-_file).

> **IAM users are kept on every node.** A user created through the Admin API, with their password,
> role and S3 credentials, works through every node once most nodes hold it; IAM changes need a
> majority of the nodes up and answer `503` otherwise. Every node keeps the users' S3 secret keys
> encrypted, and the nodes exchange them encrypted; password hashes and the rest of the traffic
> between nodes are not encrypted: keep that traffic on a private network.
> A node that has just started, or is cut off from most nodes for longer than
> `S4_IAM_MAX_STALENESS_SECS`, answers IAM users' requests `503` until most nodes agree with its
> copy of them: configure the load balancer to retry `503` on another node.
> A new cluster accepts IAM changes once every node is up; after an upgrade from a release that kept
> users on one node, once you take those users over through `/api/admin/iam/adoption`.
> Root, configured by `S4_ROOT_PASSWORD`, is the same on every node. Details, including upgrades
> from releases that kept users on one node:
> [Roles and Permissions → Cluster Mode](../05-iam/roles-and-permissions.md#cluster-mode).

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

### HAProxy Health-Check Fragment

Use readiness for health checks. The supplied Compose deployments also load a
read-only initial DOWN state before HAProxy accepts client requests, then enable
each backend through successful health checks. This closes the startup window
before the first health check. Keep the state mount together with the configuration.
For a production hot reload, save and load the current backend states through the
HAProxy Runtime API; reusing a cold-start DOWN state temporarily excludes healthy
backends. See [HAProxy server-state loading](https://docs.haproxy.org/3.1/configuration.html#4.2-load-server-state-from-file).

The fragment below configures routing and recurring health checks only. Configure
initial backend admission separately: without the initial DOWN state it can route
a request before the first check. Use the supplied Compose deployment for the
complete cold-start configuration, or load matching initial backend states in your
own deployment.

```
frontend s4
    bind *:9000
    default_backend s4_nodes

backend s4_nodes
    balance roundrobin
    option httpchk GET /minio/health/ready
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

This example shows request forwarding. Configure backend admission separately
to follow `/minio/health/ready`, including the initial startup.

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
| `S4_GC_GRACE_DAYS` | `7` | Minimum tombstone retention; must exceed max rejoin downtime, a turn over peers and 60.5 s |
| `S4_GC_GRACE_SECS` | unset | The same retention in seconds; takes precedence over days |
| `S4_TOMBSTONE_GC_INTERVAL_SECS` | `21600` | Seconds between tombstone GC passes; only completed repair rounds permit purge |
| `S4_MAX_REJOIN_DOWNTIME_DAYS` | `3` | Part of the lower bound on tombstone retention; see [Long Downtime](#long-downtime--s4_max_rejoin_downtime_days) |
| `S4_EXPECTED_NODE_IDENTITY` | unset | Identity (UUID) this data directory must belong to; node replacement tool |
| `S4_NODE_IDENTITY_CONFLICT_CHECK` | `true` | Refuse the start when a live pool member already answers for this node's identity |
| `S4_ANTI_ENTROPY_INTERVAL_SECS` | `600` | Seconds between two anti-entropy rounds of a node; each round compares it with one peer |
| `S4_ANTI_ENTROPY_MAX_KEYS_PER_ROUND` | `1000` | Most divergent keys one round settles; the rest wait for the next rounds |
| `S4_ANTI_ENTROPY_ENABLED` | `true` | `false`: the node starts no rounds but answers its peers'; its frontier and tombstone GC wait |
| `S4_SCRUBBER_FULL_SCAN_DAYS` | `30` | Full CRC32 integrity scan cycle (days) |
| `S4_HINT_TTL_HOURS` | `3` | How long a hint for a replica that missed a write is kept; `0` keeps none |

A replicated pool refuses startup when the GC safety margin is too small or its calculation
overflows; see the [configuration reference](../01-getting-started/configuration.md#federation-cluster-mode).
Anti-entropy requires every live node to announce protocol 8 or newer (current protocol: 9).
Older live nodes make rounds `skipped`; a live peer with an unknown protocol retains its turn.
After every restart, the node needs fresh complete rounds with every other owner of a key before
its tombstone can be purged, even when the grace period has already passed.

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

## Restarting a node

Stop a node with SIGTERM and wait for it to exit before restarting it. A clean shutdown
lets the next start reuse its version digests and deletion history. After a crash, a missing
or damaged shutdown checkpoint, or writes by an earlier release, the node rebuilds its
digests before serving and establishes a new boundary for the deletion history it can
confirm. Rebuilding a large index can lengthen startup. Pool placement and the progress of
an ongoing move survive; bucket configurations continue to be read through the quorums
once the pool has switched to them.

## Growing a pool

A pool grows by **appending nodes** to `S4_POOL_NODES` (Enterprise Edition: Community Edition
keeps a pool at three nodes). A larger pool places keys on a new hash ring, so part of every
bucket's records, settings and objects gets new replica nodes; the pool moves what it already
stores to them in the background while it keeps serving every request.

### Steps

1. **Run the same release on every node.** A pool grows only while every node of the new list
   runs a release that knows pool generations; a node of an earlier release keeps the growth from
   beginning.
2. **Start the new nodes** with the full list: the current nodes in their order, the new ones
   appended at the end. Nodes are only appended. A list with the nodes in another order, or with a
   node missing, is refused at start with the reason in the log, and nothing is changed.
3. **Restart the current nodes one at a time** with the same list. The order does not matter.
   Restart the next one once the one before is back: alive on every node, and with no hints left
   for it — `s4_hints_pending{peer="<its node id>"}` at 0 on every node, which takes up to half a
   minute. A node stopped while the writes it missed still wait for the node restarted before it
   leaves those keys on one replica of the two a read needs, and their reads answer `503` until
   both are back. Until every node of the new list has started with it, the pool works as before.
   Keep the new nodes running meanwhile: a node starting with the new list has to name every
   node of it, and one that is down, that it has never recorded and that gossip has already
   forgotten makes it refuse to start (`missing pool nodes: …` in its log). Started with the
   current list, it serves as before until that node is back.

Then nothing else is needed. The growth begins by itself once every node of the new list is
alive and has announced it, the move of the bucket records to more replicas has settled, and each
new node has room for its share of the pool with `S4_REBALANCE_MIN_FREE_RATIO` of its disk left
over. While one of these is missing the pool keeps serving by the current list, and
`GET /api/admin/cluster/rebalance` names the node and the reason. A second growth is refused while
one is under way.

### During the growth

- **Writes and deletes** reach a write quorum of the current replicas **and** of the new ones. If
  either group cannot reach its quorum, the write answers `503`, as it does when a quorum is
  missing in a pool that does not grow.
- **Reads** go to the current replicas of each range of the ring until that range is verified and
  cut over, then to the new ones.
- Every node of the current list hands what it stores to the new owners, a range at a time, the
  newer version winning. A new owner refuses a copy whose checksum does not match, and the copy is
  taken from another replica. Progress is stored after every page: a restart of any node, or of the
  whole pool, continues where it stopped.
- The member with the lowest identity among those alive compares each range on the new owners
  with what the current owners confirmed, and cuts it over. Verification needs every member to
  answer: while a node is down the growth waits, and the pool keeps serving.
- Once every range is cut over on every node and no request placed by the current list is still
  under way, the pool settles on the grown list. From then on a write needs the new replicas only.
- A node of an earlier release, or one restarted with the earlier list, that comes back during the
  growth has its client writes refused with `503` (`s4_pool_generation_fenced_writes_total`).
- After the pool settled and `S4_GC_GRACE_DAYS` passed, each node removes the copies of keys it
  no longer owns, once their owners hold them. Until then the former owners keep the disk space
  of those copies; compaction frees it after the removal.

### How long it takes

Each node of the current list goes through every key it stores — the ones that move and the ones
that stay — at no more than `S4_REBALANCE_MAX_KEYS_PER_SEC` keys (500 by default) and sends no more
than `S4_REBALANCE_MAX_BYTES_PER_SEC` (64 MiB/s), over `S4_REBALANCE_CONCURRENT_RANGES` ranges at a
time; the nodes work in parallel. A node therefore needs at least its key count divided by the key
rate, or the bytes it sends divided by the byte rate, whichever is longer: 10 million keys take
about 5.5 hours at the default key rate. While clients keep a node busy the move gives way to them.
Raise the limits for a faster growth on hardware that has room for it; lower them if client
latency suffers. After the last node handed over, the verifying member reads the version digests
of every range from its owners before it cuts the range over, which adds to that time.

### Watching and pausing it

```bash
# Where the growth stands on this node.
curl -H "Authorization: Bearer $TOKEN" http://any-node:9000/api/admin/cluster/rebalance

# Pause the move of the keys on every node, for example during peak hours.
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"reason": "peak hours"}' http://any-node:9000/api/admin/cluster/rebalance/pause

# Resume it.
curl -X POST -H "Authorization: Bearer $TOKEN" http://any-node:9000/api/admin/cluster/rebalance/resume
```

The answer is the view of the node asked. `state` is `stable` (the pool does not grow, or it
settled), `prepared` (the grown list is configured and the growth waits to begin) or `moving`.
`generation` is the list the node places keys by, `moving_to` the grown one. `waiting` says why
the growth waits on this node: before it begins, on every node; during it, on the member that
verifies. `ranges` counts the ranges of the ring by state. `transfer` is the move of this node's
keys — absent on a new node, which has nothing to hand over: what it went through, what it moved,
`remaining_bytes` (an estimate: what the node stores, in the share of its ranges not handed over
yet; in an erasure-coded pool it counts shards too, which do not move), `eta_secs` from the pace
since the node started or the move was last resumed, the work that waits and why, and the keys
that failed a try by reason — each is tried again.

```json
{
  "pool": "pool-1", "pool_id": 0, "state": "moving",
  "generation": {"epoch": 1, "nodes": 3}, "moving_to": {"epoch": 2, "nodes": 6},
  "waiting": null, "paused": null, "fenced_earlier_generation": true,
  "ranges": {"total": 1152, "pending": 0, "streaming": 1152, "verified": 0, "cut_over": 0},
  "transfer": {
    "handed_over": false, "items_total": 1154, "items_done": 610,
    "ranges_total": 1152, "ranges_done": 609,
    "keys_scanned": 1048576, "keys_moved": 524800, "bytes_moved": 268435456000,
    "remaining_bytes": 241591910400, "eta_secs": 3720,
    "waiting": {}, "errors": {"unavailable": 3}
  }
}
```

A pause belongs to the pool: it is written through the quorums, the node asked applies it at once
and every other node within a few seconds, a node that was down applies it when it comes back, and
it outlives restarts until it is resumed. A paused growth stops moving keys; writes still reach
both lists of replicas, so the pool keeps the cost of the growth until it is resumed. Pausing a
pool that does not grow answers `409`; a pause that could not reach a quorum answers `503` and can
be sent again.

A new node short of space receives nothing until it has room again: the ranges meant for it wait,
`transfer.waiting` names the node, and `s4_rebalance_paused{reason="no_room"}` is `1`.

The same numbers are in the metrics: `s4_pool_generation`, `s4_rebalance_ranges`,
`s4_rebalance_keys_total`, `s4_rebalance_bytes_total`, `s4_rebalance_remaining_bytes`,
`s4_rebalance_paused` and `s4_rebalance_errors_total` (see
[Prometheus Metrics → Cluster](../06-monitoring/prometheus-metrics.md#cluster)). The node log
records the beginning of the growth, every tenth range cut over, a pause and a resumption, the
settling and the removal of the former owners' copies.

### Rolling back

Before the growth began (`state` is `prepared` on every node), restart the nodes with the earlier
list and stop the new ones; the earlier release may be restored too. Once it began there is no way
back to the earlier list: the new nodes already hold writes the clients were told succeeded, and a
node whose growth began refuses the earlier list at start. Pause the growth instead. A node
returned to a release without pool generations after the growth began has its writes refused.

### A pool grown on an earlier release

An earlier release did not move anything when a pool grew, and part of its buckets and objects
answered `503` afterwards. Upgrading such a pool repairs it: on its first start with this release
every node reconciles the copies it holds outside their current owners with those owners.
It removes its own copy only after the owners hold the confirmed version and
`S4_GC_GRACE_DAYS` has passed. Do not wipe or replace the data directory of any node of such
a pool before the upgrade: those copies may be the only ones.

The earlier release deleted an object without leaving a record of the delete, so an owner that
does not hold a key cannot tell whether it deleted the key or never received it. The upgrade
gives the owners every such copy: an object deleted on the earlier release after the pool grew
may come back, while one the clients were told is stored is never lost. Check the buckets where
objects were deleted between the growth and the upgrade. This recovery rule applies to user
objects; bucket existence and settings require confirmation from their owners. An unconfirmed
foreign copy stays on disk without bringing a deleted bucket or its settings back.

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

# The pool's growth (see Growing a pool)
curl -H "Authorization: Bearer $TOKEN" http://any-node:9000/api/admin/cluster/rebalance

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
  "pool": "pool-1", "replication_factor": 3, "write_quorum": 2, "read_quorum": 2,
  "unenforced_bucket_configs": [],
  "migration_pending_records": 0
}
```

`unenforced_bucket_configs` lists the buckets whose versioning or Object Lock this node stores from
an earlier release, which accepted them in cluster mode, as `{"bucket": "…", "kind": "versioning"}`
or `"object_lock"`. Cluster mode enforces neither (see
[Object Lock → Cluster Mode](../04-features/object-lock.md#cluster-mode)); each node lists only
what it stores itself, so ask every node.

`migration_pending_records` counts this node's single-mode records still waiting to reach every
owner. Wait for zero on every node before directing clients to a newly migrated pool; see
[Moving from Single Mode](#moving-from-single-mode).

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

`cluster/repair-status` counts, since the node started, the anti-entropy rounds it ran by how they
ended (`rounds`; `rounds_completed` is the complete ones), the keys they found in different
versions on the node and its peer (`divergent_keys`), and what became of them: repaired
(`total_repairs`), failed (`repairs_failed`) or left alone as ambiguous (`repairs_ambiguous`). It
also gives the time of the last repair, the repair frontier lag, the tombstones GC holds and
purged, and whether the node starts rounds at all (`anti_entropy_enabled`). These are the live
service counters, also exported in `/metrics`. A complete round records a frontier cut at its
start minus 60.5 seconds; an incomplete or skipped round records none. Cuts are held in memory,
so a restart makes GC wait for fresh complete rounds with the other owners. The node itself
needs no cut. See [Anti-Entropy](../04-features/federation.md#anti-entropy).

```json
{
  "rounds_completed": 12, "rounds": {"complete": 12, "incomplete": 1, "skipped": 0},
  "divergent_keys": 7, "total_repairs": 7, "repairs_failed": 0, "repairs_ambiguous": 0,
  "anti_entropy_enabled": true, "last_repair": "2026-10-03T10:00:00.000Z",
  "repair_frontier_lag_secs": 65.5, "tombstones_active": 0, "tombstones_purged": 0
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
- `s4_rebalance_ranges` and `s4_rebalance_remaining_bytes` — while a pool grows, how far it got;
  `s4_rebalance_paused{reason="no_room"}` — a new node short of space.

## Node Recovery

### Short Downtime (< 3 days)

When a node comes back online after a short outage:
1. Gossip automatically detects the node is alive
2. The other nodes deliver the hints they stored for it within 30 seconds; a hint whose key has
   another replica down waits until that replica is back too (see
   [Hinted Handoff](../04-features/federation.md#hinted-handoff))
3. What the hints did not cover — hints that expired or were dropped — is brought up to date by
   anti-entropy within a few rounds (see
   [Anti-Entropy](../04-features/federation.md#anti-entropy))

No manual intervention needed.

### Long Downtime (> `S4_MAX_REJOIN_DOWNTIME_DAYS`)

A node offline longer than the max rejoin downtime (default: 3 days) may hold
objects the cluster deleted while it was away, and whose tombstones have since
been purged. Bringing its old data back could resurrect them.

`S4_MAX_REJOIN_DOWNTIME_DAYS` does two things. It constrains the tombstone GC
invariant — `gc_grace` must exceed it plus a turn over peers, the replica write timeout
and tolerated clock skew — for replicated pools, and on an
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

A replacement brought up with a fresh identity — an empty data directory
without `S4_EXPECTED_NODE_IDENTITY` — does not start. The pool keeps the dead
node's place, and its data, for the identity it recorded, so the start stops
with an error that names that identity when the pool still knows it, or points
to `GET /api/admin/cluster/topology` on any surviving node, where it is listed
as a member that is not alive. Start the machine again with
`S4_EXPECTED_NODE_IDENTITY` set to it. The same error appears when
`S4_POOL_NODES` lists the pool's addresses in a different order than on the
other nodes.

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
