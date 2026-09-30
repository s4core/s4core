# Configuration

S4 is configured through environment variables. All settings have sensible defaults for
development, so a server started with no configuration at all will run.

This page is the complete reference. Feature pages link here rather than repeating the tables.

## Server and Authentication

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `S4_BIND` | Bind host address | `127.0.0.1:9000` | `0.0.0.0:9000` |
| `S4_DATA_DIR` | Base directory for storage data | System temp dir | `/var/lib/s4` |
| `S4_ACCESS_KEY_ID` | Access key for S3 authentication. Set to an empty value, it switches login with these keys off | Random dev key at every start | `myaccesskey` |
| `S4_SECRET_ACCESS_KEY` | Secret key for S3 authentication. Set to an empty value, it switches login with these keys off | Random dev key at every start | `mysecretkey` |
| `S4_ROOT_USERNAME` | Root admin username | `root` | `admin` |
| `S4_ROOT_PASSWORD` | Root admin password (enables IAM when set) | None (IAM disabled) | `password12345` |
| `S4_JWT_SECRET` | Secret that signs admin JWTs, at least 32 bytes. Required, and identical on every node, in cluster and gateway mode with IAM — see [Admin JWT Secret](#admin-jwt-secret) | Single node: random, generated at every start (admin sessions end on restart) | Output of `openssl rand -hex 32`, generated once |
| `S4_MAX_UPLOAD_SIZE` | Maximum upload size per request | `5GB` | `10GB`, `100MB`, `1024KB` |
| `S4_TLS_CERT` | Path to TLS certificate (PEM) — enables HTTPS with `S4_TLS_KEY` | None (HTTP mode) | `/etc/ssl/certs/s4.pem` |
| `S4_TLS_KEY` | Path to TLS private key (PEM) — enables HTTPS with `S4_TLS_CERT` | None (HTTP mode) | `/etc/ssl/private/s4-key.pem` |
| `RUST_LOG` | Log level and filter | `s4_api=debug,s4_server=info` | `info`, `s4_ee=debug` |

### Admin JWT Secret

`S4_JWT_SECRET` signs admin tokens, and the admin API trusts the role inside any token signed with
it, so it is as sensitive as the root password. The server refuses to start, with an error that
names the variable, when:

- `S4_JWT_SECRET` is shorter than 32 bytes. Anyone holding an admin token could guess a short
  secret offline and sign a SuperUser token with it.
- The node runs in cluster or gateway mode with IAM in use — `S4_ROOT_PASSWORD` set, or IAM users
  left from an earlier start — and `S4_JWT_SECRET` is not set. Each node would sign with its own
  random secret, and a token would work only on the node that issued it.

Without `S4_JWT_SECRET` a single node generates a random secret at every start, so admin sessions
end on restart. Generate the value once and keep it: a value generated anew at every start or on
every node behaves like no value at all.

### Secrets from Files (`_FILE`)

Every credential above can be read from a file instead of the environment: append `_FILE` to
the variable name and give the path. This is the Docker and Kubernetes secrets convention: the
orchestrator mounts each secret as a file, so the value never shows up in `docker inspect`, in
the process environment or in a compose file kept in git.

| Variable | File variant |
|----------|--------------|
| `S4_ROOT_USERNAME` | `S4_ROOT_USERNAME_FILE` |
| `S4_ROOT_PASSWORD` | `S4_ROOT_PASSWORD_FILE` |
| `S4_ACCESS_KEY_ID` | `S4_ACCESS_KEY_ID_FILE` |
| `S4_SECRET_ACCESS_KEY` | `S4_SECRET_ACCESS_KEY_FILE` |
| `S4_JWT_SECRET` | `S4_JWT_SECRET_FILE` |

The Enterprise licence has its own file variable, `S4_LICENSE_FILE` (see
[Licensing](#licensing-enterprise-edition)).

The rules are the same for every variable:

- Trailing line breaks and a leading UTF-8 byte order mark are stripped from both forms, so
  `echo value > file` works and a secret gives the same value from a file and from a variable.
- An empty variable does not clash with its `_FILE` form, so `S4_ROOT_PASSWORD=${S4_ROOT_PASSWORD:-}`
  in a compose template can sit next to `S4_ROOT_PASSWORD_FILE`. On its own, an empty user name,
  root password or JWT secret counts as unset; empty S3 keys switch login with those keys off.
- The server refuses to start, naming the variable, when both `NAME` and `NAME_FILE` are set,
  or when the file cannot be read, is empty, is larger than 64 KiB or is not UTF-8 text. It
  never falls back to a default in those cases.

A Docker Compose example is in [Docker Deployment](../08-deployment/docker.md#credentials-from-docker-secrets).

## Lifecycle Policies

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `S4_LIFECYCLE_ENABLED` | Enable the lifecycle policy worker | `true` | `false` |
| `S4_LIFECYCLE_INTERVAL_HOURS` | Lifecycle evaluation interval, in hours | `24` | `1`, `6`, `168` |
| `S4_LIFECYCLE_DRY_RUN` | Log what would be deleted without deleting it | `false` | `true` |

## Volume Compaction

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `S4_COMPACTION_ENABLED` | Enable the volume compaction worker | `true` | `false` |
| `S4_COMPACTION_INTERVAL_HOURS` | Compaction check interval, in hours | `1` | `6`, `12`, `24` |
| `S4_COMPACTION_THRESHOLD` | Minimum fragmentation ratio that triggers compaction | `0.3` | `0.1`–`0.9` |
| `S4_COMPACTION_DRY_RUN` | Analyze without compacting | `false` | `true` |
| `S4_COMPACTION_FULL_TIME` | Daily full compaction time, `HH:MM` local | `02:00` | `03:30`, `""` to disable |
| `S4_MULTIPART_UPLOAD_TTL_HOURS` | TTL for abandoned multipart sessions, in hours | `24` | `1`, `48` |
| `S4_COMPACTION_MULTIPART_TTL_SECS` | *Dev and testing only.* Overrides the multipart TTL for the compactor, in seconds | None | `1`, `60` |

## S3 Select and Metrics

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `S4_SELECT_ENABLED` | Enable the S3 Select SQL engine | `true` | `false` |
| `S4_SELECT_MAX_MEMORY` | Per-query memory limit | `256MB` | `512MB`, `1GB` |
| `S4_SELECT_TIMEOUT` | SQL query timeout, in seconds | `60` | `120` |
| `S4_METRICS_ENABLED` | Serve Prometheus metrics | `true` | `false` |

## Federation (Cluster Mode)

See [Clustering](../08-deployment/clustering.md) for how these fit together.

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `S4_MODE` | Operating mode: `single`, `cluster`, `gateway` | `single` | `cluster` |
| `S4_CLUSTER_NAME` | Cluster name, for network isolation | `default` | `production` |
| `S4_NODE_ID` | Human-readable node name (the internal UUID is generated) | Auto | `node-1` |
| `S4_EXPECTED_NODE_IDENTITY` | Cluster identity (UUID) this data directory must belong to. Node replacement tool, not an everyday setting — see [Erasure Coding → Replacing a node](../04-features/erasure-coding.md#replacing-a-node) | unset | `0d3bb4d8-876d-4b6a-a27f-49a69d11d3ae` |
| `S4_NODE_IDENTITY_CONFLICT_CHECK` | Refuse to start when a live pool member already answers for this node's identity. The check asks every address in `S4_POOL_NODES` before joining gossip; an address that does not answer proves nothing and never refuses the start | `true` | `false` |
| `S4_NODE_GRPC_ADDR` | gRPC address for inter-node communication | — | `10.0.1.1:9100` |
| `S4_NODE_HTTP_ADDR` | HTTP address advertised to other nodes | — | `10.0.1.1:9000` |
| `S4_SEEDS` | Comma-separated seed node gRPC addresses (required in cluster and gateway mode) | — | `10.0.1.1:9100,10.0.1.2:9100` |
| `S4_POOL_NAME` | Pool this node belongs to (required in cluster mode) | — | `pool-1` |
| `S4_POOL_NODES` | Pool members, `id:addr,id:addr,…` (required in cluster mode) | — | `node-1:10.0.1.1:9100,…` |
| `S4_POOL_TYPE` | Pool storage type: `standard` or `ec` | `standard` | `ec` |
| `S4_REPLICATION_FACTOR` | Replication factor N | `3` | `3` |
| `S4_WRITE_QUORUM` | Write quorum W | `2` | `2` |
| `S4_READ_QUORUM` | Read quorum R | `2` | `2` |
| `S4_GC_GRACE_DAYS` | Tombstone GC grace period, in days | `7` | `7` |
| `S4_MAX_REJOIN_DOWNTIME_DAYS` | Maximum offline days before a returning node must rebuild before it serves erasure-coded reads. Also bounds the tombstone GC grace period. See [Erasure Coding → Coming back to the pool](../04-features/erasure-coding.md#coming-back-to-the-pool) | `3` | `3` |
| `S4_ANTI_ENTROPY_INTERVAL_SECS` | Merkle tree exchange interval, in seconds | `600` | `600` |
| `S4_SCRUBBER_FULL_SCAN_DAYS` | Full CRC32 scrub cycle, in days | `30` | `30` |
| `S4_SCRUBBER_START_DELAY_SECS` | How long the scrubber waits after start before its first cycle. Healing a damaged blob means fetching a healthy copy from a peer, and a node that has just started has not finished hearing about its peers | `30` | `30` |
| `S4_SCRUBBER_UNHEALED_RETRY_SECS` | How soon the scrubber runs again after a cycle that found damage it could not heal. A cycle that healed everything waits the full scan period | `300` | `300` |
| `S4_HINT_TTL_HOURS` | Hinted handoff TTL, in hours | `3` | `3` |

## Erasure Coding (Enterprise Edition)

Only meaningful on an erasure-coded pool (`S4_POOL_TYPE=ec`). See
[Erasure Coding](../04-features/erasure-coding.md).

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `S4_EC_NODE_TOPOLOGY` | Failure-domain labels by pool-node name: `node:zone=...,rack=...,host=...,disk_group=...;...`. Once a set's layout is recorded, a label change no longer moves its slots. Keep it identical on every node, and do not change it while the pool still has nodes of an older release, see [Slot layouts are recorded](../04-features/erasure-coding.md#slot-layouts-are-recorded-labels-do-not-move-them) | unset | `node-1:zone=z1,rack=r1,host=h1,disk_group=d1` |
| `S4_EC_TOPOLOGY_POLICY` | `strict` refuses to start on partial labels; `warn` starts with degraded topology health | `strict` | `warn` |
| `S4_EC_LRC_GROUP_DOMAIN` | Failure-domain level the members of an LRC local group are spread over: `off`, `rack`, `zone`, `host`. Applies to LRC sets added after it is set, which are laid out so that every group is in distinct domains; `S4_EC_TOPOLOGY_POLICY` decides whether nodes that cannot meet that are refused (`strict`) or give a degraded set (`warn`). Existing sets keep their layout. An unknown value stops startup. See [LRC group level](../04-features/erasure-coding.md#lrc-group-level) | `off` | `rack` |
| `S4_EC_CODEC_ENGINE` | Codec engine: `auto`, `isal`, `pure`. `auto` uses ISA-L when it was built in and the CPU supports it. `isal` refuses to start when it cannot be provided. Both engines produce identical bytes | `auto` | `isal` |
| `S4_EC_TIERING_ENABLED` | Auto-tiering gate | `false` | `true` |
| `S4_EC_TIERING_DEFER_NEW_WRITES` | Keep new writes on RF=3 and let auto-tiering encode them once cold (requires `S4_EC_TIERING_ENABLED=true`) | `false` | `true` |
| `S4_EC_TIERING_PROMOTE_AFTER_READS` | Reads within the window that bring an object back to RF=3; `0` disables automatic reverse tiering | `0` | `5` |
| `S4_EC_PROMOTED_GC_GRACE_SECS` | How long a promoted object's shards are kept after readers move to its RF=3 copy | `3600` | `7200` |
| `S4_EC_TOMBSTONE_GC_WORKER_ENABLED` | Whether this node releases the shards of deleted objects on a schedule. Off leaves the shards of every deleted erasure-coded object on disk until an operator runs the sweep by hand | `true` | `false` |
| `S4_EC_TOMBSTONE_GC_WORKER_INTERVAL_SECS` | How often that sweep runs. Nothing waits on it — the object is already gone to every client — so it is deliberately slower than the anti-entropy tick | `300` | `60` |
| `S4_EC_TOMBSTONE_GC_SCAN_BUDGET` | Manifests one sweep walks before it stops and leaves the rest to the next cycle. The sweep looks for deleted objects among every manifest the node holds, so its cost is the walk, not the findings; a budget makes each cycle a fixed slice and successive cycles cover the keyspace. A full round takes `manifests / budget` cycles, which at the defaults is hours against a 7-day delete grace | `10000` | `50000` |
| `S4_EC_TOMBSTONE_RETIRE_GRACE_SECS` | Extra grace after `S4_EC_GC_GRACE_SECS` before a tombstone is removed from every metadata node of the pool at once. It gives each shard owner a sweep of its own in which to release its shards; an owner that has not swept by then keeps shards no manifest names, which the orphan sweep collects instead | `3600` | `7200` |
| `S4_EC_MULTI_SET_ENABLED` | Allow more than one erasure set in a pool. With it off, every new object goes to the pool's default set and behaviour is identical to EC v1 | `false` | `true` |
| `S4_EC_PLACEMENT_STRATEGY` | How a new object picks its erasure set: `round_robin` or `free_space`. Only meaningful with `S4_EC_MULTI_SET_ENABLED=true` | `round_robin` | `free_space` |
| `S4_EC_SET_FULL_THRESHOLD` | Used-space ratio at which a set stops taking new objects. The set stays active and takes writes again on its own once space is freed | `0.9` | `0.85` |
| `S4_EC_CAPACITY_MAX_AGE_SECS` | How old a node capacity report may be before placement stops believing it. Past this age the set counts as unmeasured and `free_space` places as `round_robin` does | `300` | `120` |
| `S4_EC_SET_REFRESH_SECS` | How often a node re-reads the pool's erasure-set list. An operator adds a set on one node; this is the worst-case lag before the others serve its objects. Also the tick on which a node refreshes its own presence record, which is what the downtime above is measured against | `30` | `10` |
| `S4_EC_LONG_OFFLINE_REJOIN_POLICY` | What a node that was away longer than `S4_MAX_REJOIN_DOWNTIME_DAYS` may do: `needs_bootstrap` refuses erasure-coded reads until its shards are rebuilt, `require_admin_approval` waits for an explicit operator approval | `needs_bootstrap` | `require_admin_approval` |
| `S4_EC_REJOIN_GATE_ENABLED` | Whether a node held back by the rejoin verdict actually stops serving erasure-coded reads. With it off the verdict is still reached and still reported in pool health and metrics, and nothing is refused | `true` | `false` |
| `S4_EC_MINIMAL_READ_ENABLED` | Whether a read that serves bytes pulls only the `k` data shards it assembles from, instead of every shard of the chunk. On: a healthy `GET` moves `k/n` of what it used to and a short range costs one shard rather than a whole chunk. Off: the EC v1 read, which fetches and verifies all `n` on every read. Per node and reversible at any moment — nothing on disk depends on it, so a pool may run with it set either way on any of its nodes. Finding corruption in a parity shard belongs to the scrubber either way, see [Reading only what is needed](../04-features/erasure-coding.md#reading-only-what-is-needed) | `true` | `false` |
| `S4_EC_SCRUB_MEDIA_FAILURE_RATIO` | Share of one scrub pass that may fail on the medium — a detached volume, an exhausted descriptor table, an index that stopped answering — before the pass stops and reports the disk as suspect instead of judging shards one by one. Such reads condemn nothing: the shard stays with its owner, uncounted as corruption and unrepaired, and the next pass asks again. Only bytes that were read and found to differ from their recorded hash are corruption | `0.25` | `0.5` |
| `S4_EC_SMALL_PLACEMENT_NEGATIVE_TTL_SECS` | How long a node remembers that an object has no replicated-small placement record. Every object the hot path still holds answers "no EC manifest", so without this memory each of their reads would pay a quorum `HEAD` and a placement resolve that can only miss. `0` turns the memory off and restores a resolve per read | `60` | `300` |
| `S4_EC_PACKED_SEGMENTS_ENABLED` | Gather objects below `S4_EC_MIN_OBJECT_SIZE` into large segments and erasure code the segment, instead of keeping each object at RF=3. Off by default: existing segments need the unpacker before the feature can be switched back off | `false` | `true` |
| `S4_EC_PACKED_SEGMENT_TARGET_BYTES` | Size a segment grows to before it is sealed and encoded. Allowed range is eight times `S4_EC_MIN_OBJECT_SIZE` up to 256 MiB | `67108864` | `134217728` |
| `S4_EC_PACKED_SEGMENT_MAX_AGE_SECS` | How long a segment waits for more objects before it is sealed anyway. Bounds how long the first object in a segment stays at RF=3 cost | `300` | `900` |
| `S4_EC_PACKED_SEGMENT_MAX_OBJECTS` | Upper bound on the objects in one segment. Bounds the segment index, the repack cost and the atomic layout switch | `4096` | `2048` |
| `S4_EC_PACKED_SEGMENT_MAX_OPEN` | Segments kept open per erasure set. Appends inside one segment are serialized, so this is how wide small-object writes run in parallel | `4` | `8` |
| `S4_EC_PACKED_SEGMENT_REPACK_DEAD_RATIO` | Fraction of a segment that must belong to deleted objects before it is repacked. Below it a repack reads and re-encodes more than it gives back | `0.5` | `0.6` |
| `S4_EC_PACKED_SEGMENT_REPACK_MIN_DEAD_BYTES` | Dead space a segment must hold before a repack is worth an encode at all. Stops a small, highly fragmented segment from costing more work than it returns | `8388608` | `33554432` |
| `S4_EC_PACKED_SEGMENT_RETIRE_GRACE_SECS` | How long a repacked segment is kept intact after its objects move to their replacement, so that reads which started before the switch finish on it. The shards themselves then wait out `S4_EC_GC_GRACE_SECS` as any tombstoned object's do | `3600` | `7200` |

With packing on, a small object is placed in a segment at write time and its bytes stay on their RF=3
replicas until the segment is sealed and erasure coded. Durability is therefore never lower than it
would be without packing, and reads are unaffected. Once the segment is encoded and every object in
it has been read back out of EC, the RF=3 copies are released and the space saving is realised.

Deleting an object inside an encoded segment does not free its bytes: they sit inside a container
that was encoded as a whole. The space comes back through repacking — a background pass that moves a
fragmented segment's surviving objects into a fresh segment and retires the old one — and the two
thresholds above decide when that work is worth doing. A segment holding an object under Object Lock
is not repacked until the hold expires; the wait is visible as
`s4_ec_packed_repack_blocked_total` rather than reported as an error.


Each node measures the filesystem holding its data directory and broadcasts the result with its
gossip metadata, on the metadata interval. A node that cannot measure its filesystem reports
nothing rather than reporting itself as full: `free_space` then spreads objects evenly instead of
refusing to place them.

## Licensing (Enterprise Edition)

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `S4_LICENSE_KEY` | Enterprise license key string | None (runs as CE) | `eyJ…` |
| `S4_LICENSE_FILE` | Path to a file holding the license key | None | `/etc/s4/license.key` |

## Size Format

Variables that take a size — `S4_MAX_UPLOAD_SIZE`, `S4_SELECT_MAX_MEMORY` — accept
human-readable suffixes:

- `GB` or `G` — gigabytes
- `MB` or `M` — megabytes
- `KB` or `K` — kilobytes
- No suffix — bytes

Examples: `5GB`, `100MB`, `1024KB`, `5368709120`

## Example: Development Setup

```bash
export S4_ACCESS_KEY_ID=myaccesskey
export S4_SECRET_ACCESS_KEY=mysecretkey
export S4_DATA_DIR=/tmp/s4-data

./target/release/s4-server
```

## Example: Production Setup

```bash
export S4_BIND=0.0.0.0:9000
export S4_DATA_DIR=/var/lib/s4
export S4_ROOT_PASSWORD=your-strong-password
export S4_JWT_SECRET_FILE=/etc/s4/jwt_secret  # created once: openssl rand -hex 32 > /etc/s4/jwt_secret
export S4_TLS_CERT=/etc/ssl/certs/s4.pem
export S4_TLS_KEY=/etc/ssl/private/s4-key.pem
export S4_MAX_UPLOAD_SIZE=10GB

./target/release/s4-server
```

## Optional Configuration File

You can also use a `config.toml` file:

```toml
[server]
bind = "0.0.0.0:9000"

[storage]
data_path = "/var/lib/s4/volumes"
metadata_path = "/var/lib/s4/metadata_db"

[tuning]
volume_size_mb = 1024    # 1GB — maximum size of each append-only volume file
strict_sync = true       # fsync after every write
```
