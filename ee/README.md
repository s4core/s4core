# S4Core — Enterprise Edition

This directory contains the **Enterprise Edition (EE)** extensions for S4Core.

EE is licensed under the [Elastic License 2.0](LICENSE) and is source-available
for audit and review. Using EE features in production requires a valid license
key.

## What does EE add?

S4Core Community Edition (CE) is a fully functional storage cluster —
1 pool, up to 3 nodes, with quorum replication, gossip protocol, anti-entropy
repair, and background CRC32 scrubbing. EE removes scaling limits and adds
operational features:

| Feature | CE | EE |
|---------|----|----|
| Full cluster (1 pool, 3 nodes) | Yes | Yes |
| Quorum replication (RF=3, W=2, R=2) | Yes | Yes |
| SWIM Gossip + gRPC | Yes | Yes |
| Anti-Entropy (Merkle tree repair) | Yes | Yes |
| Background CRC32 Scrubber | Yes | Yes |
| Unlimited pools and nodes | — | Yes |
| Deep SHA-256 Scrubber | — | Yes |
| Rolling Upgrades (zero downtime) | — | Yes |
| Automatic Node Replacement | — | Yes |
| LDAP / SAML / OIDC Authentication | — | Yes |
| WebDAV Access | — | Yes |
| Audit Log | — | Yes |
| Federation Erasure Coding | — | Yes, with `erasure_coding` capability |

## Building

```bash
# Community Edition (no EE code in the binary)
cargo build --release

# Enterprise Edition
cargo build --release --features enterprise
```

The `enterprise` Cargo feature gate controls which code is compiled. Without it,
EE code is physically absent from the binary — it is not hidden behind a flag,
it simply does not exist in the compiled program.

## Docker Images

S4Core publishes separate Docker images for CE and EE:

| Tag | Description |
|-----|-------------|
| `s4core/s4core:latest` | Community Edition (default) |
| `s4core/s4core:v0.X.Y` | CE with version |
| `s4core/s4core:ce` | Explicit CE alias |
| `s4core/s4core:ce-v0.X.Y` | CE with version (explicit) |
| `s4core/s4core:ee` | Enterprise Edition (latest) |
| `s4core/s4core:ee-v0.X.Y` | EE with version |

CE and EE are always released with the same version number.

```bash
# Run EE with a license key
docker run -d \
  --name s4core \
  -p 9000:9000 \
  -v s4-data:/data \
  -e S4_BIND=0.0.0.0:9000 \
  -e S4_LICENSE_KEY=your-license-key-here \
  s4core/s4core:ee

# Run EE with a license file
docker run -d \
  --name s4core \
  -p 9000:9000 \
  -v s4-data:/data \
  -v /path/to/license.key:/etc/s4/license.key:ro \
  -e S4_BIND=0.0.0.0:9000 \
  -e S4_LICENSE_FILE=/etc/s4/license.key \
  s4core/s4core:ee

# Build locally
docker build .                              # CE
docker build --build-arg EDITION=ee .       # EE
```


## Activating a License

License keys are issued by the S4Core sales team — contact
[support@s4core.com](mailto:support@s4core.com). No license ships with the
repository or the Docker images.

Set one of the following environment variables before starting S4Core:

| Variable | Description |
|----------|-------------|
| `S4_LICENSE_KEY` | The license key as a string |
| `S4_LICENSE_FILE` | Path to a file containing the license key |

No internet connection or license server is required — verification is fully
offline.

## Local Dev Clusters

Two Enterprise dev compose files start six nodes behind HAProxy round-robin
(S3 on port `9000`, HAProxy stats on `8404`):

| File | Pool |
|------|------|
| `ee/docker-compose-dev.yml` | **With erasure coding**: one `ec-rs-small` RS(4,2) pool (`S4_POOL_TYPE=ec`), S3 auth disabled for plain `curl` smoke checks |
| `ee/docker-compose-dev-no-ec.yml` | **Without erasure coding**: one replicated pool, RF=3 / W=2 / R=2 (`S4_POOL_TYPE=standard`), S3 auth on |

Both read the license from `ee/license-key.txt`. That file is not in the
repository: it comes from the S4Core sales team — contact
[support@s4core.com](mailto:support@s4core.com). Without it compose refuses to
start the nodes.

Both use the `s4core/s4core:ee` image by default. The EC cluster needs an image
with erasure coding: until a release that ships it is published, build one from
the repository and pass it in `S4_EE_IMAGE`; an older EE image ignores the
`S4_EC_*` variables and starts a replicated pool.

```bash
docker build --build-arg EDITION=ee -t s4core-ee:local .
S4_EE_IMAGE=s4core-ee:local docker compose -f ee/docker-compose-dev.yml up -d

curl http://127.0.0.1:9000/health
curl http://127.0.0.1:8404/

# Reset the dev cluster and its named volumes
docker compose -f ee/docker-compose-dev.yml down -v
```

Useful overrides:

```bash
S4_EE_IMAGE=s4core/s4core:ee \
S4_LICENSE_HOST_FILE=/absolute/path/to/license.key \
S4_EE_HTTP_PORT=9000 \
docker compose -f ee/docker-compose-dev.yml up -d
```

## Behavior

| Scenario | Result |
|----------|--------|
| EE binary + valid license | Full Enterprise features, limits per license |
| EE binary + no license | Falls back to CE mode (1 pool, 3 nodes) with a warning |
| EE binary + invalid license | Falls back to CE mode with a warning |
| EE binary + expired license | Existing topology preserved; new EE operations blocked |
| CE binary | Always CE mode, license keys are ignored |

When a license expires, S4Core **does not** shut down or lose data. The existing
cluster topology is preserved. New Enterprise operations (adding pools, adding
nodes beyond CE limits) are blocked until the license is renewed.

## S4 Federation Erasure Coding

Erasure Coding (EC) is Enterprise-only. It requires:

- an EE binary built with `--features enterprise`
- a valid license with the `erasure_coding` capability
- `S4_MODE=cluster`
- `S4_POOL_TYPE=ec`
- a fixed-width pool whose node count exactly matches the selected EC profile

Current v1 status: EC pool validation, fixed-width erasure-set metadata,
runtime limits, staged transcode, EC cutover reads, repair, anti-entropy, scrub,
tombstone/orphan GC, hot-copy purge gates, metrics, admin diagnostics, and the
normal S3 hot RF=3 object path are wired in the Enterprise runtime.
PUT/COPY/POST/multipart completion record EC layout or transcode metadata after
the hot quorum commit. EC admin health reports `runtime_ready=true` when the
local runtime workers and data paths are active. It reports
`production_ready=true` only for EE builds marked as accepted for production
with `S4_EC_V1_CI_ACCEPTANCE_PASSED=true`.

### Minimal EC Pool Configuration

```bash
S4_MODE=cluster
S4_POOL_TYPE=ec
S4_EC_PROFILE=ec-rs-standard
S4_EC_TOPOLOGY_POLICY=strict
```

`ec-rs-standard` is the default profile and requires exactly 9 nodes in
`S4_POOL_NODES`.

### Built-In Profiles

| Profile | Layout | Required pool nodes | Storage overhead |
|---------|--------|---------------------|------------------|
| `ec-rs-small` | RS(4,2) | 6 | 1.50x |
| `ec-rs-standard` | RS(6,3) | 9 | 1.50x |
| `ec-rs-dense` | RS(8,3) | 11 | 1.375x |
| `ec-lrc-small` | LRC(4,2,1) | 7 | 1.75x |
| `ec-lrc-standard` | LRC(6,2,2) | 10 | 1.67x |
| `ec-lrc-dense` | LRC(10,2,2) | 14 | 1.40x |

For custom profiles:

```bash
S4_EC_PROFILE=custom
S4_EC_CODEC=rs
S4_EC_K=6
S4_EC_RS_M=3
S4_EC_MIN_OBJECT_SIZE=1048576
S4_EC_CHUNK_SIZE=6291456
```

For custom LRC, use `S4_EC_CODEC=lrc`, `S4_EC_LRC_L`, and `S4_EC_LRC_G`.
LRC v1 requires `S4_EC_K` to be divisible by `S4_EC_LRC_L`.

### EC Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `S4_POOL_TYPE` | `standard` | Set to `ec` to activate an Enterprise erasure-coded pool |
| `S4_EC_PROFILE` | `ec-rs-standard` | Built-in profile or `custom` |
| `S4_EC_CODEC` | `rs` | Custom profile codec: `rs` or `lrc` |
| `S4_EC_K` | Required for `custom` | Data shard count |
| `S4_EC_RS_M` | Required for custom RS | Reed-Solomon parity shard count |
| `S4_EC_LRC_L` | Required for custom LRC | LRC local parity group count |
| `S4_EC_LRC_G` | Required for custom LRC | LRC global parity shard count |
| `S4_EC_MIN_OBJECT_SIZE` | `1048576` | Objects below this size stay explicitly replicated inside the EC pool |
| `S4_EC_CHUNK_SIZE` | `6291456` | Logical chunk size before shard encoding |
| `S4_EC_TOPOLOGY_POLICY` | `strict` | `strict` fails on partial labels; `warn` starts degraded |
| `S4_EC_NODE_TOPOLOGY` | unset | Optional failure-domain labels by `S4_POOL_NODES` name |
| `S4_EC_TRANSCODE_ENABLED` | `true` | Allows staged transcode logic where runtime wiring is present |
| `S4_EC_INLINE_WRITE_ENABLED` | `false` | Future opt-in inline EC writes; staged hot RF=3 writes remain the v1 default |
| `S4_EC_MAX_CONCURRENT_TRANSCODES` | `2` | Max staged transcodes per node |
| `S4_EC_MAX_CONCURRENT_INLINE_WRITES` | `0` | Max inline EC writes per node |
| `S4_EC_TRANSCODE_LEASE_SECS` | `300` | Active transcode lease TTL |
| `S4_EC_FULL_VERIFY_MAX_BYTES` | `1073741824` | Full reconstruction verification limit |
| `S4_EC_RECONSTRUCTION_PROBE_CHUNKS` | `3` | Deterministic probes for larger objects |
| `S4_EC_HOT_REPLICA_GC_GRACE_SECS` | `86400` | Grace before hot RF replicas may be purged |
| `S4_EC_HOT_PURGE_MODE` | `auto` | `auto` or `manual` hot-replica purge mode |
| `S4_EC_TRANSCODE_BANDWIDTH_LIMIT` | unset | Optional transcode bandwidth cap in bytes/sec |
| `S4_EC_MAX_RETRY_ATTEMPTS` | `10` | Retry budget for staged transcode |
| `S4_EC_INITIAL_BACKOFF_SECS` | `30` | Initial staged transcode retry backoff |
| `S4_EC_MAX_BACKOFF_SECS` | `3600` | Maximum staged transcode retry backoff |
| `S4_EC_CPU_WORKERS` | derived from CPU budget | Hard cap for concurrent codec jobs |
| `S4_EC_CPU_BUDGET_PERCENT` | `60` | Target CPU budget for codec work |
| `S4_EC_MAX_INFLIGHT_ENCODE_BYTES` | `268435456` | Max logical input bytes being encoded |
| `S4_EC_REPAIR_MAX_GLOBAL_CONCURRENCY` | `4` | Max repairs running on this node |
| `S4_EC_REPAIR_MAX_NODE_CONCURRENCY` | `1` | Max repairs targeting one shard owner |
| `S4_EC_REPAIR_MAX_SET_CONCURRENCY` | `2` | Max repairs in one erasure set |
| `S4_EC_REPAIR_MAX_FAILURE_DOMAIN_CONCURRENCY` | `1` | Max repairs per failure domain |
| `S4_EC_REPAIR_MAX_QUEUE_DEPTH` | `10000` | Durable repair queue depth |
| `S4_EC_REPAIR_GLOBAL_BANDWIDTH_BYTES_PER_SEC` | unset | Optional global repair bandwidth cap |
| `S4_EC_REPAIR_FAILURE_DOMAIN_BANDWIDTH_BYTES_PER_SEC` | unset | Optional per-failure-domain repair bandwidth cap |
| `S4_EC_REPAIR_THROTTLE_RETRY_SECS` | `5` | Retry delay after repair throttling |
| `S4_EC_REPAIR_INITIAL_BACKOFF_SECS` | `30` | Initial repair retry backoff |
| `S4_EC_REPAIR_MAX_BACKOFF_SECS` | `3600` | Maximum repair retry backoff |
| `S4_MAX_REJOIN_DOWNTIME_DAYS` | `3` | Downtime after which a returning node must rebuild before it serves EC reads |
| `S4_EC_LONG_OFFLINE_REJOIN_POLICY` | `needs_bootstrap` | What a node past that limit may do: `needs_bootstrap` or `require_admin_approval` |
| `S4_EC_REJOIN_GATE_ENABLED` | `true` | Whether a held-back node actually stops serving EC reads, or only reports that it should |
| `S4_EC_REPAIR_SCAN_LIMIT` | `1024` | Max stale running repair tasks recovered in one worker pass |
| `S4_EC_REPAIR_PRIORITY_AGE_BOOST_SECS` | `600` | How long a ready repair task waits before it starts taking turns of its own, ahead of more damaged chunks; `0` leaves the order to damage alone |
| `S4_EC_REPAIR_PRIORITY_AGE_BOOST_EVERY` | `8` | One turn in this many goes to the erasure set's longest-waiting ready task |
| `S4_EC_REPAIR_RUNNING_TIMEOUT_SECS` | `900` | Recover stale running repair tasks after this timeout |
| `S4_EC_REPAIR_WORKER_ENABLED` | `true` | Start the supervised durable repair worker |
| `S4_EC_REPAIR_WORKER_POLL_MS` | `1000` | Repair worker poll interval |
| `S4_EC_REPAIR_WORKER_BATCH_SIZE` | `64` | Max repair tasks attempted per worker poll |
| `S4_EC_AE_WORKER_ENABLED` | `true` | Start scheduled EC manifest inventory audits |
| `S4_EC_AE_WORKER_INTERVAL_SECS` | `60` | EC anti-entropy worker interval |
| `S4_EC_SCRUB_WORKER_ENABLED` | `true` | Start scheduled local shard scrub passes |
| `S4_EC_SCRUB_WORKER_INTERVAL_SECS` | `60` | EC scrub worker interval |
| `S4_EC_AE_MERKLE_DEPTH` | `15` | EC manifest Merkle depth |
| `S4_EC_AE_MAX_MANIFEST_SCAN_ENTRIES` | `10000` | Max manifests scanned per anti-entropy pass |
| `S4_EC_AE_MAX_SHARD_SCAN_ENTRIES` | `10000` | Max shard records scanned per scrub pass |
| `S4_EC_SCRUB_RECONSTRUCTION_SAMPLE_CHUNKS` | `3` | Chunk samples for reconstruction scrub |
| `S4_EC_GC_GRACE_SECS` | `S4_GC_GRACE_DAYS * 86400` | EC tombstone/shard GC grace |
| `S4_EC_ORPHAN_LEASE_GRACE_SECS` | `3600` | Extra safety grace after transcode lease expiry |
| `S4_EC_ORPHAN_MIN_AGE_SECS` | `86400` | Minimum orphan shard age before physical GC |
| `S4_EC_GC_SCAN_LIMIT` | `1024` | GC records scanned per page |
| `S4_EC_GC_MAX_SHARDS_PER_OBJECT` | `100000` | Max local shards removed for one tombstone cycle |
| `S4_EC_TIERING_ENABLED` | `false` | Optional auto-tiering gate, disabled by default |
| `S4_EC_TIERING_COLD_AFTER_SECS` | `2592000` | Cold-object age for future auto-tiering |
| `S4_EC_TIERING_SCAN_LIMIT` | `4096` | Future tiering scan limit |
| `S4_EC_ADMIN_SCAN_LIMIT` | `4096` | Max EC metadata records scanned by one admin request |
| `S4_EC_V1_CI_ACCEPTANCE_PASSED` | unset | Build-time flag that marks an EE build as accepted for production. |

Topology labels use node names from `S4_POOL_NODES`:

```bash
S4_EC_NODE_TOPOLOGY='node-1:zone=z1,rack=r1,host=h1,disk_group=d1;node-2:zone=z2,rack=r2,host=h2,disk_group=d2'
```

Full architecture notes, run examples, admin endpoints, and current release
gates are documented in
[docs/04-features/erasure-coding.md](../docs/04-features/erasure-coding.md).

## Obtaining a License

Contact the S4Core team to obtain a license key for your deployment.

## Directory Structure

```
ee/
  s4-ee-license/    # License validation library
  s4-ee/            # Enterprise feature implementations
  LICENSE           # Elastic License 2.0
```

## License

All code in this directory is licensed under the
[Elastic License 2.0](LICENSE). See the license file for the full terms.

Code outside this directory is licensed under
[Apache License 2.0](../LICENSE).
