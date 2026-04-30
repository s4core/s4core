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

Set one of the following environment variables before starting S4Core:

| Variable | Description |
|----------|-------------|
| `S4_LICENSE_KEY` | The license key as a string |
| `S4_LICENSE_FILE` | Path to a file containing the license key |

No internet connection or license server is required — verification is fully
offline.

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
