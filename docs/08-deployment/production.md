# Production Recommendations

This page covers best practices for running S4 in production.

## Security

### Enable IAM

Always enable IAM in production:

```bash
openssl rand -hex 32 > /etc/s4/jwt_secret   # once, then keep the file
export S4_ROOT_PASSWORD=<strong-random-password>
export S4_JWT_SECRET_FILE=/etc/s4/jwt_secret
```

**Important:** Do not use the default password. Generate a strong random password and JWT secret.

`S4_JWT_SECRET` signs the admin tokens, and the admin API trusts the role inside a validly signed
token, so treat it like the root password. The server refuses to start when it is shorter than
32 bytes, and in cluster and gateway mode when IAM is in use and it is not set; there it must be
the same on every node. On a single node an unset secret is replaced by a random one at every
start, which is safe but ends admin sessions on restart. Details:
[Admin JWT Secret](../01-getting-started/configuration.md#admin-jwt-secret).

### Keep Credentials out of the Environment

Every credential variable also has a `_FILE` form (`S4_ROOT_PASSWORD_FILE=/run/secrets/s4_pass`)
that reads the value from a file, for Docker and Kubernetes secrets. See
[Secrets from Files](../01-getting-started/configuration.md#secrets-from-files-_file) and the
[Docker example](docker.md#credentials-from-docker-secrets).

### Enable TLS

All production deployments should use TLS:

```bash
export S4_TLS_CERT=/etc/ssl/certs/s4.pem
export S4_TLS_KEY=/etc/ssl/private/s4-key.pem
```

Use certificates from a trusted CA (e.g., Let's Encrypt) or your organization's PKI.

### Bind Address

Bind to `0.0.0.0` to accept connections from all interfaces, or restrict to a specific interface:

```bash
export S4_BIND=0.0.0.0  # All interfaces
```

Use a firewall or reverse proxy to control access.

## Storage

### Data Directory

Choose a dedicated partition or volume for S4 data:

```bash
export S4_DATA_DIR=/var/lib/s4
```

### Filesystem

- **ext4** or **XFS** are recommended
- Ensure the filesystem has enough inodes (though S4 uses very few)
- Use a filesystem with journaling for metadata integrity

### NVMe for Metadata

For best performance, place the metadata database on NVMe storage. The fjall database handles frequent small writes and benefits from low-latency storage.

### Disk Space Monitoring

Monitor disk usage and set alerts before running out of space. S4 needs space for:

- Volume files (object data)
- fjall database directory (metadata)
- Temporary files (multipart uploads in progress)

## Performance

### Upload Size

Set the maximum upload size based on your workload:

```bash
export S4_MAX_UPLOAD_SIZE=10GB
```

### Lifecycle Worker

In production, keep the lifecycle worker enabled with a reasonable interval:

```bash
export S4_LIFECYCLE_ENABLED=true
export S4_LIFECYCLE_INTERVAL_HOURS=24
```

## Monitoring

### Prometheus

Enable Prometheus metrics and configure scraping:

```bash
export S4_METRICS_ENABLED=true
```

Set up alerts for:

- High error rates (5xx responses)
- Disk usage approaching capacity
- Request latency spikes
- Server uptime resets (unexpected restarts)

### Health Checks

Use separate endpoints for process liveness and readiness to receive client traffic:

```bash
curl -f http://localhost:9000/health               # Liveness
curl -f http://localhost:9000/minio/health/ready   # Readiness
```

`/health` and `/minio/health/live` return `200 OK` once the HTTP server is running.
Use these endpoints for liveness; waiting for peers or metadata recovery must not
cause a healthy process to be restarted.

`/minio/health/ready` returns `503 Service Unavailable` with `Retry-After: 1` while
a cluster node has required local recovery to finish: bucket configuration adoption,
single-to-cluster migration, or IAM adoption or freshness confirmation. It returns
`200 OK` when those local conditions allow client traffic. The `X-S4-Readiness` header
on a refusal gives a fixed reason code; the protected Admin API and server log give
details. Recovery does not stop inter-node RPC, gossip or the diagnostic API.

Configure your load balancer to send client traffic only to ready nodes, including
at startup before the first check. The supplied HAProxy configuration starts backends
unavailable until their readiness checks succeed. Readiness
is a per-node check, separate from cluster read/write quorum availability; a background
repair or pool growth does not by itself make a node unready. If adoption requires an
operator's decision, connect directly to the node's Admin API even while the client
load balancer excludes it. Do not use readiness failure as a reason to restart it.

For Kubernetes, use separate probes:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 9000
  periodSeconds: 10
  timeoutSeconds: 2
readinessProbe:
  httpGet:
    path: /minio/health/ready
    port: 9000
  periodSeconds: 5
  timeoutSeconds: 2
```

Do not probe `/api/stats`: it counts every object, and on a cluster node every object of the pool.

## Backup

### Data Backup

Back up the entire `S4_DATA_DIR` directory:

```bash
# Stop the server for a consistent backup
systemctl stop s4-server
tar -czf s4-backup-$(date +%Y%m%d).tar.gz /var/lib/s4
systemctl start s4-server
```

For zero-downtime backups, use filesystem-level snapshots (LVM, ZFS, or cloud volume snapshots).

### Metadata Backup

The fjall database directory can be backed up independently for faster metadata recovery.

## High Availability

S4 supports two deployment models for high availability:

### Federation (Recommended)

S4 has built-in **leaderless quorum replication** (`S4_MODE=cluster`). A 3-node cluster with default quorum (N=3, W=2, R=2) tolerates 1 node failure with zero downtime. See [Cluster Deployment](clustering.md) for setup instructions and [Federation](../04-features/federation.md) for architecture details.

### Single-Node HA (Alternative)

For single-node deployments (`S4_MODE=single`, the default), you can use infrastructure-level replication:

- Filesystem-level replication (DRBD, ZFS send/receive)
- Active-Passive failover using a shared volume
- Cloud provider features (EBS snapshots, Azure Managed Disks)

## Reverse Proxy

For load balancing, TLS termination, or rate limiting, place S4 behind a reverse proxy:

### Nginx Example

```nginx
upstream s4 {
    server 127.0.0.1:9000;
}

server {
    listen 443 ssl;
    server_name s4.example.com;

    ssl_certificate /etc/ssl/certs/s4.pem;
    ssl_certificate_key /etc/ssl/private/s4-key.pem;

    client_max_body_size 10G;

    location / {
        proxy_pass http://s4;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Note: Set `client_max_body_size` to match or exceed `S4_MAX_UPLOAD_SIZE`.
