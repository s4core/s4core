# Production Recommendations

This page covers best practices for running S4 in production.

## Security

### Enable IAM

Always enable IAM in production:

```bash
export S4_ROOT_PASSWORD=<strong-random-password>
export S4_JWT_SECRET=<256-bit-crypto-random-string>
```

**Important:** Do not use the default password. Generate a strong random password and JWT secret.

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

For best performance, place the metadata database on NVMe storage. The redb database handles frequent small writes and benefits from low-latency storage.

### Disk Space Monitoring

Monitor disk usage and set alerts before running out of space. S4 needs space for:

- Volume files (object data)
- redb database (metadata)
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

Use the stats endpoint for health checks:

```bash
curl -f http://localhost:9000/api/stats
```

Returns `200 OK` when the server is running.

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

The redb database file can be backed up independently for faster metadata recovery.

## High Availability

S4 is a single-node system. For high availability:

- Use filesystem-level replication (DRBD, ZFS send/receive)
- Set up Active-Passive failover using a shared volume
- Use cloud provider features (EBS snapshots, Azure Managed Disks)

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
