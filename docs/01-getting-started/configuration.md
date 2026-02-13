# Configuration

S4 is configured through environment variables. All settings have sensible defaults for development.

## Environment Variables

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `S4_BIND` | Bind host address | `127.0.0.1:9000` | `0.0.0.0:9000`
| `S4_DATA_DIR` | Base directory for storage data | System temp dir | `/data` |
| `S4_ACCESS_KEY_ID` | Access key for S3 authentication | Auto-generated dev key | `my-secret-key-id` |
| `S4_SECRET_ACCESS_KEY` | Secret key for S3 authentication | Auto-generated dev key | `my-access-key` |
| `S4_ROOT_USERNAME` | Root admin username | `root` | `user12345` |
| `S4_ROOT_PASSWORD` | Root admin password (enables IAM) | None (IAM disabled) | `password12345` |
| `S4_JWT_SECRET` | Secret key for signing JWT tokens | Auto-generated at startup | `AHusUYfsuYA18sgAS12937ACSgj1g2kjha` |
| `S4_MAX_UPLOAD_SIZE` | Maximum upload size per request | `5GB` | `4MB` |
| `S4_TLS_CERT` | Path to TLS certificate (PEM) | None (HTTP mode) | `/certs/key.pem` |
| `S4_TLS_KEY` | Path to TLS private key (PEM) | None (HTTP mode) | `/certs/cert.pem` |
| `S4_LIFECYCLE_ENABLED` | Enable lifecycle policy worker | `true` | `false` |
| `S4_LIFECYCLE_INTERVAL_HOURS` | Lifecycle evaluation interval | `24` | `48` |
| `S4_LIFECYCLE_DRY_RUN` | Dry-run mode (log without deleting) | `false` | `true` |
| `S4_METRICS_ENABLED` | Enable Prometheus metrics | `true` | `false` |

### Size Format

The `S4_MAX_UPLOAD_SIZE` variable supports human-readable size suffixes:

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
export S4_JWT_SECRET=your-256-bit-crypto-random-string
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
metadata_path = "/var/lib/s4/metadata.redb"

[tuning]
inline_threshold = 4096  # 4KB — objects below this are stored inline in the database
volume_size_mb = 1024    # 1GB — maximum size of each append-only volume file
strict_sync = true       # fsync after every write
```
