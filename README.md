# S4 - Modern S3-Compatible Object Storage

S4 is a high-performance, S3-compatible object storage server written in Rust. It solves the inode exhaustion problem common with traditional file-based storage systems and provides advanced features like atomic directory operations and content-addressable deduplication.

## Features

- **S3 API Compatible**: Full compatibility with AWS S3 API (AWS CLI, boto3, etc.)
- **Inode Problem Solved**: Append-only log storage eliminates inode exhaustion
- **Content Deduplication**: Automatic deduplication saves 30-50% storage space
- **Object Versioning**: S3-compatible versioning with delete markers
- **Lifecycle Policies**: Automatic object expiration and cleanup of old versions
- **Atomic Operations**: Rename directories with millions of files in milliseconds
- **Strict Consistency**: Data is guaranteed to be written before returning success
- **IAM & Admin API**: Role-based access control (Reader, Writer, SuperUser) with JWT authentication
- **High Performance**: Optimized for single-node performance

## Architecture

S4 uses a hybrid storage approach:

- **Tiny Objects (< 4KB)**: Stored inline in the metadata database
- **All Other Objects (> 4KB)**: Stored in append-only volume files (~1GB each)

This approach ensures:
- Minimal inode usage (1 billion objects = ~1000 files)
- Maximum write performance (sequential writes)
- Fast recovery (metadata in ACID database)

## Quick Start

### Prerequisites

- Rust 1.70 or later
- Linux (recommended) or macOS

### Building from Source

```bash
# Clone the repository
git clone https://github.com/org/s4.git
cd s4

# Build the project
cargo build --release

# Run the server
./target/release/s4-server
```

### Docker

S4 provides official Docker images for easy deployment.

#### Using `docker run`

```bash
# Run S4 server (basic)
docker run -d \
  --name s4-server \
  -p 9000:9000 \
  -v s4-data:/data \
  s4-server:latest

# Run with custom credentials
docker run -d \
  --name s4-server \
  -p 9000:9000 \
  -v s4-data:/data \
  -e S4_ACCESS_KEY_ID=myaccesskey \
  -e S4_SECRET_ACCESS_KEY=mysecretkey \
  s4-server:latest

# Run with IAM enabled
docker run -d \
  --name s4-server \
  -p 9000:9000 \
  -v s4-data:/data \
  -e S4_ROOT_PASSWORD=password12345 \
  s4-server:latest

# Build the image locally
docker build -t s4-server .
```

#### Using Docker Compose

The project includes a `docker-compose.yml` that runs S4 server together with the web admin console.

```bash
# Run full stack (server + web console)
docker compose up --build

# Run in background
docker compose up -d --build

# Run only the server
docker compose up s4-server --build

# With custom environment variables
S4_ROOT_PASSWORD=password12345 docker compose up --build
```

After startup:
- **S4 API**: http://localhost:9000
- **Web Console**: http://localhost:3000 (login with root credentials)

**docker-compose.yml overview:**

```yaml
services:
  s4-server:
    build: .
    ports:
      - "9000:9000"
    volumes:
      - s4-data:/data
    environment:
      - S4_ROOT_PASSWORD=${S4_ROOT_PASSWORD:-}
      - S4_ACCESS_KEY_ID=${S4_ACCESS_KEY_ID:-}
      - S4_SECRET_ACCESS_KEY=${S4_SECRET_ACCESS_KEY:-}

  s4-console:
    build: ./frontend/s4-console
    ports:
      - "3000:3000"
    environment:
      - S4_BACKEND_URL=http://s4-server:9000
    depends_on:
      - s4-server
```

For web console-only development, see [frontend/README.md](frontend/README.md).

### Environment Variables

S4 is configured through environment variables:

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `S4_BIND` | S4 BIND host | `127.0.0.1` | `0.0.0.0` |
| `S4_ROOT_USERNAME` | Root admin username | `root` | `admin` |
| `S4_ROOT_PASSWORD` | Root admin password (enables IAM) | None (IAM disabled) | `password12345` |
| `S4_JWT_SECRET` | Secret key for signing JWT tokens | Auto-generated at startup (dev mode only) | `256-bit-crypto-random-string-like-this-1234567890ABCDEF` |
| `S4_ACCESS_KEY_ID` | Access key for S3 authentication | Auto-generated dev key | `myaccesskey` |
| `S4_SECRET_ACCESS_KEY` | Secret key for S3 authentication | Auto-generated dev key | `mysecretkey` |
| `S4_DATA_DIR` | Base directory for storage | System temp dir | `/var/lib/s4` |
| `S4_MAX_UPLOAD_SIZE` | Maximum upload size per request | `5GB` | `10GB`, `100MB`, `1024KB` |
| `S4_TLS_CERT` | Path to TLS certificate (PEM format) | None (HTTP mode) | `/etc/ssl/certs/s4.pem` |
| `S4_TLS_KEY` | Path to TLS private key (PEM format) | None (HTTP mode) | `/etc/ssl/private/s4-key.pem` |
| `S4_LIFECYCLE_ENABLED` | Enable lifecycle policy worker | `true` | `true`, `false`, `1`, `0` |
| `S4_LIFECYCLE_INTERVAL_HOURS` | Lifecycle evaluation interval (hours) | `24` | `1`, `6`, `24`, `168` |
| `S4_LIFECYCLE_DRY_RUN` | Dry-run mode (log without deleting) | `false` | `true`, `false`, `1`, `0` |
| `S4_METRICS_ENABLED` | Prometheus metrics | true) | false |

**Size format**: Supports `GB`/`G`, `MB`/`M`, `KB`/`K`, or bytes (no suffix).

Example (HTTP):
```bash
export S4_ACCESS_KEY_ID=myaccesskey
export S4_SECRET_ACCESS_KEY=mysecretkey
export S4_DATA_DIR=/var/lib/s4
export S4_MAX_UPLOAD_SIZE=10GB

./target/release/s4-server
```

### Using with AWS CLI

Configure AWS CLI to use S4:

```bash
aws configure set aws_access_key_id myaccesskey
aws configure set aws_secret_access_key mysecretkey
```

Basic operations:

```bash
# Create a bucket
aws --endpoint-url http://localhost:9000 s3 mb s3://mybucket

# Upload a file
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://mybucket/file.txt

# List objects
aws --endpoint-url http://localhost:9000 s3 ls s3://mybucket

# Download a file
aws --endpoint-url http://localhost:9000 s3 cp s3://mybucket/file.txt downloaded.txt

# Delete a file
aws --endpoint-url http://localhost:9000 s3 rm s3://mybucket/file.txt

# Delete a bucket
aws --endpoint-url http://localhost:9000 s3 rb s3://mybucket
```

### Versioning

S4 supports S3-compatible object versioning to preserve, retrieve, and restore every version of every object.

```bash
# Enable versioning on bucket
aws s3api put-bucket-versioning \
  --bucket mybucket \
  --versioning-configuration Status=Enabled \
  --endpoint-url https://127.0.0.1:9000 \
  --no-verify-ssl

# Upload file (version 1)
echo "version 1" | aws s3api put-object \
  --bucket mybucket \
  --key file.txt \
  --body - \
  --endpoint-url https://127.0.0.1:9000 \
  --no-verify-ssl

# Upload again (version 2)
echo "version 2" | aws s3api put-object \
  --bucket mybucket \
  --key file.txt \
  --body - \
  --endpoint-url https://127.0.0.1:9000 \
  --no-verify-ssl

# List all versions
aws s3api list-object-versions \
  --bucket mybucket \
  --prefix file.txt \
  --endpoint-url https://127.0.0.1:9000 \
  --no-verify-ssl

# Get specific version
aws s3api get-object \
  --bucket mybucket \
  --key file.txt \
  --version-id "ff495d34-c292-4af4-9d10-e186272010ed" \
  first_version.txt \
  --endpoint-url https://127.0.0.1:9000 \
  --no-verify-ssl

# Delete object (creates delete marker)
aws s3api delete-object \
  --bucket mybucket \
  --key file.txt \
  --endpoint-url https://127.0.0.1:9000 \
  --no-verify-ssl
```

### Lifecycle Policies

S4 supports automatic object expiration and cleanup based on lifecycle rules.

```bash
# Create lifecycle configuration file
cat > lifecycle.json <<'EOF'
{
  "Rules": [
    {
      "ID": "expire-logs",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "logs/"
      },
      "Expiration": {
        "Days": 30
      }
    },
    {
      "ID": "cleanup-old-versions",
      "Status": "Enabled",
      "Filter": {
        "Prefix": ""
      },
      "NoncurrentVersionExpiration": {
        "NoncurrentDays": 90
      }
    }
  ]
}
EOF

# Set lifecycle configuration
aws s3api put-bucket-lifecycle-configuration \
  --bucket mybucket \
  --lifecycle-configuration file://lifecycle.json \
  --endpoint-url https://127.0.0.1:9000 \
  --no-verify-ssl

# Get lifecycle configuration
aws s3api get-bucket-lifecycle-configuration \
  --bucket mybucket \
  --endpoint-url https://127.0.0.1:9000 \
  --no-verify-ssl

# Delete lifecycle configuration
aws s3api delete-bucket-lifecycle \
  --bucket mybucket \
  --endpoint-url https://127.0.0.1:9000 \
  --no-verify-ssl
```

**Lifecycle worker configuration:**

```bash
# Enable/disable lifecycle worker (default: enabled)
export S4_LIFECYCLE_ENABLED=true

# Set evaluation interval in hours (default: 24)
export S4_LIFECYCLE_INTERVAL_HOURS=24

# Enable dry-run mode to test without deleting (default: false)
export S4_LIFECYCLE_DRY_RUN=true
```

### IAM & Admin API

S4 includes a built-in IAM system with role-based access control. IAM is enabled when `S4_ROOT_PASSWORD` is set.

**Roles:**
- **Reader** -- can list buckets/objects and download objects
- **Writer** -- Reader permissions plus create/delete buckets and objects
- **SuperUser** -- full admin access including user management

**Starting with IAM enabled:**

```bash
export S4_ROOT_PASSWORD=password12345
./target/release/s4-server
```

**Admin API usage (curl):**

```bash
# Login (get JWT token)
TOKEN=$(curl -s -k -X POST https://localhost:9000/api/admin/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"root","password":"password12345"}' | jq -r '.token')

# List users
curl -s -k https://localhost:9000/api/admin/users \
  -H "Authorization: Bearer $TOKEN"

# Create a user
curl -s -k -X POST https://localhost:9000/api/admin/users \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"alice123","role":"Writer"}'

# Generate S3 credentials for a user
curl -s -k -X POST https://localhost:9000/api/admin/users/<user-id>/credentials \
  -H "Authorization: Bearer $TOKEN"

# Update user (change role, password, or active status)
curl -s -k -X PUT https://localhost:9000/api/admin/users/<user-id> \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"role":"Reader"}'

# Delete S3 credentials
curl -s -k -X DELETE https://localhost:9000/api/admin/users/<user-id>/credentials \
  -H "Authorization: Bearer $TOKEN"

# Delete user
curl -s -k -X DELETE https://localhost:9000/api/admin/users/<user-id> \
  -H "Authorization: Bearer $TOKEN"
```

**Using S3 with IAM credentials:**

After generating S3 credentials via the Admin API, use them with AWS CLI:

```bash
aws configure set aws_access_key_id S4AK_xxxxxxxx
aws configure set aws_secret_access_key xxxxxxxx
aws --endpoint-url https://localhost:9000 --no-verify-ssl s3 ls
```

Legacy `S4_ACCESS_KEY_ID` / `S4_SECRET_ACCESS_KEY` environment credentials continue to work as a fallback with full (SuperUser) access.

### CORS Configuration

S4 supports S3-compatible CORS (Cross-Origin Resource Sharing) for browser-based access.

```bash
# Set CORS configuration
curl -X PUT "http://localhost:9000/mybucket?cors" \
  -H "Content-Type: application/xml" \
  -d '<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
  <CORSRule>
    <AllowedOrigin>https://example.com</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedHeader>*</AllowedHeader>
    <MaxAgeSeconds>3600</MaxAgeSeconds>
  </CORSRule>
</CORSConfiguration>'

# Get CORS configuration
curl "http://localhost:9000/mybucket?cors"

# Delete CORS configuration
curl -X DELETE "http://localhost:9000/mybucket?cors"
```

### TLS/HTTPS Configuration

S4 supports TLS for encrypted connections. TLS is disabled by default and enabled automatically when both certificate and key paths are provided.

**Generating self-signed certificates (for development):**

```bash
# Generate self-signed certificate and key
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes \
  -subj "/CN=localhost"
```

**Running with TLS:**

```bash
export S4_TLS_CERT=/path/to/cert.pem
export S4_TLS_KEY=/path/to/key.pem
./target/release/s4-server
```

**Using with AWS CLI (HTTPS):**

```bash
# For self-signed certificates, use --no-verify-ssl
aws --endpoint-url https://localhost:9000 --no-verify-ssl s3 ls

# For production with valid certificates
aws --endpoint-url https://s4.example.com:9000 s3 ls
```

**Certificate requirements:**
- PEM-encoded X.509 certificate
- PEM-encoded private key (RSA, ECDSA, or Ed25519)
- Certificate chain is supported (include intermediate certs in cert.pem)

### Configuration File (Optional)

You can also use a `config.toml` file:

```toml
[server]
bind = "0.0.0.0:9000"

[storage]
data_path = "/var/lib/s4/volumes"
metadata_path = "/var/lib/s4/metadata.redb"

[tuning]
inline_threshold = 4096  # 4KB
volume_size_mb = 1024    # 1GB
strict_sync = true
```

## Documentation

- [Architecture Guide](ARCHITECTURE.md) - Detailed architecture documentation
- [Contributing Guide](CONTRIBUTING.md) - How to contribute to S4
- [API Documentation](docs/api/) - API reference documentation

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Status

🚧 **Early Development** - S4 is currently in active development. Not ready for production use.
