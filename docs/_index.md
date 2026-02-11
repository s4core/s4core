# S4 Documentation

Welcome to the S4 documentation. S4 is a high-performance, S3-compatible object storage server written in Rust.

S4 solves the inode exhaustion problem common with traditional file-based storage systems by using append-only log volumes. It provides content-addressable deduplication, object versioning, lifecycle policies, Object Lock (WORM), and a built-in IAM system with a web admin console.

## Key Highlights

- **S3 API Compatible** — works with AWS CLI, boto3, rclone, restic, and other S3 tools
- **Inode Problem Solved** — 1 billion objects use only ~1000 files on disk
- **30-50% Storage Savings** — automatic content deduplication via SHA-256
- **Strict Consistency** — data is fsynced before returning success
- **Built-in Security** — IAM with role-based access control and JWT authentication
- **Web Admin Console** — manage buckets, users, and keys from your browser

## Quick Links

- [Getting Started](01-getting-started/) — install, configure, and run S4
- [Architecture](02-architecture/) — understand how S4 works under the hood
- [S3 API Reference](03-s3-api/) — bucket and object operations
- [Features](04-features/) — versioning, lifecycle, Object Lock, deduplication, CORS, TLS
- [IAM & Admin API](05-iam/) — users, roles, and credentials
- [Monitoring](06-monitoring/) — Prometheus metrics and stats API
- [Web Console](07-web-console/) — browser-based admin UI
- [Deployment](08-deployment/) — Docker, Docker Compose, production tips
- [Development](09-development/) — build, test, and contribute
