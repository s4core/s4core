# Installation

S4 can be installed from source or run via Docker.

## Building from Source

### Prerequisites

- Rust through [rustup](https://rustup.rs): the release is pinned in `rust-toolchain.toml`, and `rustup toolchain install` in the repository installs it
- Linux (recommended) or macOS

### Build Steps

```bash
# Clone the repository
git clone https://github.com/s4core/s4core.git
cd s4core

# Build in release mode
cargo build --release

# The binary is at:
./target/release/s4-server
```

### Verify the Build

```bash
# Run all tests
cargo test --workspace

# Check formatting and linting
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
```

## Docker

S4 provides official Docker images.

### Single Container

```bash
# Build the image
docker build -t s4-server .

# Run with default settings
docker run -d \
  --name s4-server \
  -p 9000:9000 \
  -v s4-data:/data \
  s4-server:latest
```

### With Custom Credentials

```bash
docker run -d \
  --name s4-server \
  -p 9000:9000 \
  -v s4-data:/data \
  -e S4_ACCESS_KEY_ID=myaccesskey \
  -e S4_SECRET_ACCESS_KEY=mysecretkey \
  s4-server:latest
```

### With IAM Enabled

```bash
docker run -d \
  --name s4-server \
  -p 9000:9000 \
  -v s4-data:/data \
  -e S4_ROOT_PASSWORD=password12345 \
  s4-server:latest
```

### With TLS

```
mkdir -p ~/certs && cd ~/certs

openssl req -x509 -nodes -days 3650 -newkey rsa:4096 \
  -keyout key.pem -out cert.pem \
  -subj "/C=EN/ST=NY/L=NY/O=MyOrg/CN=s4-server.local"
```

```bash
docker run -d \
  --name s4-server \
  -p 9000:9000 \
  -v s4-data:/data \
  -v /home/user/certs:/certs:ro \
  -e S4_ACCESS_KEY_ID=myaccesskey \
  -e S4_SECRET_ACCESS_KEY=mysecretkey \
  -e S4_TLS_CERT=/certs/cert.pem \
  -e S4_TLS_KEY=/certs/key.pem \
  s4-server:latest
```

### Docker Compose

The project includes `docker-compose-single-node.yml`, which runs the S4 server together with the web admin console.

```bash
# Run full stack (server + web console)
docker compose -f docker-compose-single-node.yml up --build

# Run in background
docker compose -f docker-compose-single-node.yml up -d --build

# Run only the server
docker compose -f docker-compose-single-node.yml up -d --build s4core

# Stop everything
docker compose -f docker-compose-single-node.yml down
```

For a three-node cluster behind HAProxy, see [Cluster Deployment](../08-deployment/clustering.md#docker-compose).

After startup:

- **S4 API**: http://localhost:9000
- **Web Console**: http://localhost:3000 (login with root credentials)
