# Installation

S4 can be installed from source or run via Docker.

## Building from Source

### Prerequisites

- Rust 1.70 or later
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

### Docker Compose

The project includes a `docker-compose.yml` that runs S4 server together with the web admin console.

```bash
# Run full stack (server + web console)
docker compose up --build

# Run in background
docker compose up -d --build

# Run only the server
docker compose up s4-server --build

# Stop everything
docker compose down
```

After startup:

- **S4 API**: http://localhost:9000
- **Web Console**: http://localhost:3000 (login with root credentials)
