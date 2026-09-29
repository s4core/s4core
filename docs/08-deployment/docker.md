# Docker Deployment

S4 provides Docker images for easy deployment. The project includes a `Dockerfile` for the server and `docker-compose-single-node.yml`, which runs the server together with the web admin console.

## Single Container

### Build

```bash
docker build -t s4-server .
```

### Run (Basic)

```bash
docker run -d \
  --name s4-server \
  -p 9000:9000 \
  -v s4-data:/data \
  s4-server:latest
```

### Run with Credentials

```bash
docker run -d \
  --name s4-server \
  -p 9000:9000 \
  -v s4-data:/data \
  -e S4_ACCESS_KEY_ID=myaccesskey \
  -e S4_SECRET_ACCESS_KEY=mysecretkey \
  s4-server:latest
```

### Run with IAM

```bash
docker run -d \
  --name s4-server \
  -p 9000:9000 \
  -v s4-data:/data \
  -e S4_ROOT_PASSWORD=password12345 \
  -e S4_JWT_SECRET=your-256-bit-secret \
  s4-server:latest
```

### Run with TLS

```bash
docker run -d \
  --name s4-server \
  -p 9000:9000 \
  -v s4-data:/data \
  -v /path/to/certs:/certs:ro \
  -e S4_TLS_CERT=/certs/cert.pem \
  -e S4_TLS_KEY=/certs/key.pem \
  s4-server:latest
```

## Docker Compose (Full Stack)

`docker-compose-single-node.yml` builds the Community Edition server from the
repository and runs it together with the web admin console:

```yaml
services:
  s4core:
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "9000:9000"
    environment:
      - S4_BIND=0.0.0.0:9000
      - S4_DATA_DIR=/data
      - S4_ROOT_USERNAME=${S4_ROOT_USERNAME:-root}
      - S4_ROOT_PASSWORD=${S4_ROOT_PASSWORD:-password12345}
      - S4_ACCESS_KEY_ID=${S4_ACCESS_KEY_ID:-my-access-key-one}
      - S4_SECRET_ACCESS_KEY=${S4_SECRET_ACCESS_KEY:-my-secret-key-one}
    volumes:
      - s4-data:/data

  s4console:
    image: s4core/s4console:latest
    ports:
      - "3000:3000"
    environment:
      # The console reaches the server by its compose service name.
      - S4_BACKEND_URL=http://s4core:9000
    depends_on:
      s4core:
        condition: service_healthy

volumes:
  s4-data:
```

The excerpt leaves out the health check and restart policy. The root credentials
and the S3 keys are read from the shell, with the defaults above as placeholders:
set `S4_ROOT_PASSWORD`, `S4_ACCESS_KEY_ID` and `S4_SECRET_ACCESS_KEY` before
exposing the server.

### Start

```bash
# Full stack: server + web console
docker compose -f docker-compose-single-node.yml up -d --build

# Server only
docker compose -f docker-compose-single-node.yml up -d --build s4core
```

### Access Points

| Service | URL |
|---------|-----|
| S4 API | http://localhost:9000 |
| Web Console | http://localhost:3000 (log in as `root` / `S4_ROOT_PASSWORD`) |

The console needs IAM, which `S4_ROOT_PASSWORD` turns on.

### Rebuild After Changes

```bash
docker compose -f docker-compose-single-node.yml up -d --build
```

### Stop

```bash
docker compose -f docker-compose-single-node.yml down      # keep the data volume
docker compose -f docker-compose-single-node.yml down -v   # delete it as well
```

### View Logs

```bash
# All services
docker compose -f docker-compose-single-node.yml logs -f

# Server only
docker compose -f docker-compose-single-node.yml logs -f s4core
```

## Volume Management

S4 stores all data in the `/data` directory inside the container. Always use a Docker volume or bind mount to persist data.

```bash
# Named volume (recommended)
-v s4-data:/data

# Bind mount
-v /var/lib/s4:/data
```

The server runs as the unprivileged user `s4` (uid 1000), so a bind-mounted
directory has to be writable by that uid: `sudo chown 1000:1000 /var/lib/s4`.
A directory Docker creates for a missing bind source belongs to root, and the
server cannot write to it.

**Warning:** Without a volume, all data is lost when the container is removed.

## Cluster Deployment (Docker)

For deploying a multi-node S4 cluster with Docker Compose, see [Cluster Deployment](clustering.md#docker-compose).

## Resource Recommendations

| Workload | CPU | RAM | Storage |
|----------|-----|-----|---------|
| Development / Testing | 1 core | 512MB | 1GB |
| Small (< 1M objects) | 2 cores | 2GB | As needed |
| Medium (1-100M objects) | 4 cores | 8GB | As needed |
| Large (> 100M objects) | 8+ cores | 16GB+ | NVMe recommended for metadata |
