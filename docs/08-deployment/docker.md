# Docker Deployment

S4 provides Docker images for easy deployment. The project includes both a `Dockerfile` for the server and a `docker-compose.yml` for the full stack.

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

The `docker-compose.yml` runs both the S4 server and the web admin console:

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

volumes:
  s4-data:
```

### Start

```bash
# With IAM enabled
S4_ROOT_PASSWORD=password12345 docker compose up -d --build

# Basic mode
docker compose up -d --build
```

### Access Points

| Service | URL |
|---------|-----|
| S4 API | http://localhost:9000 |
| Web Console | http://localhost:3000 |

### Rebuild After Changes

```bash
docker compose up -d --build
```

### Stop

```bash
docker compose down
```

### View Logs

```bash
# All services
docker compose logs -f

# Server only
docker compose logs -f s4-server
```

## Volume Management

S4 stores all data in the `/data` directory inside the container. Always use a Docker volume or bind mount to persist data.

```bash
# Named volume (recommended)
-v s4-data:/data

# Bind mount
-v /var/lib/s4:/data
```

**Warning:** Without a volume, all data is lost when the container is removed.

## Resource Recommendations

| Workload | CPU | RAM | Storage |
|----------|-----|-----|---------|
| Development / Testing | 1 core | 512MB | 1GB |
| Small (< 1M objects) | 2 cores | 2GB | As needed |
| Medium (1-100M objects) | 4 cores | 8GB | As needed |
| Large (> 100M objects) | 8+ cores | 16GB+ | NVMe recommended for metadata |
