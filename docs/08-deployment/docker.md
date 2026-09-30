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
openssl rand -hex 32 > jwt_secret   # once; keep it with your other secrets

docker run -d \
  --name s4-server \
  -p 9000:9000 \
  -v s4-data:/data \
  -e S4_ROOT_PASSWORD=password12345 \
  -e S4_JWT_SECRET="$(cat jwt_secret)" \
  s4-server:latest
```

Without `S4_JWT_SECRET` a single node signs admin tokens with a random secret generated at
every start, so admin sessions end on restart. Setting it keeps them, as long as the value stays
the same: generate it once, not in every `docker run`. A secret shorter than 32 bytes stops the
start — see [Admin JWT Secret](../01-getting-started/configuration.md#admin-jwt-secret).

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

## Credentials from Docker Secrets

Every credential can be read from a file by appending `_FILE` to its variable
(`S4_ROOT_USERNAME`, `S4_ROOT_PASSWORD`, `S4_ACCESS_KEY_ID`, `S4_SECRET_ACCESS_KEY`,
`S4_JWT_SECRET`). Docker mounts each secret as a file under `/run/secrets/`, so the values stay out
of `docker inspect`, the process environment and the compose file. The rules are in the
[configuration reference](../01-getting-started/configuration.md#secrets-from-files-_file).

```yaml
services:
  s4core:
    image: s4core/s4core:latest
    ports:
      - "9000:9000"
    volumes:
      - s4-data:/data
    environment:
      - S4_ROOT_USERNAME_FILE=/run/secrets/s4_user
      - S4_ROOT_PASSWORD_FILE=/run/secrets/s4_pass
      - S4_ACCESS_KEY_ID_FILE=/run/secrets/s4_access_key
      - S4_SECRET_ACCESS_KEY_FILE=/run/secrets/s4_secret_key
      - S4_JWT_SECRET_FILE=/run/secrets/s4_jwt_secret
    secrets: [s4_user, s4_pass, s4_access_key, s4_secret_key, s4_jwt_secret]

secrets:
  s4_user:
    file: ./secrets/s4_user
  s4_pass:
    file: ./secrets/s4_pass
  s4_access_key:
    file: ./secrets/s4_access_key
  s4_secret_key:
    file: ./secrets/s4_secret_key
  s4_jwt_secret:
    file: ./secrets/s4_jwt_secret

volumes:
  s4-data:
```

Create the files once, for example `openssl rand -hex 32 > secrets/s4_jwt_secret`. In Docker Swarm,
declare the secrets as `external: true` and create them with `docker secret create`. If a file is
missing or empty, or a variable is set together with its `_FILE` form, the server stops at startup
and names the variable.

The server runs as user `s4` (uid 1000) inside the container. Docker Compose without Swarm
bind-mounts a secret file with its owner and mode from the host, so a file readable only by root
cannot be read and the start fails with `Permission denied`. Give the files to uid 1000:
`sudo chown 1000 secrets/* && sudo chmod 400 secrets/*`. Swarm mounts secrets readable by everyone in
the container, so there nothing needs to change.

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
