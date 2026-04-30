# S4 Server - Multi-stage Docker Build
# High-performance S3-compatible object storage server
#
# Build args:
#   EDITION  - "ce" (default) or "ee"
#   S4_VERSION - version string (e.g. "0.0.8"), typically set by CI from git tag

# ============================================
# Stage 1: Build
# ============================================
FROM rust:1.90-alpine AS builder

# Install build dependencies (protobuf-dev provides protoc for gRPC codegen)
RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static protobuf-dev

WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY s4-core ./s4-core
COPY s4-api ./s4-api
COPY s4-features ./s4-features
COPY s4-compactor ./s4-compactor
COPY s4-select ./s4-select
COPY s4-cluster ./s4-cluster
COPY s4-server ./s4-server
COPY ee ./ee

# Set version if provided (e.g. from CI tag build)
ARG S4_VERSION=""
RUN if [ -n "${S4_VERSION}" ]; then \
      VERSION="${S4_VERSION#v}"; \
      echo "Setting version to ${VERSION}"; \
      sed -i "s|^version = \"0.0.1\"|version = \"${VERSION}\"|" Cargo.toml; \
    fi

# Edition: "ce" (default) or "ee"
ARG EDITION=ce

# Build release binary — EE includes enterprise feature flag
# BuildKit cache mounts share Cargo artifacts between CE and EE builds,
# so the EE build only recompiles crates affected by the enterprise feature.
ENV RUSTFLAGS="-C target-feature=-crt-static"
RUN --mount=type=cache,target=/app/target \
    --mount=type=cache,target=/root/.cargo/registry \
    if [ "${EDITION}" = "ee" ]; then \
      echo "Building Enterprise Edition"; \
      cargo build --release --bin s4-server --features enterprise; \
    else \
      echo "Building Community Edition"; \
      cargo build --release --bin s4-server; \
    fi && \
    cp /app/target/release/s4-server /app/s4-server-binary

# ============================================
# Stage 2: Production Runner
# ============================================
FROM alpine:3.19 AS runner

# Install runtime dependencies
RUN apk add --no-cache ca-certificates libgcc

# Create non-root user for security
RUN addgroup -g 1000 s4 && adduser -u 1000 -G s4 -s /bin/sh -D s4

WORKDIR /app

# Copy binary from builder (extracted from cache mount during build)
COPY --from=builder /app/s4-server-binary /app/s4-server

# Create data directory
RUN mkdir -p /data && chown -R s4:s4 /data

# Switch to non-root user
USER s4

# Carry edition label into runtime for diagnostics
ARG EDITION=ce
ARG S4_VERSION=""
ENV S4_EDITION=${EDITION}

# OCI image labels
LABEL org.opencontainers.image.title="S4Core (${EDITION})" \
      org.opencontainers.image.description="High-performance S3-compatible object storage — ${EDITION} edition" \
      org.opencontainers.image.version="${S4_VERSION}" \
      org.opencontainers.image.vendor="S4Core Team" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://github.com/s4core/s4core"

# Default environment variables
ENV S4_DATA_DIR=/data
ENV S4_BIND=0.0.0.0:9000
ENV RUST_LOG=s4_api=info,s4_server=info

# Expose S4 HTTP and gRPC ports
EXPOSE 9000 9100

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --server-response --spider -q http://0.0.0.0:9000/health || exit 1

# Run the server
CMD ["/app/s4-server"]
