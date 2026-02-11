# S4 Server - Multi-stage Docker Build
# High-performance S3-compatible object storage server

# ============================================
# Stage 1: Build
# ============================================
FROM rust:1.88-alpine AS builder

# Install build dependencies
RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static

WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY s4-core ./s4-core
COPY s4-api ./s4-api
COPY s4-features ./s4-features
COPY s4-compactor ./s4-compactor
COPY s4-server ./s4-server

# Build release binary with static linking
ENV RUSTFLAGS="-C target-feature=-crt-static"
RUN cargo build --release --bin s4-server

# ============================================
# Stage 2: Production Runner
# ============================================
FROM alpine:3.19 AS runner

# Install runtime dependencies
RUN apk add --no-cache ca-certificates libgcc

# Create non-root user for security
RUN addgroup -g 1000 s4 && adduser -u 1000 -G s4 -s /bin/sh -D s4

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/s4-server /app/s4-server

# Create data directory
RUN mkdir -p /data && chown -R s4:s4 /data

# Switch to non-root user
USER s4

# Default environment variables
ENV S4_DATA_DIR=/data
ENV RUST_LOG=s4_api=info,s4_server=info

# Expose S4 port
EXPOSE 9000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget --server-response --spider http://0.0.0.0:9000/ 2>&1 | grep -q -E 'HTTP/1.1 (200|403)' || exit 1

# Run the server
CMD ["/app/s4-server"]
