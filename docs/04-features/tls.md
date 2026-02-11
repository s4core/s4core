# TLS / HTTPS

S4 supports TLS for encrypted connections. TLS is powered by `rustls` (a pure-Rust TLS implementation) and is enabled automatically when both a certificate and private key are provided.

## Enable TLS

Set two environment variables:

```bash
export S4_TLS_CERT=/path/to/cert.pem
export S4_TLS_KEY=/path/to/key.pem

./target/release/s4-server
```

S4 will start in HTTPS mode and log the listening address as `https://...`.

## Generate Self-Signed Certificates (Development)

For development and testing, generate a self-signed certificate:

```bash
openssl req -x509 -newkey rsa:4096 \
  -keyout key.pem -out cert.pem \
  -days 365 -nodes \
  -subj "/CN=localhost"
```

## Using with AWS CLI

### With Self-Signed Certificates

```bash
aws --endpoint-url https://localhost:9000 --no-verify-ssl s3 ls
```

The `--no-verify-ssl` flag is required for self-signed certificates.

### With Valid Certificates (Production)

```bash
aws --endpoint-url https://s4.example.com:9000 s3 ls
```

## Certificate Requirements

| Requirement | Details |
|-------------|---------|
| Format | PEM-encoded |
| Certificate type | X.509 |
| Key types supported | RSA, ECDSA, Ed25519 |
| Certificate chain | Supported (include intermediate certs in `cert.pem`) |

## Docker with TLS

Mount your certificates into the container:

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
