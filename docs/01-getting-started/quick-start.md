# Quick Start

This guide walks you through basic S4 operations using the AWS CLI.

## 1. Start the Server

```bash
export S4_ACCESS_KEY_ID=myaccesskey
export S4_SECRET_ACCESS_KEY=mysecretkey

./target/release/s4-server
# Server listens on http://127.0.0.1:9000
```

## 2. Configure AWS CLI

```bash
aws configure set aws_access_key_id myaccesskey
aws configure set aws_secret_access_key mysecretkey
```

## 3. Basic Operations

### Create a Bucket

```bash
aws --endpoint-url http://localhost:9000 s3 mb s3://mybucket
```

### Upload a File

```bash
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://mybucket/file.txt
```

### List Objects

```bash
aws --endpoint-url http://localhost:9000 s3 ls s3://mybucket
```

### Download a File

```bash
aws --endpoint-url http://localhost:9000 s3 cp s3://mybucket/file.txt downloaded.txt
```

### Delete a File

```bash
aws --endpoint-url http://localhost:9000 s3 rm s3://mybucket/file.txt
```

### Delete a Bucket

```bash
aws --endpoint-url http://localhost:9000 s3 rb s3://mybucket
```

## 4. Using curl

S4 also works with simple HTTP requests:

```bash
# Create bucket
curl -X PUT http://127.0.0.1:9000/test-bucket

# Upload object
curl -X PUT http://127.0.0.1:9000/test-bucket/hello.txt -d "Hello S4!"

# Download object
curl http://127.0.0.1:9000/test-bucket/hello.txt

# List buckets
curl http://127.0.0.1:9000/

# List objects in bucket
curl http://127.0.0.1:9000/test-bucket

# Delete object
curl -X DELETE http://127.0.0.1:9000/test-bucket/hello.txt
```

## Next Steps

- Enable [Object Versioning](../04-features/versioning.md) to keep file history
- Set up [IAM](../05-iam/) for multi-user access control
- Configure [TLS](../04-features/tls.md) for encrypted connections
- Deploy with [Docker](../08-deployment/docker.md)
