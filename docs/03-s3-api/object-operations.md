# Object Operations

S4 supports standard S3 object operations.

## PutObject

Uploads an object to a bucket.

```bash
# AWS CLI
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://mybucket/file.txt

# curl
curl -X PUT http://localhost:9000/mybucket/hello.txt -d "Hello S4!"

# With content type
curl -X PUT http://localhost:9000/mybucket/data.json \
  -H "Content-Type: application/json" \
  -d '{"key": "value"}'
```

**Response:** `200 OK` with `ETag` header.

If versioning is enabled, the response includes `x-amz-version-id`.

## GetObject

Downloads an object.

```bash
# AWS CLI
aws --endpoint-url http://localhost:9000 s3 cp s3://mybucket/file.txt downloaded.txt

# curl
curl http://localhost:9000/mybucket/hello.txt

# Get specific version
aws --endpoint-url http://localhost:9000 s3api get-object \
  --bucket mybucket \
  --key file.txt \
  --version-id "abc123-def456" \
  output.txt
```

**Response:** Object data with appropriate `Content-Type`, `ETag`, `Content-Length`, and `Last-Modified` headers.

## HeadObject

Retrieves object metadata without downloading the body.

```bash
aws --endpoint-url http://localhost:9000 s3api head-object \
  --bucket mybucket \
  --key file.txt
```

**Response:** Headers only — `Content-Type`, `Content-Length`, `ETag`, `Last-Modified`.

## DeleteObject

Deletes an object.

```bash
# AWS CLI
aws --endpoint-url http://localhost:9000 s3 rm s3://mybucket/file.txt

# curl
curl -X DELETE http://localhost:9000/mybucket/hello.txt
```

**Response:** `204 No Content`

With versioning enabled, this creates a **delete marker** instead of permanently removing the object. To permanently delete a specific version:

```bash
aws --endpoint-url http://localhost:9000 s3api delete-object \
  --bucket mybucket \
  --key file.txt \
  --version-id "abc123-def456"
```

## CopyObject

Copies an object within S4.

```bash
aws --endpoint-url http://localhost:9000 s3 cp \
  s3://mybucket/source.txt s3://mybucket/destination.txt
```

## Batch Operations

```bash
# Sync a directory
aws --endpoint-url http://localhost:9000 s3 sync ./local-dir s3://mybucket/

# Move an object
aws --endpoint-url http://localhost:9000 s3 mv s3://mybucket/old.txt s3://mybucket/new.txt

# Delete all objects in a bucket
aws --endpoint-url http://localhost:9000 s3 rm s3://mybucket --recursive
```
