# Bucket Operations

S4 supports standard S3 bucket operations.

## CreateBucket

Creates a new bucket.

```bash
# AWS CLI
aws --endpoint-url http://localhost:9000 s3 mb s3://mybucket

# curl
curl -X PUT http://localhost:9000/mybucket
```

**Response:** `200 OK`

## ListBuckets

Lists all buckets.

```bash
# AWS CLI
aws --endpoint-url http://localhost:9000 s3 ls

# curl
curl http://localhost:9000/
```

**Response:** XML listing of all buckets with names and creation dates.

## DeleteBucket

Deletes an empty bucket.

```bash
# AWS CLI
aws --endpoint-url http://localhost:9000 s3 rb s3://mybucket

# curl
curl -X DELETE http://localhost:9000/mybucket
```

**Response:** `204 No Content`

The bucket must be empty before it can be deleted.

## HeadBucket

Checks if a bucket exists.

```bash
aws --endpoint-url http://localhost:9000 s3api head-bucket --bucket mybucket
```

**Response:** `200 OK` if the bucket exists, `404 Not Found` otherwise.

## ListObjects (v2)

Lists objects in a bucket with optional prefix filtering and pagination.

```bash
# List all objects
aws --endpoint-url http://localhost:9000 s3 ls s3://mybucket

# List with prefix
aws --endpoint-url http://localhost:9000 s3 ls s3://mybucket/logs/

# curl with parameters
curl "http://localhost:9000/mybucket?list-type=2&prefix=logs/&max-keys=100"
```

**Query parameters:**

| Parameter | Description |
|-----------|-------------|
| `list-type=2` | Use ListObjectsV2 |
| `prefix` | Filter objects by key prefix |
| `delimiter` | Group keys by delimiter (usually `/`) |
| `max-keys` | Maximum number of keys to return (default: 1000) |
| `continuation-token` | Token for pagination |

**Response:** XML listing of objects with keys, sizes, ETags, and last modified dates.
