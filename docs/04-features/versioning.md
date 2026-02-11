# Object Versioning

S4 supports S3-compatible object versioning. When enabled, every overwrite or delete creates a new version instead of replacing the previous data. This allows you to preserve, retrieve, and restore any previous version of an object.

## Versioning States

A bucket can be in one of three versioning states:

| State | Behavior |
|-------|----------|
| **Unversioned** (default) | Objects are replaced on overwrite, deleted permanently |
| **Enabled** | Every PUT creates a new version with a unique version ID |
| **Suspended** | New objects get a `null` version ID; existing versions are preserved |

Once versioning is enabled on a bucket, it cannot be disabled — only suspended.

## Enable Versioning

```bash
aws s3api put-bucket-versioning \
  --bucket mybucket \
  --versioning-configuration Status=Enabled \
  --endpoint-url http://localhost:9000
```

## Check Versioning Status

```bash
aws s3api get-bucket-versioning \
  --bucket mybucket \
  --endpoint-url http://localhost:9000
```

## Upload Multiple Versions

Each PUT to the same key creates a new version:

```bash
# Version 1
echo "version 1" | aws s3api put-object \
  --bucket mybucket --key file.txt --body - \
  --endpoint-url http://localhost:9000

# Version 2
echo "version 2" | aws s3api put-object \
  --bucket mybucket --key file.txt --body - \
  --endpoint-url http://localhost:9000
```

Both PUT responses include an `x-amz-version-id` header with the unique version ID.

## List All Versions

```bash
aws s3api list-object-versions \
  --bucket mybucket \
  --prefix file.txt \
  --endpoint-url http://localhost:9000
```

Returns all versions and delete markers for matching keys.

## Get a Specific Version

```bash
aws s3api get-object \
  --bucket mybucket \
  --key file.txt \
  --version-id "ff495d34-c292-4af4-9d10-e186272010ed" \
  output.txt \
  --endpoint-url http://localhost:9000
```

## Delete Behavior

### Simple Delete (Creates Delete Marker)

```bash
aws s3api delete-object \
  --bucket mybucket \
  --key file.txt \
  --endpoint-url http://localhost:9000
```

This does **not** remove data. It creates a **delete marker** — a special version that makes the object appear deleted. A subsequent GET returns `404`.

### Permanently Delete a Version

```bash
aws s3api delete-object \
  --bucket mybucket \
  --key file.txt \
  --version-id "ff495d34-c292-4af4-9d10-e186272010ed" \
  --endpoint-url http://localhost:9000
```

### Restore a Deleted Object

Remove the delete marker to restore the most recent version:

```bash
aws s3api delete-object \
  --bucket mybucket \
  --key file.txt \
  --version-id "<delete-marker-version-id>" \
  --endpoint-url http://localhost:9000
```

## Suspend Versioning

```bash
aws s3api put-bucket-versioning \
  --bucket mybucket \
  --versioning-configuration Status=Suspended \
  --endpoint-url http://localhost:9000
```

When suspended, new objects are stored with a `null` version ID, but existing versions remain accessible.
