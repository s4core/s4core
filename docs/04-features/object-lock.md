# Object Lock

S4 supports S3-compatible Object Lock for WORM (Write Once Read Many) compliance. Object Lock prevents objects from being deleted or overwritten for a specified retention period.

## Prerequisites

Object Lock requires **versioning to be enabled** on the bucket. You must enable versioning before configuring Object Lock.

## Retention Modes

| Mode | Description |
|------|-------------|
| **GOVERNANCE** | Protected from deletion. Can be overridden by users with special permissions. |
| **COMPLIANCE** | Strictly protected. No one can delete or modify the object until the retention period expires. |

**Safety note:** COMPLIANCE mode is intentionally strict. S4 includes safety checks to ensure retention periods always expire, preventing permanently undeletable files.

## Enable Object Lock on a Bucket

```bash
# First, enable versioning
aws s3api put-bucket-versioning \
  --bucket mybucket \
  --versioning-configuration Status=Enabled \
  --endpoint-url http://localhost:9000

# Enable Object Lock with default retention
aws s3api put-object-lock-configuration \
  --bucket mybucket \
  --object-lock-configuration \
    'ObjectLockEnabled=Enabled,Rule={DefaultRetention={Mode=GOVERNANCE,Days=30}}' \
  --endpoint-url http://localhost:9000
```

## Get Object Lock Configuration

```bash
aws s3api get-object-lock-configuration \
  --bucket mybucket \
  --endpoint-url http://localhost:9000
```

## Default Retention

When a bucket has default retention configured, all new objects automatically get a retention period applied. No client-side configuration needed — just upload objects normally.

```bash
# Upload an object (automatically gets 30-day retention)
echo "important data" | aws s3api put-object \
  --bucket mybucket --key report.txt --body - \
  --endpoint-url http://localhost:9000
```

## Per-Object Retention

Set or view retention on a specific object version:

```bash
# Get version ID
VERSION_ID=$(aws s3api list-object-versions \
  --bucket mybucket --prefix report.txt \
  --endpoint-url http://localhost:9000 \
  --query 'Versions[0].VersionId' --output text)

# Set retention
aws s3api put-object-retention \
  --bucket mybucket --key report.txt \
  --version-id $VERSION_ID \
  --retention '{"Mode":"GOVERNANCE","RetainUntilDate":"2026-12-31T00:00:00Z"}' \
  --endpoint-url http://localhost:9000

# Get retention
aws s3api get-object-retention \
  --bucket mybucket --key report.txt \
  --version-id $VERSION_ID \
  --endpoint-url http://localhost:9000
```

## Legal Hold

Legal hold is a separate protection mechanism. When enabled, the object cannot be deleted regardless of retention status. Legal hold can be toggled on and off independently.

```bash
# Enable legal hold
aws s3api put-object-legal-hold \
  --bucket mybucket --key report.txt \
  --version-id $VERSION_ID \
  --legal-hold Status=ON \
  --endpoint-url http://localhost:9000

# Check legal hold status
aws s3api get-object-legal-hold \
  --bucket mybucket --key report.txt \
  --version-id $VERSION_ID \
  --endpoint-url http://localhost:9000

# Remove legal hold
aws s3api put-object-legal-hold \
  --bucket mybucket --key report.txt \
  --version-id $VERSION_ID \
  --legal-hold Status=OFF \
  --endpoint-url http://localhost:9000
```

## Delete Protection

When an object has active retention or legal hold, delete attempts are blocked:

```bash
# This will fail with AccessDenied if the object is locked
aws s3api delete-object \
  --bucket mybucket --key report.txt \
  --version-id $VERSION_ID \
  --endpoint-url http://localhost:9000
```

An object can be deleted only when:
- The retention period has expired, **AND**
- Legal hold is OFF

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/{bucket}?object-lock` | GET | Get bucket Object Lock configuration |
| `/{bucket}?object-lock` | PUT | Set bucket Object Lock configuration |
| `/{bucket}/{key}?retention&versionId=X` | GET | Get object retention |
| `/{bucket}/{key}?retention&versionId=X` | PUT | Set object retention |
| `/{bucket}/{key}?legal-hold&versionId=X` | GET | Get legal hold status |
| `/{bucket}/{key}?legal-hold&versionId=X` | PUT | Set legal hold status |
