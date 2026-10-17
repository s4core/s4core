# Lifecycle Policies

S4 supports S3-compatible lifecycle policies for automatic object management. Use lifecycle rules to automatically delete objects after a specified number of days, clean up old versions, or remove expired delete markers.

## Supported Rules

| Rule Type | Description |
|-----------|-------------|
| **Expiration (Days)** | Delete current version after N days |
| **NoncurrentVersionExpiration** | Delete old versions after N days |
| **ExpiredObjectDeleteMarker** | Remove delete markers when all versions are gone |

Rules can be filtered by key prefix (e.g., apply only to objects under `logs/`).

## Set Lifecycle Configuration

Create a JSON configuration file:

```json
{
  "Rules": [
    {
      "ID": "expire-logs",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "logs/"
      },
      "Expiration": {
        "Days": 30
      }
    },
    {
      "ID": "cleanup-old-versions",
      "Status": "Enabled",
      "Filter": {
        "Prefix": ""
      },
      "NoncurrentVersionExpiration": {
        "NoncurrentDays": 90
      }
    }
  ]
}
```

Apply it to a bucket:

```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket mybucket \
  --lifecycle-configuration file://lifecycle.json \
  --endpoint-url http://localhost:9000
```

## Get Lifecycle Configuration

```bash
aws s3api get-bucket-lifecycle-configuration \
  --bucket mybucket \
  --endpoint-url http://localhost:9000
```

## Delete Lifecycle Configuration

```bash
aws s3api delete-bucket-lifecycle \
  --bucket mybucket \
  --endpoint-url http://localhost:9000
```

## Background Worker

S4 runs a background lifecycle worker that periodically evaluates rules and deletes expired objects.

### Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `S4_LIFECYCLE_ENABLED` | Enable or disable the worker | `true` |
| `S4_LIFECYCLE_INTERVAL_HOURS` | How often rules are evaluated | `24` |
| `S4_LIFECYCLE_INTERVAL_SECS` | The same in seconds; when set, the value in hours is not read | — |
| `S4_LIFECYCLE_DRY_RUN` | Log actions without deleting | `false` |

### Dry-Run Mode

Use dry-run mode to test lifecycle rules without actually deleting any data:

```bash
export S4_LIFECYCLE_DRY_RUN=true
```

The worker will log which objects would be deleted, allowing you to verify the rules before enabling actual deletion.

### Cluster Mode

Every node runs the worker and expires its own copies of the objects. It takes the buckets of the
whole pool and each bucket's rules as the pool keeps them, so a rule set through any node expires
objects on every node — also on a node that keeps the bucket's objects but not the bucket itself.
Right after an upgrade from an earlier release, until every node of the pool has handed over the
configurations it kept on its own (see [Federation](federation.md)), a node applies the rules it
keeps itself, as the earlier release did, and the pool's rules only to a bucket it keeps none for.

Cluster mode keeps no object versions ([Versioning](versioning.md#cluster-mode)), so rules for
noncurrent versions and expired delete markers have no effect there.

## Example: Common Configurations

### Delete temp files after 7 days

```json
{
  "Rules": [
    {
      "ID": "cleanup-temp",
      "Status": "Enabled",
      "Filter": { "Prefix": "tmp/" },
      "Expiration": { "Days": 7 }
    }
  ]
}
```

### Keep only 30 days of versions

```json
{
  "Rules": [
    {
      "ID": "expire-old-versions",
      "Status": "Enabled",
      "Filter": { "Prefix": "" },
      "NoncurrentVersionExpiration": { "NoncurrentDays": 30 }
    }
  ]
}
```

### Combined: expire current + noncurrent + markers

```json
{
  "Rules": [
    {
      "ID": "full-cleanup",
      "Status": "Enabled",
      "Filter": { "Prefix": "" },
      "Expiration": { "Days": 365 },
      "NoncurrentVersionExpiration": { "NoncurrentDays": 90 },
      "ExpiredObjectDeleteMarker": true
    }
  ]
}
```
