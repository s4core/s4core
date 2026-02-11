# Authentication

S4 supports AWS Signature Version 4 for authenticating S3 API requests. This is the same authentication mechanism used by AWS S3, ensuring compatibility with all standard S3 tools and SDKs.

## How AWS Signature V4 Works

1. The client constructs a **canonical request** from the HTTP method, URI, query string, headers, and payload hash
2. The client creates a **string to sign** that includes the date, region, service, and canonical request hash
3. The client derives a **signing key** using HMAC-SHA256 with the secret access key
4. The client computes the **signature** and includes it in the `Authorization` header

S4 repeats these steps server-side and compares signatures to verify the request.

## Configuring Credentials

### Static Credentials (Environment Variables)

```bash
export S4_ACCESS_KEY_ID=myaccesskey
export S4_SECRET_ACCESS_KEY=mysecretkey
```

These credentials provide **SuperUser** access (full permissions).

### IAM-Based Credentials

When IAM is enabled (`S4_ROOT_PASSWORD` is set), you can generate per-user S3 credentials through the Admin API:

```bash
# Generate credentials for a user
curl -s -k -X POST https://localhost:9000/api/admin/users/<user-id>/credentials \
  -H "Authorization: Bearer $TOKEN"
```

The response includes an `access_key` and `secret_key` pair. Use these to configure your S3 client.

## Client Configuration

### AWS CLI

```bash
aws configure set aws_access_key_id myaccesskey
aws configure set aws_secret_access_key mysecretkey
aws configure set default.region us-east-1
```

Then use `--endpoint-url` to point to S4:

```bash
aws --endpoint-url http://localhost:9000 s3 ls
```

### boto3 (Python)

```python
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='myaccesskey',
    aws_secret_access_key='mysecretkey',
    region_name='us-east-1'
)

# List buckets
response = s3.list_buckets()
for bucket in response['Buckets']:
    print(bucket['Name'])
```

### rclone

```bash
rclone config create s4 s3 \
  provider=Other \
  access_key_id=myaccesskey \
  secret_access_key=mysecretkey \
  endpoint=http://localhost:9000

rclone ls s4:mybucket
```

## Authentication Modes

| Mode | Condition | Behavior |
|------|-----------|----------|
| **No auth** | No credentials configured | All requests are accepted |
| **Static credentials** | `S4_ACCESS_KEY_ID` + `S4_SECRET_ACCESS_KEY` set | Single user, SuperUser access |
| **IAM** | `S4_ROOT_PASSWORD` set | Multi-user with role-based access |
| **IAM + fallback** | Both IAM and static credentials | IAM users + static credentials as fallback |
