# S3 Credentials

After creating IAM users, you need to generate S3 credentials (access key + secret key) so they can authenticate using standard S3 tools.

## Generate Credentials

Only **SuperUser** accounts can generate credentials.

```bash
# Get JWT token
TOKEN=$(curl -s -k -X POST https://localhost:9000/api/admin/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"root","password":"password12345"}' | jq -r '.token')

# Create a user
USER_ID=$(curl -s -k -X POST https://localhost:9000/api/admin/users \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"alice123","role":"Writer"}' | jq -r '.id')

# Generate S3 credentials
curl -s -k -X POST https://localhost:9000/api/admin/users/$USER_ID/credentials \
  -H "Authorization: Bearer $TOKEN"
```

**Response:**

```json
{
  "access_key": "S4AKxxxxxxxxxxxxxxxxxxxx",
  "secret_key": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
}
```

**Important:** The secret key is displayed **only once** at creation time. Store it securely.

## Use S3 Credentials

Configure the AWS CLI with the generated credentials:

```bash
aws configure set aws_access_key_id S4AKxxxxxxxxxxxxxxxxxxxx
aws configure set aws_secret_access_key xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

Then use S4 as usual:

```bash
aws --endpoint-url https://localhost:9000 --no-verify-ssl s3 ls
```

The user's permissions are determined by their IAM role:

- **Reader** — can only list and download
- **Writer** — can create, upload, and delete
- **SuperUser** — full access

## Revoke Credentials

```bash
curl -s -k -X DELETE https://localhost:9000/api/admin/users/$USER_ID/credentials \
  -H "Authorization: Bearer $TOKEN"
```

After revocation, any requests using the old access key will be rejected.

## Credential Format

| Field | Format | Example |
|-------|--------|---------|
| Access Key | `S4AK` + 20 random characters | `S4AKaB3xY9mK2pQ7rW4n5t` |
| Secret Key | 40 random characters | `xK9mN2pQ7rW4n5tAb3Y6cD8eF0gH1iJ2kL3mN4o` |

## Security Notes

- Access keys are stored in the IAM database
- Secret keys are stored encrypted (not in plain text)
- Each user can have at most one active set of S3 credentials
- Generating new credentials replaces any existing ones
- Deactivating a user (`is_active: false`) immediately blocks all their S3 requests
