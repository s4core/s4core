# Roles and Permissions

S4 uses a simple role-based access control model with three built-in roles.

## Roles

| Role | S3 Read | S3 Write | Admin API | Description |
|------|---------|----------|-----------|-------------|
| **Reader** | Yes | No | No | Read-only access to S3 operations |
| **Writer** | Yes | Yes | No | Read and write access to S3 operations |
| **SuperUser** | Yes | Yes | Yes | Full access including user management |

## Permission Details

### Reader

- List buckets
- List objects in buckets
- Download (GET) objects
- HEAD requests on buckets and objects

### Writer

All Reader permissions, plus:

- Create and delete buckets
- Upload (PUT) objects
- Delete objects
- Configure bucket settings (versioning, lifecycle, CORS, Object Lock)
- Multipart upload operations

### SuperUser

All Writer permissions, plus:

- Access the Admin API
- Create, update, and delete users
- Generate and revoke S3 credentials
- View system statistics

## Enabling IAM

IAM is activated by setting the `S4_ROOT_PASSWORD` environment variable:

```bash
export S4_ROOT_PASSWORD=your-strong-password
./target/release/s4-server
```

When IAM is enabled:

1. A **root user** is created automatically on first startup
2. The root user has the **SuperUser** role
3. All S3 API requests are checked against IAM permissions
4. The Admin API becomes available at `/api/admin/`

## Root User

| Setting | Variable | Default |
|---------|----------|---------|
| Username | `S4_ROOT_USERNAME` | `root` |
| Password | `S4_ROOT_PASSWORD` | (required to enable IAM) |

The root user is created only once. On subsequent starts, the existing root user is preserved.

## Authentication Methods

S4 supports two authentication methods simultaneously:

| Method | Used By | Mechanism |
|--------|---------|-----------|
| **AWS Signature V4** | S3 API (AWS CLI, SDKs) | HMAC-SHA256 signature with access/secret keys |
| **JWT Bearer Token** | Admin API, Web Console | `Authorization: Bearer <token>` header |

### Getting a JWT Token

```bash
TOKEN=$(curl -s -X POST http://localhost:9000/api/admin/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"root","password":"your-password"}' | jq -r '.token')
```

The token is valid for 24 hours by default.

## Legacy Credentials

When `S4_ACCESS_KEY_ID` and `S4_SECRET_ACCESS_KEY` are set alongside IAM, they continue to work as a fallback with SuperUser access. This ensures backward compatibility during migration to IAM.
