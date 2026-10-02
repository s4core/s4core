# Admin API

The Admin API provides HTTP endpoints for managing users, credentials, and system information. All endpoints (except login) require a JWT token with **SuperUser** role.

## Authentication

First, obtain a JWT token:

```bash
TOKEN=$(curl -s -k -X POST https://localhost:9000/api/admin/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"root","password":"password12345"}' | jq -r '.token')
```

Use the token in subsequent requests:

```bash
curl -s -k https://localhost:9000/api/admin/users \
  -H "Authorization: Bearer $TOKEN"
```

## Endpoints

### Login

**POST** `/api/admin/login`

No authentication required.

```bash
curl -s -k -X POST https://localhost:9000/api/admin/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"root","password":"password12345"}'
```

**Response:**

```json
{
  "token": "eyJhbGciOiJIUzI1NiIs...",
  "expires_at": "2026-02-11T10:00:00Z"
}
```

A wrong password and an unknown account both answer `401` with code
`InvalidCredentials` — identical responses, so the endpoint cannot be used to
find out which account names exist.

### List Users

**GET** `/api/admin/users`

```bash
curl -s -k https://localhost:9000/api/admin/users \
  -H "Authorization: Bearer $TOKEN"
```

**Response:** Array of user objects (password hashes and secret keys are never returned).

### Create User

**POST** `/api/admin/users`

```bash
curl -s -k -X POST https://localhost:9000/api/admin/users \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"alice123","role":"Writer"}'
```

**Request body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `username` | string | Yes | 3-32 characters, alphanumeric and underscore |
| `password` | string | Yes | User password |
| `role` | string | Yes | `Reader`, `Writer`, or `SuperUser` |

### Get User

**GET** `/api/admin/users/{id}`

```bash
curl -s -k https://localhost:9000/api/admin/users/<user-id> \
  -H "Authorization: Bearer $TOKEN"
```

### Update User

**PUT** `/api/admin/users/{id}`

```bash
curl -s -k -X PUT https://localhost:9000/api/admin/users/<user-id> \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"role":"Reader"}'
```

**Request body** (all fields optional):

| Field | Type | Description |
|-------|------|-------------|
| `password` | string | New password |
| `role` | string | New role (`Reader`, `Writer`, `SuperUser`) |
| `is_active` | boolean | Enable or disable the account |

### Delete User

**DELETE** `/api/admin/users/{id}`

```bash
curl -s -k -X DELETE https://localhost:9000/api/admin/users/<user-id> \
  -H "Authorization: Bearer $TOKEN"
```

**Response:** `204 No Content`

### Generate S3 Credentials

**POST** `/api/admin/users/{id}/credentials`

```bash
curl -s -k -X POST https://localhost:9000/api/admin/users/<user-id>/credentials \
  -H "Authorization: Bearer $TOKEN"
```

**Response:**

```json
{
  "access_key": "S4AKxxxxxxxxxxxxxxxxxxxx",
  "secret_key": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
}
```

**Important:** The `secret_key` is shown **only once**. Store it securely — it cannot be retrieved again.

### Delete S3 Credentials

**DELETE** `/api/admin/users/{id}/credentials`

```bash
curl -s -k -X DELETE https://localhost:9000/api/admin/users/<user-id>/credentials \
  -H "Authorization: Bearer $TOKEN"
```

**Response:** `204 No Content`

### Create Bucket

**PUT** `/api/admin/buckets/{name}`

```bash
curl -s -k -X PUT https://localhost:9000/api/admin/buckets/my-bucket \
  -H "Authorization: Bearer $TOKEN"
```

**Response:**

```json
{ "name": "my-bucket" }
```

The name must satisfy the same S3 rules the S3 API enforces (3–63 characters,
lowercase letters, digits, hyphens and dots, starting with a letter or digit and
not ending with a hyphen) — otherwise `400 InvalidBucketName`. A name already in
use answers `409 BucketAlreadyExists`.

### Delete Bucket

**DELETE** `/api/admin/buckets/{name}`

Deletes a bucket. By default the bucket must be empty — a bucket that still
holds objects answers `409 BucketNotEmpty`, and an unknown bucket answers
`404 NoSuchBucket`.

```bash
curl -s -k -X DELETE https://localhost:9000/api/admin/buckets/my-bucket \
  -H "Authorization: Bearer $TOKEN"
```

**Force delete** — recursively deletes all objects in the bucket first:

```bash
curl -s -k -X DELETE "https://localhost:9000/api/admin/buckets/my-bucket?force=true" \
  -H "Authorization: Bearer $TOKEN"
```

**Response:** `204 No Content`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `force` | bool | `false` | When `true`, all objects are deleted before removing the bucket |

### List Bucket Objects

**GET** `/api/admin/buckets/{name}/objects`

```bash
curl -s -k "https://localhost:9000/api/admin/buckets/my-bucket/objects?max-keys=100" \
  -H "Authorization: Bearer $TOKEN"
```

**Query parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `prefix` | string | Filter objects by key prefix |
| `max-keys` | integer | Objects and directories per page: default 50, from 1 to 1000 |
| `continuation-token` | string | Opaque token from the previous page's `next_continuation_token` |

Keys below `prefix` that contain another `/` are grouped into a directory, listed
once in `common_prefixes`; a page that ends with a directory continues past all
of it. The answer is `{ "objects": [...], "common_prefixes": [...],
"is_truncated": bool, "next_continuation_token": "...", "complete": bool }`.
On a cluster node the bucket is listed from every node of the pool and merged by
key, as S3 ListObjects is, and `complete` is `false` when a node did not answer;
on a single node it is always `true`.

An unknown bucket answers `404 NoSuchBucket`; an existing bucket with nothing in
it answers `200` with an empty `objects` array.

### Bucket Stats

**GET** `/api/admin/bucket-stats`

Returns every bucket with its number of objects and their bytes.

```bash
curl -s -k https://localhost:9000/api/admin/bucket-stats \
  -H "Authorization: Bearer $TOKEN"
```

```json
{
  "buckets": [
    { "name": "photos", "objects_count": 1205, "storage_used_bytes": 4823110, "created_at": 1790812345000000000 }
  ],
  "total_storage_bytes": 4823110,
  "complete": true
}
```

`storage_used_bytes` is the size of the objects as clients see them, before
deduplication and replication; `created_at` is in nanoseconds since the epoch.
On a cluster node the numbers are the pool's, whichever node is asked: buckets
and objects are listed from every node of the pool and merged by key, so an
object counts once, at the size of its newest version, wherever its copies are.
`complete` is `false` when a node of the pool did not answer: objects only that
node holds are then missing from the numbers. On a single node it is always
`true`. Every bucket is listed in full, page by page, so the request takes
longer the more objects the buckets hold.

## Error Responses

Every failure returns JSON with a human-readable `error` and a stable,
machine-readable `code`. Branch on `code`, not on the message text:

```json
{ "error": "Bucket 'my-bucket' is not empty. Delete its objects first, or repeat the request with ?force=true",
  "code": "BucketNotEmpty" }
```

| Code | Status | Meaning |
|------|--------|---------|
| `Unauthorized` | `401` | Token missing, malformed or expired |
| `InvalidCredentials` | `401` | Login rejected: unknown account or wrong password |
| `Forbidden` | `403` | Authenticated, but the role is not SuperUser |
| `UserNotFound` | `404` | No user with this id or name |
| `UserAlreadyExists` | `409` | A user with this name already exists |
| `InvalidUsername` | `400` | Username breaks the format rules |
| `NoS3Credentials` | `404` | The user has no S3 credentials issued |
| `NoSuchBucket` | `404` | No bucket with this name |
| `BucketAlreadyExists` | `409` | A bucket with this name already exists |
| `BucketNotEmpty` | `409` | The bucket still holds objects and `force` was not set |
| `InvalidBucketName` | `400` | Bucket name breaks the S3 naming rules |
| `InvalidData` | `400` | Request body or parameters could not be interpreted |
| `InternalError` | `500` | Server-side fault (storage or IAM subsystem) |

`500` means a fault on the server, never a mistake in the request — anything the
caller can correct itself is reported as `4xx`.
