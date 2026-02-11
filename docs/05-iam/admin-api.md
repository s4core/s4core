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

## Error Responses

| Status Code | Meaning |
|-------------|---------|
| `400` | Bad request (invalid input) |
| `401` | Unauthorized (missing or invalid token) |
| `403` | Forbidden (insufficient permissions) |
| `404` | User not found |
| `409` | Conflict (username already exists) |
| `500` | Internal server error |
