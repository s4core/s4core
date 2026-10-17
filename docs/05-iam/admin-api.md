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

`expires_at` is the token's expiry, `S4_JWT_LIFETIME_HOURS` after the login (24 hours by default).
A token works only while its account keeps the password, role and active status it had at login —
see [Sessions](roles-and-permissions.md#sessions).

### Logout

**POST** `/api/admin/logout`

Any account, not only SuperUser. Ends every session of the caller's account, the one of this token
included.

```bash
curl -s -k -X POST https://localhost:9000/api/admin/logout \
  -H "Authorization: Bearer $TOKEN"
```

**Response:** `204 No Content`

### List Users

**GET** `/api/admin/users`

```bash
curl -s -k "https://localhost:9000/api/admin/users?max-keys=100" \
  -H "Authorization: Bearer $TOKEN"
```

**Response:** `{"users": [...], "is_truncated": true, "next_continuation_token": "..."}` — root
first, then the users in the order of their ids; password hashes and secret keys are never
returned. Without `max-keys` every user is in one answer. With `max-keys` (1 to 1000) the list
comes a page at a time: while `is_truncated` is `true`, ask again with
`continuation-token=<next_continuation_token>`.

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

### Revoke Sessions

**POST** `/api/admin/users/{id}/sessions/revoke`

Ends every session of the user: tokens issued before the call answer `401`. The password, role and
S3 credentials stay as they are; the user logs in again for a new token. Works for root too.

```bash
curl -s -k -X POST https://localhost:9000/api/admin/users/<user-id>/sessions/revoke \
  -H "Authorization: Bearer $TOKEN"
```

**Response:** `204 No Content`

### IAM Adoption Report

**GET** `/api/admin/iam/adoption`

Cluster mode: what each node keeps of its own after an upgrade from an earlier release — IAM users
created through it alone — for you to decide on (see
[Upgrading a cluster](roles-and-permissions.md#cluster-mode)). Every node of the pool is asked.

```bash
curl -s -k https://localhost:9000/api/admin/iam/adoption -H "Authorization: Bearer $TOKEN"
```

**Response:**
```json
{
  "mode": "cluster",
  "state": "pending",
  "changes_open": false,
  "node": "node-1",
  "report_id": "4b8f0c2e9a1d7e3f5c6b2a9d0e1f3a4b",
  "complete": true,
  "pool_size": 3,
  "majority": 2,
  "nodes": [
    { "node_id": "…", "node_name": "node-1", "answered": true, "own_users": 2, "digest": "…" }
  ],
  "users": [
    {
      "username": "bob",
      "nodes": ["node-1", "node-2"],
      "on_majority": true,
      "conflicts": ["ids", "roles"],
      "records": [
        { "id": "…", "copies": [ { "node": "node-1", "role": "Writer", "is_active": true,
          "created_at": "…", "updated_at": "…", "access_key": "S4AK…" } ] }
      ]
    }
  ],
  "conflicts": 1,
  "not_on_majority": 0,
  "decision": null
}
```

`state` is `adopted` once IAM changes are open on this node, `pending` while they are not.
`complete` is `false` when a node did not answer: its `error` says why. No secret key or password
hash is reported. `decision` is the last decision taken, when there is one. A single node answers
`"mode": "standalone", "state": "adopted"`.

### Take Over IAM

**POST** `/api/admin/iam/adoption`

Takes over the users the nodes keep of their own, as decided on the report named by `report_id`.

```bash
curl -s -k -X POST https://localhost:9000/api/admin/iam/adoption \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"report_id": "4b8f0c2e9a1d7e3f5c6b2a9d0e1f3a4b", "mode": "majority"}'
```

| `mode` | Takes over |
|--------|-----------|
| `majority` | The users whose name a majority of nodes holds |
| `union` | Every user some node holds |
| `users` | Exactly the users listed by id in `"users": ["<id>", …]` |

Of several users of one name the one created first keeps it; each other is renamed
`<name>~<first 8 characters of its id>` and disabled. Users left out are dropped.

**Response:** the users taken over (`adopted`, each with `renamed_from` when renamed), those left
out (`left_out`), and those the cluster held already (`already_adopted`, from a decision repeated
after one stopped half way). IAM changes open once every node has dropped what it kept.

### IAM Audit Log

**GET** `/api/admin/iam/audit`

Every change of IAM — a user created, changed or deleted, S3 credentials issued or deleted,
sessions revoked, root's sessions ended, the cluster taking over the users of an earlier release,
secrets encrypted again with a new key — in the order the changes were made. In cluster mode every
node holds the whole log.

```bash
curl -s -k "https://localhost:9000/api/admin/iam/audit?max-keys=100" \
  -H "Authorization: Bearer $TOKEN"
```

**Response:**

```json
{
  "entries": [
    {
      "id": "00000000000000000042_00001791504000123456_9f3a01bc",
      "at": "2026-10-09T10:00:00.123456Z",
      "actor": { "id": "8c1d…", "name": "root" },
      "action": "user_updated",
      "target_id": "5b2e…",
      "target": "alice",
      "through": "node-2",
      "epoch": 42,
      "details": { "role": "Writer -> Reader", "password": "changed" }
    }
  ],
  "is_truncated": false
}
```

`action` is one of `user_created`, `user_updated`, `user_deleted`, `credentials_issued`,
`credentials_deleted`, `sessions_revoked`, `root_sessions_revoked`, `iam_adopted`,
`secrets_resealed`, `audit_purged`. `through` is the node the change went through, `epoch` the
cluster's turn it was made in (`0` on a single node). No password, secret key or hash of one is
ever recorded. Up to 1000 entries a page, 1000 by default; while `is_truncated` is `true`, ask again
with `continuation-token=<next_continuation_token>`. Entries older than
`S4_IAM_AUDIT_RETENTION_DAYS` (90 by default) are deleted.

### IAM Status

**GET** `/api/admin/iam/status`

How IAM stands on the node that answers: whether it checks the credentials of IAM users now, how
every node of the cluster stands with its copy of the users, which node holds the cluster's turn
for a change, and whether IAM changes are open. The node answers from what it holds, without asking
another node, so it answers while the cluster is split too. SuperUser only.

```bash
curl -s -k https://localhost:9000/api/admin/iam/status \
  -H "Authorization: Bearer $TOKEN"
```

**Response (cluster mode):**

```json
{
  "mode": "cluster",
  "node": "node-1",
  "credentials": { "state": "serving", "confirmed_ago_ms": 180, "max_staleness_secs": 30 },
  "epoch": { "wall_ms": 1791545069225, "hlc": "HLC(1791545069225.0000@…)" },
  "agreeing": 2,
  "majority": 2,
  "nodes": [
    { "node": "node-1", "node_id": "…", "local": true, "status": "alive",
      "epoch": { "wall_ms": 1791545069225, "hlc": "…" }, "epoch_behind_ms": 0, "agrees": true },
    { "node": "node-2", "node_id": "…", "local": false, "status": "alive",
      "epoch": { "wall_ms": 1791545069225, "hlc": "…" }, "epoch_behind_ms": 0, "agrees": true },
    { "node": "node-3", "node_id": "…", "local": false, "status": "dead",
      "epoch": { "wall_ms": 1791545061870, "hlc": "…" }, "epoch_behind_ms": 7355, "agrees": false }
  ],
  "lease": { "max_epoch": 583, "held": { "epoch": 584, "holder": "node-2", "remaining_ms": 4210 } },
  "adoption": "adopted"
}
```

- `credentials.state` — `serving`; `starting` until most nodes first agreed with this node's copy
  after it started; `stale` once none has for longer than `S4_IAM_MAX_STALENESS_SECS`. In the last
  two the node answers IAM users `503`. `confirmed_ago_ms` is how long ago most nodes last agreed,
  the value `s4_iam_replica_lag_seconds` reports.
- `nodes` — every node of the cluster as this node sees it: alive or not, the newest change its copy
  holds (`epoch`) and whether its copy `agrees` with this node's. `agrees` is what counts:
  `epoch_behind_ms` can stay above `0` on a node that misses nothing — a user deleted before that
  node ever held it moves the others' epoch and not its own.
- `lease.held` — the change of IAM under way, if any: the node making it and how much longer its
  turn lasts as this node granted it; `null` when none is.
- `adoption` — `adopted` once IAM changes are open, `pending` while the cluster waits for you to
  take over users an earlier release kept on one node.

A single node answers `{"mode": "standalone", "credentials": {"state": "serving"}, "adoption":
"adopted"}`.

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
| `Unauthorized` | `401` | Token missing, malformed or expired, or its account was deleted, deactivated or changed since the token was issued |
| `InvalidCredentials` | `401` | Login rejected: unknown account or wrong password |
| `Forbidden` | `403` | Authenticated, but the role is not SuperUser |
| `UserNotFound` | `404` | No user with this id or name |
| `UserAlreadyExists` | `409` | A user with this name already exists |
| `RootFromEnvironment` | `409` | The request would change root, which `S4_ROOT_USERNAME` and `S4_ROOT_PASSWORD` configure |
| `IamAdoptionPending` | `409` | Cluster mode: some node still keeps IAM users an earlier release stored on it alone, and IAM changes wait until you decide on them (see [Cluster Mode](roles-and-permissions.md#cluster-mode)) |
| `IamAdoptionNotPending` | `409` | No node keeps IAM users of its own: there is nothing to take over |
| `IamAdoptionReportChanged` | `409` | The nodes no longer answer the report named by `report_id`: read it again |
| `InvalidIamAdoption` | `400` | The decision lists a user no node holds, or lists users with a mode other than `users` |
| `IamAdoptionIncomplete` | `503` | A node of the pool did not answer: a decision would not cover it. Retry when it is back |
| `InvalidUsername` | `400` | Username breaks the format rules |
| `NoS3Credentials` | `404` | The user has no S3 credentials issued |
| `NoSuchBucket` | `404` | No bucket with this name |
| `BucketAlreadyExists` | `409` | A bucket with this name already exists |
| `BucketNotEmpty` | `409` | The bucket still holds objects and `force` was not set |
| `InvalidBucketName` | `400` | Bucket name breaks the S3 naming rules |
| `InvalidData` | `400` | Request body or parameters could not be interpreted |
| `InvalidArgument` | `400` | `max-keys` is not between 1 and 1000, or `continuation-token` is not one a listing answered |
| `InternalError` | `500` | Server-side fault (storage or IAM subsystem) |
| `ServiceUnavailable` | `503` | Cluster mode: too few nodes answered to confirm the change or the read; retry when they are back. Also when another IAM change held the cluster's turn for the whole wait; the answer then carries `Retry-After` in seconds |

`500` means a fault on the server, never a mistake in the request — anything the
caller can correct itself is reported as `4xx`.
