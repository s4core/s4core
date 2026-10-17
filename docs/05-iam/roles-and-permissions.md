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

1. The **root user** logs in with `S4_ROOT_USERNAME` / `S4_ROOT_PASSWORD`
2. The root user has the **SuperUser** role
3. All S3 API requests are checked against IAM permissions
4. The Admin API becomes available at `/api/admin/`

## Root User

| Setting | Variable | Default |
|---------|----------|---------|
| Username | `S4_ROOT_USERNAME` | `root` |
| Password | `S4_ROOT_PASSWORD` | (required to enable IAM) |

Root is configured by these variables and is not stored. To change its password, set a new
`S4_ROOT_PASSWORD` and restart the server: the new password works from that start, the old one is
refused, and root's sessions from before the change end. Root's id is derived from its name, so it
stays the same across restarts and password changes, and is the same on every node.

The Admin API cannot change, delete or issue S3 credentials to root: such requests answer `409`
with code `RootFromEnvironment`. For S3 access with SuperUser rights, use `S4_ACCESS_KEY_ID` and
`S4_SECRET_ACCESS_KEY` (see [Legacy Credentials](#legacy-credentials)). Nobody else can be created
under root's name.

Earlier releases stored root as a user. That stored user can no longer log in and is not listed;
S3 credentials issued to it keep working until it is deleted (**DELETE**
`/api/admin/users/{id}` with its old id). The server names it in the log at startup.

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

The token is valid for 24 hours by default; `S4_JWT_LIFETIME_HOURS` sets another lifetime.

### Sessions

A token refers to its account, and every admin request is checked against the account: the role
is the one the account has now, not the one it had at login. A session ends at once when its user
is deleted or deactivated, gets a new password or a new role, and when:

- a SuperUser ends all sessions of a user: **POST** `/api/admin/users/{id}/sessions/revoke`
  ([Admin API](admin-api.md#revoke-sessions)); the password, role and S3 credentials stay as they
  are;
- the user logs out: **POST** `/api/admin/logout` ends every session of the caller's account, on
  every device.

After either, the user logs in again for a new token. Tokens issued by releases before this check
are refused: every user logs in again after the upgrade.

S3 requests are checked against the account at every request, so a deleted or deactivated user's
S3 credentials stop working at once.

## Cluster Mode

Users, their passwords, roles, active flag and S3 credentials are kept on every node of the
cluster, so behind a load balancer a user logs in, uses an admin session and signs S3 requests
through any node.

- **A change counts once most nodes hold it.** Creating, changing or deleting a user, issuing or
  deleting S3 credentials and revoking sessions answer success only after a majority of the
  cluster's nodes stored the change (two of three, four of six). The other nodes get it at once
  when they are up, and later otherwise.
- **Without a majority, changes are refused** with `503 ServiceUnavailable`: retry when enough
  nodes are back. Each node checks logins, admin sessions and S3 signatures against its own copy,
  without asking another node, as long as most nodes of the cluster agree with that copy.
- **A node that was down catches up before it checks credentials.** After a start, a node answers
  the logins, admin sessions and S3 requests of IAM users with `503 ServiceUnavailable` and
  `Retry-After` until it has compared its copy with most nodes of the cluster and taken every change
  it missed — usually within a second or two of joining. It never accepts a user or key deleted
  while it was down. A client behind a load balancer retries through another node.
- **A node cut off from most of the cluster stops checking IAM credentials** after
  `S4_IAM_MAX_STALENESS_SECS` (30 s) without most nodes agreeing with its copy, and answers `503`
  until they do again: a user deleted or a key removed through the rest of the cluster works on it
  for at most that long. `0` keeps it checking them for as long as it is cut off.
- **Root and the S3 credentials of the environment always work**, on a node that is catching up or
  cut off too: they are the way in when IAM users cannot be checked.
- **One user name, one user**: creating a name that exists answers `409` through every node, and
  of several creations of one name at the same moment through different nodes exactly one
  succeeds.
- **Changes take turns.** One change of IAM runs at a time across the cluster: two changes of one
  user at the same moment through different nodes — a new password through one, a new role through
  another — both hold. A change that waits longer than `S4_IAM_LEASE_WAIT_SECS` (10 s) for the
  one before it answers `503 ServiceUnavailable` with `Retry-After`. A node that stalls or is cut
  off in the middle of a change holds the others up for at most `S4_IAM_LEASE_SECS` (5 s), and
  whatever it writes after that is refused by every node.
- **Root is the same on every node**: it logs in through any node, and its token is accepted by
  every node with the same `S4_ROOT_PASSWORD` and `S4_JWT_SECRET`. Revoking root's sessions or
  logging out as root ends them on every node.
- **Every node is configured alike.** `S4_ROOT_PASSWORD`, `S4_JWT_SECRET` and
  `S4_IAM_ENCRYPTION_KEY` must be the same on every node. A node that differs is named in its log,
  in the cluster health — `critical`, with an `iam_config` section listing the setting and the
  node — and in `s4_iam_config_mismatch`. A node without the key a user's S3 secret is encrypted
  with answers that user's S3 requests `503`; the others check them.
- **Every change is in the audit log** of every node: who made it, what it did, to whom and through
  which node — read it with `GET /api/admin/iam/audit`. No password or secret key is ever written
  there.
- **Every node of the cluster must run this release.** A node of an earlier release gets no IAM
  changes, and changes need a majority of nodes that do.
- **How a node stands** — whether it checks IAM credentials now, which nodes agree with its copy,
  which node is making a change — is answered by `GET /api/admin/iam/status` on that node (see
  [Admin API → IAM Status](admin-api.md#iam-status)).

**Upgrading a cluster.** Earlier releases kept IAM users only on the node they were created
through. Nothing in that data tells a user created only through node-2 from one deleted through
node-1 and left on node-2, so the cluster does not merge it by itself: you decide.

1. Upgrade every node. Until you decide, a node that holds such users keeps serving them as before,
   only through that node, and every node refuses IAM changes with `409 IamAdoptionPending`;
   `/api/admin/cluster/health` answers `degraded` and `s4_iam_adoption_pending` is `1`. Root and the
   S3 credentials of the environment work throughout. Admin tokens of the earlier release are
   refused: log in again.
2. Read the report, through any node: `GET /api/admin/iam/adoption`. It lists every user name with
   the nodes that hold it, each node's record of it — role, active flag, dates, access key — whether
   a majority of the nodes holds it, and its conflicts: `ids` (each node created its own user of that
   name), `roles`, `active`, `passwords` (copies of one user with different passwords; users of
   different ids are not compared — each node salted its own hash). Every node must answer.
3. Decide, naming the report you read:
   `POST /api/admin/iam/adoption` with `{"report_id": "…", "mode": "majority"}` takes over the
   users whose name a majority of nodes holds; `"union"` takes every user; `"users"` with
   `"users": ["<id>", …]` takes exactly the listed ones. Of several users of one name the one
   created first keeps it; each other is renamed `<name>~<first 8 characters of its id>` and
   disabled, with its S3 credentials — enable or delete it yourself. Users left out are dropped.
4. Each node drops what it kept on its own once the decision reaches it, and IAM changes open once
   every node has — usually within a few seconds. `GET /api/admin/iam/adoption` then answers
   `"state": "adopted"` and shows the decision.

A cluster whose nodes hold no such users — a new one, or one with only the stored `root` of an
earlier release, which does not count — opens IAM changes by itself once every node is up and runs
this release. Going back to an earlier release after the decision brings no IAM user back: the
earlier release reads only root and the S3 credentials of the environment. The decision keeps the
report it was taken on, which `GET /api/admin/iam/adoption` shows.

**A single node that becomes a cluster node** brings its IAM users as its own: they appear in the
report like those of an earlier release, and the cluster takes them over only as you decide.

**Upgrading a single node.** At its first start the node converts the IAM users it stores into the
current format, with their passwords, roles and S3 credentials. An earlier release does not read
the converted users: to go back, restore the data directory from a backup taken before the
upgrade.

## Legacy Credentials

When `S4_ACCESS_KEY_ID` and `S4_SECRET_ACCESS_KEY` are set alongside IAM, they continue to work as a fallback with SuperUser access. This ensures backward compatibility during migration to IAM.
