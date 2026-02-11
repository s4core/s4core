# Console Features

The S4 Console provides a visual interface for managing your S4 storage server.

## Login

The login page authenticates against the S4 Admin API using username and password. On success, a JWT token is stored in the browser and used for all subsequent requests.

Supported users: **root** (or any user with SuperUser role).

## Dashboard

The dashboard displays key metrics and system health information.

### System Health Widget

- Server uptime (formatted as days, hours, minutes)
- Server status indicator (green = running)

### Counters Widget

- Total number of buckets
- Total number of objects
- Storage used (human-readable format)
- Deduplication ratio (percentage of savings)

### Storage Distribution Widget

- Pie chart showing storage usage per bucket
- Visual breakdown of how storage is distributed

All dashboard data is sourced from the `/api/stats` and `/api/admin/bucket-stats` endpoints.

## Bucket Management

### Bucket List

- View all buckets sorted alphabetically
- See object count and storage size for each bucket
- **Create Bucket** — enter a bucket name in a modal dialog
- **Delete Bucket** — with confirmation dialog (bucket must be empty)

### Object Browser

- Browse objects inside any bucket
- Navigate folder-like prefixes using breadcrumbs
- View object details: name, size, type, last modified date
- Pagination for buckets with many objects (50 items per page)
- File and folder icons for visual clarity

Note: Object upload is not available through the console. Use the AWS CLI or S3 SDK to upload files.

## User Management

- View all IAM users with their roles and status
- **Create User** — set username, password, and role (Reader, Writer, SuperUser)
- **Edit User** — change password, role, or active status
- **Delete User** — with confirmation dialog
- Role badges for easy identification

## Key Management

- View all users with their S3 access keys
- **Generate Keys** — creates a new access key and secret key pair for a user
- **One-time display** — the secret key is shown only once in a modal with copy buttons
- **Delete Keys** — revoke S3 credentials with confirmation
- Only available for SuperUser accounts

## Theme

The console supports both dark and light themes. Toggle between them using the theme switch in the header.

## Notifications

All actions display toast notifications:
- Success messages (e.g., "Bucket created")
- Error messages (e.g., "Access denied")
