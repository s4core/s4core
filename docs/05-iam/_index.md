# IAM & Admin API

S4 includes a built-in Identity and Access Management (IAM) system with role-based access control (RBAC). When enabled, it provides multi-user authentication, fine-grained permissions, and an Admin API for user management.

> **Cluster mode:** users, their passwords, roles and S3 credentials are kept on every node, and a
> change counts once most nodes hold it. What that means behind a load balancer, while nodes are
> down and after an upgrade: [Roles and Permissions → Cluster Mode](roles-and-permissions.md#cluster-mode).
