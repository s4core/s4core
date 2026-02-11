# High-Level Design

S4 uses a three-layer architecture inspired by the Bitcask storage model.

## System Layers

```
+------------------------------------------+
|  Layer 1: Network & Protocol (Axum)      |
|  HTTP/1.1, HTTP/2                        |
|  AWS Signature V4 Authentication         |
|  S3 API Compatibility                    |
+--------------------+---------------------+
                     |
+--------------------v---------------------+
|  Layer 2: Business Logic                 |
|  Deduplication Engine (CAS, SHA-256)     |
|  Atomic Directory Operations             |
|  IAM / ACL Policy Checks                |
+--------------------+---------------------+
                     |
+--------------------v---------------------+
|  Layer 3: Storage Engine (Bitcask-style) |
|  Hot Index: redb (ACID metadata)         |
|  Cold Data: Append-only volume files     |
|  Compactor (background GC)              |
+------------------------------------------+
```

## Design Principles

- **Single-node focus** — optimized for maximum performance on one machine, no distributed consensus overhead
- **Append-only writes** — all data is written sequentially for maximum throughput
- **ACID metadata** — redb provides crash-safe metadata storage
- **Strict consistency** — every write is fsynced before returning success
- **Content-addressable** — SHA-256 hashing enables automatic deduplication
