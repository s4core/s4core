# Data Flow

This page describes how data moves through S4 during common operations.

## Write (PUT Object)

```
Client --PUT--> S4 API
                  |
                  v
            Compute SHA-256 hash
                  |
                  v
            Check Deduplicator
           /              \
     (duplicate)       (new data)
          |                |
          |                v
          |         Write to volume file
          |                |
          v                v
     Increment         Store IndexRecord
     ref count         in redb
          |                |
          v                v
            fsync to disk
                  |
                  v
            Return HTTP 200
```

**Key points:**
- SHA-256 is computed on the full object body
- If the hash already exists in the deduplicator, the data is not written again (saving disk space)
- The reference count is incremented for the existing blob
- `fsync` is called before returning success, guaranteeing durability

## Read (GET Object)

```
Client --GET--> S4 API
                  |
                  v
            Lookup IndexRecord in redb
                  |
                  v
            Read blob from volume file
                  |
                  v
            Verify CRC32 checksum
                  |
                  v
            Return data to client
```

**Key points:**
- Metadata lookup in redb is fast (B-tree index)
- CRC32 is verified on every read to detect bit-rot
- For inline objects (< 4KB), data is read directly from redb

## Delete (DELETE Object)

### Without Versioning

```
Client --DELETE--> S4 API
                     |
                     v
               Mark as tombstone
                     |
                     v
               Remove IndexRecord from redb
                     |
                     v
               Decrement dedup ref count
                     |
                     v
               Return HTTP 204
```

### With Versioning Enabled

```
Client --DELETE--> S4 API
                     |
                     v
               Create Delete Marker
               (new version entry)
                     |
                     v
               Return HTTP 204
               + x-amz-delete-marker: true
```

The actual object data remains in the volume file. Accessing the key returns a 404, but previous versions are still accessible by version ID.

## Multipart Upload

```
Client --CreateMultipartUpload--> S4 API --> Return Upload ID

Client --UploadPart (1..N)------> S4 API --> Store each part in volume

Client --CompleteMultipartUpload-> S4 API --> Combine parts virtually
                                              Store final IndexRecord
                                              Return HTTP 200
```

Parts are stored as individual entries. On completion, they are logically combined without copying data.
