# Multipart Upload

S4 supports S3-compatible multipart uploads for large files. The AWS CLI and SDKs use multipart upload automatically for files larger than a configurable threshold (typically 8MB).

## How It Works

1. **Initiate** — create a multipart upload and receive an upload ID
2. **Upload Parts** — upload the file in chunks, each identified by a part number
3. **Complete** — finalize the upload by listing all parts

Parts are stored individually in volume files. On completion, they are logically combined without copying data.

## AWS CLI

The AWS CLI handles multipart upload automatically:

```bash
# Large files are automatically uploaded using multipart
aws --endpoint-url http://localhost:9000 s3 cp large-file.zip s3://mybucket/large-file.zip
```

To control the multipart threshold and chunk size:

```bash
aws configure set s3.multipart_threshold 64MB
aws configure set s3.multipart_chunksize 16MB
```

## Manual Multipart Upload (s3api)

```bash
# 1. Initiate multipart upload
UPLOAD_ID=$(aws --endpoint-url http://localhost:9000 s3api create-multipart-upload \
  --bucket mybucket \
  --key large-file.zip \
  --query 'UploadId' --output text)

# 2. Upload parts
aws --endpoint-url http://localhost:9000 s3api upload-part \
  --bucket mybucket \
  --key large-file.zip \
  --part-number 1 \
  --upload-id $UPLOAD_ID \
  --body part1.bin

aws --endpoint-url http://localhost:9000 s3api upload-part \
  --bucket mybucket \
  --key large-file.zip \
  --part-number 2 \
  --upload-id $UPLOAD_ID \
  --body part2.bin

# 3. Complete multipart upload
aws --endpoint-url http://localhost:9000 s3api complete-multipart-upload \
  --bucket mybucket \
  --key large-file.zip \
  --upload-id $UPLOAD_ID \
  --multipart-upload '{"Parts":[{"PartNumber":1,"ETag":"etag1"},{"PartNumber":2,"ETag":"etag2"}]}'
```

## Abort Multipart Upload

If you need to cancel an in-progress upload:

```bash
aws --endpoint-url http://localhost:9000 s3api abort-multipart-upload \
  --bucket mybucket \
  --key large-file.zip \
  --upload-id $UPLOAD_ID
```

## Upload Size Limit

The maximum upload size per request is controlled by the `S4_MAX_UPLOAD_SIZE` environment variable (default: `5GB`). This applies to both single-part and multipart uploads.
