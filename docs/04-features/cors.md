# CORS Configuration

S4 supports S3-compatible CORS (Cross-Origin Resource Sharing) for browser-based access to your storage.

## What is CORS?

CORS allows web browsers to make requests to S4 from a different origin (domain). Without CORS, browsers block cross-origin requests for security reasons.

If you are building a web application that accesses S4 directly from the browser (e.g., uploading files via JavaScript), you need to configure CORS.

## Set CORS Configuration

```bash
curl -X PUT "http://localhost:9000/mybucket?cors" \
  -H "Content-Type: application/xml" \
  -d '<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
  <CORSRule>
    <AllowedOrigin>https://example.com</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedHeader>*</AllowedHeader>
    <MaxAgeSeconds>3600</MaxAgeSeconds>
  </CORSRule>
</CORSConfiguration>'
```

## Get CORS Configuration

```bash
curl "http://localhost:9000/mybucket?cors"
```

## Delete CORS Configuration

```bash
curl -X DELETE "http://localhost:9000/mybucket?cors"
```

## CORS Rule Fields

| Field | Required | Description |
|-------|----------|-------------|
| `AllowedOrigin` | Yes | Origins allowed to make requests (e.g., `https://example.com` or `*`) |
| `AllowedMethod` | Yes | HTTP methods allowed (`GET`, `PUT`, `POST`, `DELETE`, `HEAD`) |
| `AllowedHeader` | No | Headers the browser is allowed to send (use `*` for all) |
| `ExposeHeader` | No | Response headers exposed to the browser |
| `MaxAgeSeconds` | No | How long the browser should cache the preflight response |

## Example: Allow All Origins (Development)

```xml
<CORSConfiguration>
  <CORSRule>
    <AllowedOrigin>*</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedMethod>DELETE</AllowedMethod>
    <AllowedMethod>HEAD</AllowedMethod>
    <AllowedHeader>*</AllowedHeader>
    <MaxAgeSeconds>3600</MaxAgeSeconds>
  </CORSRule>
</CORSConfiguration>
```

## Example: Restrict to Specific Domain (Production)

```xml
<CORSConfiguration>
  <CORSRule>
    <AllowedOrigin>https://app.example.com</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedHeader>Authorization</AllowedHeader>
    <AllowedHeader>Content-Type</AllowedHeader>
    <ExposeHeader>ETag</ExposeHeader>
    <ExposeHeader>x-amz-version-id</ExposeHeader>
    <MaxAgeSeconds>86400</MaxAgeSeconds>
  </CORSRule>
</CORSConfiguration>
```
