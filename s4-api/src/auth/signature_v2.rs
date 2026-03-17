// Copyright 2026 S4Core Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! AWS Signature V2 authentication.
//!
//! Implements AWS Signature Version 2 signing process for S3-compatible API.
//! This enables compatibility with legacy tools and SDKs that don't support V4.
//!
//! Based on: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAuthentication.html>

use crate::auth::Credentials;
use crate::s3::errors::S3Error;
use axum::http::{HeaderMap, Request};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use hmac::{Hmac, Mac};
use s4_features::iam::{IamStorage, User};
use sha1::Sha1;
use std::collections::BTreeMap;

type HmacSha1 = Hmac<Sha1>;

/// Sub-resources that must be included in the canonicalized resource for V2 signing.
const SUB_RESOURCES: &[&str] = &[
    "acl",
    "cors",
    "delete",
    "legal-hold",
    "lifecycle",
    "location",
    "logging",
    "notification",
    "object-lock",
    "partNumber",
    "policy",
    "requestPayment",
    "response-cache-control",
    "response-content-disposition",
    "response-content-encoding",
    "response-content-language",
    "response-content-type",
    "response-expires",
    "retention",
    "tagging",
    "torrent",
    "uploadId",
    "uploads",
    "versionId",
    "versioning",
    "versions",
    "website",
];

/// Authentication type for V2 requests.
#[derive(Debug, Clone)]
pub enum V2AuthType {
    /// Authorization header: `AWS AccessKeyId:Signature`
    Header,
    /// Query string presigned URL: `?AWSAccessKeyId=...&Signature=...&Expires=...`
    QueryString {
        /// Unix timestamp when the presigned URL expires.
        expires: i64,
    },
}

/// Extracted request data for V2 signature verification.
///
/// Same pattern as V4's `SignatureRequestData` — synchronous extraction
/// before async operations.
#[derive(Debug, Clone)]
pub struct SignatureV2RequestData {
    /// HTTP method (GET, PUT, etc.)
    pub method: String,
    /// URI path component
    pub path: String,
    /// URI query string (if any)
    pub query: Option<String>,
    /// All request headers
    pub headers: HeaderMap,
    /// Extracted access key ID
    pub access_key_id: String,
    /// Extracted signature (Base64-encoded)
    pub signature: String,
    /// Authentication type (header or query string)
    pub auth_type: V2AuthType,
}

/// Detects whether a request uses AWS Signature V2.
///
/// Returns `true` if:
/// - Authorization header starts with `"AWS "` (not `"AWS4-"`)
/// - Or query string has `AWSAccessKeyId` without `X-Amz-Algorithm`
pub fn detect_v2_auth(request: &Request<axum::body::Body>) -> bool {
    // Check Authorization header
    if let Some(auth) = request.headers().get("authorization") {
        if let Ok(auth_str) = auth.to_str() {
            if auth_str.starts_with("AWS ") && !auth_str.starts_with("AWS4-") {
                return true;
            }
        }
    }

    // Check query string for presigned V2
    if let Some(query) = request.uri().query() {
        let has_access_key = query.contains("AWSAccessKeyId");
        let has_v4_algorithm = query.contains("X-Amz-Algorithm");
        if has_access_key && !has_v4_algorithm {
            return true;
        }
    }

    false
}

impl SignatureV2RequestData {
    /// Extract V2 signature data from request synchronously.
    pub fn from_request(request: &Request<axum::body::Body>) -> Result<Self, S3Error> {
        let headers = request.headers().clone();
        let method = request.method().as_str().to_string();
        let path = request.uri().path().to_string();
        let query = request.uri().query().map(|s| s.to_string());

        // Try Authorization header first
        let auth_str = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        if let Some(ref auth_val) = auth_str {
            if auth_val.starts_with("AWS ") {
                return Self::from_header_auth(auth_val, method, path, query, headers);
            }
        }

        // Try query string presigned URL
        if let Some(ref query_str) = query {
            if query_str.contains("AWSAccessKeyId") {
                return Self::from_query_auth(&method, &path, query.as_deref(), &headers);
            }
        }

        Err(S3Error::AccessDenied)
    }

    /// Parse V2 header auth: `AWS AccessKeyId:Signature`
    fn from_header_auth(
        auth_str: &str,
        method: String,
        path: String,
        query: Option<String>,
        headers: HeaderMap,
    ) -> Result<Self, S3Error> {
        let credentials_part = auth_str.strip_prefix("AWS ").ok_or(S3Error::AccessDenied)?;

        let (access_key_id, signature) =
            credentials_part.split_once(':').ok_or(S3Error::SignatureDoesNotMatch)?;

        if access_key_id.is_empty() || signature.is_empty() {
            return Err(S3Error::SignatureDoesNotMatch);
        }

        Ok(Self {
            method,
            path,
            query,
            headers,
            access_key_id: access_key_id.to_string(),
            signature: signature.to_string(),
            auth_type: V2AuthType::Header,
        })
    }

    /// Parse V2 query string presigned URL.
    fn from_query_auth(
        method: &str,
        path: &str,
        query: Option<&str>,
        headers: &HeaderMap,
    ) -> Result<Self, S3Error> {
        let query_str = query.ok_or(S3Error::AccessDenied)?;
        let mut params = BTreeMap::new();

        for pair in query_str.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                params.insert(k.to_string(), url_decode(v));
            }
        }

        let access_key_id = params.get("AWSAccessKeyId").ok_or(S3Error::AccessDenied)?.clone();
        let signature = params.get("Signature").ok_or(S3Error::SignatureDoesNotMatch)?.clone();
        let expires_str = params.get("Expires").ok_or(S3Error::AccessDenied)?.clone();

        let expires: i64 = expires_str.parse().map_err(|_| S3Error::AccessDenied)?;

        // Validate expiration
        let now = chrono::Utc::now().timestamp();
        if now > expires {
            return Err(S3Error::AccessDenied);
        }

        Ok(Self {
            method: method.to_string(),
            path: path.to_string(),
            query: query.map(|s| s.to_string()),
            headers: headers.clone(),
            access_key_id,
            signature,
            auth_type: V2AuthType::QueryString { expires },
        })
    }
}

/// Verifies AWS Signature V2 for a request (legacy ENV mode).
pub fn verify_signature_v2(
    request: &Request<axum::body::Body>,
    credentials: &Credentials,
) -> Result<(), S3Error> {
    let req_data = SignatureV2RequestData::from_request(request)?;

    if req_data.access_key_id != credentials.access_key_id {
        return Err(S3Error::AccessDenied);
    }

    verify_v2_signature_with_secret(&req_data, &credentials.secret_access_key)
}

/// Verifies AWS Signature V2 using IAM database.
pub async fn verify_signature_v2_with_iam_data(
    req_data: SignatureV2RequestData,
    iam_storage: &IamStorage,
) -> Result<User, S3Error> {
    tracing::debug!(
        "V2 auth: method={}, path={}",
        req_data.method,
        req_data.path
    );
    tracing::debug!("V2 auth: access_key_id={}", req_data.access_key_id);

    // Look up user by access key
    let user = iam_storage
        .get_user_by_access_key_with_secret(&req_data.access_key_id)
        .await
        .map_err(|e| {
            tracing::warn!("IAM lookup error: {:?}", e);
            S3Error::AccessDenied
        })?
        .ok_or_else(|| {
            tracing::debug!("Access key not found in IAM: {}", req_data.access_key_id);
            S3Error::AccessDenied
        })?;

    let secret_key = user.secret_key.as_ref().ok_or_else(|| {
        tracing::warn!("User {} has no S3 credentials", user.username);
        S3Error::AccessDenied
    })?;

    verify_v2_signature_with_secret(&req_data, secret_key)?;

    Ok(user)
}

/// Core V2 signature verification against a known secret key.
fn verify_v2_signature_with_secret(
    req_data: &SignatureV2RequestData,
    secret_key: &str,
) -> Result<(), S3Error> {
    let string_to_sign = build_string_to_sign(req_data);

    tracing::debug!("V2 StringToSign:\n{}", string_to_sign);

    let calculated = compute_hmac_sha1_base64(secret_key.as_bytes(), string_to_sign.as_bytes());

    // Decode both signatures to bytes for constant-time comparison
    let received_bytes =
        BASE64.decode(&req_data.signature).map_err(|_| S3Error::SignatureDoesNotMatch)?;
    let calculated_bytes =
        BASE64.decode(&calculated).map_err(|_| S3Error::SignatureDoesNotMatch)?;

    if constant_time_eq(&calculated_bytes, &received_bytes) {
        return Ok(());
    }

    // Trailing slash retry — same fallback as V4.
    // NormalizePath may have stripped a trailing slash that the client signed.
    if !req_data.path.ends_with('/') {
        let alt_data = SignatureV2RequestData {
            path: format!("{}/", req_data.path),
            ..req_data.clone()
        };
        let alt_sts = build_string_to_sign(&alt_data);
        let alt_sig = compute_hmac_sha1_base64(secret_key.as_bytes(), alt_sts.as_bytes());
        if let Ok(alt_bytes) = BASE64.decode(&alt_sig) {
            if constant_time_eq(&alt_bytes, &received_bytes) {
                return Ok(());
            }
        }
    }

    tracing::debug!("V2 signature mismatch!");
    tracing::debug!("V2 received signature: {}", req_data.signature);
    tracing::debug!("V2 calculated signature: {}", calculated);

    Err(S3Error::SignatureDoesNotMatch)
}

/// Builds the StringToSign for AWS Signature V2.
///
/// Format:
/// ```text
/// HTTP-Verb\n
/// Content-MD5\n
/// Content-Type\n
/// Date (or Expires for query auth)\n
/// CanonicalizedAmzHeaders +
/// CanonicalizedResource
/// ```
fn build_string_to_sign(req_data: &SignatureV2RequestData) -> String {
    let content_md5 = header_value(&req_data.headers, "content-md5");
    let content_type = header_value(&req_data.headers, "content-type");

    let date_or_expires = match &req_data.auth_type {
        V2AuthType::Header => {
            // Use x-amz-date if present, otherwise Date header
            if req_data.headers.contains_key("x-amz-date") {
                // When x-amz-date is present, use empty string for Date in StringToSign
                String::new()
            } else {
                header_value(&req_data.headers, "date")
            }
        }
        V2AuthType::QueryString { expires } => expires.to_string(),
    };

    let amz_headers = canonicalize_amz_headers(&req_data.headers);
    let resource = canonicalize_resource(&req_data.path, req_data.query.as_deref());

    format!(
        "{}\n{}\n{}\n{}\n{}{}",
        req_data.method, content_md5, content_type, date_or_expires, amz_headers, resource
    )
}

/// Canonicalizes `x-amz-*` headers per the V2 spec.
///
/// 1. Lowercase all header names
/// 2. Sort alphabetically
/// 3. Combine multi-value headers with commas
/// 4. Unfold long headers (join continuation lines)
/// 5. Each header is `name:value\n`
fn canonicalize_amz_headers(headers: &HeaderMap) -> String {
    let mut amz_map: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for (name, value) in headers.iter() {
        let name_lower = name.as_str().to_lowercase();
        if name_lower.starts_with("x-amz-") {
            if let Ok(val_str) = value.to_str() {
                // Trim and collapse whitespace
                let normalized = val_str.split_whitespace().collect::<Vec<&str>>().join(" ");
                amz_map.entry(name_lower).or_default().push(normalized);
            }
        }
    }

    let mut result = String::new();
    for (name, values) in &amz_map {
        result.push_str(name);
        result.push(':');
        result.push_str(&values.join(","));
        result.push('\n');
    }

    result
}

/// Canonicalizes the resource string per the V2 spec.
///
/// Format: `/path` + sorted sub-resource query parameters
fn canonicalize_resource(path: &str, query: Option<&str>) -> String {
    let mut resource = path.to_string();

    if let Some(query_str) = query {
        let mut sub_resources: Vec<(String, Option<String>)> = Vec::new();

        for pair in query_str.split('&') {
            let (key, value) = match pair.split_once('=') {
                Some((k, v)) => (url_decode(k), Some(url_decode(v))),
                None => (url_decode(pair), None),
            };

            if SUB_RESOURCES.contains(&key.as_str()) {
                sub_resources.push((key, value));
            }
        }

        if !sub_resources.is_empty() {
            sub_resources.sort_by(|a, b| a.0.cmp(&b.0));
            resource.push('?');

            let parts: Vec<String> = sub_resources
                .iter()
                .map(|(k, v)| match v {
                    Some(val) if !val.is_empty() => format!("{}={}", k, val),
                    _ => k.clone(),
                })
                .collect();

            resource.push_str(&parts.join("&"));
        }
    }

    resource
}

/// Computes HMAC-SHA1 and returns Base64-encoded result.
fn compute_hmac_sha1_base64(key: &[u8], data: &[u8]) -> String {
    let mut mac = HmacSha1::new_from_slice(key).expect("HMAC-SHA1 can take key of any size");
    mac.update(data);
    let result = mac.finalize().into_bytes();
    BASE64.encode(result)
}

/// Extracts a header value as a string, returning empty string if missing.
fn header_value(headers: &HeaderMap, name: &str) -> String {
    headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("").to_string()
}

/// URL-decodes a percent-encoded string.
fn url_decode(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '%' {
            let mut hex_str = String::new();
            for _ in 0..2 {
                if let Some(hex_ch) = chars.next() {
                    hex_str.push(hex_ch);
                } else {
                    result.push('%');
                    result.push_str(&hex_str);
                    break;
                }
            }
            if hex_str.len() == 2 {
                if let Ok(byte) = u8::from_str_radix(&hex_str, 16) {
                    result.push(byte as char);
                } else {
                    result.push('%');
                    result.push_str(&hex_str);
                }
            }
        } else if ch == '+' {
            result.push(' ');
        } else {
            result.push(ch);
        }
    }

    result
}

/// Constant-time comparison of byte slices.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).fold(0, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderValue, Method, Request};

    #[test]
    fn test_detect_v2_auth_header() {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/bucket/key")
            .header("authorization", "AWS AKID:signature")
            .body(axum::body::Body::empty())
            .unwrap();

        assert!(detect_v2_auth(&req));
    }

    #[test]
    fn test_detect_v2_auth_not_v4() {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/bucket/key")
            .header(
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request",
            )
            .body(axum::body::Body::empty())
            .unwrap();

        assert!(!detect_v2_auth(&req));
    }

    #[test]
    fn test_detect_v2_query_string() {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/bucket/key?AWSAccessKeyId=AKID&Signature=sig&Expires=9999999999")
            .body(axum::body::Body::empty())
            .unwrap();

        assert!(detect_v2_auth(&req));
    }

    #[test]
    fn test_detect_v2_query_string_not_v4() {
        // V4 presigned URL has X-Amz-Algorithm
        let req = Request::builder()
            .method(Method::GET)
            .uri("/bucket/key?AWSAccessKeyId=AKID&X-Amz-Algorithm=AWS4-HMAC-SHA256")
            .body(axum::body::Body::empty())
            .unwrap();

        assert!(!detect_v2_auth(&req));
    }

    #[test]
    fn test_detect_v2_no_auth() {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/bucket/key")
            .body(axum::body::Body::empty())
            .unwrap();

        assert!(!detect_v2_auth(&req));
    }

    #[test]
    fn test_parse_header_auth() {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/bucket/key")
            .header(
                "authorization",
                "AWS AKIAIOSFODNN7EXAMPLE:frJIUN8DYpKDtOLCwo//yllqDzg=",
            )
            .header("date", "Tue, 27 Mar 2007 19:36:42 +0000")
            .body(axum::body::Body::empty())
            .unwrap();

        let data = SignatureV2RequestData::from_request(&req).unwrap();
        assert_eq!(data.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(data.signature, "frJIUN8DYpKDtOLCwo//yllqDzg=");
        assert!(matches!(data.auth_type, V2AuthType::Header));
    }

    #[test]
    fn test_parse_header_auth_invalid_format() {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/bucket/key")
            .header("authorization", "AWS invalid-no-colon")
            .body(axum::body::Body::empty())
            .unwrap();

        assert!(SignatureV2RequestData::from_request(&req).is_err());
    }

    #[test]
    fn test_parse_query_auth() {
        let expires = chrono::Utc::now().timestamp() + 3600; // 1 hour from now
        let uri = format!(
            "/bucket/key?AWSAccessKeyId=AKID&Signature=sig%3D&Expires={}",
            expires
        );
        let req = Request::builder()
            .method(Method::GET)
            .uri(uri.as_str())
            .body(axum::body::Body::empty())
            .unwrap();

        let data = SignatureV2RequestData::from_request(&req).unwrap();
        assert_eq!(data.access_key_id, "AKID");
        assert_eq!(data.signature, "sig=");
        assert!(matches!(data.auth_type, V2AuthType::QueryString { .. }));
    }

    #[test]
    fn test_query_auth_expired() {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/bucket/key?AWSAccessKeyId=AKID&Signature=sig&Expires=1000000000")
            .body(axum::body::Body::empty())
            .unwrap();

        // Expired timestamp (year 2001)
        assert!(SignatureV2RequestData::from_request(&req).is_err());
    }

    #[test]
    fn test_canonicalize_amz_headers_empty() {
        let headers = HeaderMap::new();
        assert_eq!(canonicalize_amz_headers(&headers), "");
    }

    #[test]
    fn test_canonicalize_amz_headers_sorted() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-date", HeaderValue::from_static("date-value"));
        headers.insert("x-amz-acl", HeaderValue::from_static("public-read"));

        let result = canonicalize_amz_headers(&headers);
        assert_eq!(result, "x-amz-acl:public-read\nx-amz-date:date-value\n");
    }

    #[test]
    fn test_canonicalize_amz_headers_ignores_non_amz() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("text/plain"));
        headers.insert("x-amz-acl", HeaderValue::from_static("private"));

        let result = canonicalize_amz_headers(&headers);
        assert_eq!(result, "x-amz-acl:private\n");
    }

    #[test]
    fn test_canonicalize_resource_simple() {
        assert_eq!(canonicalize_resource("/bucket/key", None), "/bucket/key");
    }

    #[test]
    fn test_canonicalize_resource_with_sub_resources() {
        let result = canonicalize_resource("/bucket/key", Some("uploadId=123&acl"));
        assert_eq!(result, "/bucket/key?acl&uploadId=123");
    }

    #[test]
    fn test_canonicalize_resource_ignores_non_sub_resources() {
        let result = canonicalize_resource("/bucket", Some("prefix=test&delimiter=/&acl"));
        assert_eq!(result, "/bucket?acl");
    }

    #[test]
    fn test_canonicalize_resource_with_version_id() {
        let result = canonicalize_resource("/bucket/key", Some("versionId=abc123"));
        assert_eq!(result, "/bucket/key?versionId=abc123");
    }

    #[test]
    fn test_compute_hmac_sha1_base64() {
        // Known test vector
        let result = compute_hmac_sha1_base64(b"key", b"data");
        assert!(!result.is_empty());
        // Verify it's valid base64
        assert!(BASE64.decode(&result).is_ok());
    }

    #[test]
    fn test_build_string_to_sign_header_auth() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "date",
            HeaderValue::from_static("Tue, 27 Mar 2007 19:36:42 +0000"),
        );
        headers.insert("content-type", HeaderValue::from_static("text/html"));
        headers.insert("content-md5", HeaderValue::from_static("md5value"));

        let req_data = SignatureV2RequestData {
            method: "PUT".to_string(),
            path: "/quotes/nelson".to_string(),
            query: None,
            headers,
            access_key_id: "AKID".to_string(),
            signature: "sig".to_string(),
            auth_type: V2AuthType::Header,
        };

        let sts = build_string_to_sign(&req_data);
        let expected = "PUT\nmd5value\ntext/html\nTue, 27 Mar 2007 19:36:42 +0000\n/quotes/nelson";
        assert_eq!(sts, expected);
    }

    #[test]
    fn test_build_string_to_sign_with_amz_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "date",
            HeaderValue::from_static("Tue, 27 Mar 2007 21:15:45 +0000"),
        );
        headers.insert("x-amz-acl", HeaderValue::from_static("public-read"));
        headers.insert(
            "x-amz-date",
            HeaderValue::from_static("Tue, 27 Mar 2007 21:20:26 +0000"),
        );

        let req_data = SignatureV2RequestData {
            method: "PUT".to_string(),
            path: "/bucket/key".to_string(),
            query: None,
            headers,
            access_key_id: "AKID".to_string(),
            signature: "sig".to_string(),
            auth_type: V2AuthType::Header,
        };

        let sts = build_string_to_sign(&req_data);
        // When x-amz-date is present, Date field in StringToSign should be empty
        assert!(sts.starts_with("PUT\n\n\n\n"));
        assert!(sts.contains("x-amz-acl:public-read\n"));
        assert!(sts.contains("x-amz-date:Tue, 27 Mar 2007 21:20:26 +0000\n"));
        assert!(sts.ends_with("/bucket/key"));
    }

    #[test]
    fn test_build_string_to_sign_query_auth() {
        let headers = HeaderMap::new();
        let req_data = SignatureV2RequestData {
            method: "GET".to_string(),
            path: "/bucket/key".to_string(),
            query: Some("AWSAccessKeyId=AKID&Signature=sig&Expires=1175139620".to_string()),
            headers,
            access_key_id: "AKID".to_string(),
            signature: "sig".to_string(),
            auth_type: V2AuthType::QueryString {
                expires: 1175139620,
            },
        };

        let sts = build_string_to_sign(&req_data);
        // For query auth, the date field should be the Expires value
        assert!(sts.contains("1175139620"));
    }

    #[test]
    fn test_end_to_end_v2_signing() {
        // Create a request, sign it with a known key, then verify
        let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let access_key = "AKIAIOSFODNN7EXAMPLE";

        let mut headers = HeaderMap::new();
        headers.insert(
            "date",
            HeaderValue::from_static("Tue, 27 Mar 2007 19:36:42 +0000"),
        );

        let req_data = SignatureV2RequestData {
            method: "GET".to_string(),
            path: "/mybucket/mykey".to_string(),
            query: None,
            headers: headers.clone(),
            access_key_id: access_key.to_string(),
            signature: String::new(), // Will be filled in
            auth_type: V2AuthType::Header,
        };

        // Compute signature
        let sts = build_string_to_sign(&req_data);
        let signature = compute_hmac_sha1_base64(secret_key.as_bytes(), sts.as_bytes());

        // Verify the signature
        let signed_req_data = SignatureV2RequestData {
            signature,
            ..req_data
        };

        assert!(verify_v2_signature_with_secret(&signed_req_data, secret_key).is_ok());
    }

    #[test]
    fn test_end_to_end_v2_wrong_key() {
        let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let wrong_key = "WRONG_SECRET_KEY";

        let mut headers = HeaderMap::new();
        headers.insert(
            "date",
            HeaderValue::from_static("Tue, 27 Mar 2007 19:36:42 +0000"),
        );

        let req_data = SignatureV2RequestData {
            method: "GET".to_string(),
            path: "/mybucket/mykey".to_string(),
            query: None,
            headers,
            access_key_id: "AKID".to_string(),
            signature: String::new(),
            auth_type: V2AuthType::Header,
        };

        // Sign with wrong key
        let sts = build_string_to_sign(&req_data);
        let signature = compute_hmac_sha1_base64(wrong_key.as_bytes(), sts.as_bytes());

        let signed_req_data = SignatureV2RequestData {
            signature,
            ..req_data
        };

        // Verify with correct key should fail
        assert!(verify_v2_signature_with_secret(&signed_req_data, secret_key).is_err());
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hell"));
    }

    #[test]
    fn test_url_decode() {
        assert_eq!(url_decode("hello"), "hello");
        assert_eq!(url_decode("hello%20world"), "hello world");
        assert_eq!(url_decode("a%3Db"), "a=b");
        assert_eq!(url_decode("test+file"), "test file");
    }
}
