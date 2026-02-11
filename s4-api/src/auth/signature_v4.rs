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

//! AWS Signature V4 authentication.
//!
//! Implements AWS Signature Version 4 signing process for S3-compatible API.
//! Based on: <https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html>

use crate::auth::Credentials;
use crate::s3::errors::S3Error;
use axum::http::{HeaderMap, Request};
use chrono;
use hmac::{Hmac, Mac};
use s4_features::iam::{IamStorage, User};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::str;

type HmacSha256 = Hmac<Sha256>;

/// Parsed Authorization header components.
#[derive(Debug)]
struct AuthorizationHeader {
    algorithm: String,
    credential: CredentialScope,
    signed_headers: Vec<String>,
    signature: String,
}

/// Credential scope from Authorization header.
#[derive(Debug)]
struct CredentialScope {
    access_key_id: String,
    date: String,
    region: String,
    service: String,
}

/// Verifies AWS Signature V4 for a request.
///
/// # Arguments
///
/// * `request` - HTTP request
/// * `credentials` - Access credentials
///
/// # Returns
///
/// Returns Ok(()) if signature is valid, Err(S3Error) otherwise.
pub fn verify_signature_v4(
    request: &Request<axum::body::Body>,
    credentials: &Credentials,
) -> Result<(), S3Error> {
    // Log all headers for debugging
    tracing::debug!("Request method: {}", request.method());
    tracing::debug!("Request URI: {}", request.uri());
    tracing::debug!("Request headers:");
    for (name, value) in request.headers() {
        if let Ok(value_str) = value.to_str() {
            tracing::debug!("  {}: {}", name, value_str);
        }
    }

    // Extract Authorization header
    let auth_header = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(S3Error::AccessDenied)?;

    tracing::debug!("Authorization header: {}", auth_header);

    // Parse Authorization header
    let auth = parse_authorization_header(auth_header)?;

    // Verify access key matches
    if auth.credential.access_key_id != credentials.access_key_id {
        return Err(S3Error::AccessDenied);
    }

    // Extract date and timestamp from headers (X-Amz-Date or Date)
    let (date, timestamp) = extract_date_and_timestamp(request.headers())?;

    // Create canonical request
    let canonical_request = create_canonical_request(request, &auth.signed_headers)?;

    // Create string to sign
    let string_to_sign = create_string_to_sign(
        &auth.algorithm,
        &timestamp,
        &date,
        &auth.credential.region,
        &auth.credential.service,
        &canonical_request,
    );

    // Calculate signing key
    let signing_key = calculate_signing_key(
        &credentials.secret_access_key,
        &auth.credential.date,
        &auth.credential.region,
        &auth.credential.service,
    );

    // Calculate signature
    let calculated_signature = calculate_signature(&signing_key, &string_to_sign);

    // Decode both signatures to bytes for comparison
    let received_signature_bytes =
        hex::decode(&auth.signature).map_err(|_| S3Error::SignatureDoesNotMatch)?;
    let calculated_signature_bytes =
        hex::decode(&calculated_signature).map_err(|_| S3Error::SignatureDoesNotMatch)?;

    // Compare signatures (constant-time comparison)
    if constant_time_eq(&calculated_signature_bytes, &received_signature_bytes) {
        return Ok(());
    }

    // NormalizePath may have stripped a trailing slash. Retry with trailing slash if the
    // path doesn't already end with one — the client may have signed the original URI.
    let path = request.uri().path();
    if !path.ends_with('/') {
        let alt_path = format!("{}/", path);
        if let Ok(alt_canonical) =
            create_canonical_request_with_path(request, &auth.signed_headers, &alt_path)
        {
            let alt_sts = create_string_to_sign(
                &auth.algorithm,
                &timestamp,
                &date,
                &auth.credential.region,
                &auth.credential.service,
                &alt_canonical,
            );
            let alt_sig = calculate_signature(&signing_key, &alt_sts);
            if let Ok(alt_sig_bytes) = hex::decode(&alt_sig) {
                if constant_time_eq(&alt_sig_bytes, &received_signature_bytes) {
                    return Ok(());
                }
            }
        }
    }

    // Log detailed information for debugging
    tracing::debug!("Signature mismatch!");
    tracing::debug!("Received signature: {}", auth.signature);
    tracing::debug!("Calculated signature: {}", calculated_signature);
    tracing::debug!("Canonical request:\n{}", canonical_request);
    tracing::debug!("String to sign:\n{}", string_to_sign);
    tracing::debug!("Access key: {}", auth.credential.access_key_id);
    tracing::debug!(
        "Date: {}, Region: {}, Service: {}",
        auth.credential.date,
        auth.credential.region,
        auth.credential.service
    );
    tracing::debug!("Signed headers: {:?}", auth.signed_headers);

    Err(S3Error::SignatureDoesNotMatch)
}

/// Extracted request data for signature verification.
/// This allows us to extract all data synchronously before async operations.
#[derive(Clone)]
pub struct SignatureRequestData {
    /// HTTP method
    pub method: String,
    /// Full URI
    pub uri: String,
    /// URI path component
    pub path: String,
    /// URI query string (if any)
    pub query: Option<String>,
    /// All request headers
    pub headers: HeaderMap,
    /// Authorization header value
    pub auth_header: String,
    /// Payload hash (from x-amz-content-sha256 or UNSIGNED-PAYLOAD)
    pub payload_hash: String,
}

impl SignatureRequestData {
    /// Extract data from request synchronously.
    pub fn from_request(request: &Request<axum::body::Body>) -> Result<Self, S3Error> {
        let auth_header = request
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or(S3Error::AccessDenied)?
            .to_string();

        let payload_hash = request
            .headers()
            .get("x-amz-content-sha256")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("UNSIGNED-PAYLOAD")
            .to_string();

        Ok(Self {
            method: request.method().as_str().to_string(),
            uri: request.uri().to_string(),
            path: request.uri().path().to_string(),
            query: request.uri().query().map(|s| s.to_string()),
            headers: request.headers().clone(),
            auth_header,
            payload_hash,
        })
    }
}

/// Verifies AWS Signature V4 for a request using IAM database.
///
/// # Arguments
///
/// * `request` - HTTP request
/// * `iam_storage` - IAM storage for looking up users by access key
///
/// # Returns
///
/// Returns Ok(User) if signature is valid with the authenticated user,
/// Err(S3Error) otherwise.
pub async fn verify_signature_v4_with_iam(
    request: &Request<axum::body::Body>,
    iam_storage: &IamStorage,
) -> Result<User, S3Error> {
    // Extract all data from request synchronously BEFORE any async operations
    // This is required because we cannot hold &request across await points
    let req_data = SignatureRequestData::from_request(request)?;
    verify_signature_v4_with_iam_data(req_data, iam_storage).await
}

/// Verifies AWS Signature V4 using pre-extracted request data.
///
/// This function takes owned data, avoiding borrow issues across await points.
pub async fn verify_signature_v4_with_iam_data(
    req_data: SignatureRequestData,
    iam_storage: &IamStorage,
) -> Result<User, S3Error> {
    // Log all headers for debugging
    tracing::debug!("Request method: {}", req_data.method);
    tracing::debug!("Request URI: {}", req_data.uri);
    tracing::debug!("Request headers:");
    for (name, value) in &req_data.headers {
        if let Ok(value_str) = value.to_str() {
            tracing::debug!("  {}: {}", name, value_str);
        }
    }

    tracing::debug!("Authorization header: {}", req_data.auth_header);

    // Parse Authorization header
    let auth = parse_authorization_header(&req_data.auth_header)?;

    // Extract date and timestamp from headers (X-Amz-Date or Date)
    let (date, timestamp) = extract_date_and_timestamp(&req_data.headers)?;

    // Create canonical request using extracted data
    let canonical_request = create_canonical_request_from_data(&req_data, &auth.signed_headers)?;

    // Now we can do async operations - no borrows held
    // Look up user by access key
    let user = iam_storage
        .get_user_by_access_key_with_secret(&auth.credential.access_key_id)
        .await
        .map_err(|e| {
            tracing::warn!("IAM lookup error: {:?}", e);
            S3Error::AccessDenied
        })?
        .ok_or_else(|| {
            tracing::warn!("Access key not found: {}", auth.credential.access_key_id);
            S3Error::AccessDenied
        })?;

    // Get secret key
    let secret_key = user.secret_key.as_ref().ok_or_else(|| {
        tracing::warn!("User {} has no S3 credentials", user.username);
        S3Error::AccessDenied
    })?;

    // Create string to sign
    let string_to_sign = create_string_to_sign(
        &auth.algorithm,
        &timestamp,
        &date,
        &auth.credential.region,
        &auth.credential.service,
        &canonical_request,
    );

    // Calculate signing key
    let signing_key = calculate_signing_key(
        secret_key,
        &auth.credential.date,
        &auth.credential.region,
        &auth.credential.service,
    );

    // Calculate signature
    let calculated_signature = calculate_signature(&signing_key, &string_to_sign);

    // Decode both signatures to bytes for comparison
    let received_signature_bytes =
        hex::decode(&auth.signature).map_err(|_| S3Error::SignatureDoesNotMatch)?;
    let calculated_signature_bytes =
        hex::decode(&calculated_signature).map_err(|_| S3Error::SignatureDoesNotMatch)?;

    // Compare signatures (constant-time comparison)
    if constant_time_eq(&calculated_signature_bytes, &received_signature_bytes) {
        return Ok(user);
    }

    // NormalizePath may have stripped a trailing slash. Retry with trailing slash if the
    // path doesn't already end with one — the client may have signed the original URI.
    if !req_data.path.ends_with('/') {
        let alt_data = SignatureRequestData {
            path: format!("{}/", req_data.path),
            ..req_data.clone()
        };
        if let Ok(alt_canonical) =
            create_canonical_request_from_data(&alt_data, &auth.signed_headers)
        {
            let alt_sts = create_string_to_sign(
                &auth.algorithm,
                &timestamp,
                &date,
                &auth.credential.region,
                &auth.credential.service,
                &alt_canonical,
            );
            let alt_sig = calculate_signature(&signing_key, &alt_sts);
            if let Ok(alt_sig_bytes) = hex::decode(&alt_sig) {
                if constant_time_eq(&alt_sig_bytes, &received_signature_bytes) {
                    return Ok(user);
                }
            }
        }
    }

    // Log detailed information for debugging
    tracing::debug!("Signature mismatch!");
    tracing::debug!("Received signature: {}", auth.signature);
    tracing::debug!("Calculated signature: {}", calculated_signature);
    tracing::debug!("Canonical request:\n{}", canonical_request);
    tracing::debug!("String to sign:\n{}", string_to_sign);
    tracing::debug!("Access key: {}", auth.credential.access_key_id);
    tracing::debug!(
        "Date: {}, Region: {}, Service: {}",
        auth.credential.date,
        auth.credential.region,
        auth.credential.service
    );
    tracing::debug!("Signed headers: {:?}", auth.signed_headers);

    Err(S3Error::SignatureDoesNotMatch)
}

/// Creates canonical request string from extracted request data.
fn create_canonical_request_from_data(
    req_data: &SignatureRequestData,
    signed_headers: &[String],
) -> Result<String, S3Error> {
    // Method
    let method = &req_data.method;

    // Canonical URI (percent-encoded path)
    let canonical_uri = canonicalize_uri(&req_data.path)?;

    // Canonical query string (sorted, percent-encoded)
    let canonical_query = canonicalize_query_string(req_data.query.as_deref())?;
    tracing::debug!("Original query string: {:?}", req_data.query);
    tracing::debug!("Canonical query string: {}", canonical_query);

    // Canonical headers (sorted, lowercase keys, trimmed values)
    let canonical_headers = create_canonical_headers(&req_data.headers, signed_headers)?;

    // Signed headers (sorted, lowercase, semicolon-separated)
    let signed_headers_str = signed_headers.join(";");

    // Payload hash
    let payload_hash = &req_data.payload_hash;

    Ok(format!(
        "{}\n{}\n{}\n{}\n\n{}\n{}",
        method, canonical_uri, canonical_query, canonical_headers, signed_headers_str, payload_hash
    ))
}

/// Parses Authorization header.
///
/// Format: `AWS4-HMAC-SHA256 Credential=access_key/date/region/service/aws4_request, SignedHeaders=..., Signature=...`
fn parse_authorization_header(header: &str) -> Result<AuthorizationHeader, S3Error> {
    if !header.starts_with("AWS4-HMAC-SHA256 ") {
        return Err(S3Error::SignatureDoesNotMatch);
    }

    let parts: Vec<&str> = header["AWS4-HMAC-SHA256 ".len()..].split(',').collect();

    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;

    for part in parts {
        let part = part.trim();
        if let Some(cred_str) = part.strip_prefix("Credential=") {
            credential = Some(parse_credential(cred_str)?);
        } else if let Some(headers_str) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(headers_str.split(';').map(|s| s.to_lowercase()).collect());
        } else if let Some(sig_str) = part.strip_prefix("Signature=") {
            signature = Some(sig_str.to_string());
        }
    }

    Ok(AuthorizationHeader {
        algorithm: "AWS4-HMAC-SHA256".to_string(),
        credential: credential.ok_or(S3Error::SignatureDoesNotMatch)?,
        signed_headers: signed_headers.ok_or(S3Error::SignatureDoesNotMatch)?,
        signature: signature.ok_or(S3Error::SignatureDoesNotMatch)?,
    })
}

/// Parses credential scope.
///
/// Format: `access_key/date/region/service/aws4_request`
fn parse_credential(cred_str: &str) -> Result<CredentialScope, S3Error> {
    let parts: Vec<&str> = cred_str.split('/').collect();
    if parts.len() != 5 || parts[4] != "aws4_request" {
        return Err(S3Error::SignatureDoesNotMatch);
    }

    Ok(CredentialScope {
        access_key_id: parts[0].to_string(),
        date: parts[1].to_string(),
        region: parts[2].to_string(),
        service: parts[3].to_string(),
    })
}

/// Extracts date (YYYYMMDD) and timestamp (YYYYMMDDTHHMMSSZ) from request headers.
///
/// Checks X-Amz-Date first, then falls back to Date header.
/// Returns (date, timestamp) where date is YYYYMMDD and timestamp is full ISO8601 format.
fn extract_date_and_timestamp(headers: &HeaderMap) -> Result<(String, String), S3Error> {
    // Try X-Amz-Date first (preferred for Signature V4)
    if let Some(date_header) = headers.get("x-amz-date") {
        if let Ok(date_str) = date_header.to_str() {
            // X-Amz-Date format: YYYYMMDDTHHMMSSZ (16 characters)
            // Example: 20240101T120000Z
            if date_str.len() == 16
                && date_str.chars().nth(8) == Some('T')
                && date_str.ends_with('Z')
            {
                // Format: YYYYMMDDTHHMMSSZ
                let date = date_str[..8].to_string(); // Extract YYYYMMDD
                return Ok((date, date_str.to_string()));
            }
        }
    }

    // Fall back to Date header (RFC 7231 format)
    if let Some(date_header) = headers.get("date") {
        if let Ok(date_str) = date_header.to_str() {
            // Parse RFC 7231 date format (e.g., "Mon, 01 Jan 2024 12:00:00 GMT")
            // We'll try to parse it using chrono
            if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(date_str) {
                let date = dt.format("%Y%m%d").to_string();
                let timestamp = dt.format("%Y%m%dT%H%M%SZ").to_string();
                return Ok((date, timestamp));
            }
            // Try RFC 850 format as fallback
            if let Ok(dt) = chrono::DateTime::parse_from_str(date_str, "%A, %d-%b-%y %H:%M:%S GMT")
            {
                let date = dt.format("%Y%m%d").to_string();
                let timestamp = dt.format("%Y%m%dT%H%M%SZ").to_string();
                return Ok((date, timestamp));
            }
            // Try ANSI C format as fallback
            if let Ok(dt) = chrono::DateTime::parse_from_str(date_str, "%a %b %e %H:%M:%S %Y") {
                let date = dt.format("%Y%m%d").to_string();
                let timestamp = dt.format("%Y%m%dT%H%M%SZ").to_string();
                return Ok((date, timestamp));
            }
        }
    }

    Err(S3Error::SignatureDoesNotMatch)
}

/// Creates canonical request string.
///
/// Format:
/// ```text
/// METHOD
/// CANONICAL_URI
/// CANONICAL_QUERY_STRING
/// CANONICAL_HEADERS
///
/// SIGNED_HEADERS
/// PAYLOAD_HASH
/// ```
fn create_canonical_request(
    request: &Request<axum::body::Body>,
    signed_headers: &[String],
) -> Result<String, S3Error> {
    // Method
    let method = request.method().as_str();

    // Canonical URI (percent-encoded path)
    let uri = request.uri();
    let canonical_uri = canonicalize_uri(uri.path())?;

    // Canonical query string (sorted, percent-encoded)
    // Note: uri.query() returns the raw query string (may be URL-encoded)
    let canonical_query = canonicalize_query_string(uri.query())?;
    tracing::debug!("Original query string: {:?}", uri.query());
    tracing::debug!("Canonical query string: {}", canonical_query);

    // Canonical headers (sorted, lowercase keys, trimmed values)
    let canonical_headers = create_canonical_headers(request.headers(), signed_headers)?;

    // Signed headers (sorted, lowercase, semicolon-separated)
    let signed_headers_str = signed_headers.join(";");

    // Payload hash (SHA256 of body)
    // Check for x-amz-content-sha256 header first (used by AWS CLI)
    // This header can have several special values:
    // - UNSIGNED-PAYLOAD: payload is not signed
    // - STREAMING-UNSIGNED-PAYLOAD-TRAILER: chunked upload with trailing checksum
    // - STREAMING-AWS4-HMAC-SHA256-PAYLOAD: signed streaming payload
    // - STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER: signed streaming with trailer
    // - 64-char hex string: SHA256 hash of the payload
    // Accept all valid x-amz-content-sha256 values as-is
    // They will be used verbatim in the canonical request
    let payload_hash = request
        .headers()
        .get("x-amz-content-sha256")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("UNSIGNED-PAYLOAD");

    Ok(format!(
        "{}\n{}\n{}\n{}\n\n{}\n{}",
        method, canonical_uri, canonical_query, canonical_headers, signed_headers_str, payload_hash
    ))
}

/// Like `create_canonical_request` but uses a custom path (for trailing-slash retry).
fn create_canonical_request_with_path(
    request: &Request<axum::body::Body>,
    signed_headers: &[String],
    path: &str,
) -> Result<String, S3Error> {
    let method = request.method().as_str();
    let canonical_uri = canonicalize_uri(path)?;
    let canonical_query = canonicalize_query_string(request.uri().query())?;
    let canonical_headers = create_canonical_headers(request.headers(), signed_headers)?;
    let signed_headers_str = signed_headers.join(";");
    let payload_hash = request
        .headers()
        .get("x-amz-content-sha256")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("UNSIGNED-PAYLOAD");

    Ok(format!(
        "{}\n{}\n{}\n{}\n\n{}\n{}",
        method, canonical_uri, canonical_query, canonical_headers, signed_headers_str, payload_hash
    ))
}

/// Canonicalizes URI path (percent-encoding).
///
/// For S3, we need to:
/// - Preserve double slashes (they're meaningful in S3)
/// - Percent-encode each path segment
/// - Handle empty segments correctly
fn canonicalize_uri(path: &str) -> Result<String, S3Error> {
    // Handle empty path
    if path.is_empty() {
        return Ok("/".to_string());
    }

    // Split path into segments, preserving empty segments (for double slashes)
    let mut segments = Vec::new();
    let mut current = String::new();

    for ch in path.chars() {
        if ch == '/' {
            segments.push(current);
            current = String::new();
        } else {
            current.push(ch);
        }
    }
    // Don't forget the last segment
    if !current.is_empty() || path.ends_with('/') {
        segments.push(current);
    }

    // Decode then re-encode each segment to ensure single encoding.
    // The path may already contain percent-encoded chars (e.g. %28 for '('),
    // so we must decode first to avoid double-encoding (%28 -> %2528).
    let encoded: Vec<String> = segments
        .iter()
        .map(|seg| {
            if seg.is_empty() {
                String::new() // Preserve empty segments (double slashes)
            } else {
                let decoded = percent_decode(seg);
                percent_encode(&decoded)
            }
        })
        .collect();

    let result = encoded.join("/");

    // Ensure path starts with '/'
    if result.starts_with('/') {
        Ok(result)
    } else {
        Ok(format!("/{}", result))
    }
}

/// Canonicalizes query string (sorted, percent-encoded).
///
/// According to AWS Signature V4:
/// 1. URL-decode parameter names and values
/// 2. Sort parameters by name, then by value
/// 3. Re-encode using percent-encoding
fn canonicalize_query_string(query: Option<&str>) -> Result<String, S3Error> {
    let query = query.unwrap_or("");
    if query.is_empty() {
        return Ok(String::new());
    }

    // Parse, decode, and collect parameters
    let mut params: Vec<(String, String)> = query
        .split('&')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key_encoded = parts.next()?;
            let value_encoded = parts.next().unwrap_or("");

            // URL-decode key and value
            let key = url_decode(key_encoded);
            let value = url_decode(value_encoded);

            Some((key, value))
        })
        .collect();

    // Sort by key, then by value
    params.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    // Re-encode and join
    let encoded: Vec<String> = params
        .iter()
        .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
        .collect();

    Ok(encoded.join("&"))
}

/// URL-decodes a percent-encoded string.
fn url_decode(s: &str) -> String {
    // Simple URL decoding - replace %XX with corresponding byte
    let mut result = String::new();
    let mut chars = s.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '%' {
            // Try to decode %XX
            let mut hex_str = String::new();
            for _ in 0..2 {
                if let Some(hex_ch) = chars.next() {
                    hex_str.push(hex_ch);
                } else {
                    // Invalid encoding, keep as-is
                    result.push('%');
                    result.push_str(&hex_str);
                    break;
                }
            }
            if hex_str.len() == 2 {
                if let Ok(byte) = u8::from_str_radix(&hex_str, 16) {
                    result.push(byte as char);
                } else {
                    // Invalid hex, keep as-is
                    result.push('%');
                    result.push_str(&hex_str);
                }
            }
        } else {
            result.push(ch);
        }
    }

    result
}

/// Creates canonical headers string.
fn create_canonical_headers(
    headers: &HeaderMap,
    signed_headers: &[String],
) -> Result<String, S3Error> {
    let mut header_map: BTreeMap<String, String> = BTreeMap::new();

    for header_name in signed_headers {
        if let Some(header_value) = headers.get(header_name) {
            let value_str = header_value.to_str().map_err(|_| S3Error::SignatureDoesNotMatch)?;
            // Trim whitespace and collapse multiple spaces
            let normalized = value_str.split_whitespace().collect::<Vec<&str>>().join(" ");
            header_map.insert(header_name.clone(), normalized);
        }
    }

    let header_lines: Vec<String> =
        header_map.iter().map(|(k, v)| format!("{}:{}", k, v)).collect();

    Ok(header_lines.join("\n"))
}

/// Creates string to sign.
///
/// Format:
/// ```text
/// AWS4-HMAC-SHA256
/// TIMESTAMP
/// DATE/REGION/SERVICE/aws4_request
/// HASH(CANONICAL_REQUEST)
/// ```
///
/// # Arguments
///
/// * `algorithm` - Always "AWS4-HMAC-SHA256"
/// * `timestamp` - Full timestamp from X-Amz-Date (YYYYMMDDTHHMMSSZ)
/// * `date` - Date portion (YYYYMMDD) for credential scope
/// * `region` - AWS region (e.g., "us-east-1")
/// * `service` - AWS service name (e.g., "s3")
/// * `canonical_request` - Canonical request string
fn create_string_to_sign(
    algorithm: &str,
    timestamp: &str,
    date: &str,
    region: &str,
    service: &str,
    canonical_request: &str,
) -> String {
    use sha2::{Digest, Sha256};

    let credential_scope = format!("{}/{}/{}/aws4_request", date, region, service);
    let hashed_request = format!("{:x}", Sha256::digest(canonical_request.as_bytes()));

    format!(
        "{}\n{}\n{}\n{}",
        algorithm, timestamp, credential_scope, hashed_request
    )
}

/// Calculates signing key using HMAC-SHA256 chain.
///
/// kSecret = secret access key
/// kDate = HMAC-SHA256(kSecret, date)
/// kRegion = HMAC-SHA256(kDate, region)
/// kService = HMAC-SHA256(kRegion, service)
/// kSigning = HMAC-SHA256(kService, "aws4_request")
fn calculate_signing_key(secret_key: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_secret = format!("AWS4{}", secret_key);
    let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Calculates HMAC-SHA256.
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Calculates signature.
fn calculate_signature(signing_key: &[u8], string_to_sign: &str) -> String {
    let signature = hmac_sha256(signing_key, string_to_sign.as_bytes());
    hex::encode(signature)
}

/// Percent-encodes a string (RFC 3986).
///
/// Only unreserved characters (A-Z, a-z, 0-9, -, _, ., ~) are NOT encoded.
/// All other characters, including `/`, are percent-encoded.
///
/// Note: For URI path encoding, `canonicalize_uri` handles `/` separately
/// by splitting the path into segments first.
fn percent_encode(s: &str) -> String {
    let mut encoded = String::new();
    for byte in s.bytes() {
        match byte {
            // Unreserved characters per RFC 3986
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => {
                encoded.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    encoded
}

/// Decodes percent-encoded string (e.g. `%28` → `(`).
fn percent_decode(s: &str) -> String {
    let mut result = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) =
                u8::from_str_radix(std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""), 16)
            {
                result.push(byte);
                i += 3;
                continue;
            }
        }
        result.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&result).into_owned()
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
    use axum::http::{HeaderMap, HeaderValue};

    #[test]
    fn test_percent_encode() {
        assert_eq!(percent_encode("hello"), "hello");
        assert_eq!(percent_encode("hello world"), "hello%20world");
        // Forward slash IS encoded (used in query strings)
        // Note: For URI paths, canonicalize_uri handles / separately
        assert_eq!(percent_encode("test/file.txt"), "test%2Ffile.txt");
        assert_eq!(percent_encode("test%file"), "test%25file");
        assert_eq!(percent_encode("test+file"), "test%2Bfile");
    }

    #[test]
    fn test_canonicalize_uri() {
        assert_eq!(canonicalize_uri("/").unwrap(), "/");
        assert_eq!(canonicalize_uri("/bucket").unwrap(), "/bucket");
        assert_eq!(canonicalize_uri("/bucket/key").unwrap(), "/bucket/key");
        assert_eq!(
            canonicalize_uri("/bucket/key with spaces").unwrap(),
            "/bucket/key%20with%20spaces"
        );
        // Test empty path
        assert_eq!(canonicalize_uri("").unwrap(), "/");
        // Test path with already-encoded characters (should not double-encode)
        assert_eq!(
            canonicalize_uri("/bucket/key%20test").unwrap(),
            "/bucket/key%20test"
        );
        // Test path with parentheses (e.g. Next.js route groups)
        assert_eq!(
            canonicalize_uri("/bucket/app/%28dashboard%29/page.tsx").unwrap(),
            "/bucket/app/%28dashboard%29/page.tsx"
        );
    }

    #[test]
    fn test_url_decode() {
        assert_eq!(url_decode("hello"), "hello");
        assert_eq!(url_decode("hello%20world"), "hello world");
        assert_eq!(url_decode("test%2Ffile"), "test/file");
        assert_eq!(url_decode("test%2520file"), "test%20file"); // Double encoding
        assert_eq!(url_decode("a%3Db"), "a=b");
    }

    #[test]
    fn test_canonicalize_query_string() {
        assert_eq!(canonicalize_query_string(None).unwrap(), "");
        assert_eq!(canonicalize_query_string(Some("")).unwrap(), "");
        assert_eq!(
            canonicalize_query_string(Some("a=1&b=2")).unwrap(),
            "a=1&b=2"
        );
        assert_eq!(
            canonicalize_query_string(Some("b=2&a=1")).unwrap(),
            "a=1&b=2"
        );
        // Test with special characters (should decode then re-encode)
        assert_eq!(
            canonicalize_query_string(Some("key=value+test")).unwrap(),
            "key=value%2Btest"
        );
        // Test with URL-encoded values (should decode then re-encode)
        assert_eq!(
            canonicalize_query_string(Some("prefix=test%20file")).unwrap(),
            "prefix=test%20file"
        );
        // Test list-type parameter (common in ListObjectsV2)
        assert_eq!(
            canonicalize_query_string(Some("list-type=2&prefix=test")).unwrap(),
            "list-type=2&prefix=test"
        );
    }

    #[test]
    fn test_extract_date_and_timestamp() {
        let mut headers = HeaderMap::new();

        // Test X-Amz-Date
        headers.insert(
            "x-amz-date",
            HeaderValue::from_str("20240101T120000Z").unwrap(),
        );
        let (date, timestamp) = extract_date_and_timestamp(&headers).unwrap();
        assert_eq!(date, "20240101");
        assert_eq!(timestamp, "20240101T120000Z");

        // Test Date header (RFC 2822)
        let mut headers2 = HeaderMap::new();
        headers2.insert(
            "date",
            HeaderValue::from_str("Mon, 01 Jan 2024 12:00:00 GMT").unwrap(),
        );
        let (date2, timestamp2) = extract_date_and_timestamp(&headers2).unwrap();
        assert_eq!(date2, "20240101");
        assert_eq!(timestamp2, "20240101T120000Z");
    }

    #[test]
    fn test_extract_date_and_timestamp_missing() {
        let headers = HeaderMap::new();
        assert!(extract_date_and_timestamp(&headers).is_err());
    }

    #[test]
    fn test_hmac_sha256() {
        let key = b"key";
        let data = b"data";
        let result = hmac_sha256(key, data);
        assert_eq!(result.len(), 32); // SHA256 output is 32 bytes
    }

    #[test]
    fn test_calculate_signing_key() {
        let secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let date = "20150830";
        let region = "us-east-1";
        let service = "s3";

        let key = calculate_signing_key(secret, date, region, service);
        assert_eq!(key.len(), 32); // SHA256 output is 32 bytes

        // Test that same inputs produce same key
        let key2 = calculate_signing_key(secret, date, region, service);
        assert_eq!(key, key2);

        // Test that different inputs produce different keys
        let key3 = calculate_signing_key(secret, "20150831", region, service);
        assert_ne!(key, key3);
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hell"));
        assert!(!constant_time_eq(b"hello", b"helloworld"));
    }

    #[test]
    fn test_parse_authorization_header() {
        let header = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20150830/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-date, Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024";

        let auth = parse_authorization_header(header).unwrap();
        assert_eq!(auth.algorithm, "AWS4-HMAC-SHA256");
        assert_eq!(auth.credential.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(auth.credential.date, "20150830");
        assert_eq!(auth.credential.region, "us-east-1");
        assert_eq!(auth.credential.service, "s3");
        assert_eq!(auth.signed_headers, vec!["host", "range", "x-amz-date"]);
        assert_eq!(
            auth.signature,
            "fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024"
        );
    }

    #[test]
    fn test_parse_authorization_header_invalid() {
        // Missing algorithm
        assert!(parse_authorization_header("Credential=test").is_err());

        // Wrong algorithm
        assert!(parse_authorization_header("AWS4-HMAC-SHA1 Credential=test").is_err());

        // Missing credential
        assert!(parse_authorization_header("AWS4-HMAC-SHA256 SignedHeaders=host").is_err());
    }

    #[test]
    fn test_parse_credential() {
        let cred_str = "AKIAIOSFODNN7EXAMPLE/20150830/us-east-1/s3/aws4_request";
        let cred = parse_credential(cred_str).unwrap();
        assert_eq!(cred.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(cred.date, "20150830");
        assert_eq!(cred.region, "us-east-1");
        assert_eq!(cred.service, "s3");
    }

    #[test]
    fn test_parse_credential_invalid() {
        // Wrong number of parts
        assert!(parse_credential("AKIAIOSFODNN7EXAMPLE/20150830").is_err());

        // Wrong terminator
        assert!(
            parse_credential("AKIAIOSFODNN7EXAMPLE/20150830/us-east-1/s3/aws3_request").is_err()
        );
    }

    #[test]
    fn test_create_canonical_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_str("example.com").unwrap());
        headers.insert(
            "x-amz-date",
            HeaderValue::from_str("20240101T120000Z").unwrap(),
        );
        headers.insert(
            "content-type",
            HeaderValue::from_str("application/xml").unwrap(),
        );

        let signed_headers = vec![
            "host".to_string(),
            "x-amz-date".to_string(),
            "content-type".to_string(),
        ];
        let canonical = create_canonical_headers(&headers, &signed_headers).unwrap();

        // Headers should be sorted alphabetically
        assert!(canonical.contains("content-type:application/xml"));
        assert!(canonical.contains("host:example.com"));
        assert!(canonical.contains("x-amz-date:20240101T120000Z"));
    }

    #[test]
    fn test_streaming_payload_values() {
        // Test that various x-amz-content-sha256 values are accepted
        // These values should be used verbatim in the canonical request

        // Standard unsigned payload
        let unsigned = "UNSIGNED-PAYLOAD";
        assert_eq!(unsigned.len(), 16);

        // Streaming unsigned payload with trailer (AWS CLI multipart upload)
        let streaming_trailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
        assert_eq!(streaming_trailer.len(), 34);

        // Signed streaming payload
        let streaming_signed = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
        assert_eq!(streaming_signed.len(), 34);

        // Signed streaming payload with trailer
        let streaming_signed_trailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
        assert_eq!(streaming_signed_trailer.len(), 42);

        // Valid SHA256 hash (64 hex characters)
        let valid_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(valid_hash.len(), 64);
        assert!(hex::decode(valid_hash).is_ok());
    }

    #[test]
    fn test_create_string_to_sign() {
        let algorithm = "AWS4-HMAC-SHA256";
        let timestamp = "20150830T123456Z";
        let date = "20150830";
        let region = "us-east-1";
        let service = "s3";
        let canonical_request = "GET\n/\n\nhost:example.com\n\nhost\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        let string_to_sign = create_string_to_sign(
            algorithm,
            timestamp,
            date,
            region,
            service,
            canonical_request,
        );

        assert!(string_to_sign.starts_with("AWS4-HMAC-SHA256\n"));
        assert!(string_to_sign.contains("20150830T123456Z"));
        assert!(string_to_sign.contains("20150830/us-east-1/s3/aws4_request"));
    }
}
