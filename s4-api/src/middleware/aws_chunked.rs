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

//! AWS Chunked Encoding decoder.
//!
//! AWS CLI v2 and AWS SDKs use a special chunked transfer encoding format
//! when uploading objects with `Content-Encoding: aws-chunked` and
//! `x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER`.
//!
//! This module provides a function to decode this format.
//!
//! AWS Chunked Format:
//! ```text
//! <hex chunk size>\r\n
//! <chunk data>\r\n
//! ... (repeat for each chunk)
//! 0\r\n
//! <trailer headers>\r\n
//! \r\n
//! ```

use crate::s3::errors::S3Error;
use axum::http::HeaderMap;
use tracing::{debug, warn};

/// Checks if the request uses AWS chunked encoding.
///
/// Returns true if Content-Encoding contains "aws-chunked" OR if
/// `x-amz-content-sha256` indicates a streaming payload (e.g.,
/// `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`). Some S3 clients (like minio-go)
/// send streaming signed payloads without setting `Content-Encoding`.
pub fn is_aws_chunked(headers: &axum::http::HeaderMap) -> bool {
    // Check Content-Encoding header
    let has_content_encoding = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("aws-chunked"))
        .unwrap_or(false);

    if has_content_encoding {
        return true;
    }

    // Check x-amz-content-sha256 for streaming payload indicators
    headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.starts_with("STREAMING-"))
        .unwrap_or(false)
}

/// Decodes AWS chunked encoded data.
///
/// The AWS chunked format consists of:
/// - `<hex size>\r\n<data>\r\n` for each chunk
/// - `0\r\n` followed by optional trailers and `\r\n` at the end
///
/// This function handles both `\r\n` and `\n` line endings for robustness.
///
/// # Arguments
///
/// * `data` - The raw aws-chunked encoded data
///
/// # Returns
///
/// The decoded payload data, or an error if the data is not valid aws-chunked format.
pub fn decode_aws_chunked(data: &[u8]) -> Result<Vec<u8>, S3Error> {
    let mut result = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        // Find the end of the chunk size line
        let line_end = find_line_end(data, pos);
        if line_end.is_none() {
            // No line ending found, could be corrupted or not chunked
            warn!(
                "AWS chunked decode: no line ending found at position {}",
                pos
            );
            return Err(S3Error::InvalidRequest(
                "Invalid aws-chunked encoding: no line ending found".to_string(),
            ));
        }
        let (line_end, crlf_len) = line_end.unwrap();

        // Parse the chunk size (may include chunk extensions after `;`)
        let size_line = &data[pos..line_end];
        let size_str = String::from_utf8_lossy(size_line);

        // Extract just the size part (before any `;` for chunk extensions)
        let size_part = size_str.split(';').next().unwrap_or(&size_str).trim();

        let chunk_size = match usize::from_str_radix(size_part, 16) {
            Ok(size) => size,
            Err(_) => {
                // Not a valid hex size, might not be aws-chunked format
                warn!(
                    "AWS chunked decode: invalid chunk size '{}' at position {}",
                    size_part, pos
                );
                return Err(S3Error::InvalidRequest(format!(
                    "Invalid aws-chunked encoding: invalid chunk size '{}'",
                    size_part
                )));
            }
        };

        debug!(
            "AWS chunked decode: chunk size {} at position {}",
            chunk_size, pos
        );

        // Move past the size line
        pos = line_end + crlf_len;

        // If chunk size is 0, we've reached the end
        if chunk_size == 0 {
            debug!(
                "AWS chunked decode: reached final chunk, total decoded: {} bytes",
                result.len()
            );
            break;
        }

        // Read chunk data
        if pos + chunk_size > data.len() {
            warn!(
                "AWS chunked decode: chunk size {} exceeds remaining data {} at position {}",
                chunk_size,
                data.len() - pos,
                pos
            );
            return Err(S3Error::InvalidRequest(
                "Invalid aws-chunked encoding: truncated chunk data".to_string(),
            ));
        }

        result.extend_from_slice(&data[pos..pos + chunk_size]);
        pos += chunk_size;

        // Skip the trailing CRLF after chunk data
        if pos < data.len() {
            if data[pos] == b'\r' && pos + 1 < data.len() && data[pos + 1] == b'\n' {
                pos += 2;
            } else if data[pos] == b'\n' {
                pos += 1;
            }
        }
    }

    Ok(result)
}

/// Validates that the decoded body length matches the `x-amz-decoded-content-length` header.
///
/// Returns `Ok(())` if the header is absent or the lengths match.
/// Returns `Err(S3Error::InvalidRequest)` if the lengths mismatch.
pub fn validate_decoded_content_length(headers: &HeaderMap, decoded: &[u8]) -> Result<(), S3Error> {
    if let Some(expected) = headers.get("x-amz-decoded-content-length") {
        if let Ok(expected_len) = expected.to_str().unwrap_or("0").parse::<usize>() {
            if decoded.len() != expected_len {
                return Err(S3Error::InvalidRequest(format!(
                    "Decoded length {} != expected {}",
                    decoded.len(),
                    expected_len
                )));
            }
        }
    }
    Ok(())
}

/// Finds the end of a line (before \r\n or \n).
///
/// Returns the position of the line end and the length of the line ending (1 or 2).
fn find_line_end(data: &[u8], start: usize) -> Option<(usize, usize)> {
    for i in start..data.len() {
        if data[i] == b'\r' && i + 1 < data.len() && data[i + 1] == b'\n' {
            return Some((i, 2));
        }
        if data[i] == b'\n' {
            return Some((i, 1));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_single_chunk() {
        let encoded = b"5\r\nhello\r\n0\r\n\r\n";
        let decoded = decode_aws_chunked(encoded).unwrap();
        assert_eq!(decoded, b"hello");
    }

    #[test]
    fn test_decode_multiple_chunks() {
        let encoded = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let decoded = decode_aws_chunked(encoded).unwrap();
        assert_eq!(decoded, b"hello world");
    }

    #[test]
    fn test_decode_with_trailer() {
        let encoded = b"5\r\nhello\r\n0\r\nx-amz-checksum-crc32:sOO8/Q==\r\n\r\n";
        let decoded = decode_aws_chunked(encoded).unwrap();
        assert_eq!(decoded, b"hello");
    }

    #[test]
    fn test_decode_hex_size() {
        let mut data = Vec::new();
        data.extend_from_slice(b"fa2\r\n");
        data.extend_from_slice(&vec![b'X'; 4002]);
        data.extend_from_slice(b"\r\n0\r\n\r\n");

        let decoded = decode_aws_chunked(&data).unwrap();
        assert_eq!(decoded.len(), 4002);
        assert!(decoded.iter().all(|&b| b == b'X'));
    }

    #[test]
    fn test_decode_with_chunk_extension() {
        let encoded = b"5;chunk-signature=abc123\r\nhello\r\n0\r\n\r\n";
        let decoded = decode_aws_chunked(encoded).unwrap();
        assert_eq!(decoded, b"hello");
    }

    #[test]
    fn test_decode_lf_only() {
        let encoded = b"5\nhello\n0\n\n";
        let decoded = decode_aws_chunked(encoded).unwrap();
        assert_eq!(decoded, b"hello");
    }

    #[test]
    fn test_not_chunked_returns_error() {
        let data = b"This is not chunked data";
        assert!(decode_aws_chunked(data).is_err());
    }

    #[test]
    fn test_truncated_chunk_returns_error() {
        // Declares 10 bytes but only has 5
        let data = b"a\r\nhello\r\n";
        assert!(decode_aws_chunked(data).is_err());
    }

    #[test]
    fn test_no_line_ending_returns_error() {
        let data = b"5hello";
        assert!(decode_aws_chunked(data).is_err());
    }

    #[test]
    fn test_invalid_hex_returns_error() {
        let data = b"xyz\r\nhello\r\n0\r\n\r\n";
        assert!(decode_aws_chunked(data).is_err());
    }

    #[test]
    fn test_decode_large_size() {
        let mut data = Vec::new();
        data.extend_from_slice(b"100000\r\n");
        data.extend_from_slice(&vec![b'Y'; 1048576]);
        data.extend_from_slice(b"\r\n0\r\n\r\n");

        let decoded = decode_aws_chunked(&data).unwrap();
        assert_eq!(decoded.len(), 1048576);
    }

    #[test]
    fn test_is_aws_chunked() {
        use axum::http::HeaderMap;

        let mut headers = HeaderMap::new();
        assert!(!is_aws_chunked(&headers));

        headers.insert("content-encoding", "gzip".parse().unwrap());
        assert!(!is_aws_chunked(&headers));

        headers.insert("content-encoding", "aws-chunked".parse().unwrap());
        assert!(is_aws_chunked(&headers));

        headers.insert("content-encoding", "aws-chunked,gzip".parse().unwrap());
        assert!(is_aws_chunked(&headers));
    }

    #[test]
    fn test_is_aws_chunked_via_content_sha256() {
        use axum::http::HeaderMap;

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-content-sha256",
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD".parse().unwrap(),
        );
        assert!(is_aws_chunked(&headers));

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-content-sha256",
            "STREAMING-UNSIGNED-PAYLOAD-TRAILER".parse().unwrap(),
        );
        assert!(is_aws_chunked(&headers));

        let mut headers = HeaderMap::new();
        headers.insert("x-amz-content-sha256", "UNSIGNED-PAYLOAD".parse().unwrap());
        assert!(!is_aws_chunked(&headers));
    }
}
