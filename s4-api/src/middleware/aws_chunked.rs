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
use axum::{body::Bytes, http::HeaderMap};
use std::{fmt, io};
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tracing::{debug, warn};

const MAX_AWS_CHUNK_HEADER_LEN: usize = 16 * 1024;
const STREAM_EMIT_CHUNK_SIZE: usize = 1024 * 1024;

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

/// Returns the expected decoded payload length from `x-amz-decoded-content-length`.
pub fn decoded_content_length(headers: &HeaderMap) -> Result<Option<u64>, S3Error> {
    let Some(expected) = headers.get("x-amz-decoded-content-length") else {
        return Ok(None);
    };

    let expected = expected.to_str().map_err(|_| {
        S3Error::InvalidRequest("Invalid x-amz-decoded-content-length header".to_string())
    })?;
    expected.parse::<u64>().map(Some).map_err(|_| {
        S3Error::InvalidRequest("Invalid x-amz-decoded-content-length header".to_string())
    })
}

/// Decodes AWS chunked data as a stream without buffering the whole payload.
///
/// The returned stream yields decoded data chunks and reports malformed chunk
/// framing as `io::ErrorKind::InvalidData`. Callers that must know the exact
/// target size should pass the decoded length from
/// `x-amz-decoded-content-length`.
pub fn decode_aws_chunked_stream<S, E>(
    stream: S,
    expected_len: Option<u64>,
) -> ReceiverStream<Result<Bytes, io::Error>>
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: fmt::Display + Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(16);

    tokio::spawn(async move {
        let mut decoder = AwsChunkedStreamDecoder::new(expected_len);
        tokio::pin!(stream);

        while let Some(item) = stream.next().await {
            let bytes = match item {
                Ok(bytes) => bytes,
                Err(e) => {
                    let _ = tx
                        .send(Err(io::Error::other(format!(
                            "error reading aws-chunked body: {e}"
                        ))))
                        .await;
                    return;
                }
            };

            match decoder.push(bytes, &tx).await {
                Ok(true) => {}
                Ok(false) => return,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            }
        }

        if let Err(e) = decoder.finish() {
            let _ = tx.send(Err(e)).await;
        }
    });

    ReceiverStream::new(rx)
}

/// Validates that the decoded body length matches the `x-amz-decoded-content-length` header.
///
/// Returns `Ok(())` if the header is absent or the lengths match.
/// Returns `Err(S3Error::InvalidRequest)` if the lengths mismatch.
pub fn validate_decoded_content_length(headers: &HeaderMap, decoded: &[u8]) -> Result<(), S3Error> {
    if let Some(expected_len) = decoded_content_length(headers)? {
        if decoded.len() as u64 != expected_len {
            return Err(S3Error::InvalidRequest(format!(
                "Decoded length {} != expected {}",
                decoded.len(),
                expected_len
            )));
        }
    }
    Ok(())
}

#[derive(Debug)]
enum DecodeState {
    ReadingSize,
    ReadingData { remaining: u64 },
    ReadingDataLineEnding,
    Done,
}

struct AwsChunkedStreamDecoder {
    state: DecodeState,
    pending: Vec<u8>,
    decoded_len: u64,
    expected_len: Option<u64>,
}

impl AwsChunkedStreamDecoder {
    fn new(expected_len: Option<u64>) -> Self {
        Self {
            state: DecodeState::ReadingSize,
            pending: Vec::new(),
            decoded_len: 0,
            expected_len,
        }
    }

    async fn push(
        &mut self,
        bytes: Bytes,
        tx: &tokio::sync::mpsc::Sender<Result<Bytes, io::Error>>,
    ) -> io::Result<bool> {
        if matches!(self.state, DecodeState::Done) {
            return Ok(true);
        }

        self.pending.extend_from_slice(&bytes);
        self.process_pending(tx).await
    }

    async fn process_pending(
        &mut self,
        tx: &tokio::sync::mpsc::Sender<Result<Bytes, io::Error>>,
    ) -> io::Result<bool> {
        loop {
            match self.state {
                DecodeState::ReadingSize => {
                    let Some((line_end, crlf_len)) = find_line_end(&self.pending, 0) else {
                        if self.pending.len() > MAX_AWS_CHUNK_HEADER_LEN {
                            return Err(invalid_data(
                                "Invalid aws-chunked encoding: chunk header too large",
                            ));
                        }
                        return Ok(true);
                    };

                    let chunk_size = parse_chunk_size(&self.pending[..line_end])?;
                    self.pending.drain(..line_end + crlf_len);

                    if chunk_size == 0 {
                        self.validate_final_length()?;
                        self.pending.clear();
                        self.state = DecodeState::Done;
                        return Ok(true);
                    }

                    self.state = DecodeState::ReadingData {
                        remaining: chunk_size,
                    };
                }
                DecodeState::ReadingData { remaining } => {
                    if remaining == 0 {
                        self.state = DecodeState::ReadingDataLineEnding;
                        continue;
                    }
                    if self.pending.is_empty() {
                        return Ok(true);
                    }

                    let take_u64 =
                        remaining.min(self.pending.len() as u64).min(STREAM_EMIT_CHUNK_SIZE as u64);
                    let take = take_u64 as usize;
                    let next_len = self.decoded_len + take_u64;
                    if let Some(expected) = self.expected_len {
                        if next_len > expected {
                            return Err(invalid_data(format!(
                                "Decoded length exceeds expected {expected}"
                            )));
                        }
                    }

                    let out = Bytes::copy_from_slice(&self.pending[..take]);
                    self.pending.drain(..take);
                    self.decoded_len = next_len;
                    self.state = DecodeState::ReadingData {
                        remaining: remaining - take_u64,
                    };

                    if tx.send(Ok(out)).await.is_err() {
                        return Ok(false);
                    }
                }
                DecodeState::ReadingDataLineEnding => {
                    if self.pending.is_empty() {
                        return Ok(true);
                    }
                    if self.pending[0] == b'\n' {
                        self.pending.drain(..1);
                        self.state = DecodeState::ReadingSize;
                        continue;
                    }
                    if self.pending[0] == b'\r' {
                        if self.pending.len() == 1 {
                            return Ok(true);
                        }
                        if self.pending[1] == b'\n' {
                            self.pending.drain(..2);
                            self.state = DecodeState::ReadingSize;
                            continue;
                        }
                    }
                    return Err(invalid_data(
                        "Invalid aws-chunked encoding: missing chunk data line ending",
                    ));
                }
                DecodeState::Done => return Ok(true),
            }
        }
    }

    fn finish(&self) -> io::Result<()> {
        match self.state {
            DecodeState::Done => Ok(()),
            DecodeState::ReadingSize if self.pending.is_empty() => Err(invalid_data(
                "Invalid aws-chunked encoding: missing final chunk",
            )),
            DecodeState::ReadingSize => Err(invalid_data(
                "Invalid aws-chunked encoding: truncated chunk header",
            )),
            DecodeState::ReadingData { .. } => Err(invalid_data(
                "Invalid aws-chunked encoding: truncated chunk data",
            )),
            DecodeState::ReadingDataLineEnding => Err(invalid_data(
                "Invalid aws-chunked encoding: missing chunk data line ending",
            )),
        }
    }

    fn validate_final_length(&self) -> io::Result<()> {
        if let Some(expected) = self.expected_len {
            if self.decoded_len != expected {
                return Err(invalid_data(format!(
                    "Decoded length {} != expected {}",
                    self.decoded_len, expected
                )));
            }
        }
        Ok(())
    }
}

fn parse_chunk_size(line: &[u8]) -> io::Result<u64> {
    let size_line = std::str::from_utf8(line)
        .map_err(|_| invalid_data("Invalid aws-chunked encoding: chunk size is not UTF-8"))?;
    let size_part = size_line.split(';').next().unwrap_or(size_line).trim();

    if size_part.is_empty() {
        return Err(invalid_data(
            "Invalid aws-chunked encoding: missing chunk size",
        ));
    }

    u64::from_str_radix(size_part, 16).map_err(|_| {
        invalid_data(format!(
            "Invalid aws-chunked encoding: invalid chunk size '{size_part}'"
        ))
    })
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
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
    use tokio_stream::StreamExt;

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

    async fn collect_streamed_decode(
        chunks: Vec<&'static [u8]>,
        expected_len: Option<u64>,
    ) -> Result<Vec<u8>, io::Error> {
        let stream = tokio_stream::iter(
            chunks
                .into_iter()
                .map(|chunk| Ok::<Bytes, io::Error>(Bytes::from_static(chunk))),
        );
        let mut decoded = decode_aws_chunked_stream(stream, expected_len);
        let mut out = Vec::new();
        while let Some(item) = decoded.next().await {
            out.extend_from_slice(&item?);
        }
        Ok(out)
    }

    #[tokio::test]
    async fn test_stream_decode_split_boundaries() {
        let decoded = collect_streamed_decode(
            vec![
                b"5\r",
                b"\nhe",
                b"llo\r\n6;chunk-signature=abc123\r\n wo",
                b"rld\r\n0\r\nx-amz-checksum-crc32:sOO8/Q==\r\n\r\n",
            ],
            Some(11),
        )
        .await
        .unwrap();
        assert_eq!(decoded, b"hello world");
    }

    #[tokio::test]
    async fn test_stream_decode_expected_length_mismatch() {
        let err = collect_streamed_decode(vec![b"5\r\nhello\r\n0\r\n\r\n"], Some(6))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("Decoded length 5 != expected 6"));
    }

    #[tokio::test]
    async fn test_stream_decode_invalid_line_ending() {
        let err = collect_streamed_decode(vec![b"5\r\nhelloX0\r\n\r\n"], Some(5))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("missing chunk data line ending"));
    }
}
