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

//! S3 Select event stream binary encoder.
//!
//! The S3 SelectObjectContent response uses a chunked binary event stream format.
//! Each message consists of:
//! 1. Total byte length (4 bytes, big-endian)
//! 2. Headers byte length (4 bytes, big-endian)
//! 3. Prelude CRC32 (4 bytes)
//! 4. Headers (variable)
//! 5. Payload (variable)
//! 6. Message CRC32 (4 bytes)
//!
//! Three event types:
//! - **Records**: Contains CSV/JSON result data
//! - **Stats**: XML with BytesScanned, BytesProcessed, BytesReturned
//! - **End**: Empty payload, signals completion

use crate::error::SelectError;
use crate::formats::csv as csv_format;
use crate::formats::json as json_format;
use crate::request::OutputSerialization;
use arrow::array::RecordBatch;

/// Encode a header as key-value pair in the event stream format.
///
/// Header format: name_len(1) + name + type(1) + value_len(2) + value
fn encode_header(name: &str, value: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    // Header name (1 byte length + name bytes)
    buf.push(name.len() as u8);
    buf.extend_from_slice(name.as_bytes());
    // Header value type: 7 = string
    buf.push(7);
    // Value length (2 bytes big-endian) + value bytes
    let value_bytes = value.as_bytes();
    buf.extend_from_slice(&(value_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(value_bytes);
    buf
}

/// Encode a complete event stream message.
fn encode_message(headers: &[u8], payload: &[u8]) -> Vec<u8> {
    // Prelude: total_length (4) + headers_length (4)
    let headers_len = headers.len() as u32;
    let total_len = 4 + 4 + 4 + headers_len + payload.len() as u32 + 4; // prelude + prelude_crc + headers + payload + message_crc

    let mut prelude = Vec::with_capacity(8);
    prelude.extend_from_slice(&total_len.to_be_bytes());
    prelude.extend_from_slice(&headers_len.to_be_bytes());

    // Prelude CRC
    let prelude_crc = crc32fast::hash(&prelude);

    let mut message = Vec::with_capacity(total_len as usize);
    message.extend_from_slice(&prelude);
    message.extend_from_slice(&prelude_crc.to_be_bytes());
    message.extend_from_slice(headers);
    message.extend_from_slice(payload);

    // Message CRC (over everything so far)
    let message_crc = crc32fast::hash(&message);
    message.extend_from_slice(&message_crc.to_be_bytes());

    message
}

/// Encode result RecordBatches as a Records event.
///
/// Serializes the batches in the output format (CSV or JSON), then wraps
/// in the event stream binary message format.
pub fn encode_records_event(
    batches: &[RecordBatch],
    output: &OutputSerialization,
) -> Result<Vec<u8>, SelectError> {
    let payload = match output {
        OutputSerialization::Csv(csv_config) => {
            csv_format::batches_to_csv_with_config(batches, Some(csv_config))?
        }
        OutputSerialization::Json => json_format::batches_to_json(batches)?,
    };

    let mut headers = Vec::new();
    headers.extend_from_slice(&encode_header(":message-type", "event"));
    headers.extend_from_slice(&encode_header(":event-type", "Records"));
    headers.extend_from_slice(&encode_header(
        ":content-type",
        match output {
            OutputSerialization::Csv(_) => "text/csv",
            OutputSerialization::Json => "application/json",
        },
    ));

    Ok(encode_message(&headers, &payload))
}

/// Encode a Stats event.
///
/// Contains XML with bytes scanned/processed/returned counts.
pub fn encode_stats_event(
    bytes_scanned: u64,
    bytes_processed: u64,
    bytes_returned: u64,
) -> Vec<u8> {
    let payload = format!(
        "<Stats><BytesScanned>{bytes_scanned}</BytesScanned>\
         <BytesProcessed>{bytes_processed}</BytesProcessed>\
         <BytesReturned>{bytes_returned}</BytesReturned></Stats>"
    );

    let mut headers = Vec::new();
    headers.extend_from_slice(&encode_header(":message-type", "event"));
    headers.extend_from_slice(&encode_header(":event-type", "Stats"));
    headers.extend_from_slice(&encode_header(":content-type", "text/xml"));

    encode_message(&headers, payload.as_bytes())
}

/// Encode the End event (signals completion).
pub fn encode_end_event() -> Vec<u8> {
    let mut headers = Vec::new();
    headers.extend_from_slice(&encode_header(":message-type", "event"));
    headers.extend_from_slice(&encode_header(":event-type", "End"));

    encode_message(&headers, &[])
}

/// Build the complete event stream response body.
///
/// Concatenates: Records event(s) + Stats event + End event.
pub fn build_event_stream_response(
    batches: &[RecordBatch],
    output: &OutputSerialization,
    bytes_scanned: u64,
) -> Result<Vec<u8>, SelectError> {
    let mut response = Vec::new();

    // Records event with result data
    if !batches.is_empty() {
        let records = encode_records_event(batches, output)?;
        response.extend_from_slice(&records);
    }

    // Calculate bytes returned
    let bytes_returned = response.len() as u64;

    // Stats event
    let stats = encode_stats_event(bytes_scanned, bytes_scanned, bytes_returned);
    response.extend_from_slice(&stats);

    // End event
    let end = encode_end_event();
    response.extend_from_slice(&end);

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_header() {
        let header = encode_header(":event-type", "Records");
        // name_len(1) + ":event-type"(11) + type(1) + value_len(2) + "Records"(7) = 22
        assert_eq!(header.len(), 1 + 11 + 1 + 2 + 7);
        assert_eq!(header[0], 11); // name length
    }

    #[test]
    fn test_encode_message_structure() {
        let headers = encode_header(":event-type", "End");
        let msg = encode_message(&headers, &[]);

        // Total = 4 (total_len) + 4 (headers_len) + 4 (prelude_crc) + headers + 0 (payload) + 4 (msg_crc)
        let expected_len = 4 + 4 + 4 + headers.len() + 4;
        assert_eq!(msg.len(), expected_len);

        // Verify total_len field
        let total_len = u32::from_be_bytes([msg[0], msg[1], msg[2], msg[3]]);
        assert_eq!(total_len as usize, expected_len);

        // Verify headers_len field
        let headers_len = u32::from_be_bytes([msg[4], msg[5], msg[6], msg[7]]);
        assert_eq!(headers_len as usize, headers.len());

        // Verify prelude CRC
        let prelude_crc_stored = u32::from_be_bytes([msg[8], msg[9], msg[10], msg[11]]);
        let prelude_crc_computed = crc32fast::hash(&msg[0..8]);
        assert_eq!(prelude_crc_stored, prelude_crc_computed);

        // Verify message CRC
        let msg_crc_start = msg.len() - 4;
        let msg_crc_stored = u32::from_be_bytes([
            msg[msg_crc_start],
            msg[msg_crc_start + 1],
            msg[msg_crc_start + 2],
            msg[msg_crc_start + 3],
        ]);
        let msg_crc_computed = crc32fast::hash(&msg[..msg_crc_start]);
        assert_eq!(msg_crc_stored, msg_crc_computed);
    }

    #[test]
    fn test_stats_event() {
        let event = encode_stats_event(1000, 1000, 500);
        assert!(!event.is_empty());

        // Verify CRC integrity
        let msg_len = event.len();
        let msg_crc_stored = u32::from_be_bytes([
            event[msg_len - 4],
            event[msg_len - 3],
            event[msg_len - 2],
            event[msg_len - 1],
        ]);
        let msg_crc_computed = crc32fast::hash(&event[..msg_len - 4]);
        assert_eq!(msg_crc_stored, msg_crc_computed);
    }

    #[test]
    fn test_end_event() {
        let event = encode_end_event();
        assert!(!event.is_empty());

        // Verify CRC integrity
        let msg_len = event.len();
        let msg_crc_stored = u32::from_be_bytes([
            event[msg_len - 4],
            event[msg_len - 3],
            event[msg_len - 2],
            event[msg_len - 1],
        ]);
        let msg_crc_computed = crc32fast::hash(&event[..msg_len - 4]);
        assert_eq!(msg_crc_stored, msg_crc_computed);
    }
}
