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

//! XML response generation for S3 compatibility.
//!
//! Generates S3-compatible XML responses for API operations.

use chrono::{TimeZone, Utc};
use s4_core::types::IndexRecord;

use crate::handlers::bucket::DeleteObjectsResult;

/// Generates XML response for ListBuckets.
///
/// # Arguments
///
/// * `buckets` - List of bucket names
///
/// # Returns
///
/// XML string conforming to S3 ListAllMyBucketsResult schema.
pub fn list_buckets_response(buckets: &[String]) -> String {
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>s4-owner</ID>
    <DisplayName>S4 Storage Owner</DisplayName>
  </Owner>
  <Buckets>
"#,
    );

    for bucket in buckets {
        xml.push_str(&format!(
            r#"    <Bucket>
      <Name>{}</Name>
      <CreationDate>{}</CreationDate>
    </Bucket>
"#,
            escape_xml(bucket),
            Utc::now().format("%Y-%m-%dT%H:%M:%S.000Z")
        ));
    }

    xml.push_str(
        r#"  </Buckets>
</ListAllMyBucketsResult>"#,
    );

    xml
}

/// Generates XML response for ListObjects (ListBucketResult).
///
/// # Arguments
///
/// * `bucket` - Bucket name
/// * `prefix` - Prefix filter used
/// * `delimiter` - Delimiter used (optional)
/// * `max_keys` - Maximum keys requested
/// * `is_truncated` - Whether there are more results beyond this page
/// * `objects` - List of objects with their records
///
/// # Returns
///
/// XML string conforming to S3 ListBucketResult schema.
pub fn list_objects_response(
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: usize,
    is_truncated: bool,
    objects: &[(String, IndexRecord)],
) -> String {
    let mut xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>{}</Name>
  <Prefix>{}</Prefix>
  <MaxKeys>{}</MaxKeys>
  <IsTruncated>{}</IsTruncated>
"#,
        escape_xml(bucket),
        escape_xml(prefix),
        max_keys,
        is_truncated
    );

    if let Some(delim) = delimiter {
        xml.push_str(&format!("  <Delimiter>{}</Delimiter>\n", escape_xml(delim)));
    }

    for (key, record) in objects {
        let last_modified = format_timestamp(record.modified_at);
        xml.push_str(&format!(
            r#"  <Contents>
    <Key>{}</Key>
    <LastModified>{}</LastModified>
    <ETag>"{}"</ETag>
    <Size>{}</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
"#,
            escape_xml(key),
            last_modified,
            escape_xml(&record.etag),
            record.size
        ));
    }

    xml.push_str("</ListBucketResult>");

    xml
}

/// Generates XML response for ListObjectsV2 (ListBucketResultV2).
///
/// # Arguments
///
/// * `bucket` - Bucket name
/// * `prefix` - Prefix filter used
/// * `delimiter` - Delimiter used (optional)
/// * `max_keys` - Maximum keys requested
/// * `is_truncated` - Whether there are more results beyond this page
/// * `objects` - List of objects with their records
/// * `continuation_token` - Continuation token for pagination (optional)
///
/// # Returns
///
/// XML string conforming to S3 ListBucketResultV2 schema.
pub fn list_objects_v2_response(
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: usize,
    is_truncated: bool,
    objects: &[(String, IndexRecord)],
    continuation_token: Option<&str>,
) -> String {
    let mut xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>{}</Name>
  <Prefix>{}</Prefix>
  <MaxKeys>{}</MaxKeys>
  <IsTruncated>{}</IsTruncated>
"#,
        escape_xml(bucket),
        escape_xml(prefix),
        max_keys,
        is_truncated
    );

    if let Some(delim) = delimiter {
        xml.push_str(&format!("  <Delimiter>{}</Delimiter>\n", escape_xml(delim)));
    }

    if let Some(token) = continuation_token {
        xml.push_str(&format!(
            "  <ContinuationToken>{}</ContinuationToken>\n",
            escape_xml(token)
        ));
    }

    if is_truncated && !objects.is_empty() {
        // Use the last object's key as NextContinuationToken
        if let Some((last_key, _)) = objects.last() {
            xml.push_str(&format!(
                "  <NextContinuationToken>{}</NextContinuationToken>\n",
                escape_xml(last_key)
            ));
        }
    }

    xml.push_str("  <KeyCount>");
    xml.push_str(&objects.len().to_string());
    xml.push_str("</KeyCount>\n");

    for (key, record) in objects {
        let last_modified = format_timestamp(record.modified_at);
        xml.push_str(&format!(
            r#"  <Contents>
    <Key>{}</Key>
    <LastModified>{}</LastModified>
    <ETag>"{}"</ETag>
    <Size>{}</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
"#,
            escape_xml(key),
            last_modified,
            escape_xml(&record.etag),
            record.size
        ));
    }

    xml.push_str("</ListBucketResult>");

    xml
}

/// Generates XML error response.
///
/// # Arguments
///
/// * `code` - S3 error code (e.g., "NoSuchBucket")
/// * `message` - Human-readable error message
/// * `resource` - Resource that caused the error
/// * `request_id` - Request ID for tracing
///
/// # Returns
///
/// XML string conforming to S3 Error schema.
pub fn error_response(code: &str, message: &str, resource: &str, request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{}</Code>
  <Message>{}</Message>
  <Resource>{}</Resource>
  <RequestId>{}</RequestId>
</Error>"#,
        escape_xml(code),
        escape_xml(message),
        escape_xml(resource),
        escape_xml(request_id)
    )
}

/// Generates XML response for CopyObject.
pub fn copy_object_response(etag: &str, last_modified: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult>
  <ETag>"{}"</ETag>
  <LastModified>{}</LastModified>
</CopyObjectResult>"#,
        escape_xml(etag),
        escape_xml(last_modified)
    )
}

/// Escapes special XML characters.
fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Formats a nanosecond timestamp as ISO 8601.
///
/// Handles invalid timestamps gracefully by falling back to Unix epoch.
fn format_timestamp(timestamp_nanos: u64) -> String {
    use chrono::LocalResult;
    let secs = (timestamp_nanos / 1_000_000_000) as i64;
    let dt = match Utc.timestamp_opt(secs, 0) {
        LocalResult::Single(dt) => dt,
        // Fallback to Unix epoch for invalid/ambiguous timestamps
        _ => Utc.timestamp_opt(0, 0).single().unwrap_or_else(Utc::now),
    };
    dt.format("%Y-%m-%dT%H:%M:%S.000Z").to_string()
}

/// Generates XML response for ListObjectVersions.
///
/// # Arguments
///
/// * `bucket` - Bucket name
/// * `prefix` - Prefix filter used
/// * `max_keys` - Maximum keys requested
/// * `result` - The ListVersionsResult from storage engine
///
/// # Returns
///
/// XML string conforming to S3 ListVersionsResult schema.
pub fn list_object_versions_response(
    bucket: &str,
    prefix: &str,
    max_keys: usize,
    result: &s4_core::ListVersionsResult,
) -> String {
    let mut xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>{}</Name>
  <Prefix>{}</Prefix>
  <MaxKeys>{}</MaxKeys>
  <IsTruncated>{}</IsTruncated>
"#,
        escape_xml(bucket),
        escape_xml(prefix),
        max_keys,
        result.is_truncated
    );

    // Add pagination markers if truncated
    if let Some(ref marker) = result.next_key_marker {
        xml.push_str(&format!(
            "  <NextKeyMarker>{}</NextKeyMarker>\n",
            escape_xml(marker)
        ));
    }
    if let Some(ref marker) = result.next_version_id_marker {
        xml.push_str(&format!(
            "  <NextVersionIdMarker>{}</NextVersionIdMarker>\n",
            escape_xml(marker)
        ));
    }

    // Add versions (sorted by key, then by version timestamp - newest first)
    for version in &result.versions {
        let last_modified = format_timestamp(version.last_modified);
        xml.push_str(&format!(
            r#"  <Version>
    <Key>{}</Key>
    <VersionId>{}</VersionId>
    <IsLatest>{}</IsLatest>
    <LastModified>{}</LastModified>
    <ETag>"{}"</ETag>
    <Size>{}</Size>
    <StorageClass>STANDARD</StorageClass>
    <Owner>
      <ID>s4-owner</ID>
      <DisplayName>S4 Storage Owner</DisplayName>
    </Owner>
  </Version>
"#,
            escape_xml(&version.key),
            escape_xml(&version.version_id),
            version.is_latest,
            last_modified,
            escape_xml(&version.etag),
            version.size
        ));
    }

    // Add delete markers
    for marker in &result.delete_markers {
        let last_modified = format_timestamp(marker.last_modified);
        xml.push_str(&format!(
            r#"  <DeleteMarker>
    <Key>{}</Key>
    <VersionId>{}</VersionId>
    <IsLatest>{}</IsLatest>
    <LastModified>{}</LastModified>
    <Owner>
      <ID>s4-owner</ID>
      <DisplayName>S4 Storage Owner</DisplayName>
    </Owner>
  </DeleteMarker>
"#,
            escape_xml(&marker.key),
            escape_xml(&marker.version_id),
            marker.is_latest,
            last_modified
        ));
    }

    xml.push_str("</ListVersionsResult>");
    xml
}

/// Generates XML response for DeleteObjects (batch delete).
///
/// # Arguments
///
/// * `result` - The DeleteObjectsResult containing deleted objects and errors
/// * `quiet` - If true, only include errors in response (omit successful deletions)
///
/// # Returns
///
/// XML string conforming to S3 DeleteResult schema.
pub fn delete_objects_response(result: &DeleteObjectsResult, quiet: bool) -> String {
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
"#,
    );

    if !quiet {
        for deleted in &result.deleted {
            xml.push_str("  <Deleted>\n");
            xml.push_str(&format!("    <Key>{}</Key>\n", escape_xml(&deleted.key)));

            if let Some(ref vid) = deleted.version_id {
                xml.push_str(&format!("    <VersionId>{}</VersionId>\n", escape_xml(vid)));
            }

            if deleted.delete_marker {
                xml.push_str("    <DeleteMarker>true</DeleteMarker>\n");
                if let Some(ref dm_vid) = deleted.delete_marker_version_id {
                    xml.push_str(&format!(
                        "    <DeleteMarkerVersionId>{}</DeleteMarkerVersionId>\n",
                        escape_xml(dm_vid)
                    ));
                }
            }

            xml.push_str("  </Deleted>\n");
        }
    }

    for error in &result.errors {
        xml.push_str("  <Error>\n");
        xml.push_str(&format!("    <Key>{}</Key>\n", escape_xml(&error.key)));

        if let Some(ref vid) = error.version_id {
            xml.push_str(&format!("    <VersionId>{}</VersionId>\n", escape_xml(vid)));
        }

        xml.push_str(&format!("    <Code>{}</Code>\n", escape_xml(&error.code)));
        xml.push_str(&format!(
            "    <Message>{}</Message>\n",
            escape_xml(&error.message)
        ));
        xml.push_str("  </Error>\n");
    }

    xml.push_str("</DeleteResult>");
    xml
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_buckets_response() {
        let buckets = vec!["bucket1".to_string(), "bucket2".to_string()];
        let xml = list_buckets_response(&buckets);

        assert!(xml.contains("<Name>bucket1</Name>"));
        assert!(xml.contains("<Name>bucket2</Name>"));
        assert!(xml.contains("ListAllMyBucketsResult"));
    }

    #[test]
    fn test_list_objects_response() {
        let objects = vec![(
            "test-key".to_string(),
            IndexRecord::new(
                1,
                0,
                100,
                [0u8; 32],
                "abc123".to_string(),
                "text/plain".to_string(),
            ),
        )];

        let xml = list_objects_response("my-bucket", "prefix/", None, 1000, false, &objects);

        assert!(xml.contains("<Name>my-bucket</Name>"));
        assert!(xml.contains("<Key>test-key</Key>"));
        assert!(xml.contains("<Size>100</Size>"));
    }

    #[test]
    fn test_escape_xml() {
        assert_eq!(escape_xml("<test>"), "&lt;test&gt;");
        assert_eq!(escape_xml("a&b"), "a&amp;b");
    }

    #[test]
    fn test_error_response() {
        let xml = error_response(
            "NoSuchBucket",
            "The bucket does not exist",
            "/bucket",
            "req123",
        );

        assert!(xml.contains("<Code>NoSuchBucket</Code>"));
        assert!(xml.contains("<RequestId>req123</RequestId>"));
    }
}
