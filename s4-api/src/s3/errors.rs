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

//! S3 error types and responses.
//!
//! Implements S3-compatible error responses with proper XML formatting
//! and HTTP status codes.

use axum::{
    body::Body,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use thiserror::Error;
use uuid::Uuid;

use super::xml;

/// S3 API errors.
#[derive(Error, Debug, Clone)]
pub enum S3Error {
    /// The specified bucket does not exist.
    #[error("The specified bucket does not exist")]
    NoSuchBucket,

    /// The specified key does not exist.
    #[error("The specified key does not exist")]
    NoSuchKey,

    /// The requested bucket name is not available.
    #[error("The requested bucket name is not available")]
    BucketAlreadyExists,

    /// The request is invalid.
    #[error("{0}")]
    InvalidRequest(String),

    /// Missing argument.
    #[error("{0}")]
    InvalidArgument(String),

    /// Access denied.
    #[error("Access Denied")]
    AccessDenied,

    /// The bucket is not empty.
    #[error("The bucket you tried to delete is not empty")]
    BucketNotEmpty,

    /// Invalid bucket name.
    #[error("The specified bucket is not valid")]
    InvalidBucketName(String),

    /// Signature does not match.
    #[error("The request signature we calculated does not match")]
    SignatureDoesNotMatch,

    /// The specified multipart upload does not exist.
    #[error("The specified multipart upload does not exist")]
    NoSuchUpload,

    /// The CORS configuration does not exist.
    #[error("The CORS configuration does not exist")]
    NoSuchCORSConfiguration,

    /// The lifecycle configuration does not exist.
    #[error("The lifecycle configuration does not exist")]
    NoSuchLifecycleConfiguration,

    /// The specified version does not exist.
    #[error("The specified version does not exist")]
    NoSuchVersion,

    /// Malformed XML in request body.
    #[error("The XML you provided was not well-formed")]
    MalformedXML,

    /// The Object Lock configuration does not exist.
    #[error("The Object Lock configuration does not exist")]
    ObjectLockConfigurationNotFound,

    /// Invalid bucket state for the operation.
    #[error("{0}")]
    InvalidBucketState(String),

    /// The object lock configuration does not exist for this object.
    #[error("The specified object does not have an Object Lock configuration")]
    NoSuchObjectLockConfiguration,

    /// Invalid retention configuration.
    #[error("{0}")]
    InvalidRetention(String),

    /// Internal error.
    #[error("{0}")]
    InternalError(String),

    /// The requested functionality is not implemented.
    #[error("{0}")]
    NotImplemented(String),

    /// The specified method is not allowed against this resource.
    #[error("The specified method is not allowed against this resource.")]
    MethodNotAllowed,

    /// The provided 'x-amz-content-sha256' header does not match what was computed.
    #[error("The provided 'x-amz-content-sha256' header does not match what was computed.")]
    XAmzContentSHA256Mismatch,

    /// One or more of the specified parts could not be found (part too small).
    #[error("Your proposed upload is smaller than the minimum allowed object size.")]
    EntityTooSmall,

    /// Bad request (generic 400).
    #[error("{0}")]
    BadRequest(String),

    /// Invalid tag key or value.
    #[error("{0}")]
    InvalidTag(String),

    /// The Content-MD5 you specified did not match what we received.
    #[error("The Content-MD5 you specified did not match what we received.")]
    BadDigest,

    /// The storage class you specified is not valid.
    #[error("The storage class you specified is not valid")]
    InvalidStorageClass,

    /// The server side encryption configuration was not found.
    #[error("The server side encryption configuration was not found")]
    ServerSideEncryptionConfigurationNotFound,
}

impl S3Error {
    /// Returns the S3 error code.
    pub fn code(&self) -> &'static str {
        match self {
            S3Error::NoSuchBucket => "NoSuchBucket",
            S3Error::NoSuchKey => "NoSuchKey",
            S3Error::BucketAlreadyExists => "BucketAlreadyExists",
            S3Error::InvalidRequest(_) => "InvalidRequest",
            S3Error::InvalidArgument(_) => "InvalidArgument",
            S3Error::AccessDenied => "AccessDenied",
            S3Error::BucketNotEmpty => "BucketNotEmpty",
            S3Error::InvalidBucketName(_) => "InvalidBucketName",
            S3Error::SignatureDoesNotMatch => "SignatureDoesNotMatch",
            S3Error::NoSuchUpload => "NoSuchUpload",
            S3Error::NoSuchCORSConfiguration => "NoSuchCORSConfiguration",
            S3Error::NoSuchLifecycleConfiguration => "NoSuchLifecycleConfiguration",
            S3Error::NoSuchVersion => "NoSuchVersion",
            S3Error::MalformedXML => "MalformedXML",
            S3Error::ObjectLockConfigurationNotFound => "ObjectLockConfigurationNotFoundError",
            S3Error::InvalidBucketState(_) => "InvalidBucketState",
            S3Error::NoSuchObjectLockConfiguration => "NoSuchObjectLockConfiguration",
            S3Error::InvalidRetention(_) => "InvalidRetention",
            S3Error::InternalError(_) => "InternalError",
            S3Error::NotImplemented(_) => "NotImplemented",
            S3Error::MethodNotAllowed => "MethodNotAllowed",
            S3Error::XAmzContentSHA256Mismatch => "XAmzContentSHA256Mismatch",
            S3Error::EntityTooSmall => "EntityTooSmall",
            S3Error::BadRequest(_) => "BadRequest",
            S3Error::InvalidTag(_) => "InvalidTag",
            S3Error::BadDigest => "BadDigest",
            S3Error::InvalidStorageClass => "InvalidStorageClass",
            S3Error::ServerSideEncryptionConfigurationNotFound => {
                "ServerSideEncryptionConfigurationNotFoundError"
            }
        }
    }

    /// Returns the HTTP status code for this error.
    pub fn status_code(&self) -> StatusCode {
        match self {
            S3Error::NoSuchBucket => StatusCode::NOT_FOUND,
            S3Error::NoSuchKey => StatusCode::NOT_FOUND,
            S3Error::BucketAlreadyExists => StatusCode::CONFLICT,
            S3Error::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            S3Error::InvalidArgument(_) => StatusCode::BAD_REQUEST,
            S3Error::AccessDenied => StatusCode::FORBIDDEN,
            S3Error::BucketNotEmpty => StatusCode::CONFLICT,
            S3Error::InvalidBucketName(_) => StatusCode::BAD_REQUEST,
            S3Error::SignatureDoesNotMatch => StatusCode::FORBIDDEN,
            S3Error::NoSuchUpload => StatusCode::NOT_FOUND,
            S3Error::NoSuchCORSConfiguration => StatusCode::NOT_FOUND,
            S3Error::NoSuchLifecycleConfiguration => StatusCode::NOT_FOUND,
            S3Error::NoSuchVersion => StatusCode::NOT_FOUND,
            S3Error::MalformedXML => StatusCode::BAD_REQUEST,
            S3Error::ObjectLockConfigurationNotFound => StatusCode::NOT_FOUND,
            S3Error::InvalidBucketState(_) => StatusCode::BAD_REQUEST,
            S3Error::NoSuchObjectLockConfiguration => StatusCode::NOT_FOUND,
            S3Error::InvalidRetention(_) => StatusCode::BAD_REQUEST,
            S3Error::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            S3Error::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            S3Error::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            S3Error::XAmzContentSHA256Mismatch => StatusCode::BAD_REQUEST,
            S3Error::EntityTooSmall => StatusCode::BAD_REQUEST,
            S3Error::BadRequest(_) => StatusCode::BAD_REQUEST,
            S3Error::InvalidTag(_) => StatusCode::BAD_REQUEST,
            S3Error::BadDigest => StatusCode::BAD_REQUEST,
            S3Error::InvalidStorageClass => StatusCode::BAD_REQUEST,
            S3Error::ServerSideEncryptionConfigurationNotFound => StatusCode::NOT_FOUND,
        }
    }
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let request_id = Uuid::new_v4().to_string();
        let xml_body = xml::error_response(self.code(), &self.to_string(), "", &request_id);

        Response::builder()
            .status(self.status_code())
            .header(header::CONTENT_TYPE, "application/xml")
            .header("x-amz-request-id", request_id)
            .body(Body::from(xml_body))
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_codes() {
        assert_eq!(S3Error::NoSuchBucket.code(), "NoSuchBucket");
        assert_eq!(S3Error::NoSuchKey.code(), "NoSuchKey");
        assert_eq!(S3Error::AccessDenied.code(), "AccessDenied");
    }

    #[test]
    fn test_status_codes() {
        assert_eq!(S3Error::NoSuchBucket.status_code(), StatusCode::NOT_FOUND);
        assert_eq!(
            S3Error::BucketAlreadyExists.status_code(),
            StatusCode::CONFLICT
        );
        assert_eq!(S3Error::AccessDenied.status_code(), StatusCode::FORBIDDEN);
    }
}
