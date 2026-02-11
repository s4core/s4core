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
    #[error("NoSuchBucket: The specified bucket does not exist")]
    NoSuchBucket,

    /// The specified key does not exist.
    #[error("NoSuchKey: The specified key does not exist")]
    NoSuchKey,

    /// The requested bucket name is not available.
    #[error("BucketAlreadyExists: The requested bucket name is not available")]
    BucketAlreadyExists,

    /// The request is invalid.
    #[error("InvalidRequest: {0}")]
    InvalidRequest(String),

    /// Access denied.
    #[error("AccessDenied: Access Denied")]
    AccessDenied,

    /// The bucket is not empty.
    #[error("BucketNotEmpty: The bucket you tried to delete is not empty")]
    BucketNotEmpty,

    /// Invalid bucket name.
    #[error("InvalidBucketName: The specified bucket is not valid")]
    InvalidBucketName(String),

    /// Signature does not match.
    #[error("SignatureDoesNotMatch: The request signature we calculated does not match")]
    SignatureDoesNotMatch,

    /// The specified multipart upload does not exist.
    #[error("NoSuchUpload: The specified multipart upload does not exist")]
    NoSuchUpload,

    /// The CORS configuration does not exist.
    #[error("NoSuchCORSConfiguration: The CORS configuration does not exist")]
    NoSuchCORSConfiguration,

    /// The lifecycle configuration does not exist.
    #[error("NoSuchLifecycleConfiguration: The lifecycle configuration does not exist")]
    NoSuchLifecycleConfiguration,

    /// The specified version does not exist.
    #[error("NoSuchVersion: The specified version does not exist")]
    NoSuchVersion,

    /// Malformed XML in request body.
    #[error("MalformedXML: The XML you provided was not well-formed")]
    MalformedXML,

    /// The Object Lock configuration does not exist.
    #[error("ObjectLockConfigurationNotFoundError: The Object Lock configuration does not exist")]
    ObjectLockConfigurationNotFound,

    /// Invalid bucket state for the operation.
    #[error("InvalidBucketState: {0}")]
    InvalidBucketState(String),

    /// The object lock configuration does not exist for this object.
    #[error("NoSuchObjectLockConfiguration: The specified object does not have an Object Lock configuration")]
    NoSuchObjectLockConfiguration,

    /// Invalid retention configuration.
    #[error("InvalidRetention: {0}")]
    InvalidRetention(String),

    /// Internal error.
    #[error("InternalError: {0}")]
    InternalError(String),
}

impl S3Error {
    /// Returns the S3 error code.
    pub fn code(&self) -> &'static str {
        match self {
            S3Error::NoSuchBucket => "NoSuchBucket",
            S3Error::NoSuchKey => "NoSuchKey",
            S3Error::BucketAlreadyExists => "BucketAlreadyExists",
            S3Error::InvalidRequest(_) => "InvalidRequest",
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
        }
    }

    /// Returns the HTTP status code for this error.
    pub fn status_code(&self) -> StatusCode {
        match self {
            S3Error::NoSuchBucket => StatusCode::NOT_FOUND,
            S3Error::NoSuchKey => StatusCode::NOT_FOUND,
            S3Error::BucketAlreadyExists => StatusCode::CONFLICT,
            S3Error::InvalidRequest(_) => StatusCode::BAD_REQUEST,
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
