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

//! CORS (Cross-Origin Resource Sharing) configuration.
//!
//! This module provides S3-compatible CORS configuration management
//! for bucket-level cross-origin access control.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// CORS configuration for a bucket.
///
/// Contains a list of CORS rules that define which origins
/// can access the bucket and how.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CORSConfiguration {
    /// List of CORS rules.
    pub rules: Vec<CORSRule>,
}

/// A single CORS rule.
///
/// Each rule defines allowed origins, methods, headers, and caching behavior
/// for cross-origin requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CORSRule {
    /// Unique identifier for this rule (optional).
    #[serde(default)]
    pub id: Option<String>,

    /// Allowed origins for CORS requests.
    /// Use "*" to allow all origins.
    pub allowed_origins: Vec<String>,

    /// Allowed HTTP methods for CORS requests.
    pub allowed_methods: Vec<String>,

    /// Allowed headers in CORS requests.
    #[serde(default)]
    pub allowed_headers: Vec<String>,

    /// Headers exposed to the browser.
    #[serde(default)]
    pub expose_headers: Vec<String>,

    /// Maximum time (in seconds) the browser can cache preflight response.
    #[serde(default)]
    pub max_age_seconds: Option<u32>,
}

impl CORSConfiguration {
    /// Creates an empty CORS configuration.
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Adds a rule to the configuration.
    pub fn add_rule(&mut self, rule: CORSRule) {
        self.rules.push(rule);
    }

    /// Validates the CORS configuration.
    pub fn validate(&self) -> Result<(), CORSError> {
        if self.rules.is_empty() {
            return Err(CORSError::NoRules);
        }

        if self.rules.len() > 100 {
            return Err(CORSError::TooManyRules);
        }

        for (i, rule) in self.rules.iter().enumerate() {
            rule.validate().map_err(|e| CORSError::InvalidRule {
                index: i,
                error: e.to_string(),
            })?;
        }

        Ok(())
    }

    /// Checks if an origin is allowed by any rule.
    pub fn is_origin_allowed(&self, origin: &str) -> bool {
        self.rules.iter().any(|r| r.is_origin_allowed(origin))
    }

    /// Finds the first rule that matches the given origin and method.
    pub fn find_matching_rule(&self, origin: &str, method: &str) -> Option<&CORSRule> {
        self.rules
            .iter()
            .find(|r| r.is_origin_allowed(origin) && r.is_method_allowed(method))
    }
}

impl CORSRule {
    /// Creates a new CORS rule builder.
    pub fn builder() -> CORSRuleBuilder {
        CORSRuleBuilder::default()
    }

    /// Validates this CORS rule.
    pub fn validate(&self) -> Result<(), CORSRuleError> {
        if self.allowed_origins.is_empty() {
            return Err(CORSRuleError::NoAllowedOrigins);
        }

        if self.allowed_methods.is_empty() {
            return Err(CORSRuleError::NoAllowedMethods);
        }

        // Validate methods
        let valid_methods: HashSet<&str> =
            ["GET", "PUT", "POST", "DELETE", "HEAD"].into_iter().collect();
        for method in &self.allowed_methods {
            if !valid_methods.contains(method.to_uppercase().as_str()) {
                return Err(CORSRuleError::InvalidMethod(method.clone()));
            }
        }

        // Validate max_age_seconds
        if let Some(max_age) = self.max_age_seconds {
            if max_age > 86400 {
                return Err(CORSRuleError::MaxAgeTooLarge);
            }
        }

        Ok(())
    }

    /// Checks if an origin is allowed by this rule.
    pub fn is_origin_allowed(&self, origin: &str) -> bool {
        for allowed in &self.allowed_origins {
            if allowed == "*" {
                return true;
            }
            if self.origin_matches(allowed, origin) {
                return true;
            }
        }
        false
    }

    /// Checks if a method is allowed by this rule.
    pub fn is_method_allowed(&self, method: &str) -> bool {
        let method_upper = method.to_uppercase();
        self.allowed_methods.iter().any(|m| m.to_uppercase() == method_upper)
    }

    /// Checks if a header is allowed by this rule.
    pub fn is_header_allowed(&self, header: &str) -> bool {
        if self.allowed_headers.iter().any(|h| h == "*") {
            return true;
        }
        let header_lower = header.to_lowercase();
        self.allowed_headers.iter().any(|h| h.to_lowercase() == header_lower)
    }

    /// Matches origin against pattern (supports wildcard prefix).
    fn origin_matches(&self, pattern: &str, origin: &str) -> bool {
        if pattern.starts_with("*.") {
            // Wildcard subdomain matching
            let suffix = &pattern[1..]; // ".example.com"
            origin.ends_with(suffix) || origin == &pattern[2..] // "example.com" matches "*.example.com"
        } else {
            pattern == origin
        }
    }
}

/// Builder for creating CORS rules.
#[derive(Default)]
pub struct CORSRuleBuilder {
    id: Option<String>,
    allowed_origins: Vec<String>,
    allowed_methods: Vec<String>,
    allowed_headers: Vec<String>,
    expose_headers: Vec<String>,
    max_age_seconds: Option<u32>,
}

impl CORSRuleBuilder {
    /// Sets the rule ID.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Adds an allowed origin.
    pub fn allowed_origin(mut self, origin: impl Into<String>) -> Self {
        self.allowed_origins.push(origin.into());
        self
    }

    /// Sets all allowed origins.
    pub fn allowed_origins(mut self, origins: Vec<String>) -> Self {
        self.allowed_origins = origins;
        self
    }

    /// Adds an allowed method.
    pub fn allowed_method(mut self, method: impl Into<String>) -> Self {
        self.allowed_methods.push(method.into());
        self
    }

    /// Sets all allowed methods.
    pub fn allowed_methods(mut self, methods: Vec<String>) -> Self {
        self.allowed_methods = methods;
        self
    }

    /// Adds an allowed header.
    pub fn allowed_header(mut self, header: impl Into<String>) -> Self {
        self.allowed_headers.push(header.into());
        self
    }

    /// Sets all allowed headers.
    pub fn allowed_headers(mut self, headers: Vec<String>) -> Self {
        self.allowed_headers = headers;
        self
    }

    /// Adds an exposed header.
    pub fn expose_header(mut self, header: impl Into<String>) -> Self {
        self.expose_headers.push(header.into());
        self
    }

    /// Sets the max age for preflight caching.
    pub fn max_age_seconds(mut self, seconds: u32) -> Self {
        self.max_age_seconds = Some(seconds);
        self
    }

    /// Builds the CORS rule.
    pub fn build(self) -> CORSRule {
        CORSRule {
            id: self.id,
            allowed_origins: self.allowed_origins,
            allowed_methods: self.allowed_methods,
            allowed_headers: self.allowed_headers,
            expose_headers: self.expose_headers,
            max_age_seconds: self.max_age_seconds,
        }
    }
}

/// Errors related to CORS configuration.
#[derive(Debug, thiserror::Error)]
pub enum CORSError {
    /// No CORS rules defined.
    #[error("CORS configuration must contain at least one rule")]
    NoRules,

    /// Too many CORS rules.
    #[error("CORS configuration cannot exceed 100 rules")]
    TooManyRules,

    /// Invalid rule at specified index.
    #[error("Invalid CORS rule at index {index}: {error}")]
    InvalidRule {
        /// Rule index.
        index: usize,
        /// Error description.
        error: String,
    },
}

/// Errors related to a single CORS rule.
#[derive(Debug, thiserror::Error)]
pub enum CORSRuleError {
    /// No allowed origins.
    #[error("CORS rule must have at least one allowed origin")]
    NoAllowedOrigins,

    /// No allowed methods.
    #[error("CORS rule must have at least one allowed method")]
    NoAllowedMethods,

    /// Invalid HTTP method.
    #[error("Invalid HTTP method: {0}")]
    InvalidMethod(String),

    /// Max age is too large.
    #[error("MaxAgeSeconds cannot exceed 86400 (24 hours)")]
    MaxAgeTooLarge,
}

/// Parses CORS configuration from S3-compatible XML.
///
/// # Arguments
///
/// * `xml` - XML string containing CORSConfiguration
///
/// # Returns
///
/// Parsed `CORSConfiguration` or error.
pub fn parse_cors_xml(xml: &str) -> Result<CORSConfiguration, CORSParseError> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut config = CORSConfiguration::new();
    let mut current_rule: Option<CORSRule> = None;
    let mut current_element = String::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                current_element = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if current_element == "CORSRule" {
                    current_rule = Some(CORSRule {
                        id: None,
                        allowed_origins: Vec::new(),
                        allowed_methods: Vec::new(),
                        allowed_headers: Vec::new(),
                        expose_headers: Vec::new(),
                        max_age_seconds: None,
                    });
                }
            }
            Ok(Event::Text(e)) => {
                if let Some(ref mut rule) = current_rule {
                    let text = e.unescape().map_err(|_| CORSParseError::InvalidXml)?.to_string();
                    match current_element.as_str() {
                        "ID" => rule.id = Some(text),
                        "AllowedOrigin" => rule.allowed_origins.push(text),
                        "AllowedMethod" => rule.allowed_methods.push(text),
                        "AllowedHeader" => rule.allowed_headers.push(text),
                        "ExposeHeader" => rule.expose_headers.push(text),
                        "MaxAgeSeconds" => {
                            rule.max_age_seconds = text.parse().ok();
                        }
                        _ => {}
                    }
                }
            }
            Ok(Event::End(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if name == "CORSRule" {
                    if let Some(rule) = current_rule.take() {
                        config.rules.push(rule);
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(CORSParseError::InvalidXml),
            _ => {}
        }
        buf.clear();
    }

    Ok(config)
}

/// Generates S3-compatible XML for CORS configuration.
///
/// # Arguments
///
/// * `config` - CORS configuration to serialize
///
/// # Returns
///
/// XML string.
pub fn cors_to_xml(config: &CORSConfiguration) -> String {
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
"#,
    );

    for rule in &config.rules {
        xml.push_str("  <CORSRule>\n");

        if let Some(ref id) = rule.id {
            xml.push_str(&format!("    <ID>{}</ID>\n", escape_xml(id)));
        }

        for origin in &rule.allowed_origins {
            xml.push_str(&format!(
                "    <AllowedOrigin>{}</AllowedOrigin>\n",
                escape_xml(origin)
            ));
        }

        for method in &rule.allowed_methods {
            xml.push_str(&format!(
                "    <AllowedMethod>{}</AllowedMethod>\n",
                escape_xml(method)
            ));
        }

        for header in &rule.allowed_headers {
            xml.push_str(&format!(
                "    <AllowedHeader>{}</AllowedHeader>\n",
                escape_xml(header)
            ));
        }

        for header in &rule.expose_headers {
            xml.push_str(&format!(
                "    <ExposeHeader>{}</ExposeHeader>\n",
                escape_xml(header)
            ));
        }

        if let Some(max_age) = rule.max_age_seconds {
            xml.push_str(&format!("    <MaxAgeSeconds>{}</MaxAgeSeconds>\n", max_age));
        }

        xml.push_str("  </CORSRule>\n");
    }

    xml.push_str("</CORSConfiguration>");
    xml
}

/// CORS XML parsing error.
#[derive(Debug, thiserror::Error)]
pub enum CORSParseError {
    /// Invalid XML format.
    #[error("Invalid XML format")]
    InvalidXml,
}

/// Escapes special XML characters.
fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cors_rule_builder() {
        let rule = CORSRule::builder()
            .id("rule1")
            .allowed_origin("https://example.com")
            .allowed_method("GET")
            .allowed_method("PUT")
            .allowed_header("Content-Type")
            .expose_header("ETag")
            .max_age_seconds(3600)
            .build();

        assert_eq!(rule.id, Some("rule1".to_string()));
        assert_eq!(rule.allowed_origins, vec!["https://example.com"]);
        assert_eq!(rule.allowed_methods, vec!["GET", "PUT"]);
        assert!(rule.validate().is_ok());
    }

    #[test]
    fn test_cors_origin_matching() {
        let rule = CORSRule::builder()
            .allowed_origin("https://example.com")
            .allowed_origin("*.test.com")
            .allowed_method("GET")
            .build();

        assert!(rule.is_origin_allowed("https://example.com"));
        assert!(rule.is_origin_allowed("https://sub.test.com"));
        assert!(rule.is_origin_allowed("test.com"));
        assert!(!rule.is_origin_allowed("https://other.com"));
    }

    #[test]
    fn test_cors_wildcard_origin() {
        let rule = CORSRule::builder().allowed_origin("*").allowed_method("GET").build();

        assert!(rule.is_origin_allowed("https://any.example.com"));
        assert!(rule.is_origin_allowed("http://localhost"));
    }

    #[test]
    fn test_cors_method_matching() {
        let rule = CORSRule::builder()
            .allowed_origin("*")
            .allowed_method("GET")
            .allowed_method("POST")
            .build();

        assert!(rule.is_method_allowed("GET"));
        assert!(rule.is_method_allowed("get"));
        assert!(rule.is_method_allowed("POST"));
        assert!(!rule.is_method_allowed("DELETE"));
    }

    #[test]
    fn test_cors_validation() {
        let mut config = CORSConfiguration::new();
        assert!(config.validate().is_err()); // No rules

        config.add_rule(CORSRule::builder().allowed_origin("*").allowed_method("GET").build());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_cors_xml_roundtrip() {
        let mut config = CORSConfiguration::new();
        config.add_rule(
            CORSRule::builder()
                .id("rule1")
                .allowed_origin("https://example.com")
                .allowed_method("GET")
                .allowed_method("PUT")
                .allowed_header("*")
                .expose_header("ETag")
                .max_age_seconds(3600)
                .build(),
        );

        let xml = cors_to_xml(&config);
        let parsed = parse_cors_xml(&xml).unwrap();

        assert_eq!(parsed.rules.len(), 1);
        assert_eq!(parsed.rules[0].id, Some("rule1".to_string()));
        assert_eq!(parsed.rules[0].allowed_origins, vec!["https://example.com"]);
        assert_eq!(parsed.rules[0].allowed_methods, vec!["GET", "PUT"]);
    }

    #[test]
    fn test_parse_s3_cors_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
  <CORSRule>
    <AllowedOrigin>http://www.example.com</AllowedOrigin>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedMethod>POST</AllowedMethod>
    <AllowedMethod>DELETE</AllowedMethod>
    <AllowedHeader>*</AllowedHeader>
  </CORSRule>
  <CORSRule>
    <AllowedOrigin>*</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <MaxAgeSeconds>3000</MaxAgeSeconds>
  </CORSRule>
</CORSConfiguration>"#;

        let config = parse_cors_xml(xml).unwrap();
        assert_eq!(config.rules.len(), 2);

        assert_eq!(
            config.rules[0].allowed_origins,
            vec!["http://www.example.com"]
        );
        assert_eq!(
            config.rules[0].allowed_methods,
            vec!["PUT", "POST", "DELETE"]
        );
        assert_eq!(config.rules[0].allowed_headers, vec!["*"]);

        assert_eq!(config.rules[1].allowed_origins, vec!["*"]);
        assert_eq!(config.rules[1].allowed_methods, vec!["GET"]);
        assert_eq!(config.rules[1].max_age_seconds, Some(3000));
    }

    #[test]
    fn test_find_matching_rule() {
        let mut config = CORSConfiguration::new();
        config.add_rule(
            CORSRule::builder()
                .id("read-only")
                .allowed_origin("https://readonly.com")
                .allowed_method("GET")
                .build(),
        );
        config.add_rule(
            CORSRule::builder()
                .id("full-access")
                .allowed_origin("https://admin.com")
                .allowed_method("GET")
                .allowed_method("PUT")
                .allowed_method("DELETE")
                .build(),
        );

        let rule = config.find_matching_rule("https://readonly.com", "GET");
        assert!(rule.is_some());
        assert_eq!(rule.unwrap().id, Some("read-only".to_string()));

        let rule = config.find_matching_rule("https://readonly.com", "PUT");
        assert!(rule.is_none()); // PUT not allowed for readonly.com

        let rule = config.find_matching_rule("https://admin.com", "DELETE");
        assert!(rule.is_some());
        assert_eq!(rule.unwrap().id, Some("full-access".to_string()));
    }
}
