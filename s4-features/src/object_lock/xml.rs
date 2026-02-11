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

//! XML parsing and generation for Object Lock configuration.
//!
//! This module handles conversion between S3-compatible XML and
//! Object Lock data structures.

use super::*;
use quick_xml::events::Event;
use quick_xml::Reader;

/// Parses Object Lock configuration from S3-compatible XML.
///
/// # Arguments
///
/// * `xml` - XML string in S3 ObjectLockConfiguration format
///
/// # Returns
///
/// Parsed configuration or error if XML is malformed
///
/// # Example
///
/// ```
/// use s4_features::object_lock::parse_object_lock_xml;
///
/// let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
/// <ObjectLockConfiguration>
///   <ObjectLockEnabled>Enabled</ObjectLockEnabled>
///   <Rule>
///     <DefaultRetention>
///       <Mode>GOVERNANCE</Mode>
///       <Days>30</Days>
///     </DefaultRetention>
///   </Rule>
/// </ObjectLockConfiguration>"#;
///
/// let config = parse_object_lock_xml(xml).unwrap();
/// assert!(config.object_lock_enabled);
/// ```
pub fn parse_object_lock_xml(xml: &str) -> Result<ObjectLockConfiguration, String> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut config = ObjectLockConfiguration::default();
    let mut current_element = String::new();
    let mut buf = Vec::new();

    // Track nested context
    let mut in_default_retention = false;
    let mut retention_mode: Option<RetentionMode> = None;
    let mut retention_days: Option<u32> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                current_element = String::from_utf8_lossy(e.name().as_ref()).to_string();

                if current_element == "DefaultRetention" {
                    in_default_retention = true;
                }
            }
            Ok(Event::Text(e)) => {
                let text = e.unescape().map_err(|_| "Invalid XML escape sequence")?.to_string();

                match current_element.as_str() {
                    "ObjectLockEnabled" => {
                        config.object_lock_enabled = text == "Enabled";
                    }
                    "Mode" if in_default_retention => {
                        retention_mode = RetentionMode::parse_str(&text);
                        if retention_mode.is_none() {
                            return Err(format!("Invalid retention mode: {}", text));
                        }
                    }
                    "Days" if in_default_retention => {
                        retention_days = text.parse().ok();
                        if retention_days.is_none() {
                            return Err(format!("Invalid retention days: {}", text));
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if name == "DefaultRetention" {
                    // Build DefaultRetention if both mode and days were provided
                    if let (Some(mode), Some(days)) = (retention_mode, retention_days) {
                        config.default_retention = Some(DefaultRetention { mode, days });
                    }
                    in_default_retention = false;
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err("Invalid XML".to_string()),
            _ => {}
        }
        buf.clear();
    }

    Ok(config)
}

/// Generates S3-compatible XML for Object Lock configuration.
///
/// # Arguments
///
/// * `config` - Object Lock configuration
///
/// # Returns
///
/// XML string in S3 ObjectLockConfiguration format
pub fn object_lock_to_xml(config: &ObjectLockConfiguration) -> String {
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
"#,
    );

    // ObjectLockEnabled
    if config.object_lock_enabled {
        xml.push_str("  <ObjectLockEnabled>Enabled</ObjectLockEnabled>\n");
    }

    // Rule with DefaultRetention
    if let Some(ref retention) = config.default_retention {
        xml.push_str("  <Rule>\n");
        xml.push_str("    <DefaultRetention>\n");
        xml.push_str(&format!("      <Mode>{}</Mode>\n", retention.mode.as_str()));
        xml.push_str(&format!("      <Days>{}</Days>\n", retention.days));
        xml.push_str("    </DefaultRetention>\n");
        xml.push_str("  </Rule>\n");
    }

    xml.push_str("</ObjectLockConfiguration>");
    xml
}

/// Parses object retention from S3-compatible XML.
///
/// # Example
///
/// ```
/// use s4_features::object_lock::parse_retention_xml;
///
/// let xml = r#"<Retention>
///   <Mode>COMPLIANCE</Mode>
///   <RetainUntilDate>2025-12-31T23:59:59Z</RetainUntilDate>
/// </Retention>"#;
///
/// let retention = parse_retention_xml(xml).unwrap();
/// assert_eq!(retention.mode.as_str(), "COMPLIANCE");
/// ```
pub fn parse_retention_xml(xml: &str) -> Result<ObjectRetention, String> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut current_element = String::new();
    let mut buf = Vec::new();

    let mut mode: Option<RetentionMode> = None;
    let mut retain_until_date: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                current_element = String::from_utf8_lossy(e.name().as_ref()).to_string();
            }
            Ok(Event::Text(e)) => {
                let text = e.unescape().map_err(|_| "Invalid XML escape sequence")?.to_string();

                match current_element.as_str() {
                    "Mode" => {
                        mode = RetentionMode::parse_str(&text);
                        if mode.is_none() {
                            return Err(format!("Invalid retention mode: {}", text));
                        }
                    }
                    "RetainUntilDate" => {
                        retain_until_date = Some(text);
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err("Invalid XML".to_string()),
            _ => {}
        }
        buf.clear();
    }

    let mode = mode.ok_or("Missing Mode element")?;
    let retain_until_date = retain_until_date.ok_or("Missing RetainUntilDate element")?;

    Ok(ObjectRetention {
        mode,
        retain_until_date,
    })
}

/// Generates S3-compatible XML for object retention.
pub fn retention_to_xml(retention: &ObjectRetention) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Mode>{}</Mode>
  <RetainUntilDate>{}</RetainUntilDate>
</Retention>"#,
        retention.mode.as_str(),
        escape_xml(&retention.retain_until_date)
    )
}

/// Parses legal hold from S3-compatible XML.
///
/// # Example
///
/// ```
/// use s4_features::object_lock::parse_legal_hold_xml;
///
/// let xml = r#"<LegalHold><Status>ON</Status></LegalHold>"#;
///
/// let hold = parse_legal_hold_xml(xml).unwrap();
/// assert_eq!(hold.status.as_str(), "ON");
/// ```
pub fn parse_legal_hold_xml(xml: &str) -> Result<LegalHold, String> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut current_element = String::new();
    let mut buf = Vec::new();

    let mut status: Option<LegalHoldStatus> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                current_element = String::from_utf8_lossy(e.name().as_ref()).to_string();
            }
            Ok(Event::Text(e)) => {
                let text = e.unescape().map_err(|_| "Invalid XML escape sequence")?.to_string();

                if current_element == "Status" {
                    status = LegalHoldStatus::parse_str(&text);
                    if status.is_none() {
                        return Err(format!("Invalid legal hold status: {}", text));
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err("Invalid XML".to_string()),
            _ => {}
        }
        buf.clear();
    }

    let status = status.ok_or("Missing Status element")?;

    Ok(LegalHold { status })
}

/// Generates S3-compatible XML for legal hold.
pub fn legal_hold_to_xml(hold: &LegalHold) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Status>{}</Status>
</LegalHold>"#,
        hold.status.as_str()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_object_lock_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ObjectLockConfiguration>
  <ObjectLockEnabled>Enabled</ObjectLockEnabled>
  <Rule>
    <DefaultRetention>
      <Mode>GOVERNANCE</Mode>
      <Days>30</Days>
    </DefaultRetention>
  </Rule>
</ObjectLockConfiguration>"#;

        let config = parse_object_lock_xml(xml).unwrap();
        assert!(config.object_lock_enabled);
        assert!(config.default_retention.is_some());

        let retention = config.default_retention.unwrap();
        assert_eq!(retention.mode, RetentionMode::GOVERNANCE);
        assert_eq!(retention.days, 30);
    }

    #[test]
    fn test_parse_object_lock_xml_without_default_retention() {
        let xml = r#"<ObjectLockConfiguration>
  <ObjectLockEnabled>Enabled</ObjectLockEnabled>
</ObjectLockConfiguration>"#;

        let config = parse_object_lock_xml(xml).unwrap();
        assert!(config.object_lock_enabled);
        assert!(config.default_retention.is_none());
    }

    #[test]
    fn test_object_lock_to_xml() {
        let config = ObjectLockConfiguration {
            object_lock_enabled: true,
            default_retention: Some(DefaultRetention {
                mode: RetentionMode::COMPLIANCE,
                days: 90,
            }),
        };

        let xml = object_lock_to_xml(&config);
        assert!(xml.contains("<ObjectLockEnabled>Enabled</ObjectLockEnabled>"));
        assert!(xml.contains("<Mode>COMPLIANCE</Mode>"));
        assert!(xml.contains("<Days>90</Days>"));
    }

    #[test]
    fn test_xml_roundtrip() {
        let original = ObjectLockConfiguration {
            object_lock_enabled: true,
            default_retention: Some(DefaultRetention {
                mode: RetentionMode::GOVERNANCE,
                days: 30,
            }),
        };

        let xml = object_lock_to_xml(&original);
        let parsed = parse_object_lock_xml(&xml).unwrap();

        assert_eq!(parsed.object_lock_enabled, original.object_lock_enabled);
        assert_eq!(
            parsed.default_retention.as_ref().unwrap().mode,
            original.default_retention.as_ref().unwrap().mode
        );
        assert_eq!(
            parsed.default_retention.as_ref().unwrap().days,
            original.default_retention.as_ref().unwrap().days
        );
    }

    #[test]
    fn test_parse_retention_xml() {
        let xml = r#"<Retention>
  <Mode>COMPLIANCE</Mode>
  <RetainUntilDate>2025-12-31T23:59:59Z</RetainUntilDate>
</Retention>"#;

        let retention = parse_retention_xml(xml).unwrap();
        assert_eq!(retention.mode, RetentionMode::COMPLIANCE);
        assert_eq!(retention.retain_until_date, "2025-12-31T23:59:59Z");
    }

    #[test]
    fn test_retention_to_xml() {
        let retention = ObjectRetention {
            mode: RetentionMode::GOVERNANCE,
            retain_until_date: "2026-01-31T00:00:00Z".to_string(),
        };

        let xml = retention_to_xml(&retention);
        assert!(xml.contains("<Mode>GOVERNANCE</Mode>"));
        assert!(xml.contains("<RetainUntilDate>2026-01-31T00:00:00Z</RetainUntilDate>"));
    }

    #[test]
    fn test_parse_legal_hold_xml() {
        let xml = r#"<LegalHold><Status>ON</Status></LegalHold>"#;

        let hold = parse_legal_hold_xml(xml).unwrap();
        assert_eq!(hold.status, LegalHoldStatus::ON);
    }

    #[test]
    fn test_legal_hold_to_xml() {
        let hold = LegalHold {
            status: LegalHoldStatus::OFF,
        };

        let xml = legal_hold_to_xml(&hold);
        assert!(xml.contains("<Status>OFF</Status>"));
    }

    #[test]
    fn test_parse_invalid_mode() {
        let xml = r#"<Retention>
  <Mode>INVALID</Mode>
  <RetainUntilDate>2025-12-31T23:59:59Z</RetainUntilDate>
</Retention>"#;

        assert!(parse_retention_xml(xml).is_err());
    }

    #[test]
    fn test_parse_missing_retain_until_date() {
        let xml = r#"<Retention><Mode>GOVERNANCE</Mode></Retention>"#;

        assert!(parse_retention_xml(xml).is_err());
    }

    #[test]
    fn test_escape_xml() {
        assert_eq!(escape_xml("test<>&\"'"), "test&lt;&gt;&amp;&quot;&apos;");
    }
}
