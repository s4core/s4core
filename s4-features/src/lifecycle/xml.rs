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

//! XML parsing and serialization for lifecycle configuration.
//!
//! Implements S3-compatible XML format for lifecycle policies.

use super::{
    Expiration, LifecycleConfiguration, LifecycleFilter, LifecycleParseError, LifecycleRule,
    NoncurrentVersionExpiration, RuleStatus,
};
use quick_xml::events::Event;
use quick_xml::Reader;

/// Parses lifecycle configuration from S3-compatible XML.
///
/// # Arguments
///
/// * `xml` - XML string containing LifecycleConfiguration
///
/// # Returns
///
/// Parsed `LifecycleConfiguration` or error.
///
/// # Example
///
/// ```
/// use s4_features::lifecycle::parse_lifecycle_xml;
///
/// let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
/// <LifecycleConfiguration>
///   <Rule>
///     <ID>expire-logs</ID>
///     <Status>Enabled</Status>
///     <Filter><Prefix>logs/</Prefix></Filter>
///     <Expiration><Days>30</Days></Expiration>
///   </Rule>
/// </LifecycleConfiguration>"#;
///
/// let config = parse_lifecycle_xml(xml).unwrap();
/// assert_eq!(config.rules.len(), 1);
/// ```
pub fn parse_lifecycle_xml(xml: &str) -> Result<LifecycleConfiguration, LifecycleParseError> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut config = LifecycleConfiguration::new();
    let mut current_rule: Option<RuleBuilder> = None;
    let mut current_element = String::new();
    let mut buf = Vec::new();

    // Track nested context
    let mut in_filter = false;
    let mut in_expiration = false;
    let mut in_noncurrent = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                current_element = String::from_utf8_lossy(e.name().as_ref()).to_string();

                match current_element.as_str() {
                    "Rule" => {
                        current_rule = Some(RuleBuilder::new());
                    }
                    "Filter" => {
                        in_filter = true;
                    }
                    "Expiration" => {
                        in_expiration = true;
                    }
                    "NoncurrentVersionExpiration" => {
                        in_noncurrent = true;
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(e)) => {
                if let Some(ref mut rule) = current_rule {
                    let text =
                        e.unescape().map_err(|_| LifecycleParseError::InvalidXml)?.to_string();

                    match current_element.as_str() {
                        "ID" => rule.id = Some(text),
                        "Status" => rule.status = RuleStatus::parse_str(&text),
                        "Prefix" if in_filter => rule.prefix = Some(text),
                        "Days" if in_expiration => {
                            rule.expiration_days = text.parse().ok();
                        }
                        "NoncurrentDays" if in_noncurrent => {
                            rule.noncurrent_days = text.parse().ok();
                        }
                        "ExpiredObjectDeleteMarker" => {
                            rule.expired_object_delete_marker = Some(text.to_lowercase() == "true");
                        }
                        _ => {}
                    }
                }
            }
            Ok(Event::End(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                match name.as_str() {
                    "Rule" => {
                        if let Some(builder) = current_rule.take() {
                            config.rules.push(builder.build()?);
                        }
                    }
                    "Filter" => {
                        in_filter = false;
                    }
                    "Expiration" => {
                        in_expiration = false;
                    }
                    "NoncurrentVersionExpiration" => {
                        in_noncurrent = false;
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(LifecycleParseError::InvalidXml),
            _ => {}
        }
        buf.clear();
    }

    Ok(config)
}

/// Generates S3-compatible XML for lifecycle configuration.
///
/// # Arguments
///
/// * `config` - Lifecycle configuration to serialize
///
/// # Returns
///
/// XML string.
pub fn lifecycle_to_xml(config: &LifecycleConfiguration) -> String {
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
"#,
    );

    for rule in &config.rules {
        xml.push_str("  <Rule>\n");
        xml.push_str(&format!("    <ID>{}</ID>\n", escape_xml(&rule.id)));
        xml.push_str(&format!("    <Status>{}</Status>\n", rule.status.as_str()));

        // Filter
        xml.push_str("    <Filter>\n");
        if let Some(ref prefix) = rule.filter.prefix {
            xml.push_str(&format!("      <Prefix>{}</Prefix>\n", escape_xml(prefix)));
        } else {
            xml.push_str("      <Prefix></Prefix>\n");
        }
        xml.push_str("    </Filter>\n");

        // Expiration
        if let Some(ref expiration) = rule.expiration {
            xml.push_str("    <Expiration>\n");
            if let Some(days) = expiration.days {
                xml.push_str(&format!("      <Days>{}</Days>\n", days));
            }
            xml.push_str("    </Expiration>\n");
        }

        // Noncurrent version expiration
        if let Some(ref noncurrent) = rule.noncurrent_version_expiration {
            xml.push_str("    <NoncurrentVersionExpiration>\n");
            xml.push_str(&format!(
                "      <NoncurrentDays>{}</NoncurrentDays>\n",
                noncurrent.noncurrent_days
            ));
            xml.push_str("    </NoncurrentVersionExpiration>\n");
        }

        // Expired object delete marker
        if let Some(true) = rule.expired_object_delete_marker {
            xml.push_str("    <ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>\n");
        }

        xml.push_str("  </Rule>\n");
    }

    xml.push_str("</LifecycleConfiguration>");
    xml
}

/// Builder for constructing LifecycleRule during parsing.
#[derive(Default)]
struct RuleBuilder {
    id: Option<String>,
    status: Option<RuleStatus>,
    prefix: Option<String>,
    expiration_days: Option<u32>,
    noncurrent_days: Option<u32>,
    expired_object_delete_marker: Option<bool>,
}

impl RuleBuilder {
    fn new() -> Self {
        Self::default()
    }

    fn build(self) -> Result<LifecycleRule, LifecycleParseError> {
        let id = self.id.ok_or_else(|| LifecycleParseError::MissingElement {
            element: "ID".to_string(),
        })?;

        let status = self.status.ok_or_else(|| LifecycleParseError::MissingElement {
            element: "Status".to_string(),
        })?;

        let filter = LifecycleFilter {
            prefix: self.prefix,
        };

        let expiration = self.expiration_days.map(|days| Expiration { days: Some(days) });

        let noncurrent_version_expiration = self
            .noncurrent_days
            .map(|noncurrent_days| NoncurrentVersionExpiration { noncurrent_days });

        Ok(LifecycleRule {
            id,
            status,
            filter,
            expiration,
            noncurrent_version_expiration,
            expired_object_delete_marker: self.expired_object_delete_marker,
        })
    }
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
    fn test_parse_basic_lifecycle_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>expire-logs</ID>
    <Status>Enabled</Status>
    <Filter>
      <Prefix>logs/</Prefix>
    </Filter>
    <Expiration>
      <Days>30</Days>
    </Expiration>
  </Rule>
</LifecycleConfiguration>"#;

        let config = parse_lifecycle_xml(xml).unwrap();
        assert_eq!(config.rules.len(), 1);

        let rule = &config.rules[0];
        assert_eq!(rule.id, "expire-logs");
        assert_eq!(rule.status, RuleStatus::Enabled);
        assert_eq!(rule.filter.prefix, Some("logs/".to_string()));
        assert_eq!(rule.expiration.as_ref().unwrap().days, Some(30));
        assert!(rule.noncurrent_version_expiration.is_none());
    }

    #[test]
    fn test_parse_multiple_rules() {
        let xml = r#"<LifecycleConfiguration>
  <Rule>
    <ID>rule1</ID>
    <Status>Enabled</Status>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
  <Rule>
    <ID>rule2</ID>
    <Status>Disabled</Status>
    <Filter><Prefix>temp/</Prefix></Filter>
    <Expiration><Days>7</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;

        let config = parse_lifecycle_xml(xml).unwrap();
        assert_eq!(config.rules.len(), 2);
        assert_eq!(config.rules[0].id, "rule1");
        assert_eq!(config.rules[0].status, RuleStatus::Enabled);
        assert_eq!(config.rules[1].id, "rule2");
        assert_eq!(config.rules[1].status, RuleStatus::Disabled);
    }

    #[test]
    fn test_parse_noncurrent_version_expiration() {
        let xml = r#"<LifecycleConfiguration>
  <Rule>
    <ID>cleanup-versions</ID>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <NoncurrentVersionExpiration>
      <NoncurrentDays>90</NoncurrentDays>
    </NoncurrentVersionExpiration>
  </Rule>
</LifecycleConfiguration>"#;

        let config = parse_lifecycle_xml(xml).unwrap();
        assert_eq!(config.rules.len(), 1);

        let rule = &config.rules[0];
        assert!(rule.expiration.is_none());
        assert_eq!(
            rule.noncurrent_version_expiration.as_ref().unwrap().noncurrent_days,
            90
        );
    }

    #[test]
    fn test_parse_expired_delete_marker() {
        let xml = r#"<LifecycleConfiguration>
  <Rule>
    <ID>cleanup-markers</ID>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>
  </Rule>
</LifecycleConfiguration>"#;

        let config = parse_lifecycle_xml(xml).unwrap();
        assert_eq!(config.rules.len(), 1);
        assert_eq!(config.rules[0].expired_object_delete_marker, Some(true));
    }

    #[test]
    fn test_parse_full_rule() {
        let xml = r#"<LifecycleConfiguration>
  <Rule>
    <ID>full-cleanup</ID>
    <Status>Enabled</Status>
    <Filter>
      <Prefix>archive/</Prefix>
    </Filter>
    <Expiration>
      <Days>365</Days>
    </Expiration>
    <NoncurrentVersionExpiration>
      <NoncurrentDays>30</NoncurrentDays>
    </NoncurrentVersionExpiration>
    <ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>
  </Rule>
</LifecycleConfiguration>"#;

        let config = parse_lifecycle_xml(xml).unwrap();
        let rule = &config.rules[0];

        assert_eq!(rule.id, "full-cleanup");
        assert_eq!(rule.status, RuleStatus::Enabled);
        assert_eq!(rule.filter.prefix, Some("archive/".to_string()));
        assert_eq!(rule.expiration.as_ref().unwrap().days, Some(365));
        assert_eq!(
            rule.noncurrent_version_expiration.as_ref().unwrap().noncurrent_days,
            30
        );
        assert_eq!(rule.expired_object_delete_marker, Some(true));
    }

    #[test]
    fn test_parse_missing_id() {
        let xml = r#"<LifecycleConfiguration>
  <Rule>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;

        let result = parse_lifecycle_xml(xml);
        assert!(matches!(
            result,
            Err(LifecycleParseError::MissingElement { .. })
        ));
    }

    #[test]
    fn test_parse_missing_status() {
        let xml = r#"<LifecycleConfiguration>
  <Rule>
    <ID>test-rule</ID>
    <Filter><Prefix></Prefix></Filter>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;

        let result = parse_lifecycle_xml(xml);
        assert!(matches!(
            result,
            Err(LifecycleParseError::MissingElement { .. })
        ));
    }

    #[test]
    fn test_lifecycle_to_xml() {
        let mut config = LifecycleConfiguration::new();
        config.add_rule(LifecycleRule {
            id: "test-rule".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::with_prefix("logs/"),
            expiration: Some(Expiration { days: Some(30) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        });

        let xml = lifecycle_to_xml(&config);

        assert!(xml.contains("<ID>test-rule</ID>"));
        assert!(xml.contains("<Status>Enabled</Status>"));
        assert!(xml.contains("<Prefix>logs/</Prefix>"));
        assert!(xml.contains("<Days>30</Days>"));
    }

    #[test]
    fn test_xml_roundtrip() {
        let mut original = LifecycleConfiguration::new();
        original.add_rule(LifecycleRule {
            id: "roundtrip-test".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::with_prefix("data/"),
            expiration: Some(Expiration { days: Some(60) }),
            noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
                noncurrent_days: 30,
            }),
            expired_object_delete_marker: Some(true),
        });

        let xml = lifecycle_to_xml(&original);
        let parsed = parse_lifecycle_xml(&xml).unwrap();

        assert_eq!(parsed.rules.len(), 1);
        let rule = &parsed.rules[0];
        assert_eq!(rule.id, "roundtrip-test");
        assert_eq!(rule.status, RuleStatus::Enabled);
        assert_eq!(rule.filter.prefix, Some("data/".to_string()));
        assert_eq!(rule.expiration.as_ref().unwrap().days, Some(60));
        assert_eq!(
            rule.noncurrent_version_expiration.as_ref().unwrap().noncurrent_days,
            30
        );
        assert_eq!(rule.expired_object_delete_marker, Some(true));
    }

    #[test]
    fn test_escape_xml_characters() {
        let mut config = LifecycleConfiguration::new();
        config.add_rule(LifecycleRule {
            id: "rule-with-<special>&chars".to_string(),
            status: RuleStatus::Enabled,
            filter: LifecycleFilter::with_prefix("test/"),
            expiration: Some(Expiration { days: Some(30) }),
            noncurrent_version_expiration: None,
            expired_object_delete_marker: None,
        });

        let xml = lifecycle_to_xml(&config);
        assert!(xml.contains("&lt;special&gt;&amp;chars"));
    }

    #[test]
    fn test_empty_prefix() {
        let xml = r#"<LifecycleConfiguration>
  <Rule>
    <ID>all-objects</ID>
    <Status>Enabled</Status>
    <Filter>
      <Prefix></Prefix>
    </Filter>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;

        let config = parse_lifecycle_xml(xml).unwrap();
        // Empty prefix element should result in None or empty string - both match all
        assert!(
            config.rules[0].filter.prefix.is_none()
                || config.rules[0].filter.prefix == Some("".to_string())
        );
    }

    #[test]
    fn test_no_filter_prefix() {
        let xml = r#"<LifecycleConfiguration>
  <Rule>
    <ID>no-prefix</ID>
    <Status>Enabled</Status>
    <Filter></Filter>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;

        let config = parse_lifecycle_xml(xml).unwrap();
        assert!(config.rules[0].filter.prefix.is_none());
    }

    #[test]
    fn test_parse_s3_real_format() {
        // Test with actual AWS S3 format
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Rule>
        <ID>Delete old logs</ID>
        <Filter>
           <Prefix>logs/</Prefix>
        </Filter>
        <Status>Enabled</Status>
        <Expiration>
             <Days>365</Days>
        </Expiration>
    </Rule>
    <Rule>
        <ID>Delete old versions</ID>
        <Filter>
           <Prefix></Prefix>
        </Filter>
        <Status>Enabled</Status>
        <NoncurrentVersionExpiration>
             <NoncurrentDays>30</NoncurrentDays>
        </NoncurrentVersionExpiration>
    </Rule>
</LifecycleConfiguration>"#;

        let config = parse_lifecycle_xml(xml).unwrap();
        assert_eq!(config.rules.len(), 2);
        assert_eq!(config.rules[0].id, "Delete old logs");
        assert_eq!(config.rules[1].id, "Delete old versions");
    }
}
