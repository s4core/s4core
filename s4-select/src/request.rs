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

//! S3 SelectObjectContent XML request parser.
//!
//! Parses the XML body of `POST /{bucket}/{key}?select&select-type=2` into
//! a [`SelectRequest`] struct that drives query execution.

use crate::error::SelectError;
use crate::formats::{InputFormat, OutputFormat};
use quick_xml::events::Event;
use quick_xml::Reader;

/// How CSV column headers are handled.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum FileHeaderInfo {
    /// Use the first row as column names.
    Use,
    /// Skip the first row (don't use as headers, don't include in data).
    Ignore,
    /// No header row — columns are `_1`, `_2`, etc.
    #[default]
    None,
}

/// Single-byte placeholder used when a multi-byte Unicode delimiter must be
/// replaced before passing data to Arrow's single-byte CSV reader.
pub const MULTIBYTE_DELIMITER_PLACEHOLDER: u8 = 0x01; // SOH — unlikely in real CSV data

/// CSV input configuration.
#[derive(Debug, Clone)]
pub struct CsvInput {
    /// How to handle the header row.
    pub file_header_info: FileHeaderInfo,
    /// Field delimiter (default: `,`).
    pub field_delimiter: u8,
    /// Record delimiter (default: `\n`).
    pub record_delimiter: u8,
    /// Quote character (default: `"`).
    pub quote_character: u8,
    /// Quote escape character (default: `"`).
    pub quote_escape_character: u8,
    /// Comment character (if set, lines starting with this are skipped).
    pub comments: Option<u8>,
    /// Whether quoted fields can contain record delimiters.
    pub allow_quoted_record_delimiter: bool,
    /// Original field delimiter string (for multi-byte Unicode delimiters).
    /// When this is `Some`, CSV data must be pre-processed to replace the
    /// multi-byte delimiter with [`MULTIBYTE_DELIMITER_PLACEHOLDER`].
    pub original_field_delimiter: Option<String>,
    /// Original quote character string (for multi-byte Unicode quote chars).
    /// When this is `Some`, CSV data must be pre-processed to replace the
    /// multi-byte quote character with the single-byte `quote_character`.
    pub original_quote_character: Option<String>,
}

impl Default for CsvInput {
    fn default() -> Self {
        Self {
            file_header_info: FileHeaderInfo::default(),
            field_delimiter: b',',
            record_delimiter: b'\n',
            quote_character: b'"',
            quote_escape_character: b'"',
            comments: None,
            allow_quoted_record_delimiter: false,
            original_field_delimiter: None,
            original_quote_character: None,
        }
    }
}

/// JSON input type.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum JsonType {
    /// Each line is a separate JSON object (JSON Lines / NDJSON).
    #[default]
    Lines,
    /// The entire content is a single JSON document (object or array).
    Document,
}

/// JSON input configuration.
#[derive(Debug, Clone, Default)]
pub struct JsonInput {
    /// Whether the JSON is a single document or one-per-line.
    pub json_type: JsonType,
}

/// Input serialization — describes the format of the stored object.
#[derive(Debug, Clone)]
pub enum InputSerialization {
    /// CSV format with configurable parsing options.
    Csv(CsvInput),
    /// JSON format.
    Json(JsonInput),
    /// Apache Parquet columnar format.
    Parquet,
}

/// CSV output configuration.
#[derive(Debug, Clone)]
pub struct CsvOutput {
    /// Field delimiter for output (default: `,`).
    pub field_delimiter: u8,
    /// Record delimiter for output (default: `\n`).
    pub record_delimiter: u8,
    /// Quote character for output (default: `"`).
    pub quote_character: u8,
    /// Quote escape character for output (default: `"`).
    pub quote_escape_character: u8,
    /// Fields are always quoted.
    pub quote_fields: QuoteFields,
}

/// Whether to always quote output fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuoteFields {
    /// Always quote every field.
    Always,
    /// Only quote fields when needed (contains delimiter, newline, or quote).
    AsNeeded,
}

impl Default for CsvOutput {
    fn default() -> Self {
        Self {
            field_delimiter: b',',
            record_delimiter: b'\n',
            quote_character: b'"',
            quote_escape_character: b'"',
            quote_fields: QuoteFields::AsNeeded,
        }
    }
}

/// Output serialization — describes the desired result format.
#[derive(Debug, Clone)]
pub enum OutputSerialization {
    /// CSV output format.
    Csv(CsvOutput),
    /// JSON output format.
    Json,
}

/// Byte range for partial object scanning.
#[derive(Debug, Clone)]
pub struct ScanRange {
    /// Start byte offset (inclusive).
    pub start: Option<u64>,
    /// End byte offset (exclusive).
    pub end: Option<u64>,
}

/// A parsed S3 SelectObjectContent request.
#[derive(Debug, Clone)]
pub struct SelectRequest {
    /// The SQL expression to execute.
    pub expression: String,
    /// Input data format and parsing options.
    pub input_serialization: InputSerialization,
    /// Desired output format.
    pub output_serialization: OutputSerialization,
    /// Whether to include progress events in the response.
    pub request_progress: bool,
    /// Optional byte range for scanning a subset of the object.
    pub scan_range: Option<ScanRange>,
}

/// S4 extended multi-object SQL query request (JSON body).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct MultiObjectSqlRequest {
    /// The SQL query to execute.
    pub sql: String,
    /// Input format: "csv", "json", or "parquet".
    #[serde(default = "default_format")]
    pub format: String,
    /// Output format: "csv" or "json" (default: "json").
    #[serde(default = "default_output")]
    pub output: String,
    /// Whether CSV input has headers (only for format="csv").
    #[serde(default)]
    pub has_headers: Option<bool>,
    /// CSV field delimiter (only for format="csv").
    #[serde(default)]
    pub delimiter: Option<String>,
}

fn default_format() -> String {
    "csv".to_string()
}

fn default_output() -> String {
    "json".to_string()
}

/// Parse the S3 SelectObjectContent XML request body.
pub fn parse_select_request(xml_bytes: &[u8]) -> Result<SelectRequest, SelectError> {
    let mut reader = Reader::from_reader(xml_bytes);
    reader.trim_text(true);

    let mut expression = String::new();
    let mut request_progress = false;
    let mut input_serialization: Option<InputSerialization> = None;
    let mut output_serialization: Option<OutputSerialization> = None;
    let mut scan_range: Option<ScanRange> = None;

    let mut buf = Vec::new();
    let mut path: Vec<String> = Vec::new();

    // CSV input building state
    let mut csv_input = CsvInput::default();
    let mut json_input = JsonInput::default();
    let mut csv_output = CsvOutput::default();
    let mut in_csv_input = false;
    let mut in_json_input = false;
    let mut in_parquet_input = false;
    let mut in_csv_output = false;
    let mut in_json_output = false;
    let mut scan_start: Option<u64> = None;
    let mut scan_end: Option<u64> = None;

    // Track whether Text events were seen for elements where empty string is meaningful.
    // When quick-xml has trim_text(true), <Element></Element> may not fire a Text event,
    // so we detect "no text seen" at the End event and treat it as empty.
    let mut csv_input_quote_char_set = false;
    let mut csv_output_quote_char_set = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                path.push(name.clone());

                match name.as_str() {
                    "CSV" if path_contains(&path, "InputSerialization") => in_csv_input = true,
                    "JSON" if path_contains(&path, "InputSerialization") => in_json_input = true,
                    "Parquet" if path_contains(&path, "InputSerialization") => {
                        in_parquet_input = true
                    }
                    "CSV" if path_contains(&path, "OutputSerialization") => in_csv_output = true,
                    "JSON" if path_contains(&path, "OutputSerialization") => in_json_output = true,
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();

                match name.as_str() {
                    "CSV" if in_csv_input && path_contains(&path, "InputSerialization") => {
                        in_csv_input = false;
                        input_serialization = Some(InputSerialization::Csv(csv_input.clone()));
                    }
                    "JSON" if in_json_input && path_contains(&path, "InputSerialization") => {
                        in_json_input = false;
                        input_serialization = Some(InputSerialization::Json(json_input.clone()));
                    }
                    "Parquet" if in_parquet_input && path_contains(&path, "InputSerialization") => {
                        in_parquet_input = false;
                        input_serialization = Some(InputSerialization::Parquet);
                    }
                    "CSV" if in_csv_output && path_contains(&path, "OutputSerialization") => {
                        in_csv_output = false;
                        output_serialization = Some(OutputSerialization::Csv(csv_output.clone()));
                    }
                    "JSON" if in_json_output && path_contains(&path, "OutputSerialization") => {
                        in_json_output = false;
                        output_serialization = Some(OutputSerialization::Json);
                    }
                    // Handle empty <QuoteCharacter></QuoteCharacter> — no Text event fires
                    // with trim_text(true), so detect it here.
                    "QuoteCharacter" if in_csv_input && !csv_input_quote_char_set => {
                        csv_input.quote_character = b'\0';
                    }
                    "QuoteCharacter" if in_csv_output && !csv_output_quote_char_set => {
                        csv_output.quote_character = b'\0';
                    }
                    "QuoteCharacter" => {
                        // Reset flags for next occurrence
                        csv_input_quote_char_set = false;
                        csv_output_quote_char_set = false;
                    }
                    "ScanRange" if scan_start.is_some() || scan_end.is_some() => {
                        scan_range = Some(ScanRange {
                            start: scan_start.take(),
                            end: scan_end.take(),
                        });
                    }
                    _ => {}
                }

                path.pop();
            }
            Ok(Event::Text(ref e)) => {
                let text = e.unescape().unwrap_or_default().to_string();
                let current = path.last().map(String::as_str).unwrap_or("");

                match current {
                    "Expression" => expression = text,
                    "Enabled" if path_contains(&path, "RequestProgress") => {
                        request_progress = text.eq_ignore_ascii_case("true");
                    }
                    // CSV input fields
                    "FileHeaderInfo" if in_csv_input => {
                        csv_input.file_header_info = match text.to_uppercase().as_str() {
                            "USE" => FileHeaderInfo::Use,
                            "IGNORE" => FileHeaderInfo::Ignore,
                            _ => FileHeaderInfo::None,
                        };
                    }
                    "FieldDelimiter" if in_csv_input => {
                        let (byte, original) = parse_delimiter(&text);
                        csv_input.field_delimiter = byte;
                        csv_input.original_field_delimiter = original;
                    }
                    "RecordDelimiter" if in_csv_input => {
                        csv_input.record_delimiter = parse_delimiter(&text).0;
                    }
                    "QuoteCharacter" if in_csv_input => {
                        csv_input_quote_char_set = true;
                        if text.is_empty() {
                            // Empty QuoteCharacter means no quoting
                            csv_input.quote_character = b'\0';
                        } else if text.chars().count() > 1 {
                            return Err(SelectError::InvalidRequest(
                                "QuoteCharacter must be a single character".to_string(),
                            ));
                        } else {
                            let (byte, original) = parse_delimiter(&text);
                            csv_input.quote_character = byte;
                            csv_input.original_quote_character = original;
                        }
                    }
                    "QuoteEscapeCharacter" if in_csv_input => {
                        csv_input.quote_escape_character = parse_delimiter(&text).0;
                    }
                    "Comments" if in_csv_input => {
                        csv_input.comments = text.bytes().next();
                    }
                    "AllowQuotedRecordDelimiter" if in_csv_input => {
                        csv_input.allow_quoted_record_delimiter = text.eq_ignore_ascii_case("true");
                    }
                    // JSON input fields
                    "Type" if in_json_input => {
                        json_input.json_type = match text.to_uppercase().as_str() {
                            "DOCUMENT" => JsonType::Document,
                            _ => JsonType::Lines,
                        };
                    }
                    // CSV output fields
                    "FieldDelimiter" if in_csv_output => {
                        csv_output.field_delimiter = parse_delimiter(&text).0;
                    }
                    "RecordDelimiter" if in_csv_output => {
                        csv_output.record_delimiter = parse_delimiter(&text).0;
                    }
                    "QuoteCharacter" if in_csv_output => {
                        csv_output_quote_char_set = true;
                        if text.is_empty() {
                            // Empty QuoteCharacter means no quoting
                            csv_output.quote_character = b'\0';
                        } else if text.chars().count() > 1 {
                            return Err(SelectError::InvalidRequest(
                                "QuoteCharacter must be a single character".to_string(),
                            ));
                        } else {
                            csv_output.quote_character = parse_delimiter(&text).0;
                        }
                    }
                    "QuoteEscapeCharacter" if in_csv_output => {
                        csv_output.quote_escape_character = parse_delimiter(&text).0;
                    }
                    "QuoteFields" if in_csv_output => {
                        csv_output.quote_fields = match text.to_uppercase().as_str() {
                            "ALWAYS" => QuoteFields::Always,
                            _ => QuoteFields::AsNeeded,
                        };
                    }
                    // Scan range
                    "Start" if path_contains(&path, "ScanRange") => {
                        scan_start = text.parse().ok();
                    }
                    "End" if path_contains(&path, "ScanRange") => {
                        scan_end = text.parse().ok();
                    }
                    _ => {}
                }
            }
            Ok(Event::Empty(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                // Handle self-closing tags like <Parquet/>, <JSON/>, <CSV/>,
                // <QuoteCharacter/> (empty = no quoting = \0)
                match name.as_str() {
                    "QuoteCharacter" if in_csv_input => {
                        csv_input.quote_character = b'\0';
                    }
                    "QuoteCharacter" if in_csv_output => {
                        csv_output.quote_character = b'\0';
                    }
                    "Parquet" if path_contains(&path, "InputSerialization") => {
                        input_serialization = Some(InputSerialization::Parquet);
                    }
                    "JSON" if path_contains(&path, "OutputSerialization") => {
                        output_serialization = Some(OutputSerialization::Json);
                    }
                    "CSV" if path_contains(&path, "OutputSerialization") => {
                        output_serialization = Some(OutputSerialization::Csv(CsvOutput::default()));
                    }
                    "JSON" if path_contains(&path, "InputSerialization") => {
                        input_serialization = Some(InputSerialization::Json(JsonInput::default()));
                    }
                    "CSV" if path_contains(&path, "InputSerialization") => {
                        input_serialization = Some(InputSerialization::Csv(CsvInput::default()));
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(SelectError::InvalidRequest(format!(
                    "Failed to parse XML: {e}"
                )));
            }
            _ => {}
        }
        buf.clear();
    }

    if expression.is_empty() {
        return Err(SelectError::InvalidRequest(
            "Missing Expression element".to_string(),
        ));
    }

    let input_serialization = input_serialization.ok_or_else(|| {
        SelectError::InvalidRequest("Missing InputSerialization element".to_string())
    })?;

    let output_serialization = output_serialization.ok_or_else(|| {
        SelectError::InvalidRequest("Missing OutputSerialization element".to_string())
    })?;

    Ok(SelectRequest {
        expression,
        input_serialization,
        output_serialization,
        request_progress,
        scan_range,
    })
}

/// Check if a path element exists anywhere in the current XML path.
fn path_contains(path: &[String], element: &str) -> bool {
    path.iter().any(|p| p == element)
}

/// Parse a delimiter from an XML text value.
///
/// Returns `(byte_delimiter, original_string_if_multibyte)`.
/// For single-byte delimiters, the original string is `None`.
/// For multi-byte Unicode delimiters (e.g., `╦`), the byte delimiter is set
/// to [`MULTIBYTE_DELIMITER_PLACEHOLDER`] and the original string is returned
/// so the caller can pre-process CSV data.
fn parse_delimiter(text: &str) -> (u8, Option<String>) {
    match text {
        "\\t" | "\t" => (b'\t', None),
        "\\n" | "\n" => (b'\n', None),
        "\\r" | "\r" => (b'\r', None),
        s if s.len() == 1 => (s.bytes().next().unwrap_or(b','), None),
        s if !s.is_empty() => {
            // Multi-byte Unicode delimiter — use a placeholder byte
            (MULTIBYTE_DELIMITER_PLACEHOLDER, Some(s.to_string()))
        }
        _ => (b',', None),
    }
}

/// Convert an [`InputSerialization`] to an [`InputFormat`].
pub fn input_serialization_to_format(input: &InputSerialization) -> InputFormat {
    match input {
        InputSerialization::Csv(_) => InputFormat::Csv,
        InputSerialization::Json(_) => InputFormat::Json,
        InputSerialization::Parquet => InputFormat::Parquet,
    }
}

/// Convert an [`OutputSerialization`] to an [`OutputFormat`].
pub fn output_serialization_to_format(output: &OutputSerialization) -> OutputFormat {
    match output {
        OutputSerialization::Csv(_) => OutputFormat::Csv,
        OutputSerialization::Json => OutputFormat::Json,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_csv_select_request() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * FROM s3object WHERE name = 'test'</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CSV>
            <FileHeaderInfo>USE</FileHeaderInfo>
            <FieldDelimiter>,</FieldDelimiter>
            <RecordDelimiter>\n</RecordDelimiter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <CSV/>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>true</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>"#;

        let req = parse_select_request(xml.as_bytes()).unwrap();
        assert_eq!(req.expression, "SELECT * FROM s3object WHERE name = 'test'");
        assert!(req.request_progress);

        match &req.input_serialization {
            InputSerialization::Csv(csv) => {
                assert_eq!(csv.file_header_info, FileHeaderInfo::Use);
                assert_eq!(csv.field_delimiter, b',');
            }
            _ => panic!("Expected CSV input"),
        }

        assert!(matches!(
            req.output_serialization,
            OutputSerialization::Csv(_)
        ));
    }

    #[test]
    fn test_parse_json_select_request() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT s.name FROM s3object s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <JSON>
            <Type>LINES</Type>
        </JSON>
    </InputSerialization>
    <OutputSerialization>
        <JSON/>
    </OutputSerialization>
</SelectObjectContentRequest>"#;

        let req = parse_select_request(xml.as_bytes()).unwrap();
        assert_eq!(req.expression, "SELECT s.name FROM s3object s");

        match &req.input_serialization {
            InputSerialization::Json(json) => {
                assert_eq!(json.json_type, JsonType::Lines);
            }
            _ => panic!("Expected JSON input"),
        }

        assert!(matches!(
            req.output_serialization,
            OutputSerialization::Json
        ));
    }

    #[test]
    fn test_parse_parquet_select_request() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT COUNT(*) FROM s3object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <Parquet/>
    </InputSerialization>
    <OutputSerialization>
        <JSON/>
    </OutputSerialization>
</SelectObjectContentRequest>"#;

        let req = parse_select_request(xml.as_bytes()).unwrap();
        assert_eq!(req.expression, "SELECT COUNT(*) FROM s3object");
        assert!(matches!(
            req.input_serialization,
            InputSerialization::Parquet
        ));
    }

    #[test]
    fn test_missing_expression_error() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <InputSerialization><CSV/></InputSerialization>
    <OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>"#;

        let result = parse_select_request(xml.as_bytes());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Expression"));
    }

    #[test]
    fn test_scan_range() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * FROM s3object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization><CSV/></InputSerialization>
    <OutputSerialization><CSV/></OutputSerialization>
    <ScanRange>
        <Start>1000</Start>
        <End>5000</End>
    </ScanRange>
</SelectObjectContentRequest>"#;

        let req = parse_select_request(xml.as_bytes()).unwrap();
        let range = req.scan_range.unwrap();
        assert_eq!(range.start, Some(1000));
        assert_eq!(range.end, Some(5000));
    }

    #[test]
    fn test_empty_quote_character_input() {
        // Empty <QuoteCharacter></QuoteCharacter> should set quote to \0 (no quoting)
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * FROM s3object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CSV>
            <FileHeaderInfo>NONE</FileHeaderInfo>
            <QuoteCharacter></QuoteCharacter>
            <QuoteEscapeCharacter>"</QuoteEscapeCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON/>
    </OutputSerialization>
</SelectObjectContentRequest>"#;

        let req = parse_select_request(xml.as_bytes()).unwrap();
        match &req.input_serialization {
            InputSerialization::Csv(csv) => {
                assert_eq!(
                    csv.quote_character, b'\0',
                    "Empty QuoteCharacter should be \\0"
                );
            }
            _ => panic!("Expected CSV input"),
        }
    }

    #[test]
    fn test_empty_quote_character_output() {
        // Empty <QuoteCharacter></QuoteCharacter> in output should set quote to \0
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * FROM s3object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization><CSV/></InputSerialization>
    <OutputSerialization>
        <CSV>
            <QuoteFields>ALWAYS</QuoteFields>
            <QuoteCharacter></QuoteCharacter>
            <QuoteEscapeCharacter>"</QuoteEscapeCharacter>
        </CSV>
    </OutputSerialization>
</SelectObjectContentRequest>"#;

        let req = parse_select_request(xml.as_bytes()).unwrap();
        match &req.output_serialization {
            OutputSerialization::Csv(csv) => {
                assert_eq!(
                    csv.quote_character, b'\0',
                    "Empty QuoteCharacter should be \\0"
                );
                assert_eq!(csv.quote_fields, QuoteFields::Always);
            }
            _ => panic!("Expected CSV output"),
        }
    }
}
