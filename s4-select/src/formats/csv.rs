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

//! CSV format parser — converts CSV bytes into Arrow RecordBatches.

use std::sync::Arc;

use crate::error::SelectError;
use crate::request::{CsvInput, CsvOutput, FileHeaderInfo, QuoteFields};
use arrow::array::RecordBatch;
use arrow::csv::reader::Format;
use arrow::csv::{ReaderBuilder, WriterBuilder};
use arrow::datatypes::{Field, Schema, SchemaRef};

/// Default batch size for CSV parsing.
const CSV_BATCH_SIZE: usize = 8192;

/// Parse CSV bytes into Arrow RecordBatches.
///
/// Uses Arrow's CSV reader which handles schema inference,
/// custom delimiters, quoting, and header configuration.
///
/// When the input uses a multi-byte Unicode delimiter (e.g., `╦`), the data
/// is pre-processed to replace it with a single-byte placeholder since Arrow's
/// CSV reader only supports single-byte delimiters.
pub fn parse_csv(
    data: &[u8],
    config: &CsvInput,
) -> Result<(SchemaRef, Vec<RecordBatch>), SelectError> {
    // Pre-process multi-byte Unicode delimiters (field delimiter and/or quote character)
    let processed;
    let csv_data =
        if config.original_field_delimiter.is_some() || config.original_quote_character.is_some() {
            let data_str = std::str::from_utf8(data).map_err(|e| {
                SelectError::DataParseError(format!("CSV data is not valid UTF-8: {e}"))
            })?;
            let mut result = data_str.to_string();

            if let Some(ref original_delim) = config.original_field_delimiter {
                result = result.replace(
                    original_delim.as_str(),
                    std::str::from_utf8(&[config.field_delimiter]).unwrap_or("\x01"),
                );
            }

            if let Some(ref original_quote) = config.original_quote_character {
                let quote_byte = [config.quote_character];
                let replacement = std::str::from_utf8(&quote_byte).unwrap_or("\"");
                result = result.replace(original_quote.as_str(), replacement);
            }

            processed = result;
            processed.as_bytes()
        } else {
            data
        };

    // Handle empty or whitespace-only CSV data — return empty result
    if csv_data.iter().all(|&b| b == b'\n' || b == b'\r' || b == b' ') {
        let schema = Arc::new(Schema::empty());
        return Ok((schema, Vec::new()));
    }

    let has_header = config.file_header_info == FileHeaderInfo::Use
        || config.file_header_info == FileHeaderInfo::Ignore;

    // First pass: infer schema
    let mut format = Format::default()
        .with_header(has_header)
        .with_delimiter(config.field_delimiter)
        .with_quote(config.quote_character);

    // Set escape character if it differs from the quote character
    // (Arrow's default is to use quote-doubling, i.e. quote == escape)
    if config.quote_escape_character != config.quote_character {
        format = format.with_escape(config.quote_escape_character);
    }

    let (inferred_schema, _records_read) = format
        .infer_schema(std::io::Cursor::new(csv_data), Some(100))
        .map_err(|e| SelectError::DataParseError(format!("CSV schema inference failed: {e}")))?;

    // For IGNORE and NONE modes: rename columns to positional _1, _2, ...
    // AWS S3 Select uses _1, _2, etc. when headers are not present or ignored.
    let schema = if config.file_header_info == FileHeaderInfo::Ignore
        || config.file_header_info == FileHeaderInfo::None
    {
        let fields: Vec<_> = inferred_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                Arc::new(Field::new(
                    format!("_{}", i + 1),
                    f.data_type().clone(),
                    f.is_nullable(),
                ))
            })
            .collect();
        Arc::new(Schema::new(fields))
    } else {
        Arc::new(inferred_schema)
    };

    // Second pass: read all data with the inferred schema
    let mut builder = ReaderBuilder::new(schema.clone())
        .with_header(has_header)
        .with_delimiter(config.field_delimiter)
        .with_quote(config.quote_character)
        .with_batch_size(CSV_BATCH_SIZE);

    if config.quote_escape_character != config.quote_character {
        builder = builder.with_escape(config.quote_escape_character);
    }

    let reader = builder
        .build(std::io::Cursor::new(csv_data))
        .map_err(|e| SelectError::DataParseError(format!("CSV reader build failed: {e}")))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| SelectError::DataParseError(format!("CSV batch error: {e}")))?;
        batches.push(batch);
    }

    Ok((schema, batches))
}

/// Format Arrow RecordBatches as CSV bytes (no header row, default settings).
pub fn batches_to_csv(batches: &[RecordBatch]) -> Result<Vec<u8>, SelectError> {
    batches_to_csv_with_config(batches, None)
}

/// Format Arrow RecordBatches as CSV bytes using output config.
///
/// Uses the `csv` crate directly for full control over quoting, escaping,
/// and delimiter settings — Arrow's writer doesn't support `QuoteStyle::Always`.
pub fn batches_to_csv_with_config(
    batches: &[RecordBatch],
    config: Option<&CsvOutput>,
) -> Result<Vec<u8>, SelectError> {
    use arrow::util::display::ArrayFormatter;

    let delimiter = config.map_or(b',', |c| c.field_delimiter);
    let quote = config.map_or(b'"', |c| c.quote_character);
    let escape = config.map_or(b'"', |c| c.quote_escape_character);
    let always_quote = config.is_some_and(|c| c.quote_fields == QuoteFields::Always);
    let terminator = config.map_or(b'\n', |c| c.record_delimiter);

    let quote_style = if always_quote {
        csv::QuoteStyle::Always
    } else {
        csv::QuoteStyle::Necessary
    };

    let mut builder = csv::WriterBuilder::new();
    builder
        .delimiter(delimiter)
        .quote(quote)
        .quote_style(quote_style)
        .terminator(csv::Terminator::Any(terminator))
        .has_headers(false);

    if escape == quote {
        builder.double_quote(true);
    } else {
        builder.double_quote(false).escape(escape);
    }

    let mut wtr = builder.from_writer(Vec::new());

    for batch in batches {
        let formatters: Vec<ArrayFormatter> = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &Default::default()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| SelectError::Internal(format!("CSV format error: {e}")))?;

        for row in 0..batch.num_rows() {
            let record: Vec<String> =
                formatters.iter().map(|fmt| fmt.value(row).to_string()).collect();
            wtr.write_record(&record)
                .map_err(|e| SelectError::Internal(format!("CSV write error: {e}")))?;
        }
    }

    wtr.flush()
        .map_err(|e| SelectError::Internal(format!("CSV flush error: {e}")))?;

    wtr.into_inner()
        .map_err(|e| SelectError::Internal(format!("CSV finalize error: {e}")))
}

/// Format Arrow RecordBatches as CSV bytes with a header row.
pub fn batches_to_csv_with_headers(batches: &[RecordBatch]) -> Result<Vec<u8>, SelectError> {
    let mut buf = Vec::new();
    {
        let mut writer = WriterBuilder::new().with_header(true).build(&mut buf);
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| SelectError::Internal(format!("CSV write error: {e}")))?;
        }
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_csv_with_headers() {
        let data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\n";
        let config = CsvInput {
            file_header_info: FileHeaderInfo::Use,
            ..CsvInput::default()
        };

        let (schema, batches) = parse_csv(data, &config).unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "age");
        assert_eq!(schema.field(2).name(), "city");
        assert!(!batches.is_empty());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_parse_csv_no_headers() {
        let data = b"Alice,30,NYC\nBob,25,LA\n";
        let config = CsvInput {
            file_header_info: FileHeaderInfo::None,
            ..CsvInput::default()
        };

        let (schema, batches) = parse_csv(data, &config).unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert!(!batches.is_empty());
    }

    #[test]
    fn test_parse_csv_tab_delimiter() {
        let data = b"name\tage\nAlice\t30\n";
        let config = CsvInput {
            file_header_info: FileHeaderInfo::Use,
            field_delimiter: b'\t',
            ..CsvInput::default()
        };

        let (schema, batches) = parse_csv(data, &config).unwrap();
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "age");
        assert!(!batches.is_empty());
    }

    #[test]
    fn test_parse_csv_no_headers_positional_names() {
        let data = b"Alice,30,NYC\nBob,25,LA\n";
        let config = CsvInput {
            file_header_info: FileHeaderInfo::None,
            ..CsvInput::default()
        };

        let (schema, _batches) = parse_csv(data, &config).unwrap();
        // FileHeaderInfo::None should generate _1, _2, _3 column names (AWS compat)
        assert_eq!(schema.field(0).name(), "_1");
        assert_eq!(schema.field(1).name(), "_2");
        assert_eq!(schema.field(2).name(), "_3");
    }

    #[test]
    fn test_parse_csv_multibyte_unicode_delimiter() {
        // CSV data using ╦ (U+2566, 3 bytes: E2 95 A6) as field delimiter
        let data = "2011╦FEMALE╦ASIAN\n2012╦MALE╦WHITE\n";
        let config = CsvInput {
            file_header_info: FileHeaderInfo::None,
            field_delimiter: crate::request::MULTIBYTE_DELIMITER_PLACEHOLDER,
            original_field_delimiter: Some("╦".to_string()),
            ..CsvInput::default()
        };

        let (schema, batches) = parse_csv(data.as_bytes(), &config).unwrap();
        assert_eq!(schema.fields().len(), 3);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_parse_csv_custom_quote_char() {
        // Mint test_5: quote_char=', escape_char="
        // Input: "col1",col2,col3 — double quotes should be literal (not CSV quotes)
        let data = b"\"col1\",col2,col3\n";
        let config = CsvInput {
            file_header_info: FileHeaderInfo::None,
            quote_character: b'\'',
            quote_escape_character: b'"',
            ..CsvInput::default()
        };

        let (_schema, batches) = parse_csv(data, &config).unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);

        // Column _1 should contain "col1" WITH the double quotes
        let col = batch.column(0);
        let arr = col.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        let val = arr.value(0);
        eprintln!("Parsed _1 value: {:?}", val);
        assert_eq!(
            val, "\"col1\"",
            "Double quotes should be preserved as literal chars when quote_char is single-quote"
        );
    }

    #[test]
    fn test_csv_output_custom_quote_char() {
        // Mint csv_output test_1: output quote_char=', escape_char='
        // Input: col1,col2,col3 (standard CSV)
        // Expected output with ALWAYS quoting + single-quote: 'col1','col2','col3'
        let data = b"col1,col2,col3\n";
        let config = CsvInput {
            file_header_info: FileHeaderInfo::None,
            ..CsvInput::default()
        };

        let (_schema, batches) = parse_csv(data, &config).unwrap();

        let out_config = CsvOutput {
            quote_character: b'\'',
            quote_escape_character: b'\'',
            quote_fields: QuoteFields::Always,
            ..CsvOutput::default()
        };

        let result = batches_to_csv_with_config(&batches, Some(&out_config)).unwrap();
        let result_str = String::from_utf8(result).unwrap();
        eprintln!("Output CSV: {:?}", result_str);
        // With QuoteFields::Always and quote=', every field should be single-quoted
        assert!(
            result_str.contains("'col1'"),
            "Expected single-quoted fields, got: {}",
            result_str
        );
    }

    #[test]
    fn test_batches_to_csv() {
        let data = b"name,age\nAlice,30\nBob,25\n";
        let config = CsvInput {
            file_header_info: FileHeaderInfo::Use,
            ..CsvInput::default()
        };

        let (_schema, batches) = parse_csv(data, &config).unwrap();
        let csv_bytes = batches_to_csv(&batches).unwrap();
        let csv_str = String::from_utf8(csv_bytes).unwrap();
        assert!(csv_str.contains("Alice"));
        assert!(csv_str.contains("Bob"));
    }
}
