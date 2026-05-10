mod csv;
mod excel;
mod html;
mod json;
mod options;
mod toml;
mod xml;
mod yaml;

pub use options::NormalizationOptions;

use std::fmt;

use serde_json::Value as JsonValue;

use crate::error::{TransformError, TransformErrorKind};
use crate::model::{InputFormat, RuleFile};
use crate::path::{get_path, parse_path};

#[derive(Clone, Copy)]
pub enum InputData<'a> {
    Text(&'a str),
    Bytes(&'a [u8]),
}

/// Normalized input records.
///
/// Streaming formats can report record-level parse or limit errors while the
/// iterator is consumed, so callers must drain or collect the iterator when
/// they need full input validation.
pub enum NormalizedRecords {
    Materialized(std::vec::IntoIter<JsonValue>),
    Streaming(Box<dyn Iterator<Item = Result<JsonValue, TransformError>>>),
}

impl fmt::Debug for NormalizedRecords {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NormalizedRecords::Materialized(_) => formatter.write_str("Materialized(..)"),
            NormalizedRecords::Streaming(_) => formatter.write_str("Streaming(..)"),
        }
    }
}

impl Iterator for NormalizedRecords {
    type Item = Result<JsonValue, TransformError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            NormalizedRecords::Materialized(iter) => iter.next().map(Ok),
            NormalizedRecords::Streaming(iter) => iter.next(),
        }
    }
}

pub fn normalize_records(
    rule: &RuleFile,
    input: InputData<'_>,
) -> Result<NormalizedRecords, TransformError> {
    normalize_records_with_options(rule, input, &NormalizationOptions::default())
}

pub fn normalize_records_with_options(
    rule: &RuleFile,
    input: InputData<'_>,
    options: &NormalizationOptions,
) -> Result<NormalizedRecords, TransformError> {
    if rule.input.format == InputFormat::Csv {
        return Ok(NormalizedRecords::Streaming(Box::new(
            csv::normalize_csv_records_iter(rule, text_input(input, options)?, options)?,
        )));
    }

    let records = match rule.input.format {
        InputFormat::Csv => unreachable!("CSV records are returned as a streaming iterator"),
        InputFormat::Json => {
            json::normalize_json_records(rule, text_input(input, options)?, options)?
        }
        InputFormat::Yaml => {
            yaml::normalize_yaml_records(rule, text_input(input, options)?, options)?
        }
        InputFormat::Toml => {
            toml::normalize_toml_records(rule, text_input(input, options)?, options)?
        }
        InputFormat::Xml => xml::normalize_xml_records(rule, text_input(input, options)?, options)?,
        InputFormat::Html => {
            html::normalize_html_records(rule, text_input(input, options)?, options)?
        }
        InputFormat::Excel => excel::normalize_excel_records(rule, input, options)?,
    };
    Ok(NormalizedRecords::Materialized(records.into_iter()))
}

fn text_input<'a>(
    input: InputData<'a>,
    options: &NormalizationOptions,
) -> Result<&'a str, TransformError> {
    let bytes = match input {
        InputData::Text(value) => value.as_bytes(),
        InputData::Bytes(bytes) => bytes,
    };
    if bytes.len() > options.max_input_bytes {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "input exceeds max_input_bytes",
        ));
    }
    let bytes = bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes);
    std::str::from_utf8(bytes).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("input must be valid UTF-8: {}", err),
        )
    })
}

pub(crate) fn enforce_records_limit(
    count: usize,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    if count > options.max_records {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "input exceeds max_records",
        ));
    }
    Ok(())
}

pub(crate) fn enforce_json_limits(
    value: &JsonValue,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    fn walk(
        value: &JsonValue,
        depth: usize,
        options: &NormalizationOptions,
    ) -> Result<(), TransformError> {
        if depth > options.max_depth {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "input exceeds max_depth",
            ));
        }
        match value {
            JsonValue::Array(items) => {
                if items.len() > options.max_array_len {
                    return Err(TransformError::new(
                        TransformErrorKind::InvalidInput,
                        "input exceeds max_array_len",
                    ));
                }
                for item in items {
                    walk(item, depth + 1, options)?;
                }
            }
            JsonValue::Object(map) => {
                for value in map.values() {
                    walk(value, depth + 1, options)?;
                }
            }
            JsonValue::String(value) => {
                if value.len() > options.max_text_bytes {
                    return Err(TransformError::new(
                        TransformErrorKind::InvalidInput,
                        "input exceeds max_text_bytes",
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    walk(value, 0, options)
}

pub(crate) fn select_records_from_document(
    value: &JsonValue,
    records_path: Option<&str>,
    path_for_error: &'static str,
    options: &NormalizationOptions,
) -> Result<Vec<JsonValue>, TransformError> {
    let records_value = match records_path {
        Some(path) => {
            let tokens = parse_path(path).map_err(|err| {
                TransformError::new(TransformErrorKind::InvalidRecordsPath, err.message())
                    .with_path(path_for_error)
            })?;
            get_path(value, &tokens).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::InvalidRecordsPath,
                    "records_path does not exist",
                )
                .with_path(path_for_error)
            })?
        }
        None => value,
    };

    match records_value {
        JsonValue::Array(items) => {
            enforce_records_limit(items.len(), options)?;
            Ok(items.clone())
        }
        JsonValue::Object(_) => {
            enforce_records_limit(1, options)?;
            Ok(vec![records_value.clone()])
        }
        _ => Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "records_path must point to an array or object",
        )),
    }
}
