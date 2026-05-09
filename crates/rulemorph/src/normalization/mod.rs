mod csv;
mod json;
mod options;

pub use options::NormalizationOptions;

use serde_json::Value as JsonValue;

use crate::error::{TransformError, TransformErrorKind};
use crate::model::{InputFormat, RuleFile};

pub enum InputData<'a> {
    Text(&'a str),
    Bytes(&'a [u8]),
}

#[derive(Debug)]
pub enum NormalizedRecords {
    Materialized(std::vec::IntoIter<JsonValue>),
}

impl Iterator for NormalizedRecords {
    type Item = Result<JsonValue, TransformError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            NormalizedRecords::Materialized(iter) => iter.next().map(Ok),
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
    let text = text_input(input, options)?;
    let records = match rule.input.format {
        InputFormat::Csv => csv::normalize_csv_records(rule, text, options)?,
        InputFormat::Json => json::normalize_json_records(rule, text, options)?,
        InputFormat::Yaml
        | InputFormat::Toml
        | InputFormat::Xml
        | InputFormat::Html
        | InputFormat::Excel => {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "input format is not supported yet",
            ));
        }
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
