use serde_json::{Map, Number as JsonNumber, Value as JsonValue};
use toml_edit::{DocumentMut, Item as TomlItem, Value as TomlValue};

use crate::error::{TransformError, TransformErrorKind};
use crate::model::RuleFile;

use super::{
    NormalizationOptions, enforce_json_limits, enforce_records_limit, select_records_from_document,
};

pub fn normalize_toml_records(
    rule: &RuleFile,
    input: &str,
    options: &NormalizationOptions,
) -> Result<Vec<JsonValue>, TransformError> {
    let json = parse_toml_json_with_limits(input, options).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to parse TOML input: {}", err),
        )
    })?;
    enforce_json_limits(&json, options)?;
    let records = select_records_from_document(
        &json,
        rule.input
            .toml
            .as_ref()
            .and_then(|toml| toml.records_path.as_deref()),
        "input.toml.records_path",
    )?;
    enforce_records_limit(records.len(), options)?;
    Ok(records)
}

fn parse_toml_json_with_limits(
    input: &str,
    options: &NormalizationOptions,
) -> Result<JsonValue, String> {
    let document = input
        .parse::<DocumentMut>()
        .map_err(|err| err.to_string())?;
    toml_item_to_json(document.as_item(), options, 0)
}

fn toml_item_to_json(
    item: &TomlItem,
    options: &NormalizationOptions,
    depth: usize,
) -> Result<JsonValue, String> {
    if depth > options.max_depth {
        return Err("input exceeds max_depth".to_string());
    }
    match item {
        TomlItem::None => Ok(JsonValue::Null),
        TomlItem::Value(value) => toml_value_to_json(value, options, depth),
        TomlItem::Table(values) => toml_table_to_json(values, options, depth + 1),
        TomlItem::ArrayOfTables(values) => {
            let mut output = Vec::new();
            for value in values.iter() {
                output.push(toml_table_to_json(value, options, depth + 1)?);
                if output.len() > options.max_array_len {
                    return Err("input exceeds max_array_len".to_string());
                }
            }
            Ok(JsonValue::Array(output))
        }
    }
}

fn toml_value_to_json(
    value: &TomlValue,
    options: &NormalizationOptions,
    depth: usize,
) -> Result<JsonValue, String> {
    if depth > options.max_depth {
        return Err("input exceeds max_depth".to_string());
    }
    match value {
        TomlValue::String(value) => {
            if value.value().len() > options.max_text_bytes {
                return Err("input exceeds max_text_bytes".to_string());
            }
            Ok(JsonValue::String(value.value().clone()))
        }
        TomlValue::Integer(value) => Ok(JsonValue::Number((*value.value()).into())),
        TomlValue::Float(value) => JsonNumber::from_f64(*value.value())
            .map(JsonValue::Number)
            .ok_or_else(|| "TOML float is not JSON-compatible".to_string()),
        TomlValue::Boolean(value) => Ok(JsonValue::Bool(*value.value())),
        TomlValue::Datetime(value) => {
            let value = value.value().to_string();
            if value.len() > options.max_text_bytes {
                return Err("input exceeds max_text_bytes".to_string());
            }
            Ok(JsonValue::String(value))
        }
        TomlValue::Array(values) => {
            if values.len() > options.max_array_len {
                return Err("input exceeds max_array_len".to_string());
            }
            let mut output = Vec::with_capacity(values.len());
            for value in values.iter() {
                output.push(toml_value_to_json(value, options, depth + 1)?);
            }
            Ok(JsonValue::Array(output))
        }
        TomlValue::InlineTable(values) => toml_inline_table_to_json(values, options, depth + 1),
    }
}

fn toml_table_to_json(
    values: &toml_edit::Table,
    options: &NormalizationOptions,
    depth: usize,
) -> Result<JsonValue, String> {
    let mut output = Map::new();
    for (key, value) in values.iter() {
        if value.is_none() {
            continue;
        }
        output.insert(
            key.to_string(),
            toml_item_to_json(value, options, depth + 1)?,
        );
    }
    Ok(JsonValue::Object(output))
}

fn toml_inline_table_to_json(
    values: &toml_edit::InlineTable,
    options: &NormalizationOptions,
    depth: usize,
) -> Result<JsonValue, String> {
    let mut output = Map::new();
    for (key, value) in values.iter() {
        output.insert(
            key.to_string(),
            toml_value_to_json(value, options, depth + 1)?,
        );
    }
    Ok(JsonValue::Object(output))
}
