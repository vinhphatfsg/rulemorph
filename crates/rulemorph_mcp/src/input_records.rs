use csv::ReaderBuilder;
use rulemorph::serde_guard::parse_json_value_strict;
use serde_json::{Map, Value};

use crate::diagnostics::parse_error_json;
use crate::errors::CallError;
use crate::path_expr::get_value_at_path;

#[derive(Clone, Copy)]
pub(crate) enum InputDataFormat {
    Json,
    Csv,
}

pub(crate) fn normalize_format(format: Option<&str>, input_text: &str) -> InputDataFormat {
    match format.map(|value| value.to_lowercase()) {
        Some(value) if value == "csv" => InputDataFormat::Csv,
        Some(value) if value == "json" => InputDataFormat::Json,
        Some(_) => InputDataFormat::Json,
        None => match input_text.trim_start().chars().next() {
            Some('{') | Some('[') => InputDataFormat::Json,
            _ => InputDataFormat::Csv,
        },
    }
}

pub(crate) fn json_records_from_value(
    value: &Value,
    records_path: Option<&str>,
) -> Result<Vec<Value>, CallError> {
    let target = if let Some(path) = records_path {
        get_value_at_path(value, path)
            .map_err(|message| {
                CallError::InvalidParams(format!("records_path is invalid: {}", message))
            })?
            .ok_or_else(|| CallError::Tool {
                message: "records_path did not match any value".to_string(),
                errors: Some(vec![parse_error_json(
                    "records_path did not match any value",
                    None,
                )]),
            })?
    } else {
        value
    };

    match target {
        Value::Array(items) => Ok(items.clone()),
        Value::Object(_) => Ok(vec![target.clone()]),
        _ => Err(CallError::Tool {
            message: "records_path must resolve to an object or array".to_string(),
            errors: Some(vec![parse_error_json(
                "records_path must resolve to an object or array",
                None,
            )]),
        }),
    }
}

pub(crate) fn parse_json_records_strict(
    input_text: &str,
    records_path: Option<&str>,
    input_path: Option<&str>,
) -> Result<Vec<Value>, CallError> {
    let value = parse_json_value_strict(input_text).map_err(|err| {
        let message = format!("failed to parse input JSON: {}", err);
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, input_path)]),
        }
    })?;
    json_records_from_value(&value, records_path)
}

pub(crate) fn parse_csv_records(text: &str) -> Result<Vec<Value>, String> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(text.as_bytes());
    let headers = reader
        .headers()
        .map_err(|err| err.to_string())?
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let trimmed = name.trim();
            if trimmed.is_empty() {
                format!("column_{}", index + 1)
            } else {
                trimmed.to_string()
            }
        })
        .collect::<Vec<_>>();

    let mut records = Vec::new();
    for result in reader.records() {
        let record = result.map_err(|err| err.to_string())?;
        let mut obj = Map::new();
        for (index, value) in record.iter().enumerate() {
            if let Some(key) = headers.get(index) {
                obj.insert(key.clone(), csv_cell_to_value(value));
            }
        }
        records.push(Value::Object(obj));
    }
    Ok(records)
}

fn csv_cell_to_value(value: &str) -> Value {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Value::Null;
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower == "true" {
        return Value::Bool(true);
    }
    if lower == "false" {
        return Value::Bool(false);
    }
    if let Ok(number) = trimmed.parse::<i64>() {
        return Value::Number(number.into());
    }
    if let Ok(number) = trimmed.parse::<f64>() {
        if let Some(number) = serde_json::Number::from_f64(number) {
            return Value::Number(number);
        }
    }
    Value::String(trimmed.to_string())
}
