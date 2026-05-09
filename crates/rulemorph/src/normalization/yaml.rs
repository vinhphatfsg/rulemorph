use serde_json::{Map, Number as JsonNumber, Value as JsonValue};
use serde_yaml::Value as YamlValue;

use crate::error::{TransformError, TransformErrorKind};
use crate::model::RuleFile;
use crate::serde_guard::parse_yaml_value_strict_with_limits;

use super::{
    NormalizationOptions, enforce_json_limits, enforce_records_limit, select_records_from_document,
};

pub fn normalize_yaml_records(
    rule: &RuleFile,
    input: &str,
    options: &NormalizationOptions,
) -> Result<Vec<JsonValue>, TransformError> {
    enforce_yaml_alias_limit(input, options)?;
    let value = parse_yaml_value_strict_with_limits(
        input,
        options.max_depth,
        options.max_yaml_expanded_nodes,
        options.max_array_len,
        options.max_text_bytes,
    )
    .map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to parse YAML input: {}", err),
        )
    })?;
    let mut node_count = 0usize;
    let json = yaml_to_json(&value, options, 0, &mut node_count)?;
    enforce_json_limits(&json, options)?;
    let records = select_records_from_document(
        &json,
        rule.input
            .yaml
            .as_ref()
            .and_then(|yaml| yaml.records_path.as_deref()),
        "input.yaml.records_path",
    )?;
    enforce_records_limit(records.len(), options)?;
    Ok(records)
}

fn enforce_yaml_alias_limit(
    input: &str,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    let aliases = input.chars().filter(|value| *value == '*').count();
    if aliases > options.max_yaml_aliases {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "input exceeds max_yaml_aliases",
        ));
    }
    Ok(())
}

fn yaml_to_json(
    value: &YamlValue,
    options: &NormalizationOptions,
    depth: usize,
    node_count: &mut usize,
) -> Result<JsonValue, TransformError> {
    if depth > options.max_depth {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "input exceeds max_depth",
        ));
    }
    *node_count = node_count.saturating_add(1);
    if *node_count > options.max_yaml_expanded_nodes {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "input exceeds max_yaml_expanded_nodes",
        ));
    }

    match value {
        YamlValue::Null => Ok(JsonValue::Null),
        YamlValue::Bool(value) => Ok(JsonValue::Bool(*value)),
        YamlValue::Number(value) => yaml_number_to_json(value),
        YamlValue::String(value) => Ok(JsonValue::String(value.clone())),
        YamlValue::Sequence(items) => {
            if items.len() > options.max_array_len {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    "input exceeds max_array_len",
                ));
            }
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(yaml_to_json(item, options, depth + 1, node_count)?);
            }
            Ok(JsonValue::Array(out))
        }
        YamlValue::Mapping(map) => {
            let mut out = Map::new();
            for (key, value) in map {
                let key = match key {
                    YamlValue::String(key) => key.clone(),
                    _ => {
                        return Err(TransformError::new(
                            TransformErrorKind::InvalidInput,
                            "YAML mapping keys must be strings",
                        ));
                    }
                };
                out.insert(key, yaml_to_json(value, options, depth + 1, node_count)?);
            }
            Ok(JsonValue::Object(out))
        }
        YamlValue::Tagged(_) => Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "YAML custom tags are not supported",
        )),
    }
}

fn yaml_number_to_json(value: &serde_yaml::Number) -> Result<JsonValue, TransformError> {
    if let Some(value) = value.as_i64() {
        return Ok(JsonValue::Number(value.into()));
    }
    if let Some(value) = value.as_u64() {
        return Ok(JsonValue::Number(value.into()));
    }
    if let Some(value) = value.as_f64() {
        if let Some(value) = JsonNumber::from_f64(value) {
            return Ok(JsonValue::Number(value));
        }
    }
    Err(TransformError::new(
        TransformErrorKind::InvalidInput,
        "YAML number is not JSON-compatible",
    ))
}
