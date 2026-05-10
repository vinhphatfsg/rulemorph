use serde_json::{Map, Number as JsonNumber, Value as JsonValue};
use serde_yaml::Value as YamlValue;

use crate::error::{TransformError, TransformErrorKind};
use crate::model::RuleFile;
use crate::serde_guard::parse_yaml_value_strict_with_limits;

use super::{NormalizationOptions, enforce_json_limits, select_records_from_document};

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
        options,
    )?;
    Ok(records)
}

fn enforce_yaml_alias_limit(
    input: &str,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    let aliases = count_yaml_alias_tokens(input);
    if aliases > options.max_yaml_aliases {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "input exceeds max_yaml_aliases",
        ));
    }
    Ok(())
}

fn count_yaml_alias_tokens(input: &str) -> usize {
    let mut count = 0usize;
    let mut block_scalar_indent: Option<usize> = None;

    for line in input.lines() {
        let indent = line.chars().take_while(|value| *value == ' ').count();
        if let Some(block_indent) = block_scalar_indent {
            if line.trim().is_empty() || indent > block_indent {
                continue;
            }
            block_scalar_indent = None;
        }

        if starts_block_scalar(line) {
            block_scalar_indent = Some(indent);
        }
        count = count.saturating_add(count_yaml_alias_tokens_in_line(line));
    }

    count
}

fn starts_block_scalar(line: &str) -> bool {
    let mut in_single = false;
    let mut in_double = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => {
                in_double = !in_double;
                while in_double {
                    match chars.next() {
                        Some('\\') => {
                            chars.next();
                        }
                        Some('"') => in_double = false,
                        Some(_) => {}
                        None => break,
                    }
                }
            }
            '#' if !in_single && !in_double => break,
            '|' | '>' if !in_single && !in_double => {
                let tail = chars.collect::<String>();
                let tail = tail.trim();
                return tail.is_empty()
                    || tail
                        .chars()
                        .all(|value| matches!(value, '+' | '-' | '0'..='9'));
            }
            _ => {}
        }
    }
    false
}

fn count_yaml_alias_tokens_in_line(line: &str) -> usize {
    let mut count = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let bytes = line.as_bytes();
    let mut index = 0usize;

    while index < bytes.len() {
        let byte = bytes[index];
        match byte {
            b'\'' if !in_double => {
                in_single = !in_single;
                index += 1;
            }
            b'"' if !in_single => {
                in_double = !in_double;
                index += 1;
            }
            b'\\' if in_double => {
                index = (index + 2).min(bytes.len());
            }
            b'#' if !in_single && !in_double => break,
            b'*' if !in_single && !in_double => {
                if is_alias_token_boundary(bytes, index) {
                    count = count.saturating_add(1);
                }
                index += 1;
            }
            _ => index += 1,
        }
    }

    count
}

fn is_alias_token_boundary(bytes: &[u8], index: usize) -> bool {
    let previous = index
        .checked_sub(1)
        .and_then(|previous| bytes.get(previous))
        .copied();
    let next = bytes.get(index + 1).copied();
    previous.is_none_or(|value| {
        value.is_ascii_whitespace() || matches!(value, b'[' | b'{' | b',' | b':' | b'-')
    }) && next.is_some_and(is_yaml_anchor_char)
}

fn is_yaml_anchor_char(value: u8) -> bool {
    value.is_ascii_alphanumeric() || matches!(value, b'_' | b'-')
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
    if let Some(value) = value.as_f64()
        && let Some(value) = JsonNumber::from_f64(value)
    {
        return Ok(JsonValue::Number(value));
    }
    Err(TransformError::new(
        TransformErrorKind::InvalidInput,
        "YAML number is not JSON-compatible",
    ))
}
