use serde_json::{Map, Number as JsonNumber, Value as JsonValue};
use serde_yaml::Value as YamlValue;

use crate::error::{TransformError, TransformErrorKind};
use crate::model::MarkdownFrontmatter;
use crate::serde_guard::parse_yaml_value_strict_with_limits;

use super::super::NormalizationOptions;

pub(super) struct SplitMarkdown<'a> {
    pub(super) frontmatter: Map<String, JsonValue>,
    pub(super) body: &'a str,
}

pub(super) fn split_frontmatter<'a>(
    mode: MarkdownFrontmatter,
    input: &'a str,
    options: &NormalizationOptions,
) -> Result<SplitMarkdown<'a>, TransformError> {
    match mode {
        MarkdownFrontmatter::None => Ok(SplitMarkdown {
            frontmatter: Map::new(),
            body: input,
        }),
        MarkdownFrontmatter::Yaml => {
            split_delimited_frontmatter(input, "---", parse_yaml_frontmatter, options)
        }
        MarkdownFrontmatter::Toml => {
            split_delimited_frontmatter(input, "+++", parse_toml_frontmatter, options)
        }
        MarkdownFrontmatter::Auto => {
            if input.starts_with("---\n") {
                split_delimited_frontmatter(input, "---", parse_yaml_frontmatter, options)
            } else if input.starts_with("+++\n") {
                split_delimited_frontmatter(input, "+++", parse_toml_frontmatter, options)
            } else {
                Ok(SplitMarkdown {
                    frontmatter: Map::new(),
                    body: input,
                })
            }
        }
    }
}

fn split_delimited_frontmatter<'a>(
    input: &'a str,
    delimiter: &str,
    parser: fn(&str, &NormalizationOptions) -> Result<Map<String, JsonValue>, TransformError>,
    options: &NormalizationOptions,
) -> Result<SplitMarkdown<'a>, TransformError> {
    let prefix = format!("{}\n", delimiter);
    if !input.starts_with(&prefix) {
        return Ok(SplitMarkdown {
            frontmatter: Map::new(),
            body: input,
        });
    }
    let rest = &input[prefix.len()..];
    let end_marker = format!("\n{}\n", delimiter);
    let Some(end) = rest.find(&end_marker) else {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "markdown frontmatter closing delimiter is missing",
        ));
    };
    let frontmatter = parser(&rest[..end], options)?;
    let body = &rest[end + end_marker.len()..];
    Ok(SplitMarkdown { frontmatter, body })
}

fn parse_yaml_frontmatter(
    input: &str,
    options: &NormalizationOptions,
) -> Result<Map<String, JsonValue>, TransformError> {
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
            format!("failed to parse YAML frontmatter: {}", err),
        )
    })?;
    let mut node_count = 0usize;
    let value = yaml_frontmatter_to_json(&value, options, 0, &mut node_count)?;
    object_frontmatter(value)
}

fn parse_toml_frontmatter(
    input: &str,
    _options: &NormalizationOptions,
) -> Result<Map<String, JsonValue>, TransformError> {
    let value: ::toml::Value = ::toml::from_str(input).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to parse TOML frontmatter: {}", err),
        )
    })?;
    let value = serde_json::to_value(value).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to convert TOML frontmatter: {}", err),
        )
    })?;
    object_frontmatter(value)
}

fn object_frontmatter(value: JsonValue) -> Result<Map<String, JsonValue>, TransformError> {
    match value {
        JsonValue::Object(map) => Ok(map),
        _ => Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "markdown frontmatter must be an object",
        )),
    }
}

fn yaml_frontmatter_to_json(
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
        YamlValue::Number(value) => yaml_frontmatter_number_to_json(value),
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
                out.push(yaml_frontmatter_to_json(
                    item,
                    options,
                    depth + 1,
                    node_count,
                )?);
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
                            "YAML frontmatter mapping keys must be strings",
                        ));
                    }
                };
                out.insert(
                    key,
                    yaml_frontmatter_to_json(value, options, depth + 1, node_count)?,
                );
            }
            Ok(JsonValue::Object(out))
        }
        YamlValue::Tagged(_) => Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "YAML frontmatter custom tags are not supported",
        )),
    }
}

fn yaml_frontmatter_number_to_json(
    value: &serde_yaml::Number,
) -> Result<JsonValue, TransformError> {
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
        "YAML frontmatter number is not JSON-compatible",
    ))
}
