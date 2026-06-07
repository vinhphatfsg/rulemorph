use serde_json::{Map, Number as JsonNumber, Value as JsonValue};
use serde_yaml::Value as YamlValue;

use crate::error::{TransformError, TransformErrorKind};
use crate::model::MarkdownFrontmatter;
use crate::normalization::yaml::aliases::enforce_yaml_alias_limit;
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
            if let Some(split) =
                split_auto_delimited_frontmatter(input, "---", parse_yaml_frontmatter, options)?
            {
                Ok(split)
            } else if let Some(split) =
                split_auto_delimited_frontmatter(input, "+++", parse_toml_frontmatter, options)?
            {
                Ok(split)
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
    let Some(rest) = strip_opening_delimiter(input, delimiter) else {
        return Ok(SplitMarkdown {
            frontmatter: Map::new(),
            body: input,
        });
    };
    let Some((frontmatter_end, body_start)) = find_closing_delimiter(rest, delimiter) else {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "markdown frontmatter closing delimiter is missing",
        ));
    };
    let frontmatter = parser(&rest[..frontmatter_end], options)?;
    let body = &rest[body_start..];
    Ok(SplitMarkdown { frontmatter, body })
}

fn split_auto_delimited_frontmatter<'a>(
    input: &'a str,
    delimiter: &str,
    parser: fn(&str, &NormalizationOptions) -> Result<Map<String, JsonValue>, TransformError>,
    options: &NormalizationOptions,
) -> Result<Option<SplitMarkdown<'a>>, TransformError> {
    let Some(rest) = strip_opening_delimiter(input, delimiter) else {
        return Ok(None);
    };
    let Some((frontmatter_end, body_start)) = find_closing_delimiter(rest, delimiter) else {
        return Ok(None);
    };
    let frontmatter = parser(&rest[..frontmatter_end], options)?;
    let body = &rest[body_start..];
    Ok(Some(SplitMarkdown { frontmatter, body }))
}

fn strip_opening_delimiter<'a>(input: &'a str, delimiter: &str) -> Option<&'a str> {
    input
        .strip_prefix(delimiter)
        .and_then(strip_required_line_ending)
}

fn strip_required_line_ending(input: &str) -> Option<&str> {
    input
        .strip_prefix("\r\n")
        .or_else(|| input.strip_prefix('\n'))
}

fn find_closing_delimiter(input: &str, delimiter: &str) -> Option<(usize, usize)> {
    let mut line_start = 0usize;
    loop {
        let tail = &input[line_start..];
        let Some(newline_offset) = tail.find('\n') else {
            return delimiter_line_matches(tail, delimiter).then_some((line_start, input.len()));
        };
        let line_end = line_start + newline_offset;
        if delimiter_line_matches(&input[line_start..line_end], delimiter) {
            return Some((line_start, line_end + 1));
        }
        line_start = line_end + 1;
    }
}

fn delimiter_line_matches(line: &str, delimiter: &str) -> bool {
    line.strip_suffix('\r').unwrap_or(line) == delimiter
}

fn parse_yaml_frontmatter(
    input: &str,
    options: &NormalizationOptions,
) -> Result<Map<String, JsonValue>, TransformError> {
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
            format!("failed to parse YAML frontmatter: {}", err),
        )
    })?;
    let mut node_count = 0usize;
    let value = yaml_frontmatter_to_json(&value, options, 0, &mut node_count)?;
    object_frontmatter(value)
}

fn parse_toml_frontmatter(
    input: &str,
    options: &NormalizationOptions,
) -> Result<Map<String, JsonValue>, TransformError> {
    let value = super::super::toml::parse_toml_json_with_limits(input, options).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to parse TOML frontmatter: {}", err),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn toml_frontmatter_split_applies_text_limits() {
        let options = NormalizationOptions {
            max_text_bytes: 4,
            ..NormalizationOptions::default()
        };

        let err = match split_frontmatter(
            MarkdownFrontmatter::Toml,
            "+++\nowner = \"docs-team\"\n+++\n# Guide",
            &options,
        ) {
            Ok(_) => panic!("toml frontmatter should enforce text limits during conversion"),
            Err(err) => err,
        };

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_text_bytes"));
    }
}
