use std::collections::HashMap;

use crate::dto_normalize::normalize_java_text;
use crate::dto_schema::{DtoField, DtoFieldType, DtoType, PrimitiveKind};

use super::strip_leading_annotations;

pub(in crate::dto_parse) fn parse_java_types(
    text: &str,
) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut pending_json_key: Option<String> = None;
    let mut pending_optional = false;
    let mut record_param_depth = 0i32;
    let normalized = normalize_java_text(text);

    for raw_line in normalized.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if line.contains(" class ") || line.starts_with("class ") {
            let name_part = if let Some(idx) = line.find("class ") {
                &line[idx + 6..]
            } else {
                line
            };
            let name = name_part
                .split(|ch: char| ch.is_whitespace() || ch == '{' || ch == '(')
                .next()
                .unwrap_or("")
                .trim();
            if !name.is_empty() {
                current = Some(name.to_string());
                types
                    .entry(name.to_string())
                    .or_insert_with(|| DtoType { fields: Vec::new() });
                order.push(name.to_string());
            }
            record_param_depth = 0;
            pending_json_key = None;
            pending_optional = false;
            continue;
        }

        if line.contains(" record ") || line.starts_with("record ") {
            let name_part = if let Some(idx) = line.find("record ") {
                &line[idx + 7..]
            } else {
                line
            };
            let name = name_part
                .split(|ch: char| ch.is_whitespace() || ch == '{' || ch == '(')
                .next()
                .unwrap_or("")
                .trim();
            if !name.is_empty() {
                current = Some(name.to_string());
                types
                    .entry(name.to_string())
                    .or_insert_with(|| DtoType { fields: Vec::new() });
                order.push(name.to_string());
                if let Some(paren_pos) = line.find('(') {
                    record_param_depth = 1;
                    line = line[paren_pos + 1..].trim();
                } else {
                    record_param_depth = 0;
                    continue;
                }
                pending_json_key = None;
                pending_optional = false;
            }
        }

        let Some(current_name) = current.clone() else {
            continue;
        };
        if line.starts_with('}') {
            current = None;
            record_param_depth = 0;
            pending_json_key = None;
            pending_optional = false;
            continue;
        }

        if record_param_depth > 0 {
            let open_parens = line.matches('(').count() as i32;
            let close_parens = line.matches(')').count() as i32;
            let next_depth = record_param_depth + open_parens - close_parens;
            if next_depth <= 0 {
                if let Some(end) = line.rfind(')') {
                    line = line[..end].trim();
                }
                record_param_depth = 0;
            } else {
                record_param_depth = next_depth;
            }
            if line.is_empty() {
                continue;
            }
            let stripped =
                strip_leading_annotations(line, &mut pending_json_key, &mut pending_optional);
            line = stripped.trim();
            if line.is_empty() {
                continue;
            }
            parse_java_field_line(
                line,
                &current_name,
                &mut types,
                &mut pending_json_key,
                &mut pending_optional,
            );
            continue;
        }

        let stripped =
            strip_leading_annotations(line, &mut pending_json_key, &mut pending_optional);
        line = stripped.trim();
        if line.is_empty() || !line.contains(';') {
            continue;
        }
        parse_java_field_line(
            line,
            &current_name,
            &mut types,
            &mut pending_json_key,
            &mut pending_optional,
        );
    }

    Ok((types, order))
}

fn parse_java_field_line(
    line: &str,
    current_name: &str,
    types: &mut HashMap<String, DtoType>,
    pending_json_key: &mut Option<String>,
    pending_optional: &mut bool,
) {
    let mut cleaned = line;
    if let Some(comment_pos) = cleaned.find("//") {
        cleaned = cleaned[..comment_pos].trim();
    }
    cleaned = cleaned.split('=').next().unwrap_or(cleaned).trim();
    cleaned = cleaned.trim_end_matches(';').trim();
    cleaned = cleaned.trim_end_matches(',').trim();
    if cleaned.is_empty() {
        return;
    }

    let modifiers = [
        "public",
        "private",
        "protected",
        "static",
        "final",
        "transient",
        "volatile",
    ];
    let mut rest = cleaned;
    loop {
        let mut stripped = None;
        for modifier in modifiers {
            if rest.starts_with(modifier) {
                let after = rest[modifier.len()..].trim_start();
                if after.len() != rest.len() {
                    stripped = Some(after);
                    break;
                }
            }
        }
        if let Some(value) = stripped {
            rest = value;
            continue;
        }
        break;
    }

    let Some(split_pos) = rest.rfind(|ch: char| ch.is_whitespace()) else {
        return;
    };
    let type_part = rest[..split_pos].trim();
    let field_name = rest[split_pos..].trim();
    if field_name.is_empty() || type_part.is_empty() {
        return;
    }

    let optional = *pending_optional || type_part.replace(' ', "").contains("Optional<");
    *pending_optional = false;

    let type_key = type_part
        .rsplit('.')
        .next()
        .unwrap_or(type_part)
        .trim()
        .trim_end_matches('>');
    let type_key = type_key.rsplit('<').next().unwrap_or(type_key).trim();
    let field_type = match type_key {
        "String" => DtoFieldType::Primitive(PrimitiveKind::String),
        "boolean" | "Boolean" => DtoFieldType::Primitive(PrimitiveKind::Bool),
        "byte" | "short" | "int" | "long" | "Byte" | "Short" | "Integer" | "Long" => {
            DtoFieldType::Primitive(PrimitiveKind::Int)
        }
        "float" | "double" | "Float" | "Double" => DtoFieldType::Primitive(PrimitiveKind::Float),
        "" => DtoFieldType::Unknown,
        other => DtoFieldType::Object(other.to_string()),
    };

    let json_key = pending_json_key
        .take()
        .unwrap_or_else(|| field_name.to_string());
    if let Some(dto_type) = types.get_mut(current_name) {
        dto_type.fields.push(DtoField {
            json_key,
            field_type,
            optional,
        });
    }
}
