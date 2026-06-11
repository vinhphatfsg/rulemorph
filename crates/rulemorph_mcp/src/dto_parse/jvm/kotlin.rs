use std::collections::HashMap;

use crate::dto_normalize::normalize_kotlin_text;
use crate::dto_schema::{DtoField, DtoFieldType, DtoType, PrimitiveKind};

use super::strip_leading_annotations;

pub(in crate::dto_parse) fn parse_kotlin_types(
    text: &str,
) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut pending_json_key: Option<String> = None;
    let mut pending_optional = false;
    let mut param_depth = 0i32;
    let normalized = normalize_kotlin_text(text);

    for raw_line in normalized.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if line.contains(" class ") || line.starts_with("class ") || line.starts_with("data class ")
        {
            let name_part = if let Some(idx) = line.find("class ") {
                &line[idx + 6..]
            } else {
                line
            };
            let name = name_part
                .split(|ch: char| ch.is_whitespace() || ch == '(' || ch == '{')
                .next()
                .unwrap_or("")
                .trim();
            if !name.is_empty() {
                current = Some(name.to_string());
                types
                    .entry(name.to_string())
                    .or_insert_with(|| DtoType { fields: Vec::new() });
                order.push(name.to_string());
                pending_json_key = None;
                pending_optional = false;
                param_depth = 0;
                if let Some(paren_pos) = line.find('(') {
                    param_depth += 1;
                    line = line[paren_pos + 1..].trim();
                } else {
                    continue;
                }
            }
        }

        let Some(current_name) = current.clone() else {
            continue;
        };
        if line.starts_with('}') {
            current = None;
            param_depth = 0;
            pending_json_key = None;
            pending_optional = false;
            continue;
        }

        if param_depth <= 0 {
            continue;
        }

        let open_parens = line.matches('(').count() as i32;
        let close_parens = line.matches(')').count() as i32;
        let next_depth = param_depth + open_parens - close_parens;
        let mut slice = line;
        if next_depth <= 0
            && let Some(end) = slice.rfind(')')
        {
            slice = slice[..end].trim();
        }

        if param_depth <= 0 && slice.is_empty() {
            param_depth = next_depth.max(0);
            continue;
        }

        let stripped =
            strip_leading_annotations(slice, &mut pending_json_key, &mut pending_optional);
        line = stripped.trim();
        if line.is_empty() {
            param_depth = next_depth.max(0);
            continue;
        }

        let line = line.trim_end_matches(',').trim();
        let rest = if let Some(stripped) = line.strip_prefix("val ") {
            stripped
        } else if let Some(stripped) = line.strip_prefix("var ") {
            stripped
        } else {
            line
        };

        let mut parts = rest.splitn(2, ':');
        let field_name = parts.next().unwrap_or("").trim();
        let type_part = parts.next().unwrap_or("").trim();
        if field_name.is_empty() || type_part.is_empty() {
            continue;
        }

        let mut optional = pending_optional;
        pending_optional = false;
        if type_part.contains('?') || type_part.contains("= null") {
            optional = true;
        }

        let type_token = type_part
            .split('=')
            .next()
            .unwrap_or(type_part)
            .trim()
            .trim_end_matches('?');
        let field_type = if type_token.contains('<') {
            DtoFieldType::Unknown
        } else {
            match type_token {
                "String" => DtoFieldType::Primitive(PrimitiveKind::String),
                "Boolean" => DtoFieldType::Primitive(PrimitiveKind::Bool),
                "Int" | "Long" | "Short" | "Byte" => DtoFieldType::Primitive(PrimitiveKind::Int),
                "Float" | "Double" => DtoFieldType::Primitive(PrimitiveKind::Float),
                "" => DtoFieldType::Unknown,
                other => DtoFieldType::Object(other.to_string()),
            }
        };

        let json_key = pending_json_key
            .take()
            .unwrap_or_else(|| field_name.to_string());
        if let Some(dto_type) = types.get_mut(&current_name) {
            dto_type.fields.push(DtoField {
                json_key,
                field_type,
                optional,
            });
        }

        param_depth = next_depth.max(0);
    }

    Ok((types, order))
}
