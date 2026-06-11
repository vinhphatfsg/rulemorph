use std::collections::HashMap;

use crate::dto_normalize::normalize_typescript_text;
use crate::dto_schema::{DtoField, DtoFieldType, DtoType, PrimitiveKind};

pub(super) fn parse_typescript_types(
    text: &str,
) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut pending_json_key: Option<String> = None;

    let normalized = normalize_typescript_text(text);
    for raw_line in normalized.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if line.starts_with("export interface ") || line.starts_with("interface ") {
            let line = line.strip_prefix("export ").unwrap_or(line);
            let name_part = line.strip_prefix("interface ").unwrap_or(line).trim();
            let name = name_part
                .split(|ch: char| ch.is_whitespace() || ch == '{')
                .next()
                .unwrap_or("")
                .trim();
            if name.is_empty() {
                continue;
            }
            current = Some(name.to_string());
            pending_json_key = None;
            types
                .entry(name.to_string())
                .or_insert_with(|| DtoType { fields: Vec::new() });
            order.push(name.to_string());
            continue;
        }

        let Some(current_name) = current.clone() else {
            continue;
        };
        if line.starts_with('}') {
            current = None;
            pending_json_key = None;
            continue;
        }

        if let Some((json_key, rest)) = parse_json_comment(line) {
            pending_json_key = Some(json_key);
            line = rest.trim();
            if line.is_empty() {
                continue;
            }
        }

        if !line.contains(':') {
            continue;
        }

        let line = line.trim_end_matches(';').trim();
        let mut parts = line.splitn(2, ':');
        let name_part = parts.next().unwrap_or("").trim();
        let type_part = parts.next().unwrap_or("").trim();
        if name_part.is_empty() || type_part.is_empty() {
            continue;
        }
        let optional = name_part.ends_with('?');
        let field_name = name_part.trim_end_matches('?').trim().to_string();

        let type_token = type_part
            .split(['|', '&'])
            .next()
            .unwrap_or("")
            .trim()
            .trim_end_matches(';');
        let field_type = if type_token.contains('[') {
            DtoFieldType::Unknown
        } else {
            match type_token {
                "string" => DtoFieldType::Primitive(PrimitiveKind::String),
                "number" => DtoFieldType::Primitive(PrimitiveKind::Float),
                "boolean" => DtoFieldType::Primitive(PrimitiveKind::Bool),
                "unknown" | "any" => DtoFieldType::Unknown,
                "" => DtoFieldType::Unknown,
                other => DtoFieldType::Object(other.to_string()),
            }
        };

        let json_key = pending_json_key
            .take()
            .unwrap_or_else(|| field_name.clone());
        if let Some(dto_type) = types.get_mut(&current_name) {
            dto_type.fields.push(DtoField {
                json_key,
                field_type,
                optional,
            });
        }
    }

    Ok((types, order))
}

fn parse_json_comment(line: &str) -> Option<(String, &str)> {
    let marker = line.find("json:")?;
    let after_marker = &line[marker + 5..];
    let quote_start = after_marker.find('"')?;
    let after_quote = &after_marker[quote_start + 1..];
    let quote_end = after_quote.find('"')?;
    let json_key = after_quote[..quote_end].to_string();
    let rest = if let Some(end) = line.find("*/") {
        &line[end + 2..]
    } else {
        ""
    };
    Some((json_key, rest))
}
