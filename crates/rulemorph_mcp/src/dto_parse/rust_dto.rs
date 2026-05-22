use std::collections::HashMap;

use crate::dto_normalize::normalize_rust_text;
use crate::dto_schema::{DtoField, DtoFieldType, DtoType, PrimitiveKind};

pub(super) fn parse_rust_types(
    text: &str,
) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut pending_json_key: Option<String> = None;

    let normalized = normalize_rust_text(text);
    for raw_line in normalized.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if line.starts_with("pub struct ") {
            let name_part = line.strip_prefix("pub struct ").unwrap_or(line).trim();
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

        if line.starts_with("#[serde") {
            if let Some(rename) = parse_serde_rename(line) {
                pending_json_key = Some(rename);
            }
            if let Some(end) = line.find(']') {
                let rest = line[end + 1..].trim();
                if rest.is_empty() {
                    continue;
                }
                line = rest;
            } else {
                continue;
            }
        }

        if !line.starts_with("pub ") {
            continue;
        }

        let line = line.trim_end_matches(',');
        let rest = line.strip_prefix("pub ").unwrap_or(line).trim();
        let mut parts = rest.splitn(2, ':');
        let field_name = parts.next().unwrap_or("").trim();
        let type_part = parts.next().unwrap_or("").trim();
        if field_name.is_empty() || type_part.is_empty() {
            continue;
        }

        let compact = type_part.replace(' ', "");
        let (type_name, optional) = if compact.starts_with("Option<") && compact.ends_with('>') {
            (compact[7..compact.len() - 1].to_string(), true)
        } else {
            (compact, false)
        };

        let type_key = type_name
            .rsplit("::")
            .next()
            .unwrap_or(&type_name)
            .to_string();
        let field_type = match type_key.as_str() {
            "String" => DtoFieldType::Primitive(PrimitiveKind::String),
            "bool" => DtoFieldType::Primitive(PrimitiveKind::Bool),
            "i8" | "i16" | "i32" | "i64" | "isize" | "u8" | "u16" | "u32" | "u64" | "usize" => {
                DtoFieldType::Primitive(PrimitiveKind::Int)
            }
            "f32" | "f64" => DtoFieldType::Primitive(PrimitiveKind::Float),
            _ if type_key.ends_with("Value") => DtoFieldType::Unknown,
            _ => DtoFieldType::Object(type_key),
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
    }

    Ok((types, order))
}

fn parse_serde_rename(line: &str) -> Option<String> {
    let marker = line.find("rename")?;
    let after_marker = &line[marker..];
    let quote_start = after_marker.find('"')?;
    let after_quote = &after_marker[quote_start + 1..];
    let quote_end = after_quote.find('"')?;
    Some(after_quote[..quote_end].to_string())
}
