use std::collections::HashMap;

use crate::dto_normalize::normalize_swift_text;
use crate::dto_schema::{DtoField, DtoFieldType, DtoType, PrimitiveKind};

use super::parse_first_quoted_value;

pub(in crate::dto_parse) fn parse_swift_types(
    text: &str,
) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut coding_keys: HashMap<String, String> = HashMap::new();
    let mut in_coding_keys = false;
    let mut coding_depth = 0i32;
    let mut type_depth = 0i32;
    let normalized = normalize_swift_text(text);

    for raw_line in normalized.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if line.contains(" struct ")
            || line.starts_with("struct ")
            || line.contains(" class ")
            || line.starts_with("class ")
        {
            let keyword_pos = if let Some(pos) = line.find("struct ") {
                pos + 7
            } else if let Some(pos) = line.find("class ") {
                pos + 6
            } else {
                0
            };
            let name_part = line[keyword_pos..].split_whitespace().next().unwrap_or("");
            let name = name_part
                .split(|ch: char| ch == ':' || ch == '{')
                .next()
                .unwrap_or("")
                .trim();
            if !name.is_empty() {
                current = Some(name.to_string());
                types
                    .entry(name.to_string())
                    .or_insert_with(|| DtoType { fields: Vec::new() });
                order.push(name.to_string());
                coding_keys.clear();
                in_coding_keys = false;
                coding_depth = 0;
                type_depth = 0;
            }
        }

        let open_braces = line.matches('{').count() as i32;
        let close_braces = line.matches('}').count() as i32;
        if current.is_some() {
            type_depth += open_braces - close_braces;
            if type_depth < 0 {
                type_depth = 0;
            }
        }

        let Some(current_name) = current.clone() else {
            continue;
        };

        if line.starts_with("enum CodingKeys") {
            in_coding_keys = true;
            coding_depth = open_braces - close_braces;
            continue;
        }

        if in_coding_keys {
            coding_depth += open_braces - close_braces;
            if line.starts_with("case ") {
                let cases = parse_swift_cases(line);
                if let Some(dto_type) = types.get_mut(&current_name) {
                    for (field, rename) in cases {
                        coding_keys.insert(field.clone(), rename.clone());
                        for existing in &mut dto_type.fields {
                            if existing.json_key == field {
                                existing.json_key = rename.clone();
                            }
                        }
                    }
                }
            }
            if coding_depth <= 0 {
                in_coding_keys = false;
                coding_depth = 0;
            }
            continue;
        }

        if type_depth == 0 && line.starts_with('}') {
            current = None;
            continue;
        }

        if !(line.starts_with("let ") || line.starts_with("var ")) {
            continue;
        }

        let rest = line.trim_end_matches(';').trim_end_matches(',').trim();
        let rest = rest
            .strip_prefix("let ")
            .or_else(|| rest.strip_prefix("var "));
        let Some(rest) = rest else { continue };
        let mut parts = rest.splitn(2, ':');
        let field_name = parts.next().unwrap_or("").trim();
        let mut type_part = parts.next().unwrap_or("").trim();
        if field_name.is_empty() || type_part.is_empty() {
            continue;
        }
        if let Some(eq_pos) = type_part.find('=') {
            type_part = type_part[..eq_pos].trim();
        }

        let mut optional = type_part.contains('?');
        let type_token = type_part.trim_end_matches('?');
        let field_type = if type_token.contains('<') {
            DtoFieldType::Unknown
        } else {
            match type_token {
                "String" => DtoFieldType::Primitive(PrimitiveKind::String),
                "Bool" => DtoFieldType::Primitive(PrimitiveKind::Bool),
                "Int" | "Int8" | "Int16" | "Int32" | "Int64" | "UInt" | "UInt8" | "UInt16"
                | "UInt32" | "UInt64" => DtoFieldType::Primitive(PrimitiveKind::Int),
                "Float" | "Double" => DtoFieldType::Primitive(PrimitiveKind::Float),
                "" => DtoFieldType::Unknown,
                other => DtoFieldType::Object(other.to_string()),
            }
        };

        if type_part.contains("Optional<") {
            optional = true;
        }

        let json_key = coding_keys
            .get(field_name)
            .cloned()
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

fn parse_swift_cases(line: &str) -> Vec<(String, String)> {
    let mut cases = Vec::new();
    let rest = line.strip_prefix("case ").unwrap_or(line).trim();
    let mut current = String::new();
    let mut in_string = false;
    let mut escape = false;

    for ch in rest.chars() {
        if in_string {
            current.push(ch);
            if escape {
                escape = false;
                continue;
            }
            if ch == '\\' {
                escape = true;
                continue;
            }
            if ch == '"' {
                in_string = false;
            }
            continue;
        }

        if ch == '"' {
            in_string = true;
            current.push(ch);
            continue;
        }

        if ch == ',' {
            push_swift_case(&mut cases, &current);
            current.clear();
            continue;
        }

        current.push(ch);
    }
    push_swift_case(&mut cases, &current);
    cases
}

fn push_swift_case(cases: &mut Vec<(String, String)>, text: &str) {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return;
    }
    let mut parts = trimmed.splitn(2, '=');
    let name = parts.next().unwrap_or("").trim();
    if name.is_empty() {
        return;
    }
    let rename = parts
        .next()
        .and_then(|value| parse_first_quoted_value(value))
        .unwrap_or_else(|| name.to_string());
    cases.push((name.to_string(), rename));
}
