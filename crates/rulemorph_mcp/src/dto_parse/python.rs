use std::collections::HashMap;

use crate::dto_normalize::normalize_python_text;
use crate::dto_schema::{DtoField, DtoFieldType, DtoType, PrimitiveKind};

use super::parse_named_argument;

pub(super) fn parse_python_types(
    text: &str,
) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut current_indent: Option<usize> = None;
    let normalized = normalize_python_text(text);

    for raw_line in normalized.lines() {
        let indent = raw_line.chars().take_while(|ch| ch.is_whitespace()).count();
        let mut line = raw_line.trim();
        let mut class_line = false;
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if line.starts_with("class ") {
            class_line = true;
            let name_part = line.strip_prefix("class ").unwrap_or(line).trim();
            let name = name_part
                .split(|ch: char| ch.is_whitespace() || ch == '(' || ch == ':')
                .next()
                .unwrap_or("")
                .trim();
            if name.is_empty() {
                continue;
            }
            current = Some(name.to_string());
            current_indent = Some(indent);
            types
                .entry(name.to_string())
                .or_insert_with(|| DtoType { fields: Vec::new() });
            order.push(name.to_string());
            if let Some(colon_pos) = line.find(':') {
                line = line[colon_pos + 1..].trim();
                if line.is_empty() {
                    continue;
                }
            } else {
                continue;
            }
        }

        if let Some(indent_level) = current_indent
            && !class_line
            && indent <= indent_level
            && !line.is_empty()
        {
            current = None;
            current_indent = None;
        }

        let Some(current_name) = current.clone() else {
            continue;
        };

        if line.starts_with('@') {
            continue;
        }

        if let Some(comment_pos) = line.find('#') {
            line = line[..comment_pos].trim();
        }
        if line.is_empty() || !line.contains(':') {
            continue;
        }

        let mut parts = line.splitn(2, ':');
        let field_name = parts.next().unwrap_or("").trim();
        let mut rest = parts.next().unwrap_or("").trim();
        rest = rest.trim_end_matches(';').trim();
        if field_name.is_empty() || rest.is_empty() {
            continue;
        }

        let mut optional = false;
        if let Some(eq_pos) = rest.find('=') {
            let (type_part, value_part) = rest.split_at(eq_pos);
            rest = type_part.trim();
            if value_part.contains("None") {
                optional = true;
            }
        }

        if rest.contains("Optional[")
            || rest.contains("None")
            || rest.contains("| None")
            || rest.contains("None |")
        {
            optional = true;
        }

        let mut type_token = rest.trim();
        if let Some(start) = type_token.find("Optional[") {
            let after = &type_token[start + "Optional[".len()..];
            if let Some(end) = after.find(']') {
                type_token = after[..end].trim();
            }
        } else if let Some(start) = type_token.find("Union[") {
            let after = &type_token[start + "Union[".len()..];
            if let Some(end) = after.find(']') {
                let inner = &after[..end];
                if let Some(first) = inner
                    .split(',')
                    .map(|item| item.trim())
                    .find(|item| !item.contains("None"))
                {
                    type_token = first;
                }
            }
        } else if type_token.contains('|')
            && let Some(first) = type_token
                .split('|')
                .map(|item| item.trim())
                .find(|item| !item.contains("None"))
        {
            type_token = first;
        }

        let type_token = type_token.trim_start_matches("typing.");
        let field_type = if type_token.contains('[')
            || type_token.contains("List")
            || type_token.contains("Dict")
            || type_token.contains("list")
            || type_token.contains("dict")
        {
            DtoFieldType::Unknown
        } else {
            match type_token {
                "str" | "string" => DtoFieldType::Primitive(PrimitiveKind::String),
                "int" => DtoFieldType::Primitive(PrimitiveKind::Int),
                "float" => DtoFieldType::Primitive(PrimitiveKind::Float),
                "bool" | "boolean" => DtoFieldType::Primitive(PrimitiveKind::Bool),
                "Any" | "any" => DtoFieldType::Unknown,
                "" => DtoFieldType::Unknown,
                other => DtoFieldType::Object(other.to_string()),
            }
        };

        let json_key = parse_python_alias(line).unwrap_or_else(|| field_name.to_string());
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

fn parse_python_alias(line: &str) -> Option<String> {
    parse_named_argument(line, "alias")
}
