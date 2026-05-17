use std::collections::HashMap;

use crate::dto_language::DtoSourceLanguage;
use crate::dto_normalize::{
    normalize_java_text, normalize_kotlin_text, normalize_python_text, normalize_rust_text,
    normalize_swift_text, normalize_typescript_text,
};
use crate::dto_schema::{DtoField, DtoFieldType, DtoSchema, DtoType, PrimitiveKind};

mod go;

use self::go::parse_go_types;

pub(crate) fn parse_dto_schema(
    text: &str,
    language: DtoSourceLanguage,
) -> Result<DtoSchema, String> {
    let (types, order) = match language {
        DtoSourceLanguage::TypeScript => parse_typescript_types(text)?,
        DtoSourceLanguage::Rust => parse_rust_types(text)?,
        DtoSourceLanguage::Python => parse_python_types(text)?,
        DtoSourceLanguage::Go => parse_go_types(text)?,
        DtoSourceLanguage::Java => parse_java_types(text)?,
        DtoSourceLanguage::Kotlin => parse_kotlin_types(text)?,
        DtoSourceLanguage::Swift => parse_swift_types(text)?,
    };

    let root = if types.contains_key("Record") {
        "Record".to_string()
    } else {
        order
            .first()
            .cloned()
            .ok_or_else(|| "no dto types found".to_string())?
    };

    Ok(DtoSchema { root, types })
}

fn parse_typescript_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
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
            .split(|ch| ch == '|' || ch == '&')
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

fn parse_rust_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
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

fn parse_first_quoted_value(text: &str) -> Option<String> {
    let mut best: Option<(usize, char)> = None;
    for quote in ['"', '\''] {
        if let Some(pos) = text.find(quote) {
            if best.map_or(true, |(best_pos, _)| pos < best_pos) {
                best = Some((pos, quote));
            }
        }
    }

    let (pos, quote) = best?;
    let after = &text[pos + 1..];
    let end = after.find(quote)?;
    Some(after[..end].to_string())
}

fn parse_quoted_value_after(line: &str, marker: &str) -> Option<String> {
    let start = line.find(marker)?;
    let after = &line[start + marker.len()..];
    parse_first_quoted_value(after)
}

fn parse_named_argument(line: &str, key: &str) -> Option<String> {
    let start = line.find(key)?;
    let after = &line[start + key.len()..];
    let eq_pos = after.find('=')?;
    let after_eq = after[eq_pos + 1..].trim_start();
    parse_first_quoted_value(after_eq)
}

fn parse_common_rename_annotation(line: &str) -> Option<String> {
    parse_quoted_value_after(line, "@JsonProperty")
        .or_else(|| parse_quoted_value_after(line, "@SerializedName"))
        .or_else(|| parse_quoted_value_after(line, "@SerialName"))
        .or_else(|| parse_quoted_value_after(line, "@Json"))
}

fn strip_leading_annotations(
    line: &str,
    pending_json_key: &mut Option<String>,
    pending_optional: &mut bool,
) -> String {
    let mut rest = line.trim();
    loop {
        if !rest.starts_with('@') {
            break;
        }
        if let Some(rename) = parse_common_rename_annotation(rest) {
            *pending_json_key = Some(rename);
        }
        if rest.starts_with("@Nullable") {
            *pending_optional = true;
        }
        if let Some(end) = rest.find(')') {
            rest = rest[end + 1..].trim();
            if rest.is_empty() {
                return String::new();
            }
        } else if let Some(space) = rest.find(' ') {
            rest = rest[space + 1..].trim();
            if rest.is_empty() {
                return String::new();
            }
        } else {
            return String::new();
        }
    }

    rest.to_string()
}

fn parse_python_alias(line: &str) -> Option<String> {
    parse_named_argument(line, "alias")
}

fn parse_python_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
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

        if let Some(indent_level) = current_indent {
            if !class_line && indent <= indent_level && !line.is_empty() {
                current = None;
                current_indent = None;
            }
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
        } else if type_token.contains('|') {
            if let Some(first) = type_token
                .split('|')
                .map(|item| item.trim())
                .find(|item| !item.contains("None"))
            {
                type_token = first;
            }
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

fn parse_java_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
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

fn parse_kotlin_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
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
        if next_depth <= 0 {
            if let Some(end) = slice.rfind(')') {
                slice = slice[..end].trim();
            }
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

fn parse_swift_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
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

fn parse_serde_rename(line: &str) -> Option<String> {
    let marker = line.find("rename")?;
    let after_marker = &line[marker..];
    let quote_start = after_marker.find('"')?;
    let after_quote = &after_marker[quote_start + 1..];
    let quote_end = after_quote.find('"')?;
    Some(after_quote[..quote_end].to_string())
}
