use crate::dto_schema::{DtoFieldType, PrimitiveKind};

pub(super) struct ParsedJavaField {
    pub(super) name: String,
    pub(super) field_type: DtoFieldType,
    pub(super) optional: bool,
}

pub(super) fn parse_field_line(line: &str, pending_optional: bool) -> Option<ParsedJavaField> {
    let cleaned = clean_field_line(line)?;
    let rest = strip_modifiers(cleaned);
    let (type_part, field_name) = split_type_and_name(rest)?;
    Some(ParsedJavaField {
        name: field_name.to_string(),
        field_type: java_field_type(type_part),
        optional: pending_optional || type_part.replace(' ', "").contains("Optional<"),
    })
}

fn clean_field_line(line: &str) -> Option<&str> {
    let mut cleaned = line;
    if let Some(comment_pos) = cleaned.find("//") {
        cleaned = cleaned[..comment_pos].trim();
    }
    cleaned = cleaned.split('=').next().unwrap_or(cleaned).trim();
    cleaned = cleaned.trim_end_matches(';').trim();
    cleaned = cleaned.trim_end_matches(',').trim();
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

fn strip_modifiers(mut rest: &str) -> &str {
    let modifiers = [
        "public",
        "private",
        "protected",
        "static",
        "final",
        "transient",
        "volatile",
    ];
    loop {
        let mut stripped = None;
        for modifier in modifiers {
            if let Some(after) = rest.strip_prefix(modifier) {
                let after = after.trim_start();
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
        return rest;
    }
}

fn split_type_and_name(rest: &str) -> Option<(&str, &str)> {
    let split_pos = rest.rfind(|ch: char| ch.is_whitespace())?;
    let type_part = rest[..split_pos].trim();
    let field_name = rest[split_pos..].trim();
    if field_name.is_empty() || type_part.is_empty() {
        None
    } else {
        Some((type_part, field_name))
    }
}

fn java_field_type(type_part: &str) -> DtoFieldType {
    let type_key = type_part
        .rsplit('.')
        .next()
        .unwrap_or(type_part)
        .trim()
        .trim_end_matches('>');
    let type_key = type_key.rsplit('<').next().unwrap_or(type_key).trim();
    match type_key {
        "String" => DtoFieldType::Primitive(PrimitiveKind::String),
        "boolean" | "Boolean" => DtoFieldType::Primitive(PrimitiveKind::Bool),
        "byte" | "short" | "int" | "long" | "Byte" | "Short" | "Integer" | "Long" => {
            DtoFieldType::Primitive(PrimitiveKind::Int)
        }
        "float" | "double" | "Float" | "Double" => DtoFieldType::Primitive(PrimitiveKind::Float),
        "" => DtoFieldType::Unknown,
        other => DtoFieldType::Object(other.to_string()),
    }
}
