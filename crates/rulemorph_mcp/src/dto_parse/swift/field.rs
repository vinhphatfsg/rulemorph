use std::collections::HashMap;

use crate::dto_schema::{DtoField, DtoFieldType, PrimitiveKind};

pub(super) fn parse_swift_field(
    line: &str,
    coding_keys: &HashMap<String, String>,
) -> Option<DtoField> {
    if !(line.starts_with("let ") || line.starts_with("var ")) {
        return None;
    }

    let rest = line.trim_end_matches(';').trim_end_matches(',').trim();
    let rest = rest
        .strip_prefix("let ")
        .or_else(|| rest.strip_prefix("var "))?;
    let mut parts = rest.splitn(2, ':');
    let field_name = parts.next().unwrap_or("").trim();
    let mut type_part = parts.next().unwrap_or("").trim();
    if field_name.is_empty() || type_part.is_empty() {
        return None;
    }
    if let Some(eq_pos) = type_part.find('=') {
        type_part = type_part[..eq_pos].trim();
    }

    let mut optional = type_part.contains('?');
    let type_token = type_part.trim_end_matches('?');
    let field_type = swift_field_type(type_token);

    if type_part.contains("Optional<") {
        optional = true;
    }

    let json_key = coding_keys
        .get(field_name)
        .cloned()
        .unwrap_or_else(|| field_name.to_string());
    Some(DtoField {
        json_key,
        field_type,
        optional,
    })
}

fn swift_field_type(type_token: &str) -> DtoFieldType {
    if type_token.contains('<') {
        return DtoFieldType::Unknown;
    }

    match type_token {
        "String" => DtoFieldType::Primitive(PrimitiveKind::String),
        "Bool" => DtoFieldType::Primitive(PrimitiveKind::Bool),
        "Int" | "Int8" | "Int16" | "Int32" | "Int64" | "UInt" | "UInt8" | "UInt16" | "UInt32"
        | "UInt64" => DtoFieldType::Primitive(PrimitiveKind::Int),
        "Float" | "Double" => DtoFieldType::Primitive(PrimitiveKind::Float),
        "" => DtoFieldType::Unknown,
        other => DtoFieldType::Object(other.to_string()),
    }
}
