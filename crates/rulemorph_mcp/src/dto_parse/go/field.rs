use crate::dto_schema::{DtoField, DtoFieldType, DtoType, PrimitiveKind};

pub(super) fn parse_go_struct_fields(body: &str, dto_type: &mut DtoType) {
    let mut chars = body.chars().peekable();
    while let Some(ch) = chars.peek().copied() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }
        if ch == '/' {
            chars.next();
            if matches!(chars.peek(), Some('/')) {
                for next in chars.by_ref() {
                    if next == '\n' {
                        break;
                    }
                }
                continue;
            }
            if matches!(chars.peek(), Some('*')) {
                chars.next();
                while let Some(next) = chars.next() {
                    if next == '*' && matches!(chars.peek(), Some('/')) {
                        chars.next();
                        break;
                    }
                }
                continue;
            }
            continue;
        }

        let field_name = read_go_token(&mut chars);
        if field_name.is_empty() {
            chars.next();
            continue;
        }
        skip_go_whitespace(&mut chars);
        let field_type = read_go_token(&mut chars);
        if field_type.is_empty() {
            continue;
        }

        skip_go_whitespace(&mut chars);
        let tag = read_go_tag(&mut chars);

        let (json_key, tag_optional, skip_field) = parse_go_json_tag(tag.as_deref());
        if skip_field {
            continue;
        }

        let mut optional = tag_optional;
        let mut type_token = field_type.trim().to_string();
        if let Some(stripped) = type_token.strip_prefix('*') {
            optional = true;
            type_token = stripped.to_string();
        }

        let field_type = go_field_type(&type_token);
        let json_key = json_key.unwrap_or_else(|| field_name.clone());
        dto_type.fields.push(DtoField {
            json_key,
            field_type,
            optional,
        });
    }
}

fn read_go_token(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> String {
    let mut token = String::new();
    while let Some(&ch) = chars.peek() {
        if ch.is_whitespace() || ch == '`' || ch == '{' || ch == '}' {
            break;
        }
        token.push(ch);
        chars.next();
    }
    token
}

fn skip_go_whitespace(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    while let Some(&ch) = chars.peek() {
        if !ch.is_whitespace() {
            break;
        }
        chars.next();
    }
}

fn read_go_tag(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<String> {
    if !matches!(chars.peek(), Some('`')) {
        return None;
    }

    chars.next();
    let mut tag_value = String::new();
    for next in chars.by_ref() {
        if next == '`' {
            break;
        }
        tag_value.push(next);
    }
    Some(tag_value)
}

fn parse_go_json_tag(tag: Option<&str>) -> (Option<String>, bool, bool) {
    let Some(tag) = tag else {
        return (None, false, false);
    };
    let Some(start) = tag.find("json:\"") else {
        return (None, false, false);
    };
    let after = &tag[start + 6..];
    let Some(end) = after.find('"') else {
        return (None, false, false);
    };
    let content = &after[..end];
    if content == "-" {
        return (None, false, true);
    }
    let mut parts = content.split(',');
    let name_part = parts.next().unwrap_or("");
    let omitempty = parts.any(|part| part.trim() == "omitempty");
    let name = if name_part.is_empty() {
        None
    } else {
        Some(name_part.to_string())
    };
    (name, omitempty, false)
}

fn go_field_type(type_token: &str) -> DtoFieldType {
    if type_token.contains('[') || type_token.contains("map[") {
        return DtoFieldType::Unknown;
    }

    match type_token {
        "string" => DtoFieldType::Primitive(PrimitiveKind::String),
        "bool" => DtoFieldType::Primitive(PrimitiveKind::Bool),
        "int" | "int8" | "int16" | "int32" | "int64" | "uint" | "uint8" | "uint16" | "uint32"
        | "uint64" | "uintptr" => DtoFieldType::Primitive(PrimitiveKind::Int),
        "float32" | "float64" => DtoFieldType::Primitive(PrimitiveKind::Float),
        "" => DtoFieldType::Unknown,
        other => DtoFieldType::Object(other.to_string()),
    }
}
