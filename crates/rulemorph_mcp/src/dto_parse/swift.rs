use std::collections::HashMap;

use crate::dto_normalize::normalize_swift_text;
use crate::dto_schema::DtoType;

mod coding_keys;
mod declaration;
mod field;

use self::coding_keys::{apply_coding_key, parse_swift_cases};
use self::declaration::parse_swift_type_name;
use self::field::parse_swift_field;

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

        if let Some(name) = parse_swift_type_name(line) {
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
                        apply_coding_key(&mut coding_keys, dto_type, field, rename);
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

        if let Some(field) = parse_swift_field(line, &coding_keys) {
            if let Some(dto_type) = types.get_mut(&current_name) {
                dto_type.fields.push(field);
            }
        }
    }

    Ok((types, order))
}
