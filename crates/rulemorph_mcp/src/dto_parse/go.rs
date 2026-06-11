use std::collections::HashMap;

use crate::dto_schema::DtoType;

mod field;

use self::field::parse_go_struct_fields;

pub(super) fn parse_go_types(
    text: &str,
) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut index = 0usize;
    let bytes = text.as_bytes();

    while index < bytes.len() {
        let slice = &text[index..];
        let Some(pos) = slice.find("type ") else {
            break;
        };
        index += pos + 5;
        let rest = &text[index..];
        let name = rest.split_whitespace().next().unwrap_or("");
        if name.is_empty() {
            index = index.saturating_add(1);
            continue;
        }
        let name_start = rest.find(name).unwrap_or(0);
        index += name_start + name.len();
        let after_name = &text[index..];
        let Some(struct_pos) = after_name.find("struct") else {
            index = index.saturating_add(1);
            continue;
        };
        index += struct_pos + "struct".len();
        let after_struct = &text[index..];
        let Some(brace_pos) = after_struct.find('{') else {
            index = index.saturating_add(1);
            continue;
        };
        index += brace_pos + 1;

        let mut brace_depth = 1usize;
        let mut body_end = index;
        while body_end < bytes.len() {
            match bytes[body_end] as char {
                '{' => brace_depth += 1,
                '}' => {
                    brace_depth -= 1;
                    if brace_depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            body_end += 1;
        }
        if brace_depth != 0 {
            break;
        }

        let body = &text[index..body_end];
        let dto_type = types
            .entry(name.to_string())
            .or_insert_with(|| DtoType { fields: Vec::new() });
        parse_go_struct_fields(body, dto_type);
        order.push(name.to_string());
        index = body_end + 1;
    }

    Ok((types, order))
}
