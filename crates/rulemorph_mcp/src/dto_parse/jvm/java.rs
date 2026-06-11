use std::collections::HashMap;

use crate::dto_normalize::normalize_java_text;
use crate::dto_schema::{DtoField, DtoType};

use super::strip_leading_annotations;

mod declaration;
mod field;

pub(in crate::dto_parse) fn parse_java_types(
    text: &str,
) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
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
            if let Some(name) = declaration::class_name(line) {
                begin_java_type(&mut types, &mut order, &mut current, &name);
            }
            record_param_depth = 0;
            pending_json_key = None;
            pending_optional = false;
            continue;
        }

        if (line.contains(" record ") || line.starts_with("record "))
            && let Some(name) = declaration::record_name(line)
        {
            begin_java_type(&mut types, &mut order, &mut current, &name);
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

fn begin_java_type(
    types: &mut HashMap<String, DtoType>,
    order: &mut Vec<String>,
    current: &mut Option<String>,
    name: &str,
) {
    *current = Some(name.to_string());
    types
        .entry(name.to_string())
        .or_insert_with(|| DtoType { fields: Vec::new() });
    order.push(name.to_string());
}

fn parse_java_field_line(
    line: &str,
    current_name: &str,
    types: &mut HashMap<String, DtoType>,
    pending_json_key: &mut Option<String>,
    pending_optional: &mut bool,
) {
    let Some(field) = field::parse_field_line(line, *pending_optional) else {
        return;
    };
    *pending_optional = false;

    let json_key = pending_json_key
        .take()
        .unwrap_or_else(|| field.name.to_string());
    if let Some(dto_type) = types.get_mut(current_name) {
        dto_type.fields.push(DtoField {
            json_key,
            field_type: field.field_type,
            optional: field.optional,
        });
    }
}
