use std::collections::HashMap;

use crate::dto_schema::DtoType;

use super::super::parse_first_quoted_value;

pub(super) fn parse_swift_cases(line: &str) -> Vec<(String, String)> {
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

pub(super) fn apply_coding_key(
    coding_keys: &mut HashMap<String, String>,
    dto_type: &mut DtoType,
    field: String,
    rename: String,
) {
    coding_keys.insert(field.clone(), rename.clone());
    for existing in &mut dto_type.fields {
        if existing.json_key == field {
            existing.json_key = rename.clone();
        }
    }
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
        .and_then(parse_first_quoted_value)
        .unwrap_or_else(|| name.to_string());
    cases.push((name.to_string(), rename));
}
