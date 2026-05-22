mod java;
mod kotlin;

pub(super) use java::parse_java_types;
pub(super) use kotlin::parse_kotlin_types;

use super::parse_first_quoted_value;

fn parse_quoted_value_after(line: &str, marker: &str) -> Option<String> {
    let start = line.find(marker)?;
    let after = &line[start + marker.len()..];
    parse_first_quoted_value(after)
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
