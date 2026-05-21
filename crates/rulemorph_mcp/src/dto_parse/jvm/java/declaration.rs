pub(super) fn class_name(line: &str) -> Option<String> {
    declaration_name(line, "class ")
}

pub(super) fn record_name(line: &str) -> Option<String> {
    declaration_name(line, "record ")
}

fn declaration_name(line: &str, keyword: &str) -> Option<String> {
    let name_part = if let Some(idx) = line.find(keyword) {
        &line[idx + keyword.len()..]
    } else {
        line
    };
    let name = name_part
        .split(|ch: char| ch.is_whitespace() || ch == '{' || ch == '(')
        .next()
        .unwrap_or("")
        .trim();
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}
