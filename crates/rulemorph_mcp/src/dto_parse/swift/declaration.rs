pub(super) fn parse_swift_type_name(line: &str) -> Option<&str> {
    if !(line.contains(" struct ")
        || line.starts_with("struct ")
        || line.contains(" class ")
        || line.starts_with("class "))
    {
        return None;
    }

    let keyword_pos = if let Some(pos) = line.find("struct ") {
        pos + 7
    } else if let Some(pos) = line.find("class ") {
        pos + 6
    } else {
        0
    };
    let name_part = line[keyword_pos..].split_whitespace().next().unwrap_or("");
    let name = name_part.split([':', '{']).next().unwrap_or("").trim();
    if name.is_empty() { None } else { Some(name) }
}
