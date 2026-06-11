use super::state::{ListKind, ListState};
use super::table::is_table_separator_line;

pub(super) fn active_markdown_line(line: &str) -> Option<&str> {
    if line.trim().is_empty() {
        return Some("");
    }
    let spaces = line
        .as_bytes()
        .iter()
        .take_while(|byte| **byte == b' ')
        .count();
    if spaces >= 4 || line.as_bytes().get(spaces).copied() == Some(b'\t') {
        None
    } else {
        Some(&line[spaces..])
    }
}

pub(super) fn block_content_line(line: &str, quote_depth: usize) -> Option<&str> {
    let active = active_markdown_line(line)?;
    let (line_quote_depth, content) = strip_blockquote_markers(active);
    (line_quote_depth == quote_depth)
        .then(|| active_markdown_line(content))
        .flatten()
}

pub(super) fn is_structural_line(trimmed: &str, paragraph_quote_depth: Option<usize>) -> bool {
    is_atx_heading_line(trimmed)
        || is_thematic_break_line(trimmed)
        || trimmed.starts_with('>')
        || is_list_item_line_for_context(trimmed, paragraph_quote_depth)
        || trimmed.starts_with("```")
        || trimmed.starts_with("~~~")
        || trimmed.starts_with('<')
        || is_table_separator_line(trimmed)
}

pub(super) fn is_atx_heading_line(trimmed: &str) -> bool {
    let bytes = trimmed.as_bytes();
    let marker_count = bytes.iter().take_while(|byte| **byte == b'#').count();
    (1..=6).contains(&marker_count)
        && (bytes.len() == marker_count || matches!(bytes[marker_count], b' ' | b'\t'))
}

pub(super) fn estimate_structural_nodes(
    trimmed: &str,
    paragraph_quote_depth: Option<usize>,
) -> usize {
    if is_thematic_break_line(trimmed) {
        1
    } else if is_list_item_line_for_context(trimmed, paragraph_quote_depth) {
        // A compact list item typically expands to list + item + paragraph + text nodes.
        4
    } else if trimmed.starts_with('>') {
        estimate_blockquote_nodes(trimmed, paragraph_quote_depth)
    } else if is_atx_heading_line(trimmed) {
        estimate_atx_heading_nodes(trimmed)
    } else if is_structural_line(trimmed, paragraph_quote_depth) {
        1
    } else if !trimmed.is_empty() {
        2
    } else {
        0
    }
}

pub(super) fn estimate_atx_heading_nodes(trimmed: &str) -> usize {
    let marker_count = trimmed
        .as_bytes()
        .iter()
        .take_while(|byte| **byte == b'#')
        .count();
    let content = strip_atx_closing_sequence(trimmed[marker_count..].trim_end());
    if content.trim().is_empty() { 1 } else { 2 }
}

pub(super) fn strip_atx_closing_sequence(content: &str) -> &str {
    let closing_count = content
        .as_bytes()
        .iter()
        .rev()
        .take_while(|byte| **byte == b'#')
        .count();
    if closing_count == 0 || closing_count == content.len() {
        return content;
    }
    let closing_start = content.len() - closing_count;
    if content.as_bytes()[closing_start - 1].is_ascii_whitespace() {
        &content[..closing_start]
    } else {
        content
    }
}

pub(super) fn estimate_blockquote_nodes(
    trimmed: &str,
    paragraph_quote_depth: Option<usize>,
) -> usize {
    let (quote_nodes, content) = strip_blockquote_markers(trimmed);
    if quote_nodes == 0 {
        return 0;
    }
    let content_paragraph_depth =
        paragraph_quote_depth.and_then(|depth| depth.checked_sub(quote_nodes));
    let content_nodes = active_markdown_line(content)
        .filter(|content| !content.is_empty())
        .map(|content| estimate_structural_nodes(content, content_paragraph_depth))
        .unwrap_or(0);
    quote_nodes.saturating_add(content_nodes)
}

pub(super) fn strip_blockquote_markers(mut line: &str) -> (usize, &str) {
    let mut quote_nodes = 0usize;
    while let Some(rest) = line.strip_prefix('>') {
        quote_nodes = quote_nodes.saturating_add(1);
        line = strip_optional_space_or_tab(rest);
    }
    (quote_nodes, line)
}

pub(super) fn strip_optional_space_or_tab(line: &str) -> &str {
    line.strip_prefix(' ')
        .or_else(|| line.strip_prefix('\t'))
        .unwrap_or(line)
}

pub(super) fn is_list_item_line_for_context(
    trimmed: &str,
    paragraph_quote_depth: Option<usize>,
) -> bool {
    is_unordered_list_item_line(trimmed)
        || is_ordered_list_item_line_for_context(trimmed, paragraph_quote_depth)
}

pub(super) fn list_state_for_preflight_line(
    trimmed: &str,
    paragraph_quote_depth: Option<usize>,
) -> Option<ListState> {
    let (quote_depth, content) = strip_blockquote_markers(trimmed);
    if quote_depth != 0 {
        return None;
    }
    if is_unordered_list_item_line(content) {
        Some(ListState {
            quote_depth,
            kind: ListKind::Unordered,
        })
    } else if is_ordered_list_item_line_for_context(trimmed, paragraph_quote_depth) {
        Some(ListState {
            quote_depth,
            kind: ListKind::Ordered,
        })
    } else {
        None
    }
}

pub(super) fn is_unordered_list_item_line(trimmed: &str) -> bool {
    marker_followed_by_space_or_tab(trimmed, b'-')
        || marker_followed_by_space_or_tab(trimmed, b'*')
        || marker_followed_by_space_or_tab(trimmed, b'+')
}

pub(super) fn is_nonempty_unordered_list_item_line(trimmed: &str) -> bool {
    unordered_list_marker_tail(trimmed).is_some_and(|tail| !tail.trim().is_empty())
}

pub(super) fn unordered_list_marker_tail(trimmed: &str) -> Option<&str> {
    for marker in [b'-', b'*', b'+'] {
        if let Some(tail) = marker_tail(trimmed, marker)
            && matches!(tail.as_bytes().first().copied(), Some(b' ' | b'\t'))
        {
            return Some(&tail[1..]);
        }
    }
    None
}

pub(super) fn marker_tail(trimmed: &str, marker: u8) -> Option<&str> {
    let bytes = trimmed.as_bytes();
    (bytes.len() >= 2 && bytes[0] == marker).then_some(&trimmed[1..])
}

pub(super) fn marker_followed_by_space_or_tab(trimmed: &str, marker: u8) -> bool {
    let bytes = trimmed.as_bytes();
    bytes.len() >= 2 && bytes[0] == marker && matches!(bytes[1], b' ' | b'\t')
}

pub(super) fn is_ordered_list_item_line_for_context(
    trimmed: &str,
    paragraph_quote_depth: Option<usize>,
) -> bool {
    let Some(start) = ordered_list_marker_start(trimmed) else {
        return false;
    };
    let (quote_depth, _) = strip_blockquote_markers(trimmed);
    if paragraph_quote_depth.is_some_and(|depth| depth >= quote_depth) {
        start == 1
    } else {
        true
    }
}

pub(super) fn ordered_list_marker_start(trimmed: &str) -> Option<u64> {
    ordered_list_marker_tail(trimmed).map(|(start, _)| start)
}

pub(super) fn ordered_list_marker_tail(trimmed: &str) -> Option<(u64, &str)> {
    let (_, content) = strip_blockquote_markers(trimmed);
    let content = active_markdown_line(content)?;
    let bytes = content.as_bytes();
    let digit_count = bytes
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if digit_count == 0 || digit_count > 9 || digit_count + 1 >= bytes.len() {
        return None;
    }
    if matches!(bytes[digit_count], b'.' | b')') && bytes[digit_count + 1].is_ascii_whitespace() {
        let start = std::str::from_utf8(&bytes[..digit_count])
            .ok()?
            .parse()
            .ok()?;
        Some((start, &content[digit_count + 2..]))
    } else {
        None
    }
}

pub(super) fn is_thematic_break_line(trimmed: &str) -> bool {
    let mut marker = None;
    let mut marker_count = 0usize;
    for byte in trimmed.bytes() {
        if byte.is_ascii_whitespace() {
            continue;
        }
        match marker {
            None if matches!(byte, b'-' | b'*' | b'_') => {
                marker = Some(byte);
                marker_count += 1;
            }
            Some(active) if byte == active => marker_count += 1,
            _ => return false,
        }
    }
    marker_count >= 3
}

pub(super) fn is_setext_underline_for_paragraph(
    trimmed: &str,
    paragraph_quote_depth: Option<usize>,
) -> bool {
    let Some(paragraph_quote_depth) = paragraph_quote_depth else {
        return false;
    };
    let (quote_depth, content) = strip_blockquote_markers(trimmed);
    if quote_depth != paragraph_quote_depth {
        return false;
    }
    let Some(content) = active_markdown_line(content) else {
        return false;
    };
    is_setext_underline_line(content)
}

pub(super) fn is_setext_underline_line(line: &str) -> bool {
    let line = line.trim();
    if line.is_empty() {
        return false;
    }
    let Some(marker) = line.as_bytes().first().copied() else {
        return false;
    };
    matches!(marker, b'=' | b'-') && line.as_bytes().iter().all(|byte| *byte == marker)
}

pub(super) fn is_paragraph_content_line(
    trimmed: &str,
    paragraph_quote_depth: Option<usize>,
) -> bool {
    !trimmed.is_empty()
        && !trimmed.starts_with('>')
        && !is_atx_heading_line(trimmed)
        && !is_thematic_break_line(trimmed)
        && !is_list_item_line_for_context(trimmed, paragraph_quote_depth)
        && !trimmed.starts_with("```")
        && !trimmed.starts_with("~~~")
        && !is_table_separator_line(trimmed)
}

pub(super) fn paragraph_quote_depth_for_line(
    trimmed: &str,
    paragraph_quote_depth: Option<usize>,
) -> Option<usize> {
    let (quote_depth, content) = strip_blockquote_markers(trimmed);
    let content = active_markdown_line(content)?;
    let content_paragraph_depth =
        paragraph_quote_depth.and_then(|depth| depth.checked_sub(quote_depth));
    is_paragraph_content_line(content, content_paragraph_depth).then_some(quote_depth)
}
