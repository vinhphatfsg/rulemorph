use std::collections::HashSet;

use comrak::nodes::{Node, NodeValue};

use crate::error::{TransformError, TransformErrorKind};

use super::super::NormalizationOptions;

pub(super) fn enforce_markdown_structural_preflight(
    input: &str,
    estimate_gfm_extensions: bool,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    let mut estimated_nodes = 1usize;
    let mut estimated_table_cells = 0usize;
    let mut active_fence: Option<ActiveFence> = None;
    let mut active_html_block: Option<ActiveHtmlBlock> = None;
    let mut pending_table_header_cells: Option<TableHeaderCandidate> = None;
    let mut active_table: Option<TableState> = None;
    let mut in_indented_code = false;
    let mut paragraph_quote_depth: Option<usize> = None;
    let mut reference_definition_quote_depth: Option<usize> = None;
    let reference_labels = collect_link_reference_labels(input);
    for line in input.lines() {
        if let Some(active) = active_fence {
            if let Some(fence_line) = fence_line_content(line, active.quote_depth)
                && is_closing_fence(fence_line, active.fence)
            {
                active_fence = None;
            }
            enforce_markdown_node_count(estimated_nodes, options)?;
            enforce_markdown_table_cell_count(estimated_table_cells, options)?;
            continue;
        }

        if let Some(active) = active_html_block {
            if let Some(html_line) = html_block_content_line(line, active.quote_depth) {
                if html_block_ends_on_line(html_line, active.end) {
                    active_html_block = None;
                }
                pending_table_header_cells = None;
                active_table = None;
                enforce_markdown_node_count(estimated_nodes, options)?;
                enforce_markdown_table_cell_count(estimated_table_cells, options)?;
                continue;
            }
            active_html_block = None;
        }

        let Some(trimmed) = active_markdown_line(line) else {
            if !in_indented_code {
                estimated_nodes = estimated_nodes.saturating_add(1);
                in_indented_code = true;
            }
            pending_table_header_cells = None;
            active_table = None;
            paragraph_quote_depth = None;
            reference_definition_quote_depth = None;
            enforce_markdown_node_count(estimated_nodes, options)?;
            enforce_markdown_table_cell_count(estimated_table_cells, options)?;
            continue;
        };
        in_indented_code = false;

        if let Some(active) = opening_fence_line(trimmed) {
            estimated_nodes = estimated_nodes.saturating_add(1);
            active_fence = Some(active);
            pending_table_header_cells = None;
            active_table = None;
            paragraph_quote_depth = None;
            reference_definition_quote_depth = None;
            enforce_markdown_node_count(estimated_nodes, options)?;
            enforce_markdown_table_cell_count(estimated_table_cells, options)?;
            continue;
        }
        if let Some(active) = opening_html_block_line(trimmed)
            && can_start_html_block(active, paragraph_quote_depth)
        {
            estimated_nodes =
                estimated_nodes.saturating_add(estimate_structural_nodes(trimmed, None));
            if let Some(html_line) = html_block_content_line(trimmed, active.quote_depth)
                && !html_block_ends_on_line(html_line, active.end)
            {
                active_html_block = Some(active);
            }
            pending_table_header_cells = None;
            active_table = None;
            paragraph_quote_depth = None;
            reference_definition_quote_depth = None;
            enforce_markdown_node_count(estimated_nodes, options)?;
            enforce_markdown_table_cell_count(estimated_table_cells, options)?;
            continue;
        }
        if reference_definition_quote_depth.is_none()
            && is_setext_underline_for_paragraph(trimmed, paragraph_quote_depth)
        {
            pending_table_header_cells = None;
            active_table = None;
            paragraph_quote_depth = None;
            reference_definition_quote_depth = None;
            enforce_markdown_node_count(estimated_nodes, options)?;
            enforce_markdown_table_cell_count(estimated_table_cells, options)?;
            continue;
        }
        let reference_definition_quote = effective_link_reference_definition(
            trimmed,
            paragraph_quote_depth,
            reference_definition_quote_depth,
        )
        .map(|(quote_depth, _)| quote_depth);
        estimated_nodes = estimated_nodes
            .saturating_add(estimate_structural_nodes(trimmed, paragraph_quote_depth));
        estimated_nodes = estimated_nodes.saturating_add(estimate_inline_nodes(
            trimmed,
            &reference_labels,
            reference_definition_quote.is_some(),
            estimate_gfm_extensions,
        ));
        paragraph_quote_depth = reference_definition_quote
            .or_else(|| paragraph_quote_depth_for_line(trimmed, paragraph_quote_depth));
        reference_definition_quote_depth = reference_definition_quote;
        if estimate_gfm_extensions {
            if let Some((quote_depth, table_line)) = table_preflight_line(trimmed) {
                let pipe_cells = pipe_table_cell_count(table_line);
                if let Some(table) = active_table {
                    match pipe_cells {
                        Some(cells) if quote_depth == table.quote_depth => {
                            estimated_nodes = estimated_nodes
                                .saturating_add(estimate_table_row_nodes(table.columns));
                            estimated_table_cells =
                                estimated_table_cells.saturating_add(table.columns);
                            pending_table_header_cells = None;
                        }
                        Some(cells) => {
                            active_table = None;
                            pending_table_header_cells =
                                Some(TableHeaderCandidate { cells, quote_depth });
                        }
                        None => {
                            active_table = None;
                            pending_table_header_cells = None;
                        }
                    }
                } else if let Some(separator_cells) = table_separator_cell_count(table_line) {
                    if let Some(header) = pending_table_header_cells.take()
                        && header.cells == separator_cells
                        && header.quote_depth == quote_depth
                    {
                        estimated_nodes = estimated_nodes
                            .saturating_add(1)
                            .saturating_add(estimate_table_row_nodes(header.cells));
                        estimated_table_cells = estimated_table_cells.saturating_add(header.cells);
                        active_table = Some(TableState {
                            columns: header.cells,
                            quote_depth,
                        });
                    } else {
                        active_table = None;
                    }
                } else if let Some(cells) = pipe_cells {
                    pending_table_header_cells = Some(TableHeaderCandidate { cells, quote_depth });
                } else {
                    pending_table_header_cells = None;
                    active_table = None;
                }
            } else {
                pending_table_header_cells = None;
                active_table = None;
            }
        } else {
            pending_table_header_cells = None;
            active_table = None;
        }
        enforce_markdown_node_count(estimated_nodes, options)?;
        enforce_markdown_table_cell_count(estimated_table_cells, options)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct Fence {
    marker: u8,
    len: usize,
}

#[derive(Clone, Copy)]
struct ActiveFence {
    fence: Fence,
    quote_depth: usize,
}

#[derive(Clone, Copy)]
struct ActiveHtmlBlock {
    end: HtmlBlockEnd,
    quote_depth: usize,
    can_interrupt_paragraph: bool,
}

#[derive(Clone, Copy)]
enum HtmlBlockEnd {
    ClosingTag(&'static str),
    Contains(&'static str),
    BlankLine,
}

#[derive(Clone, Copy)]
struct TableHeaderCandidate {
    cells: usize,
    quote_depth: usize,
}

#[derive(Clone, Copy)]
struct TableState {
    columns: usize,
    quote_depth: usize,
}

fn opening_fence(trimmed: &str) -> Option<Fence> {
    let marker = match trimmed.as_bytes().first()? {
        b'`' => b'`',
        b'~' => b'~',
        _ => return None,
    };
    let len = trimmed
        .as_bytes()
        .iter()
        .take_while(|byte| **byte == marker)
        .count();
    if len < 3 {
        return None;
    }
    if marker == b'`' && trimmed[len..].contains('`') {
        return None;
    }
    Some(Fence { marker, len })
}

fn opening_fence_line(trimmed: &str) -> Option<ActiveFence> {
    let (quote_depth, content) = strip_blockquote_markers(trimmed);
    let content = active_markdown_line(content)?;
    opening_fence(content).map(|fence| ActiveFence { fence, quote_depth })
}

fn active_markdown_line(line: &str) -> Option<&str> {
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

fn fence_line_content(line: &str, quote_depth: usize) -> Option<&str> {
    block_content_line(line, quote_depth)
}

fn block_content_line(line: &str, quote_depth: usize) -> Option<&str> {
    let active = active_markdown_line(line)?;
    let (line_quote_depth, content) = strip_blockquote_markers(active);
    (line_quote_depth == quote_depth)
        .then(|| active_markdown_line(content))
        .flatten()
}

fn html_block_content_line(line: &str, quote_depth: usize) -> Option<&str> {
    if quote_depth == 0 {
        return Some(line);
    }
    let active = active_markdown_line(line)?;
    let (line_quote_depth, content) = strip_blockquote_markers(active);
    (line_quote_depth == quote_depth).then_some(content)
}

fn is_closing_fence(trimmed: &str, fence: Fence) -> bool {
    let Some(candidate) = opening_fence(trimmed) else {
        return false;
    };
    candidate.marker == fence.marker
        && candidate.len >= fence.len
        && trimmed[candidate.len..].trim().is_empty()
}

pub(super) fn count_parsed_markdown_nodes(
    root: Node<'_>,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    let mut node_count = 0usize;
    let mut table_cell_count = 0usize;
    for node in root.descendants() {
        node_count = node_count.saturating_add(1);
        enforce_markdown_node_count(node_count, options)?;
        if matches!(&node.data.borrow().value, NodeValue::TableCell) {
            table_cell_count = table_cell_count.saturating_add(1);
            enforce_markdown_table_cell_count(table_cell_count, options)?;
        }
    }
    Ok(())
}

fn is_structural_line(trimmed: &str, paragraph_quote_depth: Option<usize>) -> bool {
    is_atx_heading_line(trimmed)
        || is_thematic_break_line(trimmed)
        || trimmed.starts_with('>')
        || is_list_item_line_for_context(trimmed, paragraph_quote_depth)
        || trimmed.starts_with("```")
        || trimmed.starts_with("~~~")
        || trimmed.starts_with('<')
        || is_table_separator_line(trimmed)
}

fn is_atx_heading_line(trimmed: &str) -> bool {
    let bytes = trimmed.as_bytes();
    let marker_count = bytes.iter().take_while(|byte| **byte == b'#').count();
    (1..=6).contains(&marker_count)
        && (bytes.len() == marker_count || matches!(bytes[marker_count], b' ' | b'\t'))
}

fn estimate_structural_nodes(trimmed: &str, paragraph_quote_depth: Option<usize>) -> usize {
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

fn estimate_atx_heading_nodes(trimmed: &str) -> usize {
    let marker_count = trimmed
        .as_bytes()
        .iter()
        .take_while(|byte| **byte == b'#')
        .count();
    let content = strip_atx_closing_sequence(trimmed[marker_count..].trim_end());
    if content.trim().is_empty() { 1 } else { 2 }
}

fn strip_atx_closing_sequence(content: &str) -> &str {
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

fn estimate_blockquote_nodes(trimmed: &str, paragraph_quote_depth: Option<usize>) -> usize {
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

fn strip_blockquote_markers(mut line: &str) -> (usize, &str) {
    let mut quote_nodes = 0usize;
    while let Some(rest) = line.strip_prefix('>') {
        quote_nodes = quote_nodes.saturating_add(1);
        line = strip_optional_space_or_tab(rest);
    }
    (quote_nodes, line)
}

fn strip_optional_space_or_tab(line: &str) -> &str {
    line.strip_prefix(' ')
        .or_else(|| line.strip_prefix('\t'))
        .unwrap_or(line)
}

fn is_list_item_line_for_context(trimmed: &str, paragraph_quote_depth: Option<usize>) -> bool {
    is_unordered_list_item_line(trimmed)
        || is_ordered_list_item_line_for_context(trimmed, paragraph_quote_depth)
}

fn is_unordered_list_item_line(trimmed: &str) -> bool {
    marker_followed_by_space_or_tab(trimmed, b'-')
        || marker_followed_by_space_or_tab(trimmed, b'*')
        || marker_followed_by_space_or_tab(trimmed, b'+')
}

fn marker_followed_by_space_or_tab(trimmed: &str, marker: u8) -> bool {
    let bytes = trimmed.as_bytes();
    bytes.len() >= 2 && bytes[0] == marker && matches!(bytes[1], b' ' | b'\t')
}

fn is_ordered_list_item_line_for_context(
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

fn ordered_list_marker_start(trimmed: &str) -> Option<u64> {
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
        std::str::from_utf8(&bytes[..digit_count])
            .ok()?
            .parse()
            .ok()
    } else {
        None
    }
}

fn is_thematic_break_line(trimmed: &str) -> bool {
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

fn is_setext_underline_for_paragraph(trimmed: &str, paragraph_quote_depth: Option<usize>) -> bool {
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

fn is_setext_underline_line(line: &str) -> bool {
    let line = line.trim();
    if line.is_empty() {
        return false;
    }
    let Some(marker) = line.as_bytes().first().copied() else {
        return false;
    };
    matches!(marker, b'=' | b'-') && line.as_bytes().iter().all(|byte| *byte == marker)
}

fn estimate_inline_nodes(
    line: &str,
    reference_labels: &HashSet<String>,
    line_is_reference_definition: bool,
    estimate_gfm_extensions: bool,
) -> usize {
    if line_is_reference_definition {
        return 0;
    }
    let gfm_nodes = if estimate_gfm_extensions {
        estimate_gfm_autolink_nodes(line).saturating_add(estimate_strikethrough_nodes(line))
    } else {
        0
    };
    line.matches("](")
        .count()
        .saturating_mul(2)
        .saturating_add(estimate_reference_link_nodes(
            line,
            reference_labels,
            line_is_reference_definition,
        ))
        .saturating_add(line.matches("![").count())
        .saturating_add(line.matches("**").count() / 2)
        .saturating_add(line.matches("__").count() / 2)
        .saturating_add(estimate_single_marker_emphasis_nodes(line))
        .saturating_add(line.matches('`').count() / 2)
        .saturating_add(estimate_inline_html_nodes(line))
        .saturating_add(gfm_nodes)
}

fn estimate_gfm_autolink_nodes(line: &str) -> usize {
    let bytes = line.as_bytes();
    let mut index = 0usize;
    let mut estimate = 0usize;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index = (index + 2).min(bytes.len()),
            b'`' => {
                let len = backtick_run_len(bytes, index);
                if let Some(closing_index) = matching_backtick_run(bytes, index + len, len) {
                    index = closing_index + len;
                } else {
                    index += len;
                }
            }
            b']' if bytes.get(index + 1).copied() == Some(b'(') => {
                index = explicit_link_destination_end(bytes, index + 2).unwrap_or(index + 1);
            }
            _ if is_gfm_url_autolink_start(bytes, index) => {
                estimate = estimate.saturating_add(2);
                index = autolink_token_end(bytes, index);
            }
            _ if is_autolink_token_boundary(bytes, index) => {
                let end = autolink_token_end(bytes, index);
                if looks_like_email_autolink(&line[index..end]) {
                    estimate = estimate.saturating_add(2);
                    index = end;
                } else {
                    index += 1;
                }
            }
            _ => index += 1,
        }
    }
    estimate
}

fn explicit_link_destination_end(bytes: &[u8], mut index: usize) -> Option<usize> {
    let mut depth = 1usize;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index = (index + 2).min(bytes.len()),
            b'(' => {
                depth = depth.saturating_add(1);
                index += 1;
            }
            b')' => {
                depth = depth.checked_sub(1)?;
                index += 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => index += 1,
        }
    }
    None
}

fn is_gfm_url_autolink_start(bytes: &[u8], index: usize) -> bool {
    is_autolink_token_boundary(bytes, index)
        && (starts_with_ascii(bytes, index, b"http://")
            || starts_with_ascii(bytes, index, b"https://")
            || starts_with_ascii(bytes, index, b"www."))
}

fn starts_with_ascii(bytes: &[u8], index: usize, prefix: &[u8]) -> bool {
    bytes
        .get(index..index.saturating_add(prefix.len()))
        .is_some_and(|value| value.eq_ignore_ascii_case(prefix))
}

fn is_autolink_token_boundary(bytes: &[u8], index: usize) -> bool {
    index == 0 || bytes[index - 1].is_ascii_whitespace() || matches!(bytes[index - 1], b'(' | b'[')
}

fn autolink_token_end(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() {
        if bytes[index].is_ascii_whitespace() || matches!(bytes[index], b'<' | b'>') {
            break;
        }
        index += 1;
    }
    index
}

fn looks_like_email_autolink(token: &str) -> bool {
    let token = token.trim_matches(|ch: char| matches!(ch, '.' | ',' | ';' | ':' | '!' | '?'));
    let Some(at) = token.find('@') else {
        return false;
    };
    at > 0
        && token[at + 1..].contains('.')
        && token[..at]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'+' | b'-'))
        && token[at + 1..]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
}

fn estimate_single_marker_emphasis_nodes(line: &str) -> usize {
    estimate_delimited_inline_nodes(line, b'*', 1)
        .saturating_add(estimate_delimited_inline_nodes(line, b'_', 1))
}

fn estimate_strikethrough_nodes(line: &str) -> usize {
    estimate_delimited_inline_nodes(line, b'~', 2)
}

fn estimate_delimited_inline_nodes(line: &str, marker: u8, delimiter_len: usize) -> usize {
    let bytes = line.as_bytes();
    let mut index = 0usize;
    let mut delimiters = 0usize;
    let mut single_marker_open = false;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index = (index + 2).min(bytes.len()),
            b'`' => {
                let len = backtick_run_len(bytes, index);
                if let Some(closing_index) = matching_backtick_run(bytes, index + len, len) {
                    index = closing_index + len;
                } else {
                    index += len;
                }
            }
            byte if byte == marker => {
                let run_len = bytes[index..]
                    .iter()
                    .take_while(|byte| **byte == marker)
                    .count();
                if delimiter_len == 1 {
                    if run_len == 1 {
                        let (can_open, can_close) =
                            single_emphasis_marker_sides(bytes, index, marker);
                        if single_marker_open && can_close {
                            delimiters = delimiters.saturating_add(2);
                            single_marker_open = false;
                        } else if can_open {
                            single_marker_open = true;
                        }
                    }
                } else {
                    delimiters = delimiters.saturating_add(run_len / delimiter_len);
                }
                index += run_len;
            }
            _ => index += 1,
        }
    }
    delimiters / 2
}

fn single_emphasis_marker_sides(bytes: &[u8], index: usize, marker: u8) -> (bool, bool) {
    let prev_space = index > 0 && bytes[index - 1].is_ascii_whitespace();
    let next_space = bytes
        .get(index + 1)
        .is_some_and(|byte| byte.is_ascii_whitespace());
    let mut can_open = !next_space && index + 1 < bytes.len();
    let mut can_close = !prev_space && index > 0;
    if marker == b'_' {
        let prev_alnum = index > 0 && bytes[index - 1].is_ascii_alphanumeric();
        let next_alnum = bytes
            .get(index + 1)
            .is_some_and(|byte| byte.is_ascii_alphanumeric());
        if prev_alnum && next_alnum {
            can_open = false;
            can_close = false;
        }
    }
    (can_open, can_close)
}

fn estimate_reference_link_nodes(
    line: &str,
    reference_labels: &HashSet<String>,
    line_is_reference_definition: bool,
) -> usize {
    if reference_labels.is_empty() || line_is_reference_definition {
        return 0;
    }

    let bytes = line.as_bytes();
    let mut index = 0usize;
    let mut estimate = 0usize;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index = (index + 2).min(bytes.len()),
            b'`' => {
                let len = backtick_run_len(bytes, index);
                if let Some(closing_index) = matching_backtick_run(bytes, index + len, len) {
                    index = closing_index + len;
                } else {
                    index += len;
                }
            }
            b'[' => {
                if index > 0 && bytes[index - 1] == b'!' {
                    index += 1;
                    continue;
                }
                let Some(close_index) = matching_closing_bracket(bytes, index + 1) else {
                    index += 1;
                    continue;
                };
                let link_label = reference_label_from_bytes(bytes, index + 1, close_index);
                match bytes.get(close_index + 1).copied() {
                    Some(b'(') => index = close_index + 1,
                    Some(b'[') => {
                        let label_start = close_index + 2;
                        let Some(label_close) = matching_closing_bracket(bytes, label_start) else {
                            index = label_start;
                            continue;
                        };
                        let label = reference_label_from_bytes(bytes, label_start, label_close)
                            .or_else(|| link_label.clone());
                        if label.is_some_and(|label| reference_labels.contains(&label)) {
                            estimate = estimate.saturating_add(2);
                        }
                        index = label_close + 1;
                    }
                    _ => {
                        if link_label
                            .as_ref()
                            .is_some_and(|label| reference_labels.contains(label))
                        {
                            estimate = estimate.saturating_add(2);
                        }
                        index = close_index + 1;
                    }
                }
            }
            _ => index += 1,
        }
    }
    estimate
}

fn matching_closing_bracket(bytes: &[u8], mut index: usize) -> Option<usize> {
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index = (index + 2).min(bytes.len()),
            b']' => return Some(index),
            _ => index += 1,
        }
    }
    None
}

fn collect_link_reference_labels(input: &str) -> HashSet<String> {
    let mut active_fence: Option<ActiveFence> = None;
    let mut active_html_block: Option<ActiveHtmlBlock> = None;
    let mut paragraph_quote_depth: Option<usize> = None;
    let mut reference_definition_quote_depth: Option<usize> = None;
    let mut labels = HashSet::new();
    for line in input.lines() {
        if let Some(active) = active_fence {
            if let Some(fence_line) = fence_line_content(line, active.quote_depth)
                && is_closing_fence(fence_line, active.fence)
            {
                active_fence = None;
            }
            continue;
        }

        if let Some(active) = active_html_block {
            if let Some(html_line) = html_block_content_line(line, active.quote_depth) {
                if html_block_ends_on_line(html_line, active.end) {
                    active_html_block = None;
                }
                continue;
            }
            active_html_block = None;
        }

        let Some(trimmed) = active_markdown_line(line) else {
            paragraph_quote_depth = None;
            reference_definition_quote_depth = None;
            continue;
        };
        if let Some(active) = opening_fence_line(trimmed) {
            active_fence = Some(active);
            paragraph_quote_depth = None;
            reference_definition_quote_depth = None;
            continue;
        }
        if let Some(active) = opening_html_block_line(trimmed)
            && can_start_html_block(active, paragraph_quote_depth)
        {
            if let Some(html_line) = html_block_content_line(trimmed, active.quote_depth)
                && !html_block_ends_on_line(html_line, active.end)
            {
                active_html_block = Some(active);
            }
            paragraph_quote_depth = None;
            reference_definition_quote_depth = None;
            continue;
        }
        if reference_definition_quote_depth.is_none()
            && is_setext_underline_for_paragraph(trimmed, paragraph_quote_depth)
        {
            paragraph_quote_depth = None;
            reference_definition_quote_depth = None;
            continue;
        }

        if let Some((quote_depth, label)) = effective_link_reference_definition(
            trimmed,
            paragraph_quote_depth,
            reference_definition_quote_depth,
        ) {
            labels.insert(label);
            reference_definition_quote_depth = Some(quote_depth);
        } else {
            reference_definition_quote_depth = None;
        }
        paragraph_quote_depth = reference_definition_quote_depth
            .or_else(|| paragraph_quote_depth_for_line(trimmed, paragraph_quote_depth));
    }
    labels
}

fn effective_link_reference_definition(
    line: &str,
    paragraph_quote_depth: Option<usize>,
    reference_definition_quote_depth: Option<usize>,
) -> Option<(usize, String)> {
    let (quote_depth, label) = link_reference_definition_label_with_quote_depth(line)?;
    if let Some(paragraph_quote_depth) = paragraph_quote_depth
        && paragraph_quote_depth > quote_depth
    {
        return (reference_definition_quote_depth == Some(paragraph_quote_depth))
            .then_some((paragraph_quote_depth, label));
    }
    (paragraph_quote_depth != Some(quote_depth)
        || reference_definition_quote_depth == Some(quote_depth))
    .then_some((quote_depth, label))
}

fn link_reference_definition_label_with_quote_depth(line: &str) -> Option<(usize, String)> {
    let (quote_depth, content) = strip_blockquote_markers(line);
    active_markdown_line(content)
        .and_then(link_reference_definition_label)
        .map(|label| (quote_depth, label))
}

fn link_reference_definition_label(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    let bytes = trimmed.as_bytes();
    if bytes.first().copied() != Some(b'[') {
        return None;
    }
    let close_index = matching_closing_bracket(bytes, 1)?;
    if bytes.get(close_index + 1).copied() != Some(b':') {
        return None;
    }
    if !has_link_reference_destination(&trimmed[close_index + 2..]) {
        return None;
    }
    reference_label_from_bytes(bytes, 1, close_index)
}

fn has_link_reference_destination(rest: &str) -> bool {
    let rest = rest.trim_start();
    if rest.is_empty() {
        return false;
    }
    let rest = if let Some(destination) = rest.strip_prefix('<') {
        let Some(tail) = bracketed_link_reference_destination_tail(destination) else {
            return false;
        };
        tail
    } else {
        let Some(tail) = unbracketed_link_reference_destination_tail(rest) else {
            return false;
        };
        tail
    }
    .trim_start();

    rest.is_empty() || has_link_reference_title(rest)
}

fn bracketed_link_reference_destination_tail(destination: &str) -> Option<&str> {
    let mut escaped = false;
    for (index, value) in destination.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if value == '\\' {
            escaped = true;
            continue;
        }
        match value {
            '<' => return None,
            '>' => return Some(&destination[index + value.len_utf8()..]),
            _ => {}
        }
    }
    None
}

fn unbracketed_link_reference_destination_tail(rest: &str) -> Option<&str> {
    let mut escaped = false;
    let mut paren_depth = 0usize;
    let mut end = rest.len();
    for (index, value) in rest.char_indices() {
        if escaped {
            escaped = false;
            if value.is_whitespace() {
                end = index;
                break;
            }
            continue;
        }
        if value == '\\' {
            escaped = true;
            continue;
        }
        if value.is_whitespace() {
            end = index;
            break;
        }
        match value {
            '(' => paren_depth = paren_depth.saturating_add(1),
            ')' => paren_depth = paren_depth.checked_sub(1)?,
            '<' => return None,
            _ => {}
        }
    }
    if end == 0 || paren_depth != 0 {
        return None;
    }
    Some(&rest[end..])
}

fn has_link_reference_title(rest: &str) -> bool {
    let mut chars = rest.chars();
    let Some(opener) = chars.next() else {
        return true;
    };
    let closer = match opener {
        '"' => '"',
        '\'' => '\'',
        '(' => ')',
        _ => return false,
    };
    let mut escaped = false;
    for (index, value) in rest.char_indices().skip(1) {
        if escaped {
            escaped = false;
            continue;
        }
        if value == '\\' {
            escaped = true;
            continue;
        }
        if value == closer {
            return rest[index + value.len_utf8()..].trim().is_empty();
        }
    }
    false
}

fn reference_label_from_bytes(bytes: &[u8], start: usize, end: usize) -> Option<String> {
    std::str::from_utf8(bytes.get(start..end)?)
        .ok()
        .and_then(normalize_reference_label)
}

fn normalize_reference_label(label: &str) -> Option<String> {
    let label = label.split_whitespace().collect::<Vec<_>>().join(" ");
    (!label.is_empty()).then(|| label.to_lowercase())
}

fn estimate_inline_html_nodes(line: &str) -> usize {
    line.as_bytes()
        .windows(2)
        .filter(|window| window[0] == b'<' && window[1].is_ascii_alphabetic())
        .count()
}

fn is_paragraph_content_line(trimmed: &str, paragraph_quote_depth: Option<usize>) -> bool {
    !trimmed.is_empty()
        && !trimmed.starts_with('>')
        && !is_atx_heading_line(trimmed)
        && !is_thematic_break_line(trimmed)
        && !is_list_item_line_for_context(trimmed, paragraph_quote_depth)
        && !trimmed.starts_with("```")
        && !trimmed.starts_with("~~~")
        && !is_table_separator_line(trimmed)
}

fn paragraph_quote_depth_for_line(
    trimmed: &str,
    paragraph_quote_depth: Option<usize>,
) -> Option<usize> {
    let (quote_depth, content) = strip_blockquote_markers(trimmed);
    let content = active_markdown_line(content)?;
    let content_paragraph_depth =
        paragraph_quote_depth.and_then(|depth| depth.checked_sub(quote_depth));
    is_paragraph_content_line(content, content_paragraph_depth).then_some(quote_depth)
}

fn opening_html_block_line(trimmed: &str) -> Option<ActiveHtmlBlock> {
    let (quote_depth, content) = strip_blockquote_markers(trimmed);
    let content = active_markdown_line(content)?;
    opening_html_block_end(content).map(|(end, can_interrupt_paragraph)| ActiveHtmlBlock {
        end,
        quote_depth,
        can_interrupt_paragraph,
    })
}

fn opening_html_block_end(line: &str) -> Option<(HtmlBlockEnd, bool)> {
    let trimmed = line.trim_start();
    if trimmed.starts_with("<!--") {
        return Some((HtmlBlockEnd::Contains("-->"), true));
    }
    if trimmed.starts_with("<?") {
        return Some((HtmlBlockEnd::Contains("?>"), true));
    }
    if starts_with_ignore_ascii_case(trimmed, "<![CDATA[") {
        return Some((HtmlBlockEnd::Contains("]]>"), true));
    }
    if starts_with_html_declaration(trimmed) {
        return Some((HtmlBlockEnd::Contains(">"), true));
    }
    if let Some(tag) = ["script", "pre", "style", "textarea"]
        .into_iter()
        .find(|tag| starts_with_html_open_tag(trimmed, tag))
    {
        return Some((HtmlBlockEnd::ClosingTag(tag), true));
    }
    if let Some(_) = html_block_tag(trimmed) {
        return Some((HtmlBlockEnd::BlankLine, true));
    }
    if starts_with_complete_html_tag_line(trimmed) {
        return Some((HtmlBlockEnd::BlankLine, false));
    }
    None
}

fn can_start_html_block(active: ActiveHtmlBlock, paragraph_quote_depth: Option<usize>) -> bool {
    active.can_interrupt_paragraph || paragraph_quote_depth != Some(active.quote_depth)
}

fn starts_with_html_open_tag(line: &str, tag: &str) -> bool {
    starts_with_html_tag(line, tag, false)
}

fn html_block_tag(line: &str) -> Option<&'static str> {
    HTML_BLOCK_TAGS
        .into_iter()
        .find(|tag| starts_with_html_tag(line, tag, true))
        .copied()
}

fn starts_with_html_tag(line: &str, tag: &str, allow_closing: bool) -> bool {
    let trimmed = line.trim_start();
    let bytes = trimmed.as_bytes();
    let tag_bytes = tag.as_bytes();
    if bytes.first().copied() != Some(b'<') {
        return false;
    }
    let tag_start = if allow_closing && bytes.get(1).copied() == Some(b'/') {
        2
    } else {
        1
    };
    if bytes.len() < tag_start + tag_bytes.len() {
        return false;
    }
    bytes[tag_start..tag_start + tag_bytes.len()].eq_ignore_ascii_case(tag_bytes)
        && matches!(
            bytes.get(tag_start + tag_bytes.len()).copied(),
            None | Some(b' ' | b'\t' | b'>' | b'/')
        )
}

fn starts_with_html_declaration(trimmed: &str) -> bool {
    let bytes = trimmed.as_bytes();
    bytes.len() >= 3 && bytes[0] == b'<' && bytes[1] == b'!' && bytes[2].is_ascii_alphabetic()
}

fn starts_with_complete_html_tag_line(line: &str) -> bool {
    let bytes = line.trim_start().as_bytes();
    if bytes.first().copied() != Some(b'<') {
        return false;
    }
    let mut index = 1usize;
    if bytes.get(index).copied() == Some(b'/') {
        index += 1;
    }
    let Some(first) = bytes.get(index).copied() else {
        return false;
    };
    if !first.is_ascii_alphabetic() {
        return false;
    }
    index += 1;
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        index += 1;
    }
    if !matches!(
        bytes.get(index).copied(),
        None | Some(b' ' | b'\t' | b'>' | b'/')
    ) {
        return false;
    }
    let mut quote = None;
    while index < bytes.len() {
        match (quote, bytes[index]) {
            (Some(active), byte) if byte == active => quote = None,
            (None, b'"' | b'\'') => quote = Some(bytes[index]),
            (None, b'>') => {
                return bytes[index + 1..].iter().all(u8::is_ascii_whitespace);
            }
            _ => {}
        }
        index += 1;
    }
    false
}

fn starts_with_ignore_ascii_case(value: &str, prefix: &str) -> bool {
    value.len() >= prefix.len()
        && value.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
}

fn html_block_ends_on_line(line: &str, end: HtmlBlockEnd) -> bool {
    match end {
        HtmlBlockEnd::ClosingTag(tag) => contains_html_closing_tag(line, tag),
        HtmlBlockEnd::Contains(needle) => line.contains(needle),
        HtmlBlockEnd::BlankLine => line.trim().is_empty(),
    }
}

fn contains_html_closing_tag(line: &str, tag: &str) -> bool {
    let needle = format!("</{tag}");
    line.as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

const HTML_BLOCK_TAGS: &[&str] = &[
    "address",
    "article",
    "aside",
    "base",
    "basefont",
    "blockquote",
    "body",
    "caption",
    "center",
    "col",
    "colgroup",
    "dd",
    "details",
    "dialog",
    "dir",
    "div",
    "dl",
    "dt",
    "fieldset",
    "figcaption",
    "figure",
    "footer",
    "form",
    "frame",
    "frameset",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "head",
    "header",
    "hr",
    "html",
    "iframe",
    "legend",
    "li",
    "link",
    "main",
    "menu",
    "menuitem",
    "nav",
    "noframes",
    "ol",
    "optgroup",
    "option",
    "p",
    "param",
    "search",
    "section",
    "summary",
    "table",
    "tbody",
    "td",
    "tfoot",
    "th",
    "thead",
    "title",
    "tr",
    "track",
    "ul",
];

fn pipe_table_cell_count(line: &str) -> Option<usize> {
    split_table_cells(line).map(|cells| cells.len())
}

fn table_preflight_line(trimmed: &str) -> Option<(usize, &str)> {
    let (quote_depth, content) = strip_blockquote_markers(trimmed);
    active_markdown_line(content).map(|line| (quote_depth, line))
}

fn is_table_separator_line(line: &str) -> bool {
    table_separator_cell_count(line).is_some()
}

fn table_separator_cell_count(line: &str) -> Option<usize> {
    let Some(cells) = split_table_cells(line) else {
        return None;
    };
    cells
        .iter()
        .all(|cell| is_table_separator_cell(cell))
        .then_some(cells.len())
}

fn split_table_cells(line: &str) -> Option<Vec<&str>> {
    let trimmed = line.trim();
    let positions = table_pipe_positions(trimmed);
    if positions.is_empty() {
        return None;
    }

    let mut start = 0usize;
    let mut end = trimmed.len();
    let mut first_delimiter = 0usize;
    let mut last_delimiter = positions.len();
    if positions.first().copied() == Some(0) {
        start = 1;
        first_delimiter = 1;
    }
    if positions.last().copied() == trimmed.len().checked_sub(1) {
        end -= 1;
        last_delimiter -= 1;
    }
    if start > end || trimmed[start..end].trim().is_empty() {
        return None;
    }

    let mut cells = Vec::new();
    let mut cell_start = start;
    for position in &positions[first_delimiter..last_delimiter] {
        cells.push(&trimmed[cell_start..*position]);
        cell_start = *position + 1;
    }
    cells.push(&trimmed[cell_start..end]);
    Some(cells)
}

fn table_pipe_positions(line: &str) -> Vec<usize> {
    let bytes = line.as_bytes();
    let mut positions = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => {
                index = (index + 2).min(bytes.len());
            }
            b'`' => {
                let len = backtick_run_len(bytes, index);
                if let Some(closing_index) = matching_backtick_run(bytes, index + len, len) {
                    index = closing_index + len;
                } else {
                    index += len;
                }
            }
            b'|' => {
                positions.push(index);
                index += 1;
            }
            _ => {
                index += 1;
            }
        }
    }
    positions
}

fn backtick_run_len(bytes: &[u8], index: usize) -> usize {
    bytes[index..]
        .iter()
        .take_while(|byte| **byte == b'`')
        .count()
}

fn matching_backtick_run(bytes: &[u8], mut index: usize, len: usize) -> Option<usize> {
    while index < bytes.len() {
        if bytes[index] == b'`' {
            let run_len = backtick_run_len(bytes, index);
            if run_len == len {
                return Some(index);
            }
            index += run_len;
        } else {
            index += 1;
        }
    }
    None
}

fn estimate_table_row_nodes(cells: usize) -> usize {
    1usize.saturating_add(cells.saturating_mul(2))
}

fn is_table_separator_cell(cell: &str) -> bool {
    let mut value = cell.trim();
    if let Some(rest) = value.strip_prefix(':') {
        value = rest;
    }
    if let Some(rest) = value.strip_suffix(':') {
        value = rest;
    }
    let mut hyphen_count = 0usize;
    for byte in value.bytes() {
        if byte == b'-' {
            hyphen_count += 1;
        } else if !byte.is_ascii_whitespace() {
            return false;
        }
    }
    hyphen_count >= 3
}

fn enforce_markdown_node_count(
    count: usize,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    if count > options.max_markdown_nodes {
        Err(invalid("input exceeds max_markdown_nodes"))
    } else {
        Ok(())
    }
}

fn enforce_markdown_table_cell_count(
    count: usize,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    if count > options.max_markdown_table_cells {
        Err(invalid("input exceeds max_markdown_table_cells"))
    } else {
        Ok(())
    }
}

fn invalid(message: impl Into<String>) -> TransformError {
    TransformError::new(TransformErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preflight_rejects_inline_heavy_input_before_parsing() {
        let input = "[x](https://example.com)".repeat(16);
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("inline-heavy input should exceed the preflight node estimate");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_skips_inline_estimate_inside_fenced_code() {
        let input = format!("```\n{}\n```", "[x](https://example.com)".repeat(16));
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(&input, true, &options)
            .expect("link-like code text should not count as inline nodes");
    }

    #[test]
    fn preflight_skips_inline_estimate_inside_html_block() {
        let input = format!("<pre>\n{}\n</pre>", "[x](https://example.com)\n".repeat(8));
        let options = NormalizationOptions {
            max_markdown_nodes: 2,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(&input, true, &options)
            .expect("link-like text inside an HTML block should not count as Markdown nodes");
    }

    #[test]
    fn preflight_skips_inline_estimate_inside_block_html_tag() {
        let input = format!("<div>\n{}</div>", "[x](https://example.com)\n".repeat(8));
        let options = NormalizationOptions {
            max_markdown_nodes: 2,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(&input, true, &options)
            .expect("link-like text inside a block HTML tag should not count as Markdown nodes");
    }

    #[test]
    fn preflight_skips_inline_estimate_inside_complete_html_tag_block() {
        let input = format!(
            "<custom-element>\n{}</custom-element>",
            "[x](https://example.com)\n".repeat(8)
        );
        let options = NormalizationOptions {
            max_markdown_nodes: 2,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(&input, true, &options).expect(
            "link-like text inside a complete HTML tag block should not count as Markdown nodes",
        );
    }

    #[test]
    fn preflight_does_not_start_complete_html_tag_block_inside_paragraph() {
        let input = format!(
            "paragraph\n<custom-element>\n{}</custom-element>",
            "[x](https://example.com)\n".repeat(4)
        );
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("type 7 HTML blocks should not hide Markdown while a paragraph is active");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_does_not_start_complete_html_tag_block_inside_blockquote_paragraph() {
        let input = format!(
            "> paragraph\n> <custom-element>\n> {}</custom-element>",
            "[x](https://example.com)\n".repeat(4)
        );
        let options = NormalizationOptions {
            max_markdown_nodes: 10,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options).expect_err(
            "type 7 HTML blocks should not hide Markdown inside a blockquote paragraph",
        );

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_does_not_start_complete_html_tag_block_after_reference_definition() {
        let input = "[x]: https://example.com\n<custom-element>\n[x](https://example.com)\n</custom-element>";
        let options = NormalizationOptions {
            max_markdown_nodes: 4,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(input, true, &options).expect_err(
            "reference definitions should keep type 7 HTML tags inside the paragraph preflight",
        );

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_does_not_start_complete_html_tag_block_after_blockquote_reference_definition() {
        let input = "> [x]: https://example.com\n> <custom-element>\n> [x](https://example.com)\n> </custom-element>";
        let options = NormalizationOptions {
            max_markdown_nodes: 6,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(input, true, &options).expect_err(
            "blockquote reference definitions should keep type 7 HTML tags inside the paragraph preflight",
        );

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_skips_table_like_text_inside_blockquote_fenced_code() {
        let input = "> ```\n> | A | B |\n> | --- | --- |\n> | x | y |\n> ```";
        let options = NormalizationOptions {
            max_markdown_table_cells: 1,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options).expect(
            "table-like code text inside a blockquote fence should not count as table cells",
        );
    }

    #[test]
    fn preflight_does_not_skip_content_after_invalid_backtick_fence_info() {
        let input = format!(
            "``` invalid ` info\n{}",
            "[x](https://example.com)\n".repeat(4)
        );
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("invalid backtick fence info should not hide later content");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_rejects_content_after_four_space_indented_fence() {
        let input = format!("    ```\n{}", "[x](https://example.com)\n".repeat(4));
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("four-space indented fence marker should not hide later content");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_allows_markdown_markers_inside_indented_code() {
        let input = format!("{}{}", "    - x\n".repeat(8), "    # title\n".repeat(8));
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(&input, true, &options)
            .expect("indented code markers should not count as active Markdown structure");
    }

    #[test]
    fn preflight_allows_blank_lines_without_node_growth() {
        let input = "\n".repeat(20);
        let options = NormalizationOptions {
            max_markdown_nodes: 1,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(&input, true, &options)
            .expect("blank lines should not count as parsed Markdown nodes");
    }

    #[test]
    fn preflight_counts_non_heading_hash_lines_as_paragraphs() {
        let input = "#tag\n".repeat(4);
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("non-heading hash lines should count as paragraph/text nodes");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_counts_atx_heading_text_nodes_before_parsing() {
        let input = "# title\n".repeat(5);
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("heading text nodes should count toward the preflight node estimate");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_counts_thematic_breaks_as_single_nodes() {
        let input = "***\n---\n_ _ _\n";
        let options = NormalizationOptions {
            max_markdown_nodes: 4,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("thematic breaks should count as one parsed block node each");
    }

    #[test]
    fn preflight_allows_setext_headings_without_underline_node_growth() {
        let input = "Title\n===\nSubtitle\n---\n";
        let options = NormalizationOptions {
            max_markdown_nodes: 5,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("setext underline lines should not add paragraph nodes");
    }

    #[test]
    fn preflight_allows_empty_atx_headings_with_closing_markers() {
        let input = "# #\n".repeat(2);
        let options = NormalizationOptions {
            max_markdown_nodes: 3,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(&input, true, &options)
            .expect("closing markers without heading content should not add text nodes");
    }

    #[test]
    fn preflight_rejects_list_heavy_input_before_parsing() {
        let input = "- x\n".repeat(4);
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("compact list-heavy input should exceed the preflight node estimate");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_treats_non_one_ordered_marker_inside_paragraph_as_text() {
        let input = "intro\n2. not a list\n3. still paragraph\n";
        let options = NormalizationOptions {
            max_markdown_nodes: 7,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("ordered markers above one should not interrupt an active paragraph");
    }

    #[test]
    fn preflight_keeps_one_ordered_marker_as_paragraph_interrupting_list() {
        let input = "intro\n1. list\n";
        let options = NormalizationOptions {
            max_markdown_nodes: 5,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(input, true, &options)
            .expect_err("ordered marker one can interrupt an active paragraph");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_counts_gfm_autolinks_before_parsing() {
        let input = "https://example.com support@example.com ".repeat(3);
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("GFM autolinks should exceed the preflight node estimate");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_ignores_gfm_autolinks_when_extensions_are_disabled() {
        let input = "https://example.com support@example.com ".repeat(3);
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(&input, false, &options)
            .expect("CommonMark mode should not count GFM bare URL autolinks");
    }

    #[test]
    fn preflight_does_not_double_count_angle_bracket_autolinks_as_gfm_bare_links() {
        let input = "<https://example.com> <user@example.com>";
        let options = NormalizationOptions {
            max_markdown_nodes: 5,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("angle-bracket autolinks should not also count as GFM bare autolinks");
    }

    #[test]
    fn preflight_counts_single_marker_emphasis_before_parsing() {
        let input = "*x* _y_ ".repeat(4);
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("single-marker emphasis should exceed the preflight node estimate");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_counts_gfm_strikethrough_before_parsing() {
        let input = "~~x~~ ".repeat(6);
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("GFM strikethrough should exceed the preflight node estimate");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_counts_reference_links_before_parsing() {
        let input = format!("{}\n\n[ref]: https://example.com", "[x][ref] ".repeat(5));
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("reference links should exceed the preflight node estimate");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_counts_shortcut_reference_links_before_parsing() {
        let input = format!("{}\n\n[x]: https://example.com", "[x] ".repeat(5));
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("shortcut reference links should exceed the preflight node estimate");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_counts_collapsed_reference_links_before_parsing() {
        let input = format!("{}\n\n[x]: https://example.com", "[x][] ".repeat(5));
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("collapsed reference links should exceed the preflight node estimate");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_ignores_undefined_reference_labels() {
        let input = "A [not-a-link]\n[real]: not-url";
        let options = NormalizationOptions {
            max_markdown_nodes: 5,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("undefined reference labels should not count as reference link nodes");
    }

    #[test]
    fn preflight_ignores_reference_text_inside_code_span() {
        let input = format!("{}\n\n[x]: https://example.com", "`[x]` ".repeat(3));
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(&input, true, &options)
            .expect("bracket text inside code spans should not count as reference link nodes");
    }

    #[test]
    fn preflight_ignores_reference_definitions_inside_fenced_code() {
        let input = "A [x]\n```\n[x]: https://example.com\n```";
        let options = NormalizationOptions {
            max_markdown_nodes: 4,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("reference definitions inside fenced code should not enable shortcut links");
    }

    #[test]
    fn preflight_ignores_reference_definitions_inside_html_block() {
        let input = "A [x]\n<div>\n[x]: https://example.com\n</div>";
        let options = NormalizationOptions {
            max_markdown_nodes: 4,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("reference definitions inside HTML blocks should not enable shortcut links");
    }

    #[test]
    fn preflight_ignores_reference_definition_without_destination() {
        let input = "A [x]\n[x]:";
        let options = NormalizationOptions {
            max_markdown_nodes: 5,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("reference labels without destinations should not enable shortcut links");
    }

    #[test]
    fn preflight_ignores_blockquote_reference_definition_without_destination() {
        let input = "> A [x]\n> [x]:";
        let options = NormalizationOptions {
            max_markdown_nodes: 7,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options).expect(
            "blockquote reference labels without destinations should not enable shortcut links",
        );
    }

    #[test]
    fn preflight_ignores_reference_definition_with_trailing_text() {
        let input = "A [x] [x] [x] [x] [x]\n[x]: foo bar";
        let options = NormalizationOptions {
            max_markdown_nodes: 5,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("reference definitions with trailing text should not enable shortcut links");
    }

    #[test]
    fn preflight_ignores_reference_definition_with_unbalanced_destination_parens() {
        let input = "[x]: foo(bar\nA [x]";
        let options = NormalizationOptions {
            max_markdown_nodes: 5,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("unbalanced destination parens should not enable shortcut links");
    }

    #[test]
    fn preflight_ignores_reference_definition_with_nested_bracketed_destination_start() {
        let input = "[x]: <foo<bar>\nA [x] [x] [x] [x] [x]";
        let options = NormalizationOptions {
            max_markdown_nodes: 7,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("nested bracketed destination starts should not enable shortcut links");
    }

    #[test]
    fn preflight_ignores_reference_definition_with_escaped_destination_space() {
        let input = "[x]: foo\\ bar\nA [x] [x] [x] [x] [x]";
        let options = NormalizationOptions {
            max_markdown_nodes: 5,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("escaped spaces should not make unbracketed destinations valid");
    }

    #[test]
    fn preflight_counts_reference_definition_with_balanced_destination_parens() {
        let input = "[x]: foo(bar)\nA [x] [x]";
        let options = NormalizationOptions {
            max_markdown_nodes: 5,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(input, true, &options)
            .expect_err("balanced destination parens should enable shortcut links");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_counts_reference_definition_with_escaped_destination_parens() {
        let input = "[x]: foo\\(bar\\)\nA [x] [x]";
        let options = NormalizationOptions {
            max_markdown_nodes: 5,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(input, true, &options)
            .expect_err("escaped destination parens should keep the reference definition valid");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_counts_reference_definition_with_escaped_bracketed_destination_start() {
        let input = "[x]: <foo\\<bar>\nA [x] [x]";
        let options = NormalizationOptions {
            max_markdown_nodes: 5,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(input, true, &options)
            .expect_err("escaped bracketed destination starts should keep the definition valid");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_ignores_blockquote_reference_definition_with_trailing_text() {
        let input = "> A [x] [x] [x] [x] [x]\n> [x]: foo bar";
        let options = NormalizationOptions {
            max_markdown_nodes: 7,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options).expect(
            "blockquote reference definitions with trailing text should not enable shortcut links",
        );
    }

    #[test]
    fn preflight_ignores_reference_definition_inside_paragraph() {
        let input = "intro\n[x]: not-url\nA [x] [x] [x] [x] [x]";
        let options = NormalizationOptions {
            max_markdown_nodes: 7,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("reference-like text inside a paragraph should not enable shortcut links");
    }

    #[test]
    fn preflight_counts_reference_links_on_reference_like_paragraph_line() {
        let input = "intro\n[x]: https://example.com [y] [y]\n\n[y]: https://example.com";
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(input, true, &options)
            .expect_err("reference links on paragraph text should count before parsing");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_ignores_blockquote_reference_definition_inside_paragraph() {
        let input = "> intro\n> [x]: not-url\n> A [x] [x] [x] [x] [x]";
        let options = NormalizationOptions {
            max_markdown_nodes: 10,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options).expect(
            "reference-like text inside a blockquote paragraph should not enable shortcut links",
        );
    }

    #[test]
    fn preflight_ignores_blockquote_lazy_continuation_reference_definition_inside_paragraph() {
        let input = "> intro\n[x]: not-url\nA [x] [x] [x] [x] [x]";
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options).expect(
            "lazy continuation text inside a blockquote paragraph should not enable shortcut links",
        );
    }

    #[test]
    fn preflight_counts_reference_links_after_blockquote_reference_lazy_continuation() {
        let input = "> [x]: https://example.com\n[y]: https://example.com\nA [y] [y]";
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(input, true, &options).expect_err(
            "blockquote reference-definition-only continuation should enable shortcut links",
        );

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_counts_reference_links_on_blockquote_lazy_continuation_line() {
        let input = "> intro\n[x]: https://example.com [y] [y]\n\n[y]: https://example.com";
        let options = NormalizationOptions {
            max_markdown_nodes: 9,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(input, true, &options).expect_err(
            "reference links on blockquote lazy continuation text should count before parsing",
        );

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_counts_blockquote_list_items_before_parsing() {
        let input = "> - x\n".repeat(4);
        let options = NormalizationOptions {
            max_markdown_nodes: 12,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("blockquote list items should exceed the preflight node estimate");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_rejects_tab_delimited_list_items_before_parsing() {
        let input = "-\tx\n".repeat(4);
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("tab-delimited list items should exceed the preflight node estimate");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_allows_pipe_text_without_table_cells() {
        let input = "echo a | sed s/a/b/ | wc\n".repeat(2);
        let options = NormalizationOptions {
            max_markdown_table_cells: 1,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(&input, true, &options)
            .expect("ordinary pipe text should not count as table cells before parsing");
    }

    #[test]
    fn preflight_ignores_escaped_and_code_span_pipes_in_table_cells() {
        let input = "| Field | Value |\n| --- | --- |\n| a \\| b | `c | d` |";
        let options = NormalizationOptions {
            max_markdown_table_cells: 5,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("escaped and code-span pipes should stay inside their table cells");
    }

    #[test]
    fn preflight_counts_unmatched_backtick_table_pipes_before_parsing() {
        let input = format!(
            "| Field | Type |\n| --- | --- |\n{}",
            "| `id | string |\n".repeat(3)
        );
        let options = NormalizationOptions {
            max_markdown_table_cells: 5,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("unmatched backticks should not hide later table separators");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_table_cells"));
    }

    #[test]
    fn preflight_counts_blockquote_gfm_table_cells_before_parsing() {
        let input = "> | Field | Type |\n> | --- | --- |\n> | id | string |";
        let options = NormalizationOptions {
            max_markdown_table_cells: 3,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(input, true, &options)
            .expect_err("blockquote tables should count table cells before parsing");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_table_cells"));
    }

    #[test]
    fn preflight_does_not_merge_table_header_across_blockquote_boundary() {
        let input = "| Field | Type |\n> | --- | --- |\n> | id | string |";
        let options = NormalizationOptions {
            max_markdown_table_cells: 1,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("table header and delimiter should not merge across quote boundaries");
    }

    #[test]
    fn preflight_does_not_continue_blockquote_table_after_quote_boundary() {
        let input = "> | Field | Type |\n> | --- | --- |\n| id | string |\n| name | string |";
        let options = NormalizationOptions {
            max_markdown_table_cells: 3,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("blockquote table rows should not continue after the quote boundary");
    }

    #[test]
    fn preflight_ignores_header_delimiter_mismatch_as_non_table() {
        let input = "| A | B |\n| --- | --- | --- |\n| x | y |";
        let options = NormalizationOptions {
            max_markdown_table_cells: 1,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("mismatched header and delimiter columns should not count as a table");
    }

    #[test]
    fn preflight_counts_short_table_rows_as_header_width() {
        let input = "| A | B | C |\n| --- | --- | --- |\n| x |";
        let options = NormalizationOptions {
            max_markdown_table_cells: 5,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(input, true, &options)
            .expect_err("short table rows should count padded cells before parsing");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_table_cells"));
    }

    #[test]
    fn preflight_counts_long_table_rows_as_header_width() {
        let input = "| A | B |\n| --- | --- |\n| x | y | ignored | ignored |";
        let options = NormalizationOptions {
            max_markdown_table_cells: 4,
            ..NormalizationOptions::default()
        };

        enforce_markdown_structural_preflight(input, true, &options)
            .expect("extra table row cells should not overcount parsed table cells");
    }

    #[test]
    fn preflight_rejects_gfm_table_cells_before_parsing() {
        let input = "| Field | Type |\n| --- | --- |\n| id | string |";
        let options = NormalizationOptions {
            max_markdown_table_cells: 1,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(input, true, &options)
            .expect_err("table cells should exceed the preflight table-cell estimate");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_table_cells"));
    }

    #[test]
    fn preflight_counts_gfm_table_rows_toward_nodes() {
        let input = format!(
            "| Field | Type |\n| --- | --- |\n{}",
            "| id | string |\n".repeat(4)
        );
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            max_markdown_table_cells: 100,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("table rows should exceed the preflight node estimate");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }

    #[test]
    fn preflight_counts_plain_paragraphs_toward_nodes() {
        let input = "plain\n\n".repeat(4);
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("plain paragraphs should exceed the preflight node estimate");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }
}
