use comrak::nodes::{Node, NodeValue};

use crate::error::{TransformError, TransformErrorKind};

use super::super::NormalizationOptions;

pub(super) fn enforce_markdown_structural_preflight(
    input: &str,
    estimate_tables: bool,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    let mut estimated_nodes = 1usize;
    let mut estimated_table_cells = 0usize;
    let mut active_fence: Option<ActiveFence> = None;
    let mut pending_table_header_cells: Option<TableHeaderCandidate> = None;
    let mut active_table: Option<TableState> = None;
    let mut in_indented_code = false;
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

        let Some(trimmed) = active_markdown_line(line) else {
            if !in_indented_code {
                estimated_nodes = estimated_nodes.saturating_add(1);
                in_indented_code = true;
            }
            pending_table_header_cells = None;
            active_table = None;
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
            enforce_markdown_node_count(estimated_nodes, options)?;
            enforce_markdown_table_cell_count(estimated_table_cells, options)?;
            continue;
        }
        estimated_nodes = estimated_nodes.saturating_add(estimate_structural_nodes(trimmed));
        estimated_nodes = estimated_nodes.saturating_add(estimate_inline_nodes(trimmed));
        if estimate_tables {
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
    let active = active_markdown_line(line)?;
    let (line_quote_depth, content) = strip_blockquote_markers(active);
    (line_quote_depth == quote_depth)
        .then(|| active_markdown_line(content))
        .flatten()
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

fn is_structural_line(trimmed: &str) -> bool {
    trimmed.is_empty()
        || trimmed.starts_with('#')
        || trimmed.starts_with('>')
        || is_list_item_line(trimmed)
        || trimmed.starts_with("```")
        || trimmed.starts_with("~~~")
        || trimmed.starts_with('<')
        || is_table_separator_line(trimmed)
}

fn estimate_structural_nodes(trimmed: &str) -> usize {
    if is_list_item_line(trimmed) {
        // A compact list item typically expands to list + item + paragraph + text nodes.
        4
    } else if trimmed.starts_with('>') {
        estimate_blockquote_nodes(trimmed)
    } else if is_structural_line(trimmed) {
        1
    } else if !trimmed.is_empty() {
        2
    } else {
        0
    }
}

fn estimate_blockquote_nodes(trimmed: &str) -> usize {
    let (quote_nodes, content) = strip_blockquote_markers(trimmed);
    if quote_nodes == 0 {
        return 0;
    }
    let content_nodes = active_markdown_line(content)
        .filter(|content| !content.is_empty())
        .map(estimate_structural_nodes)
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

fn is_list_item_line(trimmed: &str) -> bool {
    is_unordered_list_item_line(trimmed) || is_ordered_list_item_line(trimmed)
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

fn is_ordered_list_item_line(trimmed: &str) -> bool {
    let bytes = trimmed.as_bytes();
    let digit_count = bytes
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if digit_count == 0 || digit_count > 9 || digit_count + 1 >= bytes.len() {
        return false;
    }
    matches!(bytes[digit_count], b'.' | b')') && bytes[digit_count + 1].is_ascii_whitespace()
}

fn estimate_inline_nodes(line: &str) -> usize {
    line.matches("](")
        .count()
        .saturating_mul(2)
        .saturating_add(line.matches("![").count())
        .saturating_add(line.matches("**").count() / 2)
        .saturating_add(line.matches("__").count() / 2)
        .saturating_add(line.matches('`').count() / 2)
        .saturating_add(estimate_inline_html_nodes(line))
}

fn estimate_inline_html_nodes(line: &str) -> usize {
    line.as_bytes()
        .windows(2)
        .filter(|window| window[0] == b'<' && window[1].is_ascii_alphabetic())
        .count()
}

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
