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
    let mut active_fence = None;
    let mut pending_table_header_cells = None;
    let mut in_table = false;
    for line in input.lines() {
        let trimmed = line.trim_start();
        if let Some(fence) = active_fence {
            if let Some(fence_line) = fence_line_content(line)
                && is_closing_fence(fence_line, fence)
            {
                active_fence = None;
            }
            enforce_markdown_node_count(estimated_nodes, options)?;
            enforce_markdown_table_cell_count(estimated_table_cells, options)?;
            continue;
        }
        if let Some(fence_line) = fence_line_content(line) {
            if let Some(fence) = opening_fence(fence_line) {
                estimated_nodes = estimated_nodes.saturating_add(1);
                active_fence = Some(fence);
                pending_table_header_cells = None;
                in_table = false;
                enforce_markdown_node_count(estimated_nodes, options)?;
                enforce_markdown_table_cell_count(estimated_table_cells, options)?;
                continue;
            }
        }
        estimated_nodes = estimated_nodes.saturating_add(estimate_structural_nodes(trimmed));
        estimated_nodes = estimated_nodes.saturating_add(estimate_inline_nodes(trimmed));
        if estimate_tables {
            let pipe_cells = pipe_table_cell_count(trimmed);
            if is_table_separator_line(trimmed) {
                if let Some(header_cells) = pending_table_header_cells.take() {
                    estimated_nodes = estimated_nodes
                        .saturating_add(1)
                        .saturating_add(estimate_table_row_nodes(header_cells));
                    estimated_table_cells = estimated_table_cells.saturating_add(header_cells);
                    in_table = true;
                } else {
                    in_table = false;
                }
            } else if let Some(cells) = pipe_cells {
                if in_table {
                    estimated_nodes =
                        estimated_nodes.saturating_add(estimate_table_row_nodes(cells));
                    estimated_table_cells = estimated_table_cells.saturating_add(cells);
                } else {
                    pending_table_header_cells = Some(cells);
                }
            } else {
                pending_table_header_cells = None;
                in_table = false;
            }
        } else {
            pending_table_header_cells = None;
            in_table = false;
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
    (len >= 3).then_some(Fence { marker, len })
}

fn fence_line_content(line: &str) -> Option<&str> {
    let spaces = line
        .as_bytes()
        .iter()
        .take_while(|byte| **byte == b' ')
        .count();
    (spaces <= 3).then_some(&line[spaces..])
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
        2
    } else if is_structural_line(trimmed) {
        1
    } else if !trimmed.is_empty() {
        2
    } else {
        0
    }
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

fn is_table_separator_line(line: &str) -> bool {
    let Some(cells) = split_table_cells(line) else {
        return false;
    };
    cells.into_iter().all(is_table_separator_cell)
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
    let mut active_code_span_len = None;
    let mut index = 0usize;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => {
                index = (index + 2).min(bytes.len());
            }
            b'`' => {
                let len = bytes[index..]
                    .iter()
                    .take_while(|byte| **byte == b'`')
                    .count();
                if active_code_span_len == Some(len) {
                    active_code_span_len = None;
                } else if active_code_span_len.is_none() {
                    active_code_span_len = Some(len);
                }
                index += len;
            }
            b'|' if active_code_span_len.is_none() => {
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
