use comrak::nodes::{Node, NodeValue};

use crate::error::{TransformError, TransformErrorKind};

use super::super::NormalizationOptions;

pub(super) fn enforce_markdown_structural_preflight(
    input: &str,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    let mut estimated_nodes = 1usize;
    let mut estimated_table_cells = 0usize;
    let mut active_fence = None;
    for line in input.lines() {
        let trimmed = line.trim_start();
        if let Some(fence) = active_fence {
            if is_closing_fence(trimmed, fence) {
                active_fence = None;
            }
            enforce_markdown_node_count(estimated_nodes, options)?;
            enforce_markdown_table_cell_count(estimated_table_cells, options)?;
            continue;
        }
        if let Some(fence) = opening_fence(trimmed) {
            estimated_nodes = estimated_nodes.saturating_add(1);
            active_fence = Some(fence);
            enforce_markdown_node_count(estimated_nodes, options)?;
            enforce_markdown_table_cell_count(estimated_table_cells, options)?;
            continue;
        }
        estimated_nodes = estimated_nodes.saturating_add(estimate_structural_nodes(trimmed));
        estimated_nodes = estimated_nodes.saturating_add(estimate_inline_nodes(trimmed));
        if trimmed.contains('|') {
            estimated_table_cells = estimated_table_cells
                .saturating_add(trimmed.matches('|').count().saturating_sub(1));
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
        || trimmed.contains('|')
}

fn estimate_structural_nodes(trimmed: &str) -> usize {
    if is_list_item_line(trimmed) {
        // A compact list item typically expands to list + item + paragraph + text nodes.
        4
    } else if trimmed.starts_with('>') {
        2
    } else if is_structural_line(trimmed) {
        1
    } else {
        0
    }
}

fn is_list_item_line(trimmed: &str) -> bool {
    is_unordered_list_item_line(trimmed) || is_ordered_list_item_line(trimmed)
}

fn is_unordered_list_item_line(trimmed: &str) -> bool {
    trimmed
        .strip_prefix("- ")
        .or_else(|| trimmed.strip_prefix("* "))
        .or_else(|| trimmed.strip_prefix("+ "))
        .is_some()
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

        let err = enforce_markdown_structural_preflight(&input, &options)
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

        enforce_markdown_structural_preflight(&input, &options)
            .expect("link-like code text should not count as inline nodes");
    }

    #[test]
    fn preflight_rejects_list_heavy_input_before_parsing() {
        let input = "- x\n".repeat(4);
        let options = NormalizationOptions {
            max_markdown_nodes: 8,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, &options)
            .expect_err("compact list-heavy input should exceed the preflight node estimate");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }
}
