use super::inline::{backtick_run_len, matching_backtick_run};
use super::line::{active_markdown_line, strip_blockquote_markers};

pub(super) fn pipe_table_cell_count(line: &str) -> Option<usize> {
    split_table_cells(line).map(|cells| cells.len())
}

pub(super) fn table_preflight_line(trimmed: &str) -> Option<(usize, &str)> {
    let (quote_depth, content) = strip_blockquote_markers(trimmed);
    active_markdown_line(content).map(|line| (quote_depth, line))
}

pub(super) fn is_table_separator_line(line: &str) -> bool {
    table_separator_cell_count(line).is_some()
}

pub(super) fn table_separator_cell_count(line: &str) -> Option<usize> {
    let cells = split_table_cells(line)?;
    cells
        .iter()
        .all(|cell| is_table_separator_cell(cell))
        .then_some(cells.len())
}

pub(super) fn split_table_cells(line: &str) -> Option<Vec<&str>> {
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

pub(super) fn table_pipe_positions(line: &str) -> Vec<usize> {
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

pub(super) fn estimate_table_row_nodes(cells: usize) -> usize {
    1usize.saturating_add(cells.saturating_mul(2))
}

pub(super) fn is_table_separator_cell(cell: &str) -> bool {
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
