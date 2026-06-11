use comrak::nodes::{Node, NodeValue};

use crate::error::{TransformError, TransformErrorKind};

use super::super::NormalizationOptions;

mod fence;
mod html;
mod inline;
mod line;
mod reference;
mod state;
mod table;

#[cfg(test)]
mod tests;

use fence::{fence_line_content, is_closing_fence, opening_fence_line};
use html::{
    can_start_html_block, html_block_content_line, html_block_ends_on_line, opening_html_block_line,
};
use inline::estimate_inline_nodes;
use line::{
    active_markdown_line, estimate_structural_nodes, is_setext_underline_for_paragraph,
    list_state_for_preflight_line, paragraph_quote_depth_for_line,
};
use reference::{
    ReferenceTitleState, collect_link_reference_labels, effective_link_reference_definition,
    effective_link_reference_title_continuation,
};
use state::{ActiveFence, ActiveHtmlBlock, ListState, TableHeaderCandidate, TableState};
use table::{
    estimate_table_row_nodes, pipe_table_cell_count, table_preflight_line,
    table_separator_cell_count,
};

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
    let mut reference_title_state: Option<ReferenceTitleState> = None;
    let mut active_list: Option<ListState> = None;
    let reference_labels = collect_link_reference_labels(input);
    let lines = input.lines().collect::<Vec<_>>();
    for (line_index, line) in lines.iter().enumerate() {
        let line = *line;
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
            active_list = None;
            paragraph_quote_depth = None;
            reference_definition_quote_depth = None;
            reference_title_state = None;
            enforce_markdown_node_count(estimated_nodes, options)?;
            enforce_markdown_table_cell_count(estimated_table_cells, options)?;
            continue;
        };
        in_indented_code = false;

        if let Some(next_reference_title_state) = effective_link_reference_title_continuation(
            trimmed,
            &lines[line_index + 1..],
            paragraph_quote_depth,
            reference_title_state,
        ) {
            pending_table_header_cells = None;
            active_table = None;
            active_list = None;
            paragraph_quote_depth = None;
            reference_definition_quote_depth = None;
            reference_title_state = next_reference_title_state;
            enforce_markdown_node_count(estimated_nodes, options)?;
            enforce_markdown_table_cell_count(estimated_table_cells, options)?;
            continue;
        }

        if let Some(active) = opening_fence_line(trimmed) {
            estimated_nodes = estimated_nodes.saturating_add(1);
            active_fence = Some(active);
            pending_table_header_cells = None;
            active_table = None;
            active_list = None;
            paragraph_quote_depth = None;
            reference_definition_quote_depth = None;
            reference_title_state = None;
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
            active_list = None;
            paragraph_quote_depth = None;
            reference_definition_quote_depth = None;
            reference_title_state = None;
            enforce_markdown_node_count(estimated_nodes, options)?;
            enforce_markdown_table_cell_count(estimated_table_cells, options)?;
            continue;
        }
        if reference_definition_quote_depth.is_none()
            && is_setext_underline_for_paragraph(trimmed, paragraph_quote_depth)
        {
            pending_table_header_cells = None;
            active_table = None;
            active_list = None;
            paragraph_quote_depth = None;
            reference_definition_quote_depth = None;
            reference_title_state = None;
            enforce_markdown_node_count(estimated_nodes, options)?;
            enforce_markdown_table_cell_count(estimated_table_cells, options)?;
            continue;
        }
        let reference_definition = effective_link_reference_definition(
            trimmed,
            paragraph_quote_depth,
            reference_definition_quote_depth,
        );
        let reference_definition_quote = reference_definition
            .as_ref()
            .map(|definition| definition.quote_depth);
        reference_title_state = reference_definition.as_ref().and_then(|definition| {
            definition.title_pending.then_some(ReferenceTitleState {
                quote_depth: definition.quote_depth,
                closer: None,
            })
        });
        let line_is_reference_definition = reference_definition_quote.is_some();
        let fallback_nodes = if line_is_reference_definition {
            active_list = None;
            0
        } else if let Some(list) = list_state_for_preflight_line(trimmed, paragraph_quote_depth) {
            let nodes = if active_list == Some(list) { 3 } else { 4 };
            active_list = Some(list);
            nodes
        } else {
            active_list = None;
            estimate_structural_nodes(trimmed, paragraph_quote_depth)
        };
        estimated_nodes = estimated_nodes.saturating_add(fallback_nodes);
        estimated_nodes = estimated_nodes.saturating_add(estimate_inline_nodes(
            trimmed,
            &reference_labels,
            line_is_reference_definition,
            estimate_gfm_extensions,
        ));
        paragraph_quote_depth = reference_definition_quote
            .or_else(|| paragraph_quote_depth_for_line(trimmed, paragraph_quote_depth));
        reference_definition_quote_depth = reference_definition_quote;
        if estimate_gfm_extensions && !line_is_reference_definition {
            if let Some((quote_depth, table_line)) = table_preflight_line(trimmed) {
                let pipe_cells = pipe_table_cell_count(table_line);
                if let Some(table) = active_table {
                    match pipe_cells {
                        Some(cells) if quote_depth == table.quote_depth => {
                            estimated_nodes = estimated_nodes.saturating_sub(fallback_nodes);
                            estimated_nodes = estimated_nodes
                                .saturating_add(estimate_table_row_nodes(table.columns));
                            estimated_table_cells =
                                estimated_table_cells.saturating_add(table.columns);
                            pending_table_header_cells = None;
                        }
                        Some(cells) => {
                            active_table = None;
                            pending_table_header_cells = Some(TableHeaderCandidate {
                                cells,
                                quote_depth,
                                fallback_nodes,
                            });
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
                            .saturating_sub(header.fallback_nodes)
                            .saturating_sub(fallback_nodes);
                        estimated_nodes = estimated_nodes
                            .saturating_add(header.quote_depth)
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
                    pending_table_header_cells = Some(TableHeaderCandidate {
                        cells,
                        quote_depth,
                        fallback_nodes,
                    });
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
