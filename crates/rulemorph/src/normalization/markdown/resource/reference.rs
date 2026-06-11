use std::collections::HashSet;

use super::fence::{fence_line_content, is_closing_fence, opening_fence, opening_fence_line};
use super::html::{
    can_start_html_block, html_block_content_line, html_block_ends_on_line, opening_html_block_end,
    opening_html_block_line,
};
use super::inline::{backtick_run_len, matching_backtick_run};
use super::line::{
    active_markdown_line, is_atx_heading_line, is_nonempty_unordered_list_item_line,
    is_setext_underline_for_paragraph, is_setext_underline_line, is_thematic_break_line,
    ordered_list_marker_tail, paragraph_quote_depth_for_line, strip_blockquote_markers,
};
use super::state::{ActiveFence, ActiveHtmlBlock};

pub(super) fn estimate_reference_link_nodes(
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

pub(super) fn matching_closing_bracket(bytes: &[u8], mut index: usize) -> Option<usize> {
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index = (index + 2).min(bytes.len()),
            b']' => return Some(index),
            _ => index += 1,
        }
    }
    None
}

pub(super) fn collect_link_reference_labels(input: &str) -> HashSet<String> {
    let mut active_fence: Option<ActiveFence> = None;
    let mut active_html_block: Option<ActiveHtmlBlock> = None;
    let mut paragraph_quote_depth: Option<usize> = None;
    let mut reference_definition_quote_depth: Option<usize> = None;
    let mut reference_title_state: Option<ReferenceTitleState> = None;
    let mut labels = HashSet::new();
    let lines = input.lines().collect::<Vec<_>>();
    for (line_index, line) in lines.iter().enumerate() {
        let line = *line;
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
            reference_title_state = None;
            continue;
        };
        if let Some(next_reference_title_state) = effective_link_reference_title_continuation(
            trimmed,
            &lines[line_index + 1..],
            paragraph_quote_depth,
            reference_title_state,
        ) {
            paragraph_quote_depth = None;
            reference_definition_quote_depth = None;
            reference_title_state = next_reference_title_state;
            continue;
        }
        if let Some(active) = opening_fence_line(trimmed) {
            active_fence = Some(active);
            paragraph_quote_depth = None;
            reference_definition_quote_depth = None;
            reference_title_state = None;
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
            reference_title_state = None;
            continue;
        }
        if reference_definition_quote_depth.is_none()
            && is_setext_underline_for_paragraph(trimmed, paragraph_quote_depth)
        {
            paragraph_quote_depth = None;
            reference_definition_quote_depth = None;
            reference_title_state = None;
            continue;
        }

        if let Some(definition) = effective_link_reference_definition(
            trimmed,
            paragraph_quote_depth,
            reference_definition_quote_depth,
        ) {
            labels.insert(definition.label);
            reference_definition_quote_depth = Some(definition.quote_depth);
            reference_title_state = definition.title_pending.then_some(ReferenceTitleState {
                quote_depth: definition.quote_depth,
                closer: None,
            });
        } else {
            reference_definition_quote_depth = None;
            reference_title_state = None;
        }
        paragraph_quote_depth = reference_definition_quote_depth
            .or_else(|| paragraph_quote_depth_for_line(trimmed, paragraph_quote_depth));
    }
    labels
}

pub(super) struct LinkReferenceDefinition {
    pub(super) quote_depth: usize,
    pub(super) label: String,
    pub(super) title_pending: bool,
}

#[derive(Clone, Copy)]
pub(super) struct ReferenceTitleState {
    pub(super) quote_depth: usize,
    pub(super) closer: Option<char>,
}

pub(super) fn effective_link_reference_definition(
    line: &str,
    paragraph_quote_depth: Option<usize>,
    reference_definition_quote_depth: Option<usize>,
) -> Option<LinkReferenceDefinition> {
    let mut definition = link_reference_definition_with_quote_depth(line)?;
    if let Some(paragraph_quote_depth) = paragraph_quote_depth
        && paragraph_quote_depth > definition.quote_depth
    {
        if reference_definition_quote_depth == Some(paragraph_quote_depth) {
            definition.quote_depth = paragraph_quote_depth;
            return Some(definition);
        }
        return None;
    }
    (paragraph_quote_depth != Some(definition.quote_depth)
        || reference_definition_quote_depth == Some(definition.quote_depth))
    .then_some(definition)
}

pub(super) fn effective_link_reference_title_continuation(
    line: &str,
    remaining_lines: &[&str],
    paragraph_quote_depth: Option<usize>,
    reference_title_state: Option<ReferenceTitleState>,
) -> Option<Option<ReferenceTitleState>> {
    let state = reference_title_state?;
    let (quote_depth, content) = strip_blockquote_markers(line);
    let content = active_markdown_line(content)?;
    if reference_title_continuation_stops(content) {
        return None;
    }
    let effective_quote_depth = if (state.quote_depth > 0 && quote_depth == 0)
        || paragraph_quote_depth
            .is_some_and(|depth| depth > quote_depth && state.quote_depth == depth)
    {
        state.quote_depth
    } else {
        quote_depth
    };
    if effective_quote_depth != state.quote_depth {
        return None;
    }
    match state.closer {
        Some(closer) => Some((!link_reference_title_closes(content, closer, 0)).then_some(state)),
        None => {
            let (closer, content_start) = link_reference_title_start(content)?;
            if link_reference_title_closes(content, closer, content_start) {
                Some(None)
            } else {
                reference_title_has_future_closer(remaining_lines, state.quote_depth, closer)
                    .then_some(Some(ReferenceTitleState {
                        quote_depth: state.quote_depth,
                        closer: Some(closer),
                    }))
            }
        }
    }
}

pub(super) fn link_reference_definition_with_quote_depth(
    line: &str,
) -> Option<LinkReferenceDefinition> {
    let (quote_depth, content) = strip_blockquote_markers(line);
    active_markdown_line(content)
        .and_then(link_reference_definition)
        .map(|(label, title_pending)| LinkReferenceDefinition {
            quote_depth,
            label,
            title_pending,
        })
}

pub(super) fn link_reference_definition(line: &str) -> Option<(String, bool)> {
    let trimmed = line.trim_start();
    let bytes = trimmed.as_bytes();
    if bytes.first().copied() != Some(b'[') {
        return None;
    }
    let close_index = matching_closing_bracket(bytes, 1)?;
    if bytes.get(close_index + 1).copied() != Some(b':') {
        return None;
    }
    let title_pending = link_reference_destination_title_pending(&trimmed[close_index + 2..])?;
    let label = reference_label_from_bytes(bytes, 1, close_index)?;
    Some((label, title_pending))
}

pub(super) fn link_reference_destination_title_pending(rest: &str) -> Option<bool> {
    let rest = rest.trim_start();
    if rest.is_empty() {
        return None;
    }
    let rest = if let Some(destination) = rest.strip_prefix('<') {
        bracketed_link_reference_destination_tail(destination)?
    } else {
        unbracketed_link_reference_destination_tail(rest)?
    }
    .trim_start();

    if rest.is_empty() {
        Some(true)
    } else {
        has_link_reference_title(rest).then_some(false)
    }
}

pub(super) fn link_reference_title_start(line: &str) -> Option<(char, usize)> {
    let start = line.len().saturating_sub(line.trim_start().len());
    let opener = line[start..].chars().next()?;
    let closer = match opener {
        '"' => '"',
        '\'' => '\'',
        '(' => ')',
        _ => return None,
    };
    Some((closer, start + opener.len_utf8()))
}

pub(super) fn link_reference_title_closes(line: &str, closer: char, start_index: usize) -> bool {
    let mut escaped = false;
    for (index, value) in line
        .char_indices()
        .skip_while(|(index, _)| *index < start_index)
    {
        if escaped {
            escaped = false;
            continue;
        }
        if value == '\\' {
            escaped = true;
            continue;
        }
        if value == closer {
            return line[index + value.len_utf8()..].trim().is_empty();
        }
    }
    false
}

pub(super) fn reference_title_has_future_closer(
    remaining_lines: &[&str],
    quote_depth: usize,
    closer: char,
) -> bool {
    for line in remaining_lines {
        let Some(content) = reference_title_content_line(line, quote_depth) else {
            return false;
        };
        if reference_title_continuation_stops(content) {
            return false;
        }
        if link_reference_title_closes(content, closer, 0) {
            return true;
        }
    }
    false
}

pub(super) fn reference_title_continuation_stops(content: &str) -> bool {
    content.trim().is_empty()
        || opening_fence(content).is_some()
        || is_atx_heading_line(content)
        || is_thematic_break_line(content)
        || is_setext_underline_line(content)
        || opening_html_block_end(content).is_some_and(|(_, can_interrupt)| can_interrupt)
        || is_nonempty_unordered_list_item_line(content)
        || ordered_list_marker_tail(content)
            .is_some_and(|(start, tail)| start == 1 && !tail.trim().is_empty())
}

pub(super) fn reference_title_content_line(line: &str, quote_depth: usize) -> Option<&str> {
    let active = active_markdown_line(line)?;
    let (line_quote_depth, content) = strip_blockquote_markers(active);
    if line_quote_depth == quote_depth {
        return active_markdown_line(content);
    }
    (quote_depth > 0 && line_quote_depth == 0).then_some(active)
}

pub(super) fn bracketed_link_reference_destination_tail(destination: &str) -> Option<&str> {
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

pub(super) fn unbracketed_link_reference_destination_tail(rest: &str) -> Option<&str> {
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

pub(super) fn has_link_reference_title(rest: &str) -> bool {
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

pub(super) fn reference_label_from_bytes(bytes: &[u8], start: usize, end: usize) -> Option<String> {
    std::str::from_utf8(bytes.get(start..end)?)
        .ok()
        .and_then(normalize_reference_label)
}

pub(super) fn normalize_reference_label(label: &str) -> Option<String> {
    let label = label.split_whitespace().collect::<Vec<_>>().join(" ");
    (!label.is_empty()).then(|| label.to_lowercase())
}
