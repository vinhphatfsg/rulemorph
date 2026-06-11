use std::collections::HashSet;

use super::reference::estimate_reference_link_nodes;

pub(super) fn estimate_inline_nodes(
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
    estimate_explicit_link_nodes(line)
        .saturating_add(estimate_reference_link_nodes(
            line,
            reference_labels,
            line_is_reference_definition,
        ))
        .saturating_add(estimate_image_nodes(line))
        .saturating_add(line.matches("**").count() / 2)
        .saturating_add(line.matches("__").count() / 2)
        .saturating_add(estimate_single_marker_emphasis_nodes(line))
        .saturating_add(line.matches('`').count() / 2)
        .saturating_add(estimate_inline_html_nodes(line))
        .saturating_add(gfm_nodes)
}

pub(super) fn estimate_explicit_link_nodes(line: &str) -> usize {
    let bytes = line.as_bytes();
    let mut index = 0usize;
    let mut estimate = 0usize;
    let mut bracket_stack = Vec::new();
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
                bracket_stack.push(is_unescaped_image_opener(bytes, index));
                index += 1;
            }
            b']' if bytes.get(index + 1).copied() == Some(b'(') => {
                if !bracket_stack.pop().unwrap_or(false) {
                    estimate = estimate.saturating_add(2);
                }
                index = explicit_link_destination_end(bytes, index + 2).unwrap_or(index + 2);
            }
            b']' => {
                bracket_stack.pop();
                index += 1;
            }
            _ => index += 1,
        }
    }
    estimate
}

pub(super) fn is_unescaped_image_opener(bytes: &[u8], bracket_index: usize) -> bool {
    bracket_index > 0
        && bytes[bracket_index - 1] == b'!'
        && !is_escaped_byte(bytes, bracket_index - 1)
}

pub(super) fn is_escaped_byte(bytes: &[u8], index: usize) -> bool {
    let mut slash_count = 0usize;
    let mut cursor = index;
    while cursor > 0 && bytes[cursor - 1] == b'\\' {
        slash_count += 1;
        cursor -= 1;
    }
    slash_count % 2 == 1
}

pub(super) fn estimate_image_nodes(line: &str) -> usize {
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
            b'!' if bytes.get(index + 1).copied() == Some(b'[') => {
                estimate = estimate.saturating_add(1);
                index += 2;
            }
            _ => index += 1,
        }
    }
    estimate
}

pub(super) fn estimate_gfm_autolink_nodes(line: &str) -> usize {
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

pub(super) fn explicit_link_destination_end(bytes: &[u8], mut index: usize) -> Option<usize> {
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

pub(super) fn is_gfm_url_autolink_start(bytes: &[u8], index: usize) -> bool {
    is_autolink_token_boundary(bytes, index)
        && (starts_with_ascii(bytes, index, b"http://")
            || starts_with_ascii(bytes, index, b"https://")
            || starts_with_ascii(bytes, index, b"www."))
}

pub(super) fn starts_with_ascii(bytes: &[u8], index: usize, prefix: &[u8]) -> bool {
    bytes
        .get(index..index.saturating_add(prefix.len()))
        .is_some_and(|value| value.eq_ignore_ascii_case(prefix))
}

pub(super) fn is_autolink_token_boundary(bytes: &[u8], index: usize) -> bool {
    index == 0 || bytes[index - 1].is_ascii_whitespace() || matches!(bytes[index - 1], b'(' | b'[')
}

pub(super) fn autolink_token_end(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() {
        if bytes[index].is_ascii_whitespace() || matches!(bytes[index], b'<' | b'>') {
            break;
        }
        index += 1;
    }
    index
}

pub(super) fn looks_like_email_autolink(token: &str) -> bool {
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

pub(super) fn estimate_single_marker_emphasis_nodes(line: &str) -> usize {
    estimate_delimited_inline_nodes(line, b'*', 1)
        .saturating_add(estimate_delimited_inline_nodes(line, b'_', 1))
}

pub(super) fn estimate_strikethrough_nodes(line: &str) -> usize {
    estimate_delimited_inline_nodes(line, b'~', 2)
}

pub(super) fn estimate_delimited_inline_nodes(
    line: &str,
    marker: u8,
    delimiter_len: usize,
) -> usize {
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

pub(super) fn single_emphasis_marker_sides(bytes: &[u8], index: usize, marker: u8) -> (bool, bool) {
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

pub(super) fn estimate_inline_html_nodes(line: &str) -> usize {
    let bytes = line.as_bytes();
    let mut index = 0usize;
    let mut count = 0usize;
    while index < bytes.len() {
        if bytes[index] == b'<'
            && (bytes
                .get(index + 1)
                .is_some_and(|byte| byte.is_ascii_alphabetic())
                || (bytes.get(index + 1).copied() == Some(b'/')
                    && bytes
                        .get(index + 2)
                        .is_some_and(|byte| byte.is_ascii_alphabetic())))
        {
            count = count.saturating_add(1);
        }
        index += 1;
    }
    count
}

pub(super) fn backtick_run_len(bytes: &[u8], index: usize) -> usize {
    bytes[index..]
        .iter()
        .take_while(|byte| **byte == b'`')
        .count()
}

pub(super) fn matching_backtick_run(bytes: &[u8], mut index: usize, len: usize) -> Option<usize> {
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
