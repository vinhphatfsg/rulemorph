use super::line::{active_markdown_line, block_content_line, strip_blockquote_markers};
use super::state::{ActiveFence, Fence};

pub(super) fn opening_fence(trimmed: &str) -> Option<Fence> {
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

pub(super) fn opening_fence_line(trimmed: &str) -> Option<ActiveFence> {
    let (quote_depth, content) = strip_blockquote_markers(trimmed);
    let content = active_markdown_line(content)?;
    opening_fence(content).map(|fence| ActiveFence { fence, quote_depth })
}

pub(super) fn fence_line_content(line: &str, quote_depth: usize) -> Option<&str> {
    block_content_line(line, quote_depth)
}

pub(super) fn is_closing_fence(trimmed: &str, fence: Fence) -> bool {
    let Some(candidate) = opening_fence(trimmed) else {
        return false;
    };
    candidate.marker == fence.marker
        && candidate.len >= fence.len
        && trimmed[candidate.len..].trim().is_empty()
}
