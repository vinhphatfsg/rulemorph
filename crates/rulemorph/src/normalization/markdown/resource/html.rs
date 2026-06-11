use super::line::{active_markdown_line, strip_blockquote_markers};
use super::state::{ActiveHtmlBlock, HtmlBlockEnd};

pub(super) fn html_block_content_line(line: &str, quote_depth: usize) -> Option<&str> {
    if quote_depth == 0 {
        return Some(line);
    }
    let active = active_markdown_line(line)?;
    let (line_quote_depth, content) = strip_blockquote_markers(active);
    (line_quote_depth == quote_depth).then_some(content)
}

pub(super) fn opening_html_block_line(trimmed: &str) -> Option<ActiveHtmlBlock> {
    let (quote_depth, content) = strip_blockquote_markers(trimmed);
    let content = active_markdown_line(content)?;
    opening_html_block_end(content).map(|(end, can_interrupt_paragraph)| ActiveHtmlBlock {
        end,
        quote_depth,
        can_interrupt_paragraph,
    })
}

pub(super) fn opening_html_block_end(line: &str) -> Option<(HtmlBlockEnd, bool)> {
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
    if html_block_tag(trimmed).is_some() {
        return Some((HtmlBlockEnd::BlankLine, true));
    }
    if starts_with_complete_html_tag_line(trimmed) {
        return Some((HtmlBlockEnd::BlankLine, false));
    }
    None
}

pub(super) fn can_start_html_block(
    active: ActiveHtmlBlock,
    paragraph_quote_depth: Option<usize>,
) -> bool {
    active.can_interrupt_paragraph || paragraph_quote_depth != Some(active.quote_depth)
}

pub(super) fn starts_with_html_open_tag(line: &str, tag: &str) -> bool {
    starts_with_html_tag(line, tag, false)
}

pub(super) fn html_block_tag(line: &str) -> Option<&'static str> {
    HTML_BLOCK_TAGS
        .iter()
        .find(|tag| starts_with_html_tag(line, tag, true))
        .copied()
}

pub(super) fn starts_with_html_tag(line: &str, tag: &str, allow_closing: bool) -> bool {
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

pub(super) fn starts_with_html_declaration(trimmed: &str) -> bool {
    let bytes = trimmed.as_bytes();
    bytes.len() >= 3 && bytes[0] == b'<' && bytes[1] == b'!' && bytes[2].is_ascii_alphabetic()
}

pub(super) fn starts_with_complete_html_tag_line(line: &str) -> bool {
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

pub(super) fn starts_with_ignore_ascii_case(value: &str, prefix: &str) -> bool {
    value.len() >= prefix.len()
        && value.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
}

pub(super) fn html_block_ends_on_line(line: &str, end: HtmlBlockEnd) -> bool {
    match end {
        HtmlBlockEnd::ClosingTag(tag) => contains_html_closing_tag(line, tag),
        HtmlBlockEnd::Contains(needle) => line.contains(needle),
        HtmlBlockEnd::BlankLine => line.trim().is_empty(),
    }
}

pub(super) fn contains_html_closing_tag(line: &str, tag: &str) -> bool {
    let needle = format!("</{tag}");
    let bytes = line.as_bytes();
    let needle = needle.as_bytes();
    let mut index = 0usize;
    while index + needle.len() <= bytes.len() {
        if bytes[index..index + needle.len()].eq_ignore_ascii_case(needle)
            && html_closing_tag_boundary(&bytes[index + needle.len()..])
        {
            return true;
        }
        index += 1;
    }
    false
}

pub(super) fn html_closing_tag_boundary(tail: &[u8]) -> bool {
    let mut index = 0usize;
    while tail
        .get(index)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        index += 1;
    }
    tail.get(index).copied() == Some(b'>')
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
