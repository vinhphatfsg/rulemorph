use super::*;

pub(super) fn plain_text(node: Node<'_>) -> String {
    let mut out = String::new();
    collect_plain_text(node, &mut out);
    out
}

fn collect_plain_text(node: Node<'_>, out: &mut String) {
    let separate_children = {
        let data = node.data.borrow();
        matches!(
            data.value,
            NodeValue::Document
                | NodeValue::BlockQuote
                | NodeValue::List(_)
                | NodeValue::Item(_)
                | NodeValue::TaskItem(_)
                | NodeValue::Table(_)
                | NodeValue::TableRow(_)
        )
    };
    {
        let data = node.data.borrow();
        match &data.value {
            NodeValue::Text(text) => out.push_str(text),
            NodeValue::Code(code) => out.push_str(&code.literal),
            NodeValue::CodeBlock(code) => out.push_str(code.literal.trim_end_matches('\n')),
            NodeValue::SoftBreak | NodeValue::LineBreak => out.push(' '),
            NodeValue::HtmlBlock(html) => out.push_str(&html_to_text(&html.literal)),
            NodeValue::HtmlInline(html) => out.push_str(&html_to_text(html)),
            _ => {}
        }
    }
    let mut first_child = true;
    for child in node.children() {
        if separate_children && !first_child && !out.ends_with(char::is_whitespace) {
            out.push(' ');
        }
        let before = out.len();
        collect_plain_text(child, out);
        if separate_children && before != out.len() && !out.ends_with(char::is_whitespace) {
            out.push(' ');
        }
        first_child = false;
    }
}

pub(super) fn push_body_text(out: &mut String, value: &str, markdown: &MarkdownInput) {
    let value = normalize_text(value, markdown);
    if value.is_empty() {
        return;
    }
    if !out.is_empty() && !out.ends_with(char::is_whitespace) && !value.starts_with(no_space_before)
    {
        out.push(' ');
    }
    out.push_str(&value);
}

pub(super) fn normalize_text(value: &str, markdown: &MarkdownInput) -> String {
    let value = if markdown.trim_text {
        value.trim()
    } else {
        value
    };
    if markdown.collapse_whitespace {
        value.split_whitespace().collect::<Vec<_>>().join(" ")
    } else {
        value.to_string()
    }
}

pub(super) fn html_to_text(value: &str) -> String {
    let mut out = String::new();
    let mut in_tag = false;
    for ch in value.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out
}

pub(super) fn normalize_raw_html(value: &str, markdown: &MarkdownInput) -> String {
    if markdown.trim_text {
        value.trim().to_string()
    } else {
        value.to_string()
    }
}

pub(super) fn no_space_before(ch: char) -> bool {
    matches!(ch, '.' | ',' | ';' | ':' | '?' | '!' | ')' | ']')
}
