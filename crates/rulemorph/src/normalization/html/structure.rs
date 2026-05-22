use crate::error::TransformError;

use super::{NormalizationOptions, invalid};

pub(super) fn enforce_html_structural_preflight(
    input: &str,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    let mut scanner = HtmlStructureScanner::new(input);
    let mut nodes = 0usize;
    while let Some(token) = scanner.next_token() {
        match token {
            HtmlToken::StartTag { name, raw_text } => {
                nodes = nodes.saturating_add(1);
                enforce_html_node_count(nodes, options)?;
                if raw_text {
                    scanner.skip_raw_text_element(name);
                }
            }
            HtmlToken::Markup => {
                nodes = nodes.saturating_add(1);
                enforce_html_node_count(nodes, options)?;
            }
            HtmlToken::EndTag => {}
        }
    }
    Ok(())
}

enum HtmlToken<'a> {
    StartTag { name: &'a str, raw_text: bool },
    EndTag,
    Markup,
}

struct HtmlStructureScanner<'a> {
    input: &'a str,
    offset: usize,
}

impl<'a> HtmlStructureScanner<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, offset: 0 }
    }

    fn next_token(&mut self) -> Option<HtmlToken<'a>> {
        while self.offset < self.input.len() {
            let relative = self.input[self.offset..].find('<')?;
            let start = self.offset + relative;
            self.offset = start + 1;
            let rest = &self.input[self.offset..];
            let Some(next) = rest.as_bytes().first().copied() else {
                return None;
            };
            match next {
                b'!' => {
                    self.offset = self.find_tag_end(self.offset).unwrap_or(self.input.len());
                    return Some(HtmlToken::Markup);
                }
                b'?' => {
                    self.offset = self.find_tag_end(self.offset).unwrap_or(self.input.len());
                    return Some(HtmlToken::Markup);
                }
                b'/' => {
                    self.offset = self.find_tag_end(self.offset).unwrap_or(self.input.len());
                    return Some(HtmlToken::EndTag);
                }
                value if is_html_name_start(value) => {
                    let name_start = self.offset;
                    let mut name_end = name_start;
                    while name_end < self.input.len()
                        && is_html_name_char(self.input.as_bytes()[name_end])
                    {
                        name_end += 1;
                    }
                    let raw_text = is_raw_text_element(&self.input[name_start..name_end]);
                    self.offset = self.find_tag_end(name_end).unwrap_or(self.input.len());
                    return Some(HtmlToken::StartTag {
                        name: &self.input[name_start..name_end],
                        raw_text,
                    });
                }
                _ => {}
            }
        }
        None
    }

    fn skip_raw_text_element(&mut self, name: &str) {
        let mut search_from = self.offset;
        while search_from < self.input.len() {
            let Some(relative) = self.input[search_from..].find("</") else {
                self.offset = self.input.len();
                return;
            };
            let tag_start = search_from + relative;
            let name_start = tag_start + 2;
            let name_end = name_start.saturating_add(name.len());
            if name_end <= self.input.len()
                && self.input[name_start..name_end].eq_ignore_ascii_case(name)
                && self
                    .input
                    .as_bytes()
                    .get(name_end)
                    .is_none_or(|value| !is_html_name_char(*value))
            {
                self.offset = self.find_tag_end(name_end).unwrap_or(self.input.len());
                return;
            }
            search_from = name_start;
        }
        self.offset = self.input.len();
    }

    fn find_tag_end(&self, start: usize) -> Option<usize> {
        let bytes = self.input.as_bytes();
        let mut index = start;
        let mut quote = None;
        while index < bytes.len() {
            let byte = bytes[index];
            if let Some(active_quote) = quote {
                if byte == active_quote {
                    quote = None;
                }
            } else if byte == b'"' || byte == b'\'' {
                quote = Some(byte);
            } else if byte == b'>' {
                return Some(index + 1);
            }
            index += 1;
        }
        None
    }
}

fn is_html_name_start(value: u8) -> bool {
    value.is_ascii_alphabetic()
}

fn is_html_name_char(value: u8) -> bool {
    value.is_ascii_alphanumeric() || matches!(value, b'-' | b'_' | b':')
}

fn is_raw_text_element(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "script" | "style" | "textarea" | "title"
    )
}

pub(super) fn enforce_html_node_count(
    count: usize,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    if count > options.max_html_nodes {
        return Err(invalid("input exceeds max_html_nodes"));
    }
    Ok(())
}
