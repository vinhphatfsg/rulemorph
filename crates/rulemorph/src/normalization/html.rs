use scraper::{ElementRef, Html, Selector};
use serde_json::{Map, Value as JsonValue};

use crate::error::{TransformError, TransformErrorKind};
use crate::model::{HtmlInput, HtmlValueKind, RuleFile};

use super::{NormalizationOptions, enforce_json_limits, enforce_records_limit};

struct CompiledHtmlField {
    name: String,
    selector: Option<Selector>,
    value: HtmlValueKind,
    attr: Option<String>,
    multiple: bool,
}

pub fn normalize_html_records(
    rule: &RuleFile,
    input: &str,
    options: &NormalizationOptions,
) -> Result<Vec<JsonValue>, TransformError> {
    let html = rule.input.html.as_ref().ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            "input.html is required when format=html",
        )
    })?;
    let records_selector = parse_selector(&html.records_selector, "input.html.records_selector")?;
    let fields = compile_fields(html)?;
    enforce_html_structural_preflight(input, options)?;
    let document = Html::parse_fragment(input);
    enforce_html_node_count(document.tree.nodes().count(), options)?;

    let mut records = Vec::new();
    for record_element in document.select(&records_selector) {
        let record = record_to_json(record_element, &fields, html, options)?;
        enforce_json_limits(&record, options)?;
        records.push(record);
        enforce_records_limit(records.len(), options)?;
    }
    Ok(records)
}

fn compile_fields(html: &HtmlInput) -> Result<Vec<CompiledHtmlField>, TransformError> {
    let mut fields = Vec::with_capacity(html.fields.len());
    for (name, field) in &html.fields {
        fields.push(CompiledHtmlField {
            name: name.clone(),
            selector: field
                .selector
                .as_deref()
                .map(|selector| parse_selector(selector, "input.html.fields.*.selector"))
                .transpose()?,
            value: field.value,
            attr: field.attr.clone(),
            multiple: field.multiple,
        });
    }
    Ok(fields)
}

fn parse_selector(selector: &str, path: &'static str) -> Result<Selector, TransformError> {
    Selector::parse(selector).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to parse HTML selector: {:?}", err),
        )
        .with_path(path)
    })
}

fn record_to_json(
    record_element: ElementRef<'_>,
    fields: &[CompiledHtmlField],
    html: &HtmlInput,
    options: &NormalizationOptions,
) -> Result<JsonValue, TransformError> {
    let mut object = Map::new();
    for field in fields {
        if field.multiple {
            let mut values = Vec::new();
            extract_multiple_values(record_element, field, html, options, &mut values)?;
            object.insert(field.name.clone(), JsonValue::Array(values));
            continue;
        }
        if let Some(element) = first_field_element(record_element, field)
            && let Some(value) = extract_value(element, field, html, options)?
        {
            object.insert(field.name.clone(), JsonValue::String(value));
        }
    }
    Ok(JsonValue::Object(object))
}

fn first_field_element<'a>(
    record_element: ElementRef<'a>,
    field: &CompiledHtmlField,
) -> Option<ElementRef<'a>> {
    match &field.selector {
        Some(selector) => record_element.select(selector).next(),
        None => Some(record_element),
    }
}

fn extract_multiple_values(
    record_element: ElementRef<'_>,
    field: &CompiledHtmlField,
    html: &HtmlInput,
    options: &NormalizationOptions,
    values: &mut Vec<JsonValue>,
) -> Result<(), TransformError> {
    match &field.selector {
        Some(selector) => {
            for element in record_element.select(selector) {
                push_extracted_value(element, field, html, options, values)?;
            }
        }
        None => push_extracted_value(record_element, field, html, options, values)?,
    }
    Ok(())
}

fn push_extracted_value(
    element: ElementRef<'_>,
    field: &CompiledHtmlField,
    html: &HtmlInput,
    options: &NormalizationOptions,
    values: &mut Vec<JsonValue>,
) -> Result<(), TransformError> {
    if let Some(value) = extract_value(element, field, html, options)? {
        values.push(JsonValue::String(value));
        if values.len() > options.max_array_len {
            return Err(invalid("input exceeds max_array_len"));
        }
    }
    Ok(())
}

fn extract_value(
    element: ElementRef<'_>,
    field: &CompiledHtmlField,
    html: &HtmlInput,
    options: &NormalizationOptions,
) -> Result<Option<String>, TransformError> {
    let value = match field.value {
        HtmlValueKind::Text => normalize_text(&element.text().collect::<Vec<_>>().join(""), html),
        HtmlValueKind::Html => element.inner_html(),
        HtmlValueKind::Attr => {
            let attr = field.attr.as_deref().ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::InvalidInput,
                    "html field attr is required when value=attr",
                )
            })?;
            let Some(value) = element.attr(attr) else {
                return Ok(None);
            };
            normalize_text(value, html)
        }
    };
    if value.len() > options.max_text_bytes {
        return Err(invalid("input exceeds max_text_bytes"));
    }
    Ok(Some(value))
}

fn enforce_html_structural_preflight(
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

fn normalize_text(value: &str, html: &HtmlInput) -> String {
    let value = if html.trim_text { value.trim() } else { value };
    if html.collapse_whitespace {
        value.split_whitespace().collect::<Vec<_>>().join(" ")
    } else {
        value.to_string()
    }
}

fn enforce_html_node_count(
    count: usize,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    if count > options.max_html_nodes {
        return Err(invalid("input exceeds max_html_nodes"));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> TransformError {
    TransformError::new(TransformErrorKind::InvalidInput, message)
}
