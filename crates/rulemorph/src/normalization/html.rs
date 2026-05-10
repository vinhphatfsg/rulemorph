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
    enforce_html_tag_preflight(input, options)?;
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

fn enforce_html_tag_preflight(
    input: &str,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    let mut tags = 0usize;
    let bytes = input.as_bytes();
    for window in bytes.windows(2) {
        if window[0] != b'<' {
            continue;
        }
        let next = window[1];
        if next == b'/' || next.is_ascii_whitespace() {
            continue;
        }
        if next.is_ascii_alphabetic() || next == b'!' || next == b'?' {
            tags = tags.saturating_add(1);
            if tags > options.max_html_nodes {
                return Err(invalid("input exceeds max_html_nodes"));
            }
        }
    }
    Ok(())
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
