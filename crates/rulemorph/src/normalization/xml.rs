use quick_xml::events::{BytesStart, Event};
use quick_xml::name::ResolveResult;
use quick_xml::reader::NsReader as XmlReader;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap};

use crate::error::{TransformError, TransformErrorKind};
use crate::model::{RuleFile, XmlInput, XmlNamespacePolicy};
use crate::xml_name::is_xml_name;

use super::{NormalizationOptions, enforce_json_limits, enforce_records_limit};
use shape::{XmlAttribute, XmlNode, select_xml_records, xml_node_to_json};

mod shape;

pub fn normalize_xml_records(
    rule: &RuleFile,
    input: &str,
    options: &NormalizationOptions,
) -> Result<Vec<JsonValue>, TransformError> {
    let xml = rule.input.xml.as_ref().ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            "input.xml is required when format=xml",
        )
    })?;
    let root = parse_xml_tree(input, xml, options)?;
    let path = parse_xml_records_path(&xml.records_path)?;
    let mut selected = Vec::new();
    select_xml_records(&root, &path, &mut selected);
    if selected.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidRecordsPath,
            "xml.records_path does not match any elements",
        )
        .with_path("input.xml.records_path"));
    }
    let mut records = Vec::with_capacity(selected.len());
    for node in selected {
        records.push(xml_node_to_json(node, xml, options, 0)?);
        enforce_records_limit(records.len(), options)?;
    }
    for record in &records {
        enforce_json_limits(record, options)?;
    }
    Ok(records)
}

fn parse_xml_records_path(path: &str) -> Result<Vec<&str>, TransformError> {
    if path.is_empty()
        || path.contains('[')
        || path.contains(']')
        || !path.split('.').all(is_xml_name)
    {
        return Err(TransformError::new(
            TransformErrorKind::InvalidRecordsPath,
            "xml.records_path must be a dot-separated element path",
        )
        .with_path("input.xml.records_path"));
    }
    Ok(path.split('.').collect())
}

fn parse_xml_tree(
    input: &str,
    xml: &XmlInput,
    options: &NormalizationOptions,
) -> Result<XmlNode, TransformError> {
    let mut reader = XmlReader::from_str(input);
    reader.trim_text(false);
    let mut stack = Vec::new();
    let mut root = None;
    let mut node_count = 0usize;
    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) => {
                node_count = node_count.saturating_add(1);
                enforce_xml_node_count(node_count, options)?;
                if stack.len() >= options.max_depth {
                    return Err(invalid("input exceeds max_depth"));
                }
                enforce_namespace_rebinding(&event, &reader)?;
                stack.push(start_node(&event, xml, &reader)?);
            }
            Ok(Event::Empty(event)) => {
                node_count = node_count.saturating_add(1);
                enforce_xml_node_count(node_count, options)?;
                if stack.len() >= options.max_depth {
                    return Err(invalid("input exceeds max_depth"));
                }
                enforce_namespace_rebinding(&event, &reader)?;
                let node = start_node(&event, xml, &reader)?;
                attach_node(node, &mut stack, &mut root)?;
            }
            Ok(Event::Text(event)) => {
                let text = event.unescape().map_err(xml_err)?.into_owned();
                append_text(&mut stack, text, options)?;
            }
            Ok(Event::CData(event)) => {
                let text = String::from_utf8(event.into_inner().into_owned()).map_err(|err| {
                    TransformError::new(
                        TransformErrorKind::InvalidInput,
                        format!("failed to parse XML CDATA: {}", err),
                    )
                })?;
                append_text(&mut stack, text, options)?;
            }
            Ok(Event::End(_)) => {
                let node = stack
                    .pop()
                    .ok_or_else(|| invalid("XML close tag without matching start tag"))?;
                attach_node(node, &mut stack, &mut root)?;
            }
            Ok(Event::Decl(_)) | Ok(Event::Comment(_)) => {}
            Ok(Event::DocType(_)) => return Err(invalid("XML DTD is not supported")),
            Ok(Event::PI(_)) => {
                return Err(invalid("XML processing instructions are not supported"));
            }
            Ok(Event::Eof) => break,
            Err(err) => return Err(xml_err(err)),
        }
    }
    if !stack.is_empty() {
        return Err(invalid(
            "XML document ended before all elements were closed",
        ));
    }
    root.ok_or_else(|| invalid("XML document has no root element"))
}

fn start_node(
    event: &BytesStart<'_>,
    xml: &XmlInput,
    reader: &XmlReader<&[u8]>,
) -> Result<XmlNode, TransformError> {
    let (name, canonical_name) = normalize_element_name(event, xml, reader)?;
    let mut attributes = Vec::new();
    let mut seen = BTreeMap::<String, String>::new();
    for attr in event.attributes() {
        let attr = attr.map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to parse XML attribute: {}", err),
            )
        })?;
        if is_xmlns_declaration(attr.key.as_ref()) {
            continue;
        }
        let (name, canonical_name) = normalize_attribute_name(attr.key, xml, reader)?;
        let key = format!("{}{}", xml.attr_prefix, name);
        if let Some(previous) = seen.insert(key.clone(), canonical_name.clone()) {
            if previous != canonical_name {
                return Err(invalid("XML attribute namespace collision"));
            }
            return Err(invalid("XML duplicate attribute key"));
        }
        let value = attr
            .decode_and_unescape_value(reader)
            .map_err(xml_err)?
            .into_owned();
        attributes.push(XmlAttribute {
            key,
            canonical_name,
            value,
        });
    }
    Ok(XmlNode {
        name,
        canonical_name,
        attributes,
        text: String::new(),
        children: Vec::new(),
    })
}

fn normalize_element_name(
    event: &BytesStart<'_>,
    xml: &XmlInput,
    reader: &XmlReader<&[u8]>,
) -> Result<(String, String), TransformError> {
    let raw = raw_name(event.name().as_ref())?;
    let (namespace, local) = reader.resolve_element(event.name());
    normalize_resolved_name(raw, namespace, local.as_ref(), xml)
}

fn normalize_attribute_name(
    name: quick_xml::name::QName<'_>,
    xml: &XmlInput,
    reader: &XmlReader<&[u8]>,
) -> Result<(String, String), TransformError> {
    let raw = raw_name(name.as_ref())?;
    let (namespace, local) = reader.resolve_attribute(name);
    normalize_resolved_name(raw, namespace, local.as_ref(), xml)
}

fn normalize_resolved_name(
    raw: String,
    namespace: ResolveResult<'_>,
    local: &[u8],
    xml: &XmlInput,
) -> Result<(String, String), TransformError> {
    let local = raw_name(local)?;
    let namespace = namespace_uri(namespace)?;
    let visible_name = match xml.namespaces {
        XmlNamespacePolicy::Qualified => raw,
        XmlNamespacePolicy::Strip => local.clone(),
    };
    Ok((visible_name, canonical_name(namespace.as_deref(), &local)))
}

fn namespace_uri(namespace: ResolveResult<'_>) -> Result<Option<String>, TransformError> {
    match namespace {
        ResolveResult::Unbound => Ok(None),
        ResolveResult::Bound(namespace) => Ok(Some(raw_name(namespace.as_ref())?)),
        ResolveResult::Unknown(prefix) => Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            format!(
                "XML namespace prefix is not declared: {}",
                String::from_utf8_lossy(&prefix)
            ),
        )),
    }
}

fn canonical_name(namespace: Option<&str>, local: &str) -> String {
    match namespace {
        Some(namespace) => format!("{{{}}}{}", namespace, local),
        None => local.to_string(),
    }
}

fn enforce_namespace_rebinding(
    event: &BytesStart<'_>,
    reader: &XmlReader<&[u8]>,
) -> Result<(), TransformError> {
    let mut namespace_bindings = HashMap::new();
    for attr in event.attributes() {
        let attr = attr.map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to parse XML attribute: {}", err),
            )
        })?;
        let key = attr.key.as_ref();
        if !is_xmlns_declaration(key) {
            continue;
        }
        let prefix = namespace_prefix(key)?;
        let value = attr
            .decode_and_unescape_value(reader)
            .map_err(xml_err)?
            .into_owned();
        if let Some(previous) = namespace_bindings.insert(prefix.clone(), value.clone())
            && previous != value
        {
            return Err(invalid(
                "XML namespace declaration conflicts on the same element",
            ));
        }
    }
    Ok(())
}

fn is_xmlns_declaration(key: &[u8]) -> bool {
    key == b"xmlns" || key.starts_with(b"xmlns:")
}

fn namespace_prefix(key: &[u8]) -> Result<String, TransformError> {
    if key == b"xmlns" {
        return Ok(String::new());
    }
    raw_name(&key[b"xmlns:".len()..])
}

fn raw_name(raw: &[u8]) -> Result<String, TransformError> {
    std::str::from_utf8(raw)
        .map(|value| value.to_string())
        .map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("XML name is not valid UTF-8: {}", err),
            )
        })
}

fn append_text(
    stack: &mut [XmlNode],
    text: String,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    let Some(current) = stack.last_mut() else {
        if text.trim().is_empty() {
            return Ok(());
        }
        return Err(invalid("XML text outside root element"));
    };
    if current
        .text
        .len()
        .checked_add(text.len())
        .is_none_or(|len| len > options.max_text_bytes)
    {
        return Err(invalid("input exceeds max_text_bytes"));
    }
    current.text.push_str(&text);
    Ok(())
}

fn attach_node(
    node: XmlNode,
    stack: &mut [XmlNode],
    root: &mut Option<XmlNode>,
) -> Result<(), TransformError> {
    if let Some(parent) = stack.last_mut() {
        parent.children.push(node);
        return Ok(());
    }
    if root.is_some() {
        return Err(invalid("XML document must have a single root element"));
    }
    *root = Some(node);
    Ok(())
}

fn enforce_xml_node_count(
    count: usize,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    if count > options.max_xml_nodes {
        return Err(invalid("input exceeds max_xml_nodes"));
    }
    Ok(())
}

fn xml_err(err: impl std::fmt::Display) -> TransformError {
    TransformError::new(
        TransformErrorKind::InvalidInput,
        format!("failed to parse XML input: {}", err),
    )
}

fn invalid(message: impl Into<String>) -> TransformError {
    TransformError::new(TransformErrorKind::InvalidInput, message)
}
