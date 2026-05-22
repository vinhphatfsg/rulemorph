use quick_xml::events::Event;
use quick_xml::reader::NsReader as XmlReader;

use crate::error::{TransformError, TransformErrorKind};
use crate::model::XmlInput;

use super::super::NormalizationOptions;
use super::invalid;
use super::names::{enforce_namespace_rebinding, start_node};
use super::shape::XmlNode;

pub(super) fn parse_xml_tree(
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
