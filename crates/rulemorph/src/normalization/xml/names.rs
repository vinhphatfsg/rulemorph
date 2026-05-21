use quick_xml::events::BytesStart;
use quick_xml::name::ResolveResult;
use quick_xml::reader::NsReader as XmlReader;
use std::collections::{BTreeMap, HashMap};

use crate::error::{TransformError, TransformErrorKind};
use crate::model::{XmlInput, XmlNamespacePolicy};

use super::invalid;
use super::shape::{XmlAttribute, XmlNode};

pub(super) fn start_node(
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

pub(super) fn enforce_namespace_rebinding(
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

fn xml_err(err: impl std::fmt::Display) -> TransformError {
    TransformError::new(
        TransformErrorKind::InvalidInput,
        format!("failed to parse XML input: {}", err),
    )
}
