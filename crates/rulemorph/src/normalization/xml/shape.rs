use serde_json::{Map, Value as JsonValue};
use std::collections::BTreeMap;

use crate::error::TransformError;
use crate::model::XmlInput;

use super::{NormalizationOptions, invalid};

#[derive(Debug)]
pub(super) struct XmlNode {
    pub(super) name: String,
    pub(super) canonical_name: String,
    pub(super) attributes: Vec<XmlAttribute>,
    pub(super) text: String,
    pub(super) children: Vec<XmlNode>,
}

#[derive(Debug)]
pub(super) struct XmlAttribute {
    pub(super) key: String,
    pub(super) canonical_name: String,
    pub(super) value: String,
}

pub(super) fn xml_node_to_json(
    node: &XmlNode,
    xml: &XmlInput,
    options: &NormalizationOptions,
    depth: usize,
) -> Result<JsonValue, TransformError> {
    if depth > options.max_depth {
        return Err(invalid("input exceeds max_depth"));
    }
    let mut object = Map::new();
    let mut inserted_attributes = BTreeMap::<String, String>::new();
    for attribute in &node.attributes {
        if let Some(previous) =
            inserted_attributes.insert(attribute.key.clone(), attribute.canonical_name.clone())
            && previous != attribute.canonical_name
        {
            return Err(invalid("XML attribute namespace collision"));
        }
        checked_insert(
            &mut object,
            attribute.key.clone(),
            JsonValue::String(attribute.value.clone()),
        )?;
    }
    let text = normalize_text(&node.text, xml);
    if !text.is_empty() {
        checked_insert(&mut object, xml.text_key.clone(), JsonValue::String(text))?;
    }
    let mut child_groups = BTreeMap::<String, (String, Vec<JsonValue>)>::new();
    for child in &node.children {
        let entry = child_groups
            .entry(child.name.clone())
            .or_insert_with(|| (child.canonical_name.clone(), Vec::new()));
        if entry.0 != child.canonical_name {
            return Err(invalid("XML namespace collision"));
        }
        entry
            .1
            .push(xml_node_to_json(child, xml, options, depth + 1)?);
    }
    for (key, (_qualified_name, values)) in child_groups {
        if values.len() > options.max_array_len {
            return Err(invalid("input exceeds max_array_len"));
        }
        checked_insert(&mut object, key, JsonValue::Array(values))?;
    }
    Ok(JsonValue::Object(object))
}

fn normalize_text(value: &str, xml: &XmlInput) -> String {
    let value = if xml.trim_text { value.trim() } else { value };
    if xml.collapse_whitespace {
        value.split_whitespace().collect::<Vec<_>>().join(" ")
    } else {
        value.to_string()
    }
}

fn checked_insert(
    object: &mut Map<String, JsonValue>,
    key: String,
    value: JsonValue,
) -> Result<(), TransformError> {
    if object.insert(key, value).is_some() {
        return Err(invalid("XML namespace or key collision"));
    }
    Ok(())
}
