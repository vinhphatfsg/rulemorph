use serde_json::Value as JsonValue;

use crate::error::{TransformError, TransformErrorKind};
use crate::model::RuleFile;
use crate::xml_name::is_xml_name;

use super::{NormalizationOptions, enforce_json_limits, enforce_records_limit};
use parser::parse_xml_tree;
use shape::{select_xml_records, xml_node_to_json};

mod names;
mod parser;
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

fn invalid(message: impl Into<String>) -> TransformError {
    TransformError::new(TransformErrorKind::InvalidInput, message)
}
