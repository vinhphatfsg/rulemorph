use serde_json::Value as JsonValue;

use crate::error::{TransformError, TransformErrorKind};
use crate::model::RuleFile;

use super::{NormalizationOptions, enforce_json_limits, enforce_records_limit};
use parser::parse_xml_tree;
use records_path::{parse_xml_records_path, select_xml_records};
use shape::xml_node_to_json;

mod names;
mod parser;
mod records_path;
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

fn invalid(message: impl Into<String>) -> TransformError {
    TransformError::new(TransformErrorKind::InvalidInput, message)
}
