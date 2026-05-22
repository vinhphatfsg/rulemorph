use crate::error::{TransformError, TransformErrorKind};
use crate::xml_name::is_xml_name;

use super::shape::XmlNode;

pub(super) fn parse_xml_records_path(path: &str) -> Result<Vec<&str>, TransformError> {
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

pub(super) fn select_xml_records<'a>(
    node: &'a XmlNode,
    path: &[&str],
    selected: &mut Vec<&'a XmlNode>,
) {
    if path.is_empty() {
        selected.push(node);
        return;
    }
    if node.name != path[0] {
        return;
    }
    if path.len() == 1 {
        selected.push(node);
        return;
    }
    for child in &node.children {
        select_xml_records(child, &path[1..], selected);
    }
}
