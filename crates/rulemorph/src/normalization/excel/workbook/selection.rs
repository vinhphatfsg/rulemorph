use std::collections::HashMap;

use quick_xml::events::Event;
use quick_xml::name::ResolveResult;
use quick_xml::reader::{NsReader, Reader as XmlReader};

use crate::error::{TransformError, TransformErrorKind};
use crate::model::{ExcelInput, ExcelSheetRef};

use super::super::invalid;
use super::super::xml::local_name;
use super::OFFICE_RELATIONSHIPS_NS;

struct WorkbookSheet {
    name: String,
    relationship_id: String,
}

pub(in crate::normalization::excel) fn selected_worksheet_path(
    workbook_xml: &str,
    workbook_rels: &str,
    excel: &ExcelInput,
) -> Result<String, TransformError> {
    let sheets = parse_workbook_sheets(workbook_xml)?;
    let relationships = parse_workbook_relationships(workbook_rels)?;
    let selected = match &excel.sheet {
        Some(ExcelSheetRef::Name(name)) => sheets
            .iter()
            .find(|sheet| sheet.name == *name)
            .ok_or_else(|| invalid("Excel sheet was not found"))?,
        Some(ExcelSheetRef::Index(index)) => sheets
            .get(*index)
            .ok_or_else(|| invalid("Excel sheet index is out of range"))?,
        None => sheets
            .first()
            .ok_or_else(|| invalid("Excel workbook has no sheets"))?,
    };
    relationships
        .get(&selected.relationship_id)
        .cloned()
        .ok_or_else(|| invalid("Excel selected sheet relationship was not found"))
}

fn parse_workbook_sheets(workbook_xml: &str) -> Result<Vec<WorkbookSheet>, TransformError> {
    let mut reader = NsReader::from_str(workbook_xml);
    reader.trim_text(false);
    let mut sheets = Vec::new();
    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) | Ok(Event::Empty(event))
                if event.local_name().as_ref() == b"sheet" =>
            {
                let mut name = None;
                let mut relationship_id = None;
                for attr in event.attributes() {
                    let attr = attr.map_err(|err| {
                        TransformError::new(
                            TransformErrorKind::InvalidInput,
                            format!("failed to parse Excel workbook XML attribute: {}", err),
                        )
                    })?;
                    match attr.key.as_ref() {
                        b"name" => {
                            name = Some(
                                attr.decode_and_unescape_value(&reader)
                                    .map_err(|err| {
                                        TransformError::new(
                                            TransformErrorKind::InvalidInput,
                                            format!(
                                                "failed to decode Excel workbook sheet name: {}",
                                                err
                                            ),
                                        )
                                    })?
                                    .into_owned(),
                            )
                        }
                        _ => {
                            let (namespace, local_name) = reader.resolve_attribute(attr.key);
                            if local_name.as_ref() == b"id"
                                && matches!(
                                    namespace,
                                    ResolveResult::Bound(namespace)
                                        if namespace.as_ref() == OFFICE_RELATIONSHIPS_NS
                                )
                            {
                                if relationship_id.is_some() {
                                    return Err(invalid(
                                        "Excel workbook sheet has multiple relationships",
                                    ));
                                }
                                relationship_id = Some(
                                    attr.decode_and_unescape_value(&reader)
                                        .map_err(|err| {
                                            TransformError::new(
                                                TransformErrorKind::InvalidInput,
                                                format!(
                                                    "failed to decode Excel workbook sheet relationship: {}",
                                                    err
                                                ),
                                            )
                                        })?
                                        .into_owned(),
                                )
                            }
                        }
                    }
                }
                let name = name.ok_or_else(|| invalid("Excel workbook sheet is missing name"))?;
                let relationship_id = relationship_id
                    .ok_or_else(|| invalid("Excel workbook sheet is missing relationship"))?;
                sheets.push(WorkbookSheet {
                    name,
                    relationship_id,
                });
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(err) => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to parse Excel workbook XML: {}", err),
                ));
            }
        }
    }
    Ok(sheets)
}

fn parse_workbook_relationships(
    workbook_rels: &str,
) -> Result<HashMap<String, String>, TransformError> {
    let mut reader = XmlReader::from_str(workbook_rels);
    reader.trim_text(false);
    let mut relationships = HashMap::new();
    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) | Ok(Event::Empty(event))
                if local_name(event.name().as_ref()) == b"Relationship" =>
            {
                let mut id = None;
                let mut target = None;
                let mut relationship_type = None;
                for attr in event.attributes() {
                    let attr = attr.map_err(|err| {
                        TransformError::new(
                            TransformErrorKind::InvalidInput,
                            format!("failed to parse Excel workbook relationship: {}", err),
                        )
                    })?;
                    match local_name(attr.key.as_ref()) {
                        b"Id" => {
                            id = Some(String::from_utf8_lossy(attr.value.as_ref()).to_string())
                        }
                        b"Target" => {
                            target = Some(String::from_utf8_lossy(attr.value.as_ref()).to_string())
                        }
                        b"Type" => {
                            relationship_type =
                                Some(String::from_utf8_lossy(attr.value.as_ref()).to_string())
                        }
                        _ => {}
                    }
                }
                if relationship_type
                    .as_deref()
                    .is_some_and(|value| value.ends_with("/worksheet"))
                {
                    let id =
                        id.ok_or_else(|| invalid("Excel worksheet relationship is missing id"))?;
                    let target = target
                        .ok_or_else(|| invalid("Excel worksheet relationship is missing target"))?;
                    relationships.insert(id, resolve_workbook_relationship_target(&target)?);
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(err) => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to parse Excel workbook relationships: {}", err),
                ));
            }
        }
    }
    Ok(relationships)
}

fn resolve_workbook_relationship_target(target: &str) -> Result<String, TransformError> {
    if target.contains("..") || target.contains('\\') {
        return Err(invalid("Excel worksheet relationship target is invalid"));
    }
    let target = target.trim_start_matches('/');
    if target.is_empty() {
        return Err(invalid("Excel worksheet relationship target is invalid"));
    }
    if target.starts_with("xl/") {
        if target.starts_with("xl/worksheets/") && target.ends_with(".xml") {
            Ok(target.to_string())
        } else {
            Err(invalid("Excel worksheet relationship target is invalid"))
        }
    } else if target.starts_with("worksheets/") && target.ends_with(".xml") {
        Ok(format!("xl/{target}"))
    } else {
        Err(invalid("Excel worksheet relationship target is invalid"))
    }
}
