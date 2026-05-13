use std::collections::HashMap;

use quick_xml::Writer as XmlWriter;
use quick_xml::events::Event;
use quick_xml::name::ResolveResult;
use quick_xml::reader::{NsReader, Reader as XmlReader};

use crate::error::{TransformError, TransformErrorKind};
use crate::model::{ExcelInput, ExcelSheetRef};

use super::invalid;
use super::xml::local_name;

const OFFICE_RELATIONSHIPS_NS: &[u8] =
    b"http://schemas.openxmlformats.org/officeDocument/2006/relationships";

enum CalamineRelationshipPrefix {
    R,
    Relationships,
}

impl CalamineRelationshipPrefix {
    fn attr_name(&self) -> &'static str {
        match self {
            Self::R => "r:id",
            Self::Relationships => "relationships:id",
        }
    }

    fn namespace_attr(&self) -> &'static str {
        match self {
            Self::R => "xmlns:r",
            Self::Relationships => "xmlns:relationships",
        }
    }
}

struct WorkbookRewritePlan {
    prefix: CalamineRelationshipPrefix,
    add_namespace_attr: bool,
}

pub(super) fn rewrite_workbook_for_calamine(
    workbook_xml: &str,
) -> Result<Option<String>, TransformError> {
    let Some(plan) = workbook_rewrite_plan(workbook_xml)? else {
        return Ok(None);
    };

    let mut reader = NsReader::from_str(workbook_xml);
    reader.trim_text(false);
    let mut output = Vec::with_capacity(workbook_xml.len() + 128);
    let mut writer = XmlWriter::new(&mut output);
    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) => {
                let rewritten = rewrite_workbook_event(event.to_owned(), &reader, &plan)?;
                writer.write_event(Event::Start(rewritten)).map_err(|err| {
                    TransformError::new(
                        TransformErrorKind::InvalidInput,
                        format!("failed to rewrite Excel workbook XML: {}", err),
                    )
                })?;
            }
            Ok(Event::Empty(event)) => {
                let rewritten = rewrite_workbook_event(event.to_owned(), &reader, &plan)?;
                writer.write_event(Event::Empty(rewritten)).map_err(|err| {
                    TransformError::new(
                        TransformErrorKind::InvalidInput,
                        format!("failed to rewrite Excel workbook XML: {}", err),
                    )
                })?;
            }
            Ok(Event::Eof) => break,
            Ok(event) => {
                writer.write_event(event).map_err(|err| {
                    TransformError::new(
                        TransformErrorKind::InvalidInput,
                        format!("failed to rewrite Excel workbook XML: {}", err),
                    )
                })?;
            }
            Err(err) => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to parse Excel workbook XML: {}", err),
                ));
            }
        }
    }
    String::from_utf8(output).map(Some).map_err(|err| {
        invalid(format!(
            "failed to encode rewritten Excel workbook XML: {}",
            err
        ))
    })
}

fn workbook_rewrite_plan(
    workbook_xml: &str,
) -> Result<Option<WorkbookRewritePlan>, TransformError> {
    let mut reader = NsReader::from_str(workbook_xml);
    reader.trim_text(false);
    let mut needs_rewrite = false;
    let mut r_namespace = None;
    let mut relationships_namespace = None;
    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) | Ok(Event::Empty(event))
                if event.local_name().as_ref() == b"workbook" =>
            {
                for attr in event.attributes() {
                    let attr = attr.map_err(|err| {
                        TransformError::new(
                            TransformErrorKind::InvalidInput,
                            format!("failed to parse Excel workbook XML attribute: {}", err),
                        )
                    })?;
                    match attr.key.as_ref() {
                        b"xmlns:r" => r_namespace = Some(attr.value.as_ref().to_vec()),
                        b"xmlns:relationships" => {
                            relationships_namespace = Some(attr.value.as_ref().to_vec())
                        }
                        _ => {}
                    }
                }
            }
            Ok(Event::Start(event)) | Ok(Event::Empty(event))
                if event.local_name().as_ref() == b"sheet" =>
            {
                for attr in event.attributes() {
                    let attr = attr.map_err(|err| {
                        TransformError::new(
                            TransformErrorKind::InvalidInput,
                            format!("failed to parse Excel workbook XML attribute: {}", err),
                        )
                    })?;
                    let (namespace, local_name) = reader.resolve_attribute(attr.key);
                    if local_name.as_ref() == b"id"
                        && matches!(
                            namespace,
                            ResolveResult::Bound(namespace)
                                if namespace.as_ref() == OFFICE_RELATIONSHIPS_NS
                        )
                        && !is_calamine_relationship_attr(attr.key.as_ref())
                    {
                        needs_rewrite = true;
                    }
                }
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
    if !needs_rewrite {
        return Ok(None);
    }

    if r_namespace
        .as_deref()
        .is_none_or(|namespace| namespace == OFFICE_RELATIONSHIPS_NS)
    {
        return Ok(Some(WorkbookRewritePlan {
            prefix: CalamineRelationshipPrefix::R,
            add_namespace_attr: r_namespace.is_none(),
        }));
    }
    if relationships_namespace
        .as_deref()
        .is_none_or(|namespace| namespace == OFFICE_RELATIONSHIPS_NS)
    {
        return Ok(Some(WorkbookRewritePlan {
            prefix: CalamineRelationshipPrefix::Relationships,
            add_namespace_attr: relationships_namespace.is_none(),
        }));
    }
    Err(invalid(
        "Excel workbook relationship namespace conflicts with supported prefixes",
    ))
}

fn rewrite_workbook_event(
    mut event: quick_xml::events::BytesStart<'static>,
    reader: &NsReader<&[u8]>,
    plan: &WorkbookRewritePlan,
) -> Result<quick_xml::events::BytesStart<'static>, TransformError> {
    if event.local_name().as_ref() == b"workbook" && plan.add_namespace_attr {
        event.push_attribute((
            plan.prefix.namespace_attr().as_bytes(),
            OFFICE_RELATIONSHIPS_NS,
        ));
    }
    if event.local_name().as_ref() != b"sheet" {
        return Ok(event);
    }

    let mut relationship_value = None;
    let mut has_calamine_relationship_attr = false;
    for attr in event.attributes() {
        let attr = attr.map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to parse Excel workbook XML attribute: {}", err),
            )
        })?;
        let (namespace, local_name) = reader.resolve_attribute(attr.key);
        let is_office_relationship = matches!(
            namespace,
            ResolveResult::Bound(namespace) if namespace.as_ref() == OFFICE_RELATIONSHIPS_NS
        );
        if is_calamine_relationship_attr(attr.key.as_ref()) && !is_office_relationship {
            return Err(invalid(
                "Excel workbook sheet relationship uses an invalid namespace",
            ));
        }
        if local_name.as_ref() == b"id" && is_office_relationship {
            if is_calamine_relationship_attr(attr.key.as_ref()) {
                has_calamine_relationship_attr = true;
            }
            relationship_value = Some(attr.value.as_ref().to_vec());
        }
    }
    if !has_calamine_relationship_attr && let Some(value) = relationship_value {
        event.push_attribute((plan.prefix.attr_name().as_bytes(), value.as_slice()));
    }
    Ok(event)
}

fn is_calamine_relationship_attr(name: &[u8]) -> bool {
    matches!(name, b"r:id" | b"relationships:id")
}

struct WorkbookSheet {
    name: String,
    relationship_id: String,
}

pub(super) fn selected_worksheet_path(
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
