use quick_xml::Writer as XmlWriter;
use quick_xml::events::Event;
use quick_xml::name::ResolveResult;
use quick_xml::reader::NsReader;

use crate::error::{TransformError, TransformErrorKind};

use super::super::invalid;
use super::OFFICE_RELATIONSHIPS_NS;

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

pub(in crate::normalization::excel) fn rewrite_workbook_for_calamine(
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
