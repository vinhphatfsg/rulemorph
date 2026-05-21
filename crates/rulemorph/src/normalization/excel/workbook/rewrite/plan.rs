use quick_xml::events::Event;
use quick_xml::name::ResolveResult;
use quick_xml::reader::NsReader;

use crate::error::{TransformError, TransformErrorKind};

use super::super::super::invalid;
use super::super::OFFICE_RELATIONSHIPS_NS;

pub(super) enum CalamineRelationshipPrefix {
    R,
    Relationships,
}

impl CalamineRelationshipPrefix {
    pub(super) fn attr_name(&self) -> &'static str {
        match self {
            Self::R => "r:id",
            Self::Relationships => "relationships:id",
        }
    }

    pub(super) fn namespace_attr(&self) -> &'static str {
        match self {
            Self::R => "xmlns:r",
            Self::Relationships => "xmlns:relationships",
        }
    }
}

pub(super) struct WorkbookRewritePlan {
    pub(super) prefix: CalamineRelationshipPrefix,
    pub(super) add_namespace_attr: bool,
}

pub(super) fn workbook_rewrite_plan(
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

pub(super) fn is_calamine_relationship_attr(name: &[u8]) -> bool {
    matches!(name, b"r:id" | b"relationships:id")
}
