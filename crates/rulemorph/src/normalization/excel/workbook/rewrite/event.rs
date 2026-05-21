use quick_xml::events::BytesStart;
use quick_xml::name::ResolveResult;
use quick_xml::reader::NsReader;

use crate::error::{TransformError, TransformErrorKind};

use super::super::super::invalid;
use super::super::OFFICE_RELATIONSHIPS_NS;
use super::plan::{WorkbookRewritePlan, is_calamine_relationship_attr};

pub(super) fn rewrite_workbook_event(
    mut event: BytesStart<'static>,
    reader: &NsReader<&[u8]>,
    plan: &WorkbookRewritePlan,
) -> Result<BytesStart<'static>, TransformError> {
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
