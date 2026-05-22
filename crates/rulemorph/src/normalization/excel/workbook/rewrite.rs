use quick_xml::Writer as XmlWriter;
use quick_xml::events::Event;
use quick_xml::reader::NsReader;

use crate::error::{TransformError, TransformErrorKind};

use super::super::invalid;

mod event;
mod plan;

use self::event::rewrite_workbook_event;
use self::plan::workbook_rewrite_plan;

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
