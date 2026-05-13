use quick_xml::events::Event;
use quick_xml::reader::Reader as XmlReader;

use crate::error::{TransformError, TransformErrorKind};
use crate::model::ExcelFormulaPolicy;

use super::invalid;
use super::range::parse_cell_ref;
use super::xml::local_name;

#[derive(Default)]
pub(super) struct WorksheetCounts {
    pub(super) rows: usize,
    pub(super) cells: usize,
    pub(super) max_row: usize,
    pub(super) max_col: usize,
}

pub(super) fn inspect_worksheet_xml(
    worksheet: &str,
    formula_policy: ExcelFormulaPolicy,
) -> Result<WorksheetCounts, TransformError> {
    let mut reader = XmlReader::from_str(worksheet);
    reader.trim_text(false);
    let mut counts = WorksheetCounts::default();
    let mut in_cell = false;
    let mut cell_has_formula = false;
    let mut cell_has_value = false;
    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) => match local_name(event.name().as_ref()) {
                b"row" => {
                    counts.rows = counts.rows.saturating_add(1);
                    update_row_extent(&mut counts, event.attributes())?;
                }
                b"c" => {
                    counts.cells = counts.cells.saturating_add(1);
                    update_cell_extent(&mut counts, event.attributes())?;
                    in_cell = true;
                    cell_has_formula = false;
                    cell_has_value = false;
                }
                b"f" if in_cell => {
                    reject_shared_formula(event.attributes())?;
                    cell_has_formula = true;
                }
                b"v" if in_cell => cell_has_value = true,
                _ => {}
            },
            Ok(Event::Empty(event)) => match local_name(event.name().as_ref()) {
                b"row" => {
                    counts.rows = counts.rows.saturating_add(1);
                    update_row_extent(&mut counts, event.attributes())?;
                }
                b"c" => {
                    counts.cells = counts.cells.saturating_add(1);
                    update_cell_extent(&mut counts, event.attributes())?;
                }
                b"f" if in_cell => {
                    reject_shared_formula(event.attributes())?;
                    cell_has_formula = true;
                }
                b"v" if in_cell => cell_has_value = true,
                _ => {}
            },
            Ok(Event::End(event)) if local_name(event.name().as_ref()) == b"c" => {
                if formula_policy == ExcelFormulaPolicy::Cached
                    && cell_has_formula
                    && !cell_has_value
                {
                    return Err(invalid("Excel formula cell is missing a cached value"));
                }
                in_cell = false;
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(err) => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to parse Excel worksheet XML: {}", err),
                ));
            }
        }
    }
    Ok(counts)
}

fn update_row_extent(
    counts: &mut WorksheetCounts,
    attributes: quick_xml::events::attributes::Attributes<'_>,
) -> Result<(), TransformError> {
    if let Some(value) = find_attr_value(attributes, b"r")? {
        let row = parse_positive_usize_attr(&value, "Excel row reference is invalid")?;
        counts.max_row = counts.max_row.max(row);
    } else {
        counts.max_row = counts.max_row.max(counts.rows);
    }
    Ok(())
}

fn update_cell_extent(
    counts: &mut WorksheetCounts,
    attributes: quick_xml::events::attributes::Attributes<'_>,
) -> Result<(), TransformError> {
    if let Some(value) = find_attr_value(attributes, b"r")? {
        let parsed = parse_cell_ref(&value)?;
        counts.max_col = counts.max_col.max(parsed.col + 1);
        if let Some(row) = parsed.row {
            counts.max_row = counts.max_row.max(row + 1);
        }
    }
    Ok(())
}

fn reject_shared_formula(
    attributes: quick_xml::events::attributes::Attributes<'_>,
) -> Result<(), TransformError> {
    let mut formula_type = None;
    let mut has_shared_index = false;
    let mut has_ref = false;
    for attr in attributes {
        let attr = attr.map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to parse Excel XML attribute: {}", err),
            )
        })?;
        match local_name(attr.key.as_ref()) {
            b"t" => formula_type = Some(String::from_utf8_lossy(attr.value.as_ref()).to_string()),
            b"si" => has_shared_index = true,
            b"ref" => has_ref = true,
            _ => {}
        }
    }
    if formula_type
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("shared"))
        || has_shared_index
        || has_ref
    {
        return Err(invalid("Excel shared formulas are not supported"));
    }
    Ok(())
}

fn find_attr_value(
    attributes: quick_xml::events::attributes::Attributes<'_>,
    name: &[u8],
) -> Result<Option<String>, TransformError> {
    for attr in attributes {
        let attr = attr.map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to parse Excel XML attribute: {}", err),
            )
        })?;
        if local_name(attr.key.as_ref()) == name {
            return Ok(Some(
                String::from_utf8_lossy(attr.value.as_ref()).to_string(),
            ));
        }
    }
    Ok(None)
}

fn parse_positive_usize_attr(value: &str, message: &str) -> Result<usize, TransformError> {
    let value = value.parse::<usize>().map_err(|_| invalid(message))?;
    if value == 0 {
        return Err(invalid(message));
    }
    Ok(value)
}
