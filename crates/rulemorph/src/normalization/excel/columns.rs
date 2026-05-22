use std::collections::HashSet;

use calamine::Data;

use crate::error::TransformError;
use crate::model::ExcelInput;

use super::invalid;
use super::range::{CellWindow, column_letters_to_index};

pub(super) fn selected_column_indexes(
    excel: &ExcelInput,
    rows: &[&[Data]],
    window: CellWindow,
    max_width: usize,
) -> Result<Vec<usize>, TransformError> {
    if max_width == 0 {
        return Err(invalid("Excel selected range has no columns"));
    }
    if !excel.has_header {
        let columns = excel
            .columns
            .as_ref()
            .filter(|columns| !columns.is_empty())
            .ok_or_else(|| invalid("excel.columns is required when has_header=false"))?;
        return columns
            .iter()
            .map(|column| column_letters_to_index(&column.column))
            .collect();
    }

    let header_row_index = excel.header_row.saturating_sub(1);
    let header_row = rows
        .get(header_row_index)
        .ok_or_else(|| invalid("Excel header row was not found"))?;
    if header_row.is_empty() {
        return Err(invalid("Excel header row has no columns"));
    }
    let end_col = window
        .end_col
        .unwrap_or_else(|| max_width.saturating_sub(1))
        .min(header_row.len().saturating_sub(1));
    if window.start_col > end_col {
        return Err(invalid("Excel selected range has no columns"));
    }
    Ok((window.start_col..=end_col).collect())
}

pub(super) fn read_header_names(
    rows: &[&[Data]],
    window: CellWindow,
    selected_columns: &[usize],
    header_row: usize,
) -> Result<Vec<String>, TransformError> {
    let row_index = header_row.saturating_sub(1);
    if row_index < window.start_row {
        return Err(invalid("Excel header_row is before selected range"));
    }
    let row = rows
        .get(row_index)
        .ok_or_else(|| invalid("Excel header row was not found"))?;
    selected_columns
        .iter()
        .map(
            |column_index| match row.get(*column_index).unwrap_or(&Data::Empty) {
                Data::String(value) => Ok(value.trim().to_string()),
                Data::DateTimeIso(value) => Ok(value.trim().to_string()),
                Data::Empty => Err(invalid("Excel header must not be blank")),
                value => Ok(value.to_string().trim().to_string()),
            },
        )
        .collect()
}

pub(super) fn read_explicit_column_names(
    excel: &ExcelInput,
) -> Result<Vec<String>, TransformError> {
    let columns = excel
        .columns
        .as_ref()
        .filter(|columns| !columns.is_empty())
        .ok_or_else(|| invalid("excel.columns is required when has_header=false"))?;
    Ok(columns.iter().map(|column| column.name.clone()).collect())
}

pub(super) fn validate_header_names(headers: &[String]) -> Result<(), TransformError> {
    let mut seen = HashSet::new();
    for header in headers {
        if header.trim().is_empty() {
            return Err(invalid("Excel header must not be blank"));
        }
        if !seen.insert(header.clone()) {
            return Err(invalid("Excel header must be unique"));
        }
    }
    Ok(())
}
