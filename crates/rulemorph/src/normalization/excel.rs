use std::collections::HashSet;
use std::io::Cursor;

use calamine::{Data, Reader, open_workbook_auto_from_rs};
use serde_json::{Map, Value as JsonValue};

use crate::error::{TransformError, TransformErrorKind};
use crate::model::{ExcelFormulaPolicy, ExcelInput, ExcelSheetRef, RuleFile};

use super::{InputData, NormalizationOptions, enforce_records_limit};

mod cell;
mod range;
mod workbook;
mod xlsx;
mod xml;

use self::cell::{cell_at, excel_cell_to_json, formula_at};
use self::range::{CellWindow, column_letters_to_index, parse_cell_window};
use self::xlsx::{preflight_xlsx_package, xlsx_bytes_for_calamine};

pub fn normalize_excel_records(
    rule: &RuleFile,
    input: InputData<'_>,
    options: &NormalizationOptions,
) -> Result<Vec<JsonValue>, TransformError> {
    let excel = rule.input.excel.as_ref().ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            "input.excel is required when format=excel",
        )
    })?;
    let bytes = excel_input_bytes(input, options)?;
    preflight_xlsx_package(bytes, excel, options)?;

    let calamine_bytes = xlsx_bytes_for_calamine(bytes)?;
    let cursor = Cursor::new(calamine_bytes.as_ref());
    let mut workbook = open_workbook_auto_from_rs(cursor).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to open Excel workbook: {}", err),
        )
    })?;

    let sheet_names = workbook.sheet_names().to_owned();
    if sheet_names.is_empty() {
        return Err(invalid("Excel workbook has no sheets"));
    }
    if sheet_names.len() > options.max_excel_sheets {
        return Err(invalid("input exceeds max_excel_sheets"));
    }
    let sheet_name = select_sheet_name(excel, &sheet_names)?;
    let range = workbook.worksheet_range(&sheet_name).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to read Excel sheet: {}", err),
        )
    })?;
    let formulas = if excel.formula == ExcelFormulaPolicy::Cached {
        None
    } else {
        Some(workbook.worksheet_formula(&sheet_name).map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to read Excel formulas: {}", err),
            )
        })?)
    };

    let rows: Vec<&[Data]> = range.rows().collect();
    let formula_end = formulas
        .as_ref()
        .and_then(|formulas| formulas.end())
        .map(|(row, col)| (row as usize + 1, col as usize + 1))
        .unwrap_or((0, 0));
    let row_count = rows.len().max(formula_end.0);
    let window = parse_cell_window(excel.range.as_deref())?;
    let max_width = rows.iter().map(|row| row.len()).max().unwrap_or(0);
    let max_width = max_width.max(formula_end.1);
    if row_count > options.max_excel_rows {
        return Err(invalid("input exceeds max_excel_rows"));
    }
    let selected_columns = selected_column_indexes(excel, &rows, window, max_width)?;
    let cell_count = row_count
        .checked_mul(selected_columns.len())
        .ok_or_else(|| invalid("input exceeds max_excel_cells"))?;
    if cell_count > options.max_excel_cells {
        return Err(invalid("input exceeds max_excel_cells"));
    }

    let headers = if excel.has_header {
        read_header_names(&rows, window, &selected_columns, excel.header_row)?
    } else {
        read_explicit_column_names(excel)?
    };
    validate_header_names(&headers)?;

    let data_start_row = if excel.has_header {
        excel
            .data_start_row
            .map(|row| row.saturating_sub(1))
            .unwrap_or_else(|| excel.header_row)
    } else {
        excel
            .data_start_row
            .map(|row| row.saturating_sub(1))
            .unwrap_or(window.start_row)
    }
    .max(window.start_row);
    let data_end_row = window
        .end_row
        .unwrap_or_else(|| row_count.saturating_sub(1));

    let mut records = Vec::new();
    if data_start_row >= row_count || data_start_row > data_end_row {
        return Ok(records);
    }
    for row_index in data_start_row..=data_end_row.min(row_count.saturating_sub(1)) {
        let mut obj = Map::new();
        for (field_index, column_index) in selected_columns.iter().enumerate() {
            let cell = cell_at(&rows, row_index, *column_index);
            let formula = formula_at(formulas.as_ref(), row_index, *column_index);
            if let Some(value) = excel_cell_to_json(cell, formula, excel, options)? {
                obj.insert(headers[field_index].clone(), value);
            }
        }
        if obj.is_empty() {
            continue;
        }
        records.push(JsonValue::Object(obj));
        enforce_records_limit(records.len(), options)?;
    }

    Ok(records)
}

fn excel_input_bytes<'a>(
    input: InputData<'a>,
    options: &NormalizationOptions,
) -> Result<&'a [u8], TransformError> {
    let bytes = match input {
        InputData::Text(_) => {
            return Err(invalid("Excel input must be provided as bytes"));
        }
        InputData::Bytes(bytes) => bytes,
    };
    if bytes.len() > options.max_input_bytes {
        return Err(invalid("input exceeds max_input_bytes"));
    }
    Ok(bytes)
}

fn select_sheet_name(excel: &ExcelInput, sheet_names: &[String]) -> Result<String, TransformError> {
    match &excel.sheet {
        Some(ExcelSheetRef::Name(name)) => {
            if sheet_names.iter().any(|sheet_name| sheet_name == name) {
                Ok(name.clone())
            } else {
                Err(invalid("Excel sheet was not found"))
            }
        }
        Some(ExcelSheetRef::Index(index)) => sheet_names
            .get(*index)
            .cloned()
            .ok_or_else(|| invalid("Excel sheet index is out of range")),
        None => sheet_names
            .first()
            .cloned()
            .ok_or_else(|| invalid("Excel workbook has no sheets")),
    }
}

fn selected_column_indexes(
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

fn read_header_names(
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

fn read_explicit_column_names(excel: &ExcelInput) -> Result<Vec<String>, TransformError> {
    let columns = excel
        .columns
        .as_ref()
        .filter(|columns| !columns.is_empty())
        .ok_or_else(|| invalid("excel.columns is required when has_header=false"))?;
    Ok(columns.iter().map(|column| column.name.clone()).collect())
}

fn validate_header_names(headers: &[String]) -> Result<(), TransformError> {
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

fn invalid(message: impl Into<String>) -> TransformError {
    TransformError::new(TransformErrorKind::InvalidInput, message)
}
