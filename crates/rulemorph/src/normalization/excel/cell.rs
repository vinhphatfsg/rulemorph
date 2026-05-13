use calamine::{Data, ExcelDateTime, Range};
use serde_json::{Number as JsonNumber, Value as JsonValue};

use super::*;

pub(super) fn excel_cell_to_json(
    cell: Option<&Data>,
    formula: Option<&String>,
    excel: &ExcelInput,
    options: &NormalizationOptions,
) -> Result<Option<JsonValue>, TransformError> {
    let cell = cell.unwrap_or(&Data::Empty);
    if let Some(formula) = formula.filter(|formula| !formula.trim().is_empty()) {
        match excel.formula {
            ExcelFormulaPolicy::Error => {
                return Err(invalid("Excel formula cell is not supported"));
            }
            ExcelFormulaPolicy::Formula => {
                return checked_string(formula.clone(), options).map(Some);
            }
            ExcelFormulaPolicy::Cached => {
                if matches!(cell, Data::Empty) {
                    return Err(invalid("Excel formula cell is missing a cached value"));
                }
            }
        }
    }
    match cell {
        Data::Empty => Ok(None),
        Data::String(value) => checked_string(value.clone(), options).map(Some),
        Data::Float(value) => excel_float_to_json(*value).map(Some),
        Data::Int(value) => Ok(Some(JsonValue::Number((*value).into()))),
        Data::Bool(value) => Ok(Some(JsonValue::Bool(*value))),
        Data::DateTime(value) => excel_datetime_to_json(value, excel.date),
        Data::DateTimeIso(value) | Data::DurationIso(value) => {
            checked_string(value.clone(), options).map(Some)
        }
        Data::Error(_) => Err(invalid("Excel error cell is not supported")),
    }
}

fn excel_datetime_to_json(
    value: &ExcelDateTime,
    policy: ExcelDatePolicy,
) -> Result<Option<JsonValue>, TransformError> {
    match policy {
        ExcelDatePolicy::Serial => JsonNumber::from_f64(value.as_f64())
            .map(JsonValue::Number)
            .map(Some)
            .ok_or_else(|| invalid("Excel datetime serial is not JSON-compatible")),
        ExcelDatePolicy::Iso8601 | ExcelDatePolicy::String => {
            let datetime = value
                .as_datetime()
                .ok_or_else(|| invalid("Excel datetime is invalid"))?;
            let text = datetime.format("%Y-%m-%dT%H:%M:%S").to_string();
            Ok(Some(JsonValue::String(text)))
        }
    }
}

fn checked_string(
    value: String,
    options: &NormalizationOptions,
) -> Result<JsonValue, TransformError> {
    if value.len() > options.max_text_bytes {
        return Err(invalid("input exceeds max_text_bytes"));
    }
    Ok(JsonValue::String(value))
}

fn excel_float_to_json(value: f64) -> Result<JsonValue, TransformError> {
    if !value.is_finite() {
        return Err(invalid("Excel float is not JSON-compatible"));
    }
    if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        return Ok(JsonValue::Number((value as i64).into()));
    }
    JsonNumber::from_f64(value)
        .map(JsonValue::Number)
        .ok_or_else(|| invalid("Excel float is not JSON-compatible"))
}

pub(super) fn cell_at<'a>(rows: &'a [&'a [Data]], row: usize, col: usize) -> Option<&'a Data> {
    rows.get(row).and_then(|row| row.get(col))
}

pub(super) fn formula_at(
    formulas: Option<&Range<String>>,
    row: usize,
    col: usize,
) -> Option<&String> {
    let row = u32::try_from(row).ok()?;
    let col = u32::try_from(col).ok()?;
    formulas.and_then(|formulas| formulas.get_value((row, col)))
}
