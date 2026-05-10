use std::collections::HashSet;
use std::io::{Cursor, Read};

use calamine::{Data, ExcelDateTime, Reader, open_workbook_auto_from_rs};
use quick_xml::Reader as XmlReader;
use quick_xml::events::Event;
use serde_json::{Map, Number as JsonNumber, Value as JsonValue};
use zip::ZipArchive;

use crate::error::{TransformError, TransformErrorKind};
use crate::model::{ExcelDatePolicy, ExcelFormulaPolicy, ExcelInput, ExcelSheetRef, RuleFile};

use super::{InputData, NormalizationOptions, enforce_records_limit};

#[derive(Clone, Copy)]
struct CellWindow {
    start_row: usize,
    end_row: Option<usize>,
    start_col: usize,
    end_col: Option<usize>,
}

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
    preflight_xlsx_package(bytes, options)?;

    let cursor = Cursor::new(bytes);
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
    let formula_rows = formulas
        .as_ref()
        .map(|formulas| formulas.rows().collect::<Vec<_>>())
        .unwrap_or_default();
    let window = parse_cell_window(excel.range.as_deref())?;
    let max_width = rows.iter().map(|row| row.len()).max().unwrap_or(0);
    if rows.len() > options.max_excel_rows {
        return Err(invalid("input exceeds max_excel_rows"));
    }
    let selected_columns = selected_column_indexes(excel, &rows, window, max_width)?;
    let cell_count = rows
        .len()
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
        .unwrap_or_else(|| rows.len().saturating_sub(1));

    let mut records = Vec::new();
    if data_start_row >= rows.len() || data_start_row > data_end_row {
        return Ok(records);
    }
    for row_index in data_start_row..=data_end_row.min(rows.len().saturating_sub(1)) {
        let mut obj = Map::new();
        for (field_index, column_index) in selected_columns.iter().enumerate() {
            let cell = cell_at(&rows, row_index, *column_index);
            let formula = formula_at(&formula_rows, row_index, *column_index);
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

fn preflight_xlsx_package(
    bytes: &[u8],
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    let mut archive = ZipArchive::new(Cursor::new(bytes)).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("invalid Excel ZIP package: {}", err),
        )
    })?;
    if archive.len() > options.max_excel_zip_entries {
        return Err(invalid("input exceeds max_excel_zip_entries"));
    }

    let mut total_uncompressed = 0usize;
    let mut content_types = None;
    let mut worksheet_count = 0usize;
    let mut total_rows = 0usize;
    let mut total_cells = 0usize;
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to inspect Excel ZIP entry: {}", err),
            )
        })?;
        let name = entry.name().to_string();
        let lower_name = name.to_ascii_lowercase();
        let size =
            usize::try_from(entry.size()).map_err(|_| invalid("Excel ZIP entry too large"))?;
        if size > options.max_excel_entry_uncompressed_bytes {
            return Err(invalid("input exceeds max_excel_entry_uncompressed_bytes"));
        }
        total_uncompressed = total_uncompressed
            .checked_add(size)
            .ok_or_else(|| invalid("input exceeds max_excel_uncompressed_bytes"))?;
        if total_uncompressed > options.max_excel_uncompressed_bytes {
            return Err(invalid("input exceeds max_excel_uncompressed_bytes"));
        }
        if lower_name.ends_with("vbaproject.bin") {
            return Err(invalid("Excel macros are not supported"));
        }
        if lower_name == "[content_types].xml" {
            content_types = Some(read_zip_text(&mut entry)?);
        } else if lower_name.starts_with("xl/worksheets/") && lower_name.ends_with(".xml") {
            let worksheet = read_zip_text(&mut entry)?;
            worksheet_count = worksheet_count.saturating_add(1);
            if worksheet_count > options.max_excel_sheets {
                return Err(invalid("input exceeds max_excel_sheets"));
            }
            let counts = inspect_worksheet_xml(&worksheet)?;
            total_rows = total_rows
                .checked_add(counts.rows)
                .ok_or_else(|| invalid("input exceeds max_excel_rows"))?;
            total_cells = total_cells
                .checked_add(counts.cells)
                .ok_or_else(|| invalid("input exceeds max_excel_cells"))?;
            if total_rows > options.max_excel_rows {
                return Err(invalid("input exceeds max_excel_rows"));
            }
            if total_cells > options.max_excel_cells {
                return Err(invalid("input exceeds max_excel_cells"));
            }
            if counts.max_row > options.max_excel_rows {
                return Err(invalid("input exceeds max_excel_rows"));
            }
            let dense_cells = counts
                .max_row
                .checked_mul(counts.max_col)
                .ok_or_else(|| invalid("input exceeds max_excel_cells"))?;
            if dense_cells > options.max_excel_cells {
                return Err(invalid("input exceeds max_excel_cells"));
            }
        } else if lower_name.ends_with(".rels") {
            let rels = read_zip_text(&mut entry)?;
            reject_external_relationships(&rels)?;
        } else if lower_name == "xl/sharedstrings.xml" {
            if size > options.max_excel_shared_string_bytes {
                return Err(invalid("input exceeds max_excel_shared_string_bytes"));
            }
            let shared_strings = read_zip_text(&mut entry)?;
            if count_xml_elements(&shared_strings, b"si")? > options.max_excel_shared_strings {
                return Err(invalid("input exceeds max_excel_shared_strings"));
            }
        } else if lower_name == "xl/styles.xml" {
            let styles = read_zip_text(&mut entry)?;
            if count_xml_elements(&styles, b"xf")? > options.max_excel_styles {
                return Err(invalid("input exceeds max_excel_styles"));
            }
        }
    }

    let content_types =
        content_types.ok_or_else(|| invalid("Excel package is missing [Content_Types].xml"))?;
    let lower_content_types = content_types.to_ascii_lowercase();
    if lower_content_types.contains("macroenabled")
        || lower_content_types.contains("application/vnd.ms-excel.sheet.binary.macroenabled")
    {
        return Err(invalid("Excel macros are not supported"));
    }
    if !lower_content_types
        .contains("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml")
    {
        return Err(invalid("only .xlsx workbooks are supported"));
    }
    Ok(())
}

#[derive(Default)]
struct WorksheetCounts {
    rows: usize,
    cells: usize,
    max_row: usize,
    max_col: usize,
}

fn inspect_worksheet_xml(worksheet: &str) -> Result<WorksheetCounts, TransformError> {
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
                if cell_has_formula && !cell_has_value {
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

fn read_zip_text<R: Read>(reader: &mut R) -> Result<String, TransformError> {
    let mut text = String::new();
    reader.read_to_string(&mut text).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to read Excel ZIP XML: {}", err),
        )
    })?;
    Ok(text)
}

fn reject_external_relationships(rels: &str) -> Result<(), TransformError> {
    let lower = rels.to_ascii_lowercase();
    let compact = lower
        .chars()
        .filter(|value| !value.is_ascii_whitespace())
        .collect::<String>();
    if compact.contains("targetmode=\"external\"")
        || compact.contains("targetmode='external'")
        || compact.contains("target=\"http:")
        || compact.contains("target=\"https:")
        || compact.contains("target=\"file:")
        || compact.contains("target='http:")
        || compact.contains("target='https:")
        || compact.contains("target='file:")
    {
        return Err(invalid("Excel external relationships are not supported"));
    }
    Ok(())
}

fn count_xml_elements(xml: &str, name: &[u8]) -> Result<usize, TransformError> {
    let mut reader = XmlReader::from_str(xml);
    reader.trim_text(false);
    let mut count = 0usize;
    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) | Ok(Event::Empty(event)) => {
                if local_name(event.name().as_ref()) == name {
                    count = count.saturating_add(1);
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(err) => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to parse Excel XML: {}", err),
                ));
            }
        }
    }
    Ok(count)
}

fn local_name(name: &[u8]) -> &[u8] {
    name.iter()
        .rposition(|value| *value == b':')
        .map(|index| &name[index + 1..])
        .unwrap_or(name)
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

fn excel_cell_to_json(
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

fn cell_at<'a>(rows: &'a [&'a [Data]], row: usize, col: usize) -> Option<&'a Data> {
    rows.get(row).and_then(|row| row.get(col))
}

fn formula_at<'a>(rows: &'a [&'a [String]], row: usize, col: usize) -> Option<&'a String> {
    rows.get(row).and_then(|row| row.get(col))
}

fn parse_cell_window(range: Option<&str>) -> Result<CellWindow, TransformError> {
    let Some(range) = range else {
        return Ok(CellWindow {
            start_row: 0,
            end_row: None,
            start_col: 0,
            end_col: None,
        });
    };
    let (start, end) = range
        .split_once(':')
        .ok_or_else(|| invalid("Excel range must use A:D or A1:D100 form"))?;
    let start = parse_cell_ref(start)?;
    let end = parse_cell_ref(end)?;
    if start.col > end.col {
        return Err(invalid("Excel range start column is after end column"));
    }
    if let (Some(start_row), Some(end_row)) = (start.row, end.row)
        && start_row > end_row
    {
        return Err(invalid("Excel range start row is after end row"));
    }
    Ok(CellWindow {
        start_row: start.row.unwrap_or(0),
        end_row: end.row,
        start_col: start.col,
        end_col: Some(end.col),
    })
}

struct ParsedCellRef {
    col: usize,
    row: Option<usize>,
}

fn parse_cell_ref(value: &str) -> Result<ParsedCellRef, TransformError> {
    let mut letters = String::new();
    let mut digits = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphabetic() && digits.is_empty() {
            letters.push(ch.to_ascii_uppercase());
        } else if ch.is_ascii_digit() {
            digits.push(ch);
        } else {
            return Err(invalid("Excel cell reference is invalid"));
        }
    }
    if letters.is_empty() {
        return Err(invalid("Excel cell reference requires a column"));
    }
    let col = column_letters_to_index(&letters)?;
    let row = if digits.is_empty() {
        None
    } else {
        let row = digits
            .parse::<usize>()
            .map_err(|_| invalid("Excel cell reference row is invalid"))?;
        if row == 0 {
            return Err(invalid("Excel cell reference row must be 1-based"));
        }
        Some(row - 1)
    };
    Ok(ParsedCellRef { col, row })
}

fn column_letters_to_index(value: &str) -> Result<usize, TransformError> {
    let mut index = 0usize;
    for ch in value.chars() {
        if !ch.is_ascii_alphabetic() {
            return Err(invalid("Excel column reference is invalid"));
        }
        let value = (ch.to_ascii_uppercase() as u8 - b'A' + 1) as usize;
        index = index
            .checked_mul(26)
            .and_then(|index| index.checked_add(value))
            .ok_or_else(|| invalid("Excel column reference is too large"))?;
    }
    if index == 0 {
        return Err(invalid("Excel column reference is required"));
    }
    Ok(index - 1)
}

fn invalid(message: impl Into<String>) -> TransformError {
    TransformError::new(TransformErrorKind::InvalidInput, message)
}
