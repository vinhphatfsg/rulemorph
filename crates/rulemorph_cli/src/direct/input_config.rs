use std::collections::HashSet;
use std::path::{Path, PathBuf};

use csv::ReaderBuilder;

use super::output_spec::{DirectOutputSpec, expr_has_evaluated_root_ref};
use super::{DirectArgs, DirectFormatArg};

const MAX_DIRECT_TABULAR_FIELDS: usize = 10_000;
const MAX_DIRECT_TABULAR_HEADER_BYTES: usize = 256 * 1024;
const MAX_DIRECT_TABULAR_HEADER_BYTES_TOTAL: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum DirectInputFormat {
    Csv,
    Json,
    Excel,
}

pub(super) struct DirectInputConfig {
    pub(super) rule_config: serde_json::Value,
    pub(super) unwrap_single_output: bool,
}

pub(super) fn resolve_direct_input_format(
    override_format: Option<DirectFormatArg>,
    input_path: Option<&PathBuf>,
    input: &[u8],
) -> DirectInputFormat {
    if let Some(format) = override_format {
        return match format {
            DirectFormatArg::Csv => DirectInputFormat::Csv,
            DirectFormatArg::Json => DirectInputFormat::Json,
            DirectFormatArg::Excel => DirectInputFormat::Excel,
        };
    }

    if let Some(path) = input_path {
        if path == Path::new("-") {
            return detect_stdin_format(input);
        }

        return match path
            .extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("csv") => DirectInputFormat::Csv,
            Some("xlsx") => DirectInputFormat::Excel,
            Some("json") => DirectInputFormat::Json,
            _ => DirectInputFormat::Json,
        };
    }

    detect_stdin_format(input)
}

fn detect_stdin_format(input: &[u8]) -> DirectInputFormat {
    match first_meaningful_byte(input) {
        Some(b'{') | Some(b'[') => DirectInputFormat::Json,
        _ => DirectInputFormat::Csv,
    }
}

pub(super) fn output_cell_record_budget(
    input_format: DirectInputFormat,
    input: &[u8],
    max_records: usize,
) -> usize {
    match input_format {
        DirectInputFormat::Json if matches!(first_meaningful_byte(input), Some(b'{')) => 1,
        _ => max_records,
    }
}

pub(super) fn validate_direct_options(
    args: &DirectArgs,
    input_format: DirectInputFormat,
) -> Result<(), String> {
    if args.headers.is_some() && input_format != DirectInputFormat::Csv {
        return Err("--headers requires CSV direct input; pass -f csv when the input should be parsed as CSV".to_string());
    }
    if has_excel_options(args) && input_format != DirectInputFormat::Excel {
        return Err("--excel-* options require Excel direct input; pass -f excel or use an .xlsx input file".to_string());
    }
    if args.excel_sheet.is_some() && args.excel_sheet_index.is_some() {
        return Err("--excel-sheet and --excel-sheet-index cannot be used together".to_string());
    }
    Ok(())
}

fn has_excel_options(args: &DirectArgs) -> bool {
    args.excel_data_range.is_some()
        || args.excel_header_row.is_some()
        || args.excel_sheet.is_some()
        || args.excel_sheet_index.is_some()
}

pub(super) fn build_direct_input_config(
    args: &DirectArgs,
    input_format: DirectInputFormat,
    input: &[u8],
    output_spec: &DirectOutputSpec,
) -> Result<DirectInputConfig, String> {
    match input_format {
        DirectInputFormat::Json => Ok(DirectInputConfig {
            rule_config: serde_json::json!({}),
            unwrap_single_output: matches!(first_meaningful_byte(input), Some(b'{')),
        }),
        DirectInputFormat::Csv => Ok(DirectInputConfig {
            rule_config: build_csv_config(args, input, output_spec)?,
            unwrap_single_output: should_unwrap_tabular_output(args, input_format),
        }),
        DirectInputFormat::Excel => Ok(DirectInputConfig {
            rule_config: build_excel_config(args)?,
            unwrap_single_output: should_unwrap_tabular_output(args, input_format),
        }),
    }
}

fn should_unwrap_tabular_output(args: &DirectArgs, input_format: DirectInputFormat) -> bool {
    args.format.is_none()
        || args.headers.is_some()
        || has_excel_options(args)
        || matches!(
            (args.format, input_format),
            (Some(DirectFormatArg::Excel), DirectInputFormat::Excel)
        )
}

fn build_csv_config(
    args: &DirectArgs,
    input: &[u8],
    output_spec: &DirectOutputSpec,
) -> Result<serde_json::Value, String> {
    if let Some(headers) = args.headers.as_deref() {
        return csv_config_from_headers_arg(headers);
    }
    if output_spec
        .exprs()
        .iter()
        .any(|expr| expr_uses_root_numeric_input_ref(expr))
    {
        return csv_config_from_first_record(input);
    }
    Ok(serde_json::json!({
        "has_header": true
    }))
}

fn csv_config_from_headers_arg(headers: &str) -> Result<serde_json::Value, String> {
    if headers.len() > MAX_DIRECT_TABULAR_HEADER_BYTES_TOTAL {
        return Err(format!(
            "--headers total size must be at most {} bytes",
            MAX_DIRECT_TABULAR_HEADER_BYTES_TOTAL
        ));
    }

    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(headers.as_bytes());
    let mut records = reader.records();
    let record = match records.next() {
        Some(Ok(record)) => record,
        Some(Err(err)) => return Err(format!("failed to parse --headers as CSV: {}", err)),
        None => return Err("--headers must contain at least one field".to_string()),
    };
    if let Some(result) = records.next() {
        return match result {
            Ok(_) => Err("--headers must contain exactly one CSV record".to_string()),
            Err(err) => Err(format!("failed to parse --headers as CSV: {}", err)),
        };
    }
    let field_names = record.iter().map(str::to_string).collect::<Vec<_>>();
    csv_config_from_field_names(field_names, "--headers")
}

fn csv_config_from_first_record(input: &[u8]) -> Result<serde_json::Value, String> {
    let mut reader = ReaderBuilder::new().has_headers(false).from_reader(input);
    let mut records = reader.records();
    let record = match records.next() {
        Some(Ok(record)) => record,
        Some(Err(err)) => return Err(format!("failed to infer CSV columns: {}", err)),
        None => return Err("failed to infer CSV columns: input has no records".to_string()),
    };
    if record.is_empty() {
        return Err("failed to infer CSV columns: first record has no fields".to_string());
    }
    let field_names = (0..record.len())
        .map(|index| index.to_string())
        .collect::<Vec<_>>();
    csv_config_from_field_names(field_names, "inferred CSV columns")
}

fn csv_config_from_field_names(
    field_names: Vec<String>,
    source_label: &str,
) -> Result<serde_json::Value, String> {
    validate_tabular_field_names(&field_names, source_label)?;
    let columns = field_names
        .into_iter()
        .map(|name| serde_json::json!({ "name": name }))
        .collect::<Vec<_>>();
    Ok(serde_json::json!({
        "has_header": false,
        "columns": columns
    }))
}

fn validate_tabular_field_names(field_names: &[String], source_label: &str) -> Result<(), String> {
    if field_names.len() > MAX_DIRECT_TABULAR_FIELDS {
        return Err(format!(
            "{} has too many fields; maximum is {}",
            source_label, MAX_DIRECT_TABULAR_FIELDS
        ));
    }

    let mut seen = HashSet::new();
    let mut total_bytes = 0usize;
    for field_name in field_names {
        if field_name.trim().is_empty() {
            return Err(format!(
                "{} must not contain blank field names",
                source_label
            ));
        }
        let field_bytes = field_name.len();
        if field_bytes > MAX_DIRECT_TABULAR_HEADER_BYTES {
            return Err(format!(
                "{} field names must be at most {} bytes each",
                source_label, MAX_DIRECT_TABULAR_HEADER_BYTES
            ));
        }
        total_bytes = total_bytes
            .checked_add(field_bytes)
            .ok_or_else(|| format!("{} total size is too large", source_label))?;
        if total_bytes > MAX_DIRECT_TABULAR_HEADER_BYTES_TOTAL {
            return Err(format!(
                "{} total size must be at most {} bytes",
                source_label, MAX_DIRECT_TABULAR_HEADER_BYTES_TOTAL
            ));
        }
        if !seen.insert(field_name.clone()) {
            return Err(format!("{} must be unique", source_label));
        }
    }
    Ok(())
}

fn expr_uses_root_numeric_input_ref(expr: &serde_json::Value) -> bool {
    expr_has_evaluated_root_ref(expr, "input")
}

fn build_excel_config(args: &DirectArgs) -> Result<serde_json::Value, String> {
    let header_row = args
        .excel_header_row
        .ok_or_else(|| "--excel-header-row is required for Excel direct input".to_string())?;
    if header_row == 0 {
        return Err("--excel-header-row must be 1-based".to_string());
    }
    let data_range = args
        .excel_data_range
        .as_deref()
        .ok_or_else(|| "--excel-data-range is required for Excel direct input".to_string())?;
    let data_range = parse_excel_data_range(data_range)?;
    if header_row >= data_range.start_row {
        return Err("--excel-data-range is the data range and must start after --excel-header-row; use A2:C100 when --excel-header-row is 1".to_string());
    }

    let core_range = format!(
        "{}{}:{}{}",
        data_range.start_col, header_row, data_range.end_col, data_range.end_row
    );
    let mut excel = serde_json::Map::new();
    excel.insert("has_header".to_string(), serde_json::Value::Bool(true));
    excel.insert(
        "header_row".to_string(),
        serde_json::Value::Number(header_row.into()),
    );
    excel.insert(
        "data_start_row".to_string(),
        serde_json::Value::Number(data_range.start_row.into()),
    );
    excel.insert("range".to_string(), serde_json::Value::String(core_range));
    if let Some(sheet) = args.excel_sheet.as_deref() {
        excel.insert(
            "sheet".to_string(),
            serde_json::Value::String(sheet.to_string()),
        );
    }
    if let Some(sheet_index) = args.excel_sheet_index {
        excel.insert(
            "sheet".to_string(),
            serde_json::Value::Number(sheet_index.into()),
        );
    }
    Ok(serde_json::Value::Object(excel))
}

struct ParsedExcelDataRange {
    start_col: String,
    start_row: usize,
    end_col: String,
    end_row: usize,
}

fn parse_excel_data_range(value: &str) -> Result<ParsedExcelDataRange, String> {
    let (start, end) = value
        .split_once(':')
        .ok_or_else(|| "--excel-data-range must use A2:D10 form".to_string())?;
    let (start_col, start_col_index, start_row) =
        parse_excel_cell_ref(start, "--excel-data-range")?;
    let (end_col, end_col_index, end_row) = parse_excel_cell_ref(end, "--excel-data-range")?;
    if start_col_index > end_col_index {
        return Err("--excel-data-range start column is after end column".to_string());
    }
    if start_row > end_row {
        return Err("--excel-data-range start row is after end row".to_string());
    }
    Ok(ParsedExcelDataRange {
        start_col,
        start_row,
        end_col,
        end_row,
    })
}

fn parse_excel_cell_ref(value: &str, label: &str) -> Result<(String, usize, usize), String> {
    let mut letters = String::new();
    let mut digits = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphabetic() && digits.is_empty() {
            letters.push(ch.to_ascii_uppercase());
        } else if ch.is_ascii_digit() {
            digits.push(ch);
        } else {
            return Err(format!("{} contains an invalid cell reference", label));
        }
    }
    if letters.is_empty() || digits.is_empty() {
        return Err(format!(
            "{} must include column letters and row numbers",
            label
        ));
    }
    let column_index = excel_column_letters_to_index(&letters, label)?;
    let row = digits
        .parse::<usize>()
        .map_err(|_| format!("{} row is invalid", label))?;
    if row == 0 {
        return Err(format!("{} row must be 1-based", label));
    }
    Ok((letters, column_index, row))
}

fn excel_column_letters_to_index(value: &str, label: &str) -> Result<usize, String> {
    let mut index = 0usize;
    for ch in value.chars() {
        if !ch.is_ascii_alphabetic() {
            return Err(format!("{} column is invalid", label));
        }
        let value = (ch.to_ascii_uppercase() as u8 - b'A' + 1) as usize;
        index = index
            .checked_mul(26)
            .and_then(|index| index.checked_add(value))
            .ok_or_else(|| format!("{} column is too large", label))?;
    }
    if index == 0 {
        return Err(format!("{} column is required", label));
    }
    Ok(index)
}

fn first_meaningful_byte(input: &[u8]) -> Option<u8> {
    strip_utf8_bom(input)
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
}

fn strip_utf8_bom(input: &[u8]) -> &[u8] {
    input.strip_prefix(b"\xef\xbb\xbf").unwrap_or(input)
}
