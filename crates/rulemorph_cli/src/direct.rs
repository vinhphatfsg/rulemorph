use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};

use csv::ReaderBuilder;
use rulemorph::{
    InputData, NormalizationOptions, RuleFile, RuleFormat, parse_rule_file_with_format,
    transform_input_with_warnings_with_base_dir_and_options,
    transform_stream_input_with_base_dir_and_options, validate_rule_file,
};

use super::emit::{emit_transform_error, emit_transform_warnings, emit_validation_errors};
use super::input::{load_context, load_input_bytes_from_path_or_stdin, load_normalization_options};
use super::output::{
    create_output_writer, emit_text_output, serialize_json_output, write_json_line,
};
use super::{DirectFormatArg, ErrorFormat, LimitsProfileArg};

mod output_spec;

use output_spec::{
    DirectOutputMode, DirectOutputSpec, expr_has_evaluated_root_ref, parse_direct_output_spec,
};

const DIRECT_VALUE_TARGET: &str = "__rulemorph_direct_value";
const MAX_DIRECT_TABULAR_FIELDS: usize = 10_000;
const MAX_DIRECT_TABULAR_HEADER_BYTES: usize = 256 * 1024;
const MAX_DIRECT_TABULAR_HEADER_BYTES_TOTAL: usize = 8 * 1024 * 1024;

pub(crate) struct DirectArgs {
    pub(crate) rule: Option<String>,
    pub(crate) fields: Vec<String>,
    pub(crate) output_map: Option<String>,
    pub(crate) input: Option<PathBuf>,
    pub(crate) format: Option<DirectFormatArg>,
    pub(crate) headers: Option<String>,
    pub(crate) excel_data_range: Option<String>,
    pub(crate) excel_header_row: Option<usize>,
    pub(crate) excel_sheet: Option<String>,
    pub(crate) excel_sheet_index: Option<usize>,
    pub(crate) output: Option<PathBuf>,
    pub(crate) ndjson: bool,
    pub(crate) error_format: Option<ErrorFormat>,
    pub(crate) limits: Vec<String>,
    pub(crate) limits_profile: Option<LimitsProfileArg>,
    pub(crate) limits_file: Option<PathBuf>,
    pub(crate) context: Option<PathBuf>,
}

pub(crate) fn run(args: DirectArgs) -> i32 {
    let options = match load_normalization_options(
        args.limits_profile,
        args.limits_file.as_ref(),
        &args.limits,
    ) {
        Ok(options) => options,
        Err(message) => {
            eprintln!("{}", message);
            return 2;
        }
    };

    let input =
        match load_input_bytes_from_path_or_stdin(args.input.as_ref(), options.max_input_bytes) {
            Ok(value) => value,
            Err(code) => return code,
        };

    let input_format = resolve_direct_input_format(args.format, args.input.as_ref(), &input);
    if let Err(message) = validate_direct_options(&args, input_format) {
        eprintln!("{}", message);
        return 2;
    }

    let output_cell_record_budget =
        output_cell_record_budget(input_format, &input, options.max_records);
    let output_spec = match parse_direct_output_spec(&args, &options, output_cell_record_budget) {
        Ok(output_spec) => output_spec,
        Err(err) => {
            eprintln!("{}", err.message);
            return err.exit_code;
        }
    };
    let input_config = match build_direct_input_config(&args, input_format, &input, &output_spec) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{}", message);
            return 2;
        }
    };

    let rule = match build_direct_rule(&output_spec, input_format, input_config.rule_config) {
        Ok(rule) => rule,
        Err(message) => {
            eprintln!("{}", message);
            return 1;
        }
    };
    if let Err(errors) = validate_rule_file(&rule) {
        emit_validation_errors(&errors, args.error_format.unwrap_or(ErrorFormat::Text));
        return 2;
    }

    let context = match load_context(&args.context) {
        Ok(context) => context,
        Err(code) => return code,
    };

    if args.ndjson {
        return run_direct_ndjson(
            &rule,
            &input,
            context.as_ref(),
            args.output,
            args.error_format.unwrap_or(ErrorFormat::Text),
            &options,
            output_spec.output_mode(),
        );
    }

    let (output, warnings) = match transform_input_with_warnings_with_base_dir_and_options(
        &rule,
        InputData::Bytes(&input),
        context.as_ref(),
        Path::new("."),
        &options,
    ) {
        Ok(result) => result,
        Err(err) => {
            emit_transform_error(&err, args.error_format.unwrap_or(ErrorFormat::Text));
            return 3;
        }
    };

    let output = unwrap_direct_output(
        output,
        output_spec.output_mode(),
        input_config.unwrap_single_output,
    );
    let output_text = match serialize_json_output(&output) {
        Ok(text) => text,
        Err(()) => return 1,
    };

    emit_transform_warnings(&warnings, args.error_format.unwrap_or(ErrorFormat::Text));

    if emit_text_output(&output_text, args.output.as_ref()).is_err() {
        return 1;
    }

    0
}

fn run_direct_ndjson(
    rule: &RuleFile,
    input: &[u8],
    context: Option<&serde_json::Value>,
    output: Option<PathBuf>,
    error_format: ErrorFormat,
    options: &NormalizationOptions,
    output_mode: DirectOutputMode,
) -> i32 {
    let stream = match transform_stream_input_with_base_dir_and_options(
        rule,
        InputData::Bytes(input),
        context,
        Path::new("."),
        options,
    ) {
        Ok(stream) => stream,
        Err(err) => {
            emit_transform_error(&err, error_format);
            return 3;
        }
    };

    let mut writer = match create_output_writer(output.as_ref()) {
        Ok(writer) => writer,
        Err(()) => return 1,
    };

    for item in stream {
        let item = match item {
            Ok(item) => item,
            Err(err) => {
                emit_transform_error(&err, error_format);
                return 3;
            }
        };

        emit_transform_warnings(&item.warnings, error_format);

        let output = match item.output {
            Some(output) => output,
            None => continue,
        };
        let output = unwrap_direct_record(output, output_mode);
        if write_json_line(&mut writer, &output).is_err() {
            return 1;
        }
    }

    if let Err(err) = writer.flush() {
        eprintln!("failed to write output: {}", err);
        return 1;
    }

    0
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DirectInputFormat {
    Csv,
    Json,
    Excel,
}

struct DirectInputConfig {
    rule_config: serde_json::Value,
    unwrap_single_output: bool,
}

fn build_direct_rule(
    output_spec: &DirectOutputSpec,
    input_format: DirectInputFormat,
    input_config: serde_json::Value,
) -> Result<rulemorph::RuleFile, String> {
    let input_format_name = match input_format {
        DirectInputFormat::Csv => "csv",
        DirectInputFormat::Json => "json",
        DirectInputFormat::Excel => "excel",
    };
    let mut input = serde_json::Map::new();
    input.insert(
        "format".to_string(),
        serde_json::Value::String(input_format_name.to_string()),
    );
    input.insert(input_format_name.to_string(), input_config);
    let mappings = match output_spec {
        DirectOutputSpec::Rule(expr) => vec![serde_json::json!({
            "target": DIRECT_VALUE_TARGET,
            "expr": expr.clone()
        })],
        DirectOutputSpec::Fields(fields) | DirectOutputSpec::OutputMap(fields) => fields
            .iter()
            .map(|field| {
                serde_json::json!({
                    "target": field.target,
                    "expr": field.expr
                })
            })
            .collect(),
    };
    let rule_json = serde_json::json!({
        "version": 2,
        "input": serde_json::Value::Object(input),
        "mappings": mappings
    });
    let source = serde_json::to_string(&rule_json)
        .map_err(|err| format!("failed to encode inline rule: {}", err))?;
    parse_rule_file_with_format(&source, RuleFormat::Json)
        .map_err(|err| format!("failed to parse inline rule: {}", err))
}

fn resolve_direct_input_format(
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

fn output_cell_record_budget(
    input_format: DirectInputFormat,
    input: &[u8],
    max_records: usize,
) -> usize {
    match input_format {
        DirectInputFormat::Json if matches!(first_meaningful_byte(input), Some(b'{')) => 1,
        _ => max_records,
    }
}

fn validate_direct_options(
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

fn build_direct_input_config(
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

fn unwrap_direct_output(
    output: serde_json::Value,
    output_mode: DirectOutputMode,
    unwrap_single_output: bool,
) -> serde_json::Value {
    match output {
        serde_json::Value::Array(records) if unwrap_single_output && records.len() == 1 => {
            unwrap_direct_record(records.into_iter().next().unwrap(), output_mode)
        }
        serde_json::Value::Array(records) => serde_json::Value::Array(
            records
                .into_iter()
                .map(|record| unwrap_direct_record(record, output_mode))
                .collect(),
        ),
        value => value,
    }
}

fn unwrap_direct_record(
    record: serde_json::Value,
    output_mode: DirectOutputMode,
) -> serde_json::Value {
    match output_mode {
        DirectOutputMode::RecordObject => record,
        DirectOutputMode::Value => match record {
            serde_json::Value::Object(mut object) => object
                .remove(DIRECT_VALUE_TARGET)
                .unwrap_or(serde_json::Value::Null),
            value => value,
        },
    }
}
