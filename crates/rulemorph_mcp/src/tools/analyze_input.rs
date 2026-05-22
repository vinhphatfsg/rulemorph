use serde_json::{Map, Value, json};

use crate::args::{get_optional_json_value, get_optional_string, get_optional_usize};
use crate::diagnostics::parse_error_json;
use crate::errors::CallError;
use crate::input_analysis::{analyze_records, stats_to_json};
use crate::input_records::{
    InputDataFormat, json_records_from_value, normalize_format, parse_csv_records,
    parse_json_records_strict,
};
use crate::sandbox::read_allowed_to_string;

pub(crate) fn run_analyze_input_tool(args: &Map<String, Value>) -> Result<Value, CallError> {
    let input_path = get_optional_string(args, "input_path").map_err(CallError::InvalidParams)?;
    let input_text = get_optional_string(args, "input_text").map_err(CallError::InvalidParams)?;
    let input_json =
        get_optional_json_value(args, "input_json").map_err(CallError::InvalidParams)?;
    let format = get_optional_string(args, "format").map_err(CallError::InvalidParams)?;
    let records_path =
        get_optional_string(args, "records_path").map_err(CallError::InvalidParams)?;
    let max_paths = get_optional_usize(args, "max_paths").map_err(CallError::InvalidParams)?;

    let input_source_count =
        input_path.is_some() as u8 + input_text.is_some() as u8 + input_json.is_some() as u8;
    if input_source_count == 0 {
        return Err(CallError::InvalidParams(
            "input_path, input_text, or input_json is required".to_string(),
        ));
    }
    if input_source_count > 1 {
        return Err(CallError::InvalidParams(
            "input_path, input_text, and input_json are mutually exclusive".to_string(),
        ));
    }

    if input_json.is_some()
        && format
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case("csv"))
    {
        return Err(CallError::InvalidParams(
            "format must be json when input_json is provided".to_string(),
        ));
    }

    let input_text = match (input_path.as_deref(), input_text.as_deref()) {
        (Some(path), None) => read_allowed_to_string(path, "input")?,
        (None, Some(text)) => text.to_string(),
        (None, None) => String::new(),
        _ => {
            return Err(CallError::InvalidParams(
                "input_path, input_text, or input_json is required".to_string(),
            ));
        }
    };

    let records = if let Some(value) = input_json {
        json_records_from_value(&value, records_path.as_deref())?
    } else {
        match normalize_format(format.as_deref(), &input_text) {
            InputDataFormat::Json => parse_json_records_strict(
                &input_text,
                records_path.as_deref(),
                input_path.as_deref(),
            )?,
            InputDataFormat::Csv => parse_csv_records(&input_text).map_err(|err| {
                let message = format!("failed to parse input CSV: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, input_path.as_deref())]),
                }
            })?,
        }
    };

    let stats = analyze_records(&records, max_paths);
    let paths_json = stats_to_json(&stats);

    let summary = json!({
        "records": records.len(),
        "paths": stats.len()
    });

    let meta = json!({
        "summary": summary,
        "paths": paths_json
    });
    let text = serde_json::to_string_pretty(&meta)
        .unwrap_or_else(|_| "{\"error\":\"failed to serialize analysis\"}".to_string());

    Ok(json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "meta": meta
    }))
}
