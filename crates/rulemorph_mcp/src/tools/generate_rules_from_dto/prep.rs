use serde_json::{Map, Value};

use crate::args::{get_optional_json_value, get_optional_string, get_optional_usize};
use crate::diagnostics::parse_error_json;
use crate::dto_language::{DtoSourceLanguage, parse_dto_source_language};
use crate::errors::CallError;
use crate::input_records::{
    InputDataFormat, json_records_from_value, normalize_format, parse_csv_records,
    parse_json_records_strict,
};
use crate::sandbox::read_allowed_to_string;

pub(super) struct PreparedDtoGeneration {
    pub(super) dto_text: String,
    pub(super) dto_language: DtoSourceLanguage,
    pub(super) records: Vec<Value>,
    pub(super) records_path: Option<String>,
    pub(super) format_str: String,
    pub(super) max_candidates: usize,
}

pub(super) fn prepare_dto_generation(
    args: &Map<String, Value>,
) -> Result<PreparedDtoGeneration, CallError> {
    let dto_text = get_optional_string(args, "dto_text").map_err(CallError::InvalidParams)?;
    let dto_language =
        get_optional_string(args, "dto_language").map_err(CallError::InvalidParams)?;
    let input_path = get_optional_string(args, "input_path").map_err(CallError::InvalidParams)?;
    let input_text = get_optional_string(args, "input_text").map_err(CallError::InvalidParams)?;
    let input_json =
        get_optional_json_value(args, "input_json").map_err(CallError::InvalidParams)?;
    let format = get_optional_string(args, "format").map_err(CallError::InvalidParams)?;
    let records_path =
        get_optional_string(args, "records_path").map_err(CallError::InvalidParams)?;
    let max_candidates =
        get_optional_usize(args, "max_candidates").map_err(CallError::InvalidParams)?;

    let dto_text =
        dto_text.ok_or_else(|| CallError::InvalidParams("dto_text is required".to_string()))?;
    let dto_language = dto_language
        .ok_or_else(|| CallError::InvalidParams("dto_language is required".to_string()))?;
    let dto_language =
        parse_dto_source_language(&dto_language).map_err(CallError::InvalidParams)?;

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
    if format.as_deref().is_some_and(|value| {
        !value.eq_ignore_ascii_case("csv") && !value.eq_ignore_ascii_case("json")
    }) {
        return Err(CallError::InvalidParams(
            "format must be csv or json".to_string(),
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

    let has_input_json = input_json.is_some();
    let parse_format = if has_input_json {
        InputDataFormat::Json
    } else if let Some(format) = format.as_deref() {
        if format.eq_ignore_ascii_case("csv") {
            InputDataFormat::Csv
        } else {
            InputDataFormat::Json
        }
    } else {
        normalize_format(None, &input_text)
    };

    let records = match (parse_format, input_json) {
        (InputDataFormat::Json, Some(value)) => {
            json_records_from_value(&value, records_path.as_deref())?
        }
        (InputDataFormat::Json, None) => {
            parse_json_records_strict(&input_text, records_path.as_deref(), input_path.as_deref())?
        }
        (InputDataFormat::Csv, _) => parse_csv_records(&input_text).map_err(|err| {
            let message = format!("failed to parse input CSV: {}", err);
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![parse_error_json(&message, input_path.as_deref())]),
            }
        })?,
    };

    let format_str = if has_input_json {
        "json".to_string()
    } else if let Some(format) = format.as_deref() {
        if format.eq_ignore_ascii_case("csv") {
            "csv".to_string()
        } else {
            "json".to_string()
        }
    } else {
        match parse_format {
            InputDataFormat::Csv => "csv".to_string(),
            InputDataFormat::Json => "json".to_string(),
        }
    };

    Ok(PreparedDtoGeneration {
        dto_text,
        dto_language,
        records,
        records_path,
        format_str,
        max_candidates: max_candidates.unwrap_or(3),
    })
}
