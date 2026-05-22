use serde_json::{Map, Value};

use crate::args::{
    get_optional_bool, get_optional_json_value, get_optional_object, get_optional_string,
    get_optional_usize,
};
use crate::diagnostics::parse_error_json;
use crate::errors::CallError;
use crate::rule_source::{
    OwnedInput, load_rule_from_source, rule_has_file_branch, validate_transform_format,
};
use crate::rules_yaml::apply_format_override;
use crate::sandbox::{read_allowed_bytes, read_allowed_to_string};

use super::PreparedTransform;

pub(super) fn prepare_transform(args: &Map<String, Value>) -> Result<PreparedTransform, CallError> {
    let rules_path = get_optional_string(args, "rules_path").map_err(CallError::InvalidParams)?;
    let rules_text = get_optional_string(args, "rules_text").map_err(CallError::InvalidParams)?;
    let rules_format =
        get_optional_string(args, "rules_format").map_err(CallError::InvalidParams)?;
    let input_path = get_optional_string(args, "input_path").map_err(CallError::InvalidParams)?;
    let input_text = get_optional_string(args, "input_text").map_err(CallError::InvalidParams)?;
    let input_json =
        get_optional_json_value(args, "input_json").map_err(CallError::InvalidParams)?;
    let context_path =
        get_optional_string(args, "context_path").map_err(CallError::InvalidParams)?;
    let context_json =
        get_optional_object(args, "context_json").map_err(CallError::InvalidParams)?;
    let format = get_optional_string(args, "format").map_err(CallError::InvalidParams)?;
    let ndjson = get_optional_bool(args, "ndjson")
        .map_err(CallError::InvalidParams)?
        .unwrap_or(false);
    let validate = get_optional_bool(args, "validate")
        .map_err(CallError::InvalidParams)?
        .unwrap_or(false);
    let output_path = get_optional_string(args, "output_path").map_err(CallError::InvalidParams)?;
    let max_output_bytes =
        get_optional_usize(args, "max_output_bytes").map_err(CallError::InvalidParams)?;
    let preview_rows =
        get_optional_usize(args, "preview_rows").map_err(CallError::InvalidParams)?;
    let return_output_json = get_optional_bool(args, "return_output_json")
        .map_err(CallError::InvalidParams)?
        .unwrap_or(false);

    let rule_source_count = rules_path.is_some() as u8 + rules_text.is_some() as u8;
    if rule_source_count == 0 {
        return Err(CallError::InvalidParams(
            "rules_path or rules_text is required".to_string(),
        ));
    }
    if rule_source_count > 1 {
        return Err(CallError::InvalidParams(
            "rules_path and rules_text are mutually exclusive".to_string(),
        ));
    }

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

    if context_path.is_some() && context_json.is_some() {
        return Err(CallError::InvalidParams(
            "context_path and context_json are mutually exclusive".to_string(),
        ));
    }

    if input_json.is_some()
        && format
            .as_deref()
            .is_some_and(|value| !value.eq_ignore_ascii_case("json"))
    {
        return Err(CallError::InvalidParams(
            "format must be json when input_json is provided".to_string(),
        ));
    }
    validate_transform_format(format.as_deref())?;

    let (mut rule, yaml, base_dir) = load_rule_from_source(
        rules_path.as_deref(),
        rules_text.as_deref(),
        rules_format.as_deref(),
    )?;
    if rules_text.is_some() && rule_has_file_branch(&rule) {
        return Err(CallError::InvalidParams(
            "rules_text cannot use branch file references; use rules_path under an allowed root"
                .to_string(),
        ));
    }

    let input = match (
        input_path.as_deref(),
        input_text.as_deref(),
        input_json.as_ref(),
    ) {
        (Some(path), None, None) => OwnedInput::Bytes(read_allowed_bytes(path, "input")?),
        (None, Some(text), None) => OwnedInput::Text(text.to_string()),
        (None, None, Some(value)) => serde_json::to_string(value)
            .map_err(|err| {
                let message = format!("failed to serialize input JSON: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, None)]),
                }
            })
            .map(OwnedInput::Text)?,
        _ => {
            return Err(CallError::InvalidParams(
                "input_path, input_text, or input_json is required".to_string(),
            ));
        }
    };

    let context_value = match (context_path.as_deref(), context_json.as_ref()) {
        (Some(path), None) => {
            let data = read_allowed_to_string(path, "context")?;
            Some(serde_json::from_str(&data).map_err(|err| {
                let message = format!("failed to parse context JSON: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, Some(path))]),
                }
            })?)
        }
        (None, Some(value)) => Some(value.clone()),
        (None, None) => None,
        _ => None,
    };

    let format_override = if input_json.is_some() {
        Some("json".to_string())
    } else {
        format
    };
    apply_format_override(&mut rule, format_override.as_deref())
        .map_err(CallError::InvalidParams)?;

    Ok(PreparedTransform {
        rule,
        yaml,
        base_dir,
        input,
        context_value,
        ndjson,
        validate,
        output_path,
        max_output_bytes,
        preview_rows,
        return_output_json,
    })
}
