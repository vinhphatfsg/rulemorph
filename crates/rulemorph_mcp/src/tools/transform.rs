mod prep;

use std::path::PathBuf;

use rulemorph::{
    RuleFile, transform_input_with_warnings, transform_input_with_warnings_with_base_dir,
    validate_rule_file_with_source, validate_rule_file_with_source_and_base_dir,
};
use serde_json::{Map, Value, json};

use crate::diagnostics::{
    parse_error_json, preview_ndjson, transform_error_json, transform_error_to_text,
    transform_to_ndjson, truncate_to_bytes, validation_errors_to_text, validation_errors_to_values,
    warnings_to_json,
};
use crate::errors::{CallError, io_error_json};
use crate::rule_source::OwnedInput;
use crate::sandbox::write_allowed_output;
use prep::prepare_transform;

struct PreparedTransform {
    rule: RuleFile,
    yaml: String,
    base_dir: Option<PathBuf>,
    input: OwnedInput,
    context_value: Option<Value>,
    ndjson: bool,
    validate: bool,
    output_path: Option<String>,
    max_output_bytes: Option<usize>,
    preview_rows: Option<usize>,
    return_output_json: bool,
}

pub(crate) fn run_transform_tool(args: &Map<String, Value>) -> Result<Value, CallError> {
    let prepared = prepare_transform(args)?;

    if prepared.validate {
        let validation_result = match prepared.base_dir.as_deref() {
            Some(base_dir) => validate_rule_file_with_source_and_base_dir(
                &prepared.rule,
                &prepared.yaml,
                base_dir,
            ),
            None => validate_rule_file_with_source(&prepared.rule, &prepared.yaml),
        };
        if let Err(errors) = validation_result {
            let error_text = validation_errors_to_text(&errors);
            let error_values = validation_errors_to_values(&errors);
            return Err(CallError::Tool {
                message: error_text,
                errors: Some(error_values),
            });
        }
    }

    let (output_value, output_text, warnings) = if prepared.ndjson {
        let (output_text, warnings) = transform_to_ndjson(
            &prepared.rule,
            prepared.input.as_input_data(),
            prepared.context_value.as_ref(),
            prepared.base_dir.as_deref(),
        )?;
        (None, output_text, warnings)
    } else {
        let (output, warnings) = match prepared.base_dir.as_deref() {
            Some(base_dir) => transform_input_with_warnings_with_base_dir(
                &prepared.rule,
                prepared.input.as_input_data(),
                prepared.context_value.as_ref(),
                base_dir,
            ),
            None => transform_input_with_warnings(
                &prepared.rule,
                prepared.input.as_input_data(),
                prepared.context_value.as_ref(),
            ),
        }
        .map_err(|err| CallError::Tool {
            message: transform_error_to_text(&err),
            errors: Some(vec![transform_error_json(&err)]),
        })?;
        let output_text = serde_json::to_string(&output).map_err(|err| {
            let message = format!("failed to serialize output JSON: {}", err);
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![parse_error_json(&message, None)]),
            }
        })?;
        (Some(output), output_text, warnings)
    };

    if let Some(path) = prepared.output_path.as_deref() {
        write_allowed_output(path, &output_text).map_err(|err| {
            let message = err;
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![io_error_json(&message, Some(path))]),
            }
        })?;
    }

    let output_bytes = output_text.len();
    let mut response_text = output_text.clone();
    let mut truncated = false;

    if prepared.ndjson
        && let Some(limit) = prepared.preview_rows
    {
        let preview = preview_ndjson(&output_text, limit);
        if preview.len() != output_text.len() {
            truncated = true;
        }
        response_text = preview;
    }

    if let Some(max_bytes) = prepared.max_output_bytes {
        if output_bytes > max_bytes {
            truncated = true;
        }
        if response_text.len() > max_bytes {
            response_text = truncate_to_bytes(&response_text, max_bytes).to_string();
            truncated = true;
        }
    }

    let mut result = json!({
        "content": [
            {
                "type": "text",
                "text": response_text
            }
        ]
    });

    let exceeds_max = prepared
        .max_output_bytes
        .is_some_and(|max| output_bytes > max);
    let mut meta = serde_json::Map::new();
    if !warnings.is_empty() {
        meta.insert("warnings".to_string(), warnings_to_json(&warnings));
    }
    if let Some(path) = prepared.output_path {
        meta.insert("output_path".to_string(), json!(path));
    }
    if truncated {
        meta.insert("output_bytes".to_string(), json!(output_bytes));
        meta.insert("truncated".to_string(), json!(true));
    }
    if prepared.return_output_json
        && !prepared.ndjson
        && !exceeds_max
        && let Some(output) = output_value
    {
        meta.insert("output".to_string(), output);
    }
    if !meta.is_empty() {
        result["meta"] = Value::Object(meta);
    }

    Ok(result)
}
