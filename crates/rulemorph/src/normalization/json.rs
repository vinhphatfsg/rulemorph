use serde_json::Value as JsonValue;

use crate::error::{TransformError, TransformErrorKind};
use crate::model::RuleFile;
use crate::path::{get_path, parse_path};
use crate::serde_guard::parse_json_value_strict;

use super::{NormalizationOptions, enforce_json_limits, enforce_records_limit};

pub fn normalize_json_records(
    rule: &RuleFile,
    input: &str,
    options: &NormalizationOptions,
) -> Result<Vec<JsonValue>, TransformError> {
    let value = parse_json_value_strict(input).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to parse JSON input: {}", err),
        )
    })?;
    enforce_json_limits(&value, options)?;

    let records_value = match rule
        .input
        .json
        .as_ref()
        .and_then(|json| json.records_path.as_deref())
    {
        Some(path) => {
            let tokens = parse_path(path).map_err(|err| {
                TransformError::new(TransformErrorKind::InvalidRecordsPath, err.message())
                    .with_path("input.json.records_path")
            })?;
            get_path(&value, &tokens).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::InvalidRecordsPath,
                    "records_path does not exist",
                )
                .with_path("input.json.records_path")
            })?
        }
        None => &value,
    };

    let records = match records_value {
        JsonValue::Array(items) => items.clone(),
        JsonValue::Object(_) => vec![records_value.clone()],
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "records_path must point to an array or object",
            ));
        }
    };
    enforce_records_limit(records.len(), options)?;
    Ok(records)
}
