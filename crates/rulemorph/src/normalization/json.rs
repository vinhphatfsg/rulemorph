use serde_json::Value as JsonValue;

use crate::error::{TransformError, TransformErrorKind};
use crate::model::RuleFile;
use crate::serde_guard::parse_json_value_strict;

use super::{
    NormalizationOptions, enforce_json_limits, enforce_records_limit, select_records_from_document,
};

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

    let records = select_records_from_document(
        &value,
        rule.input
            .json
            .as_ref()
            .and_then(|json| json.records_path.as_deref()),
        "input.json.records_path",
    )?;
    enforce_records_limit(records.len(), options)?;
    Ok(records)
}
