use serde_json::Value as JsonValue;

use crate::error::{TransformError, TransformErrorKind};
use crate::path::{get_path, parse_path};

use super::NormalizationOptions;

pub(crate) fn enforce_records_limit(
    count: usize,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    if count > options.max_records {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "input exceeds max_records",
        ));
    }
    Ok(())
}

pub(crate) fn enforce_json_limits(
    value: &JsonValue,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    fn walk(
        value: &JsonValue,
        depth: usize,
        options: &NormalizationOptions,
    ) -> Result<(), TransformError> {
        if depth > options.max_depth {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "input exceeds max_depth",
            ));
        }
        match value {
            JsonValue::Array(items) => {
                if items.len() > options.max_array_len {
                    return Err(TransformError::new(
                        TransformErrorKind::InvalidInput,
                        "input exceeds max_array_len",
                    ));
                }
                for item in items {
                    walk(item, depth + 1, options)?;
                }
            }
            JsonValue::Object(map) => {
                for value in map.values() {
                    walk(value, depth + 1, options)?;
                }
            }
            JsonValue::String(value) => {
                if value.len() > options.max_text_bytes {
                    return Err(TransformError::new(
                        TransformErrorKind::InvalidInput,
                        "input exceeds max_text_bytes",
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    walk(value, 0, options)
}

pub(crate) fn select_records_from_document(
    value: &JsonValue,
    records_path: Option<&str>,
    path_for_error: &'static str,
    options: &NormalizationOptions,
) -> Result<Vec<JsonValue>, TransformError> {
    let records_value = match records_path {
        Some(path) => {
            let tokens = parse_path(path).map_err(|err| {
                TransformError::new(TransformErrorKind::InvalidRecordsPath, err.message())
                    .with_path(path_for_error)
            })?;
            get_path(value, &tokens).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::InvalidRecordsPath,
                    "records_path does not exist",
                )
                .with_path(path_for_error)
            })?
        }
        None => value,
    };

    match records_value {
        JsonValue::Array(items) => {
            enforce_records_limit(items.len(), options)?;
            Ok(items.clone())
        }
        JsonValue::Object(_) => {
            enforce_records_limit(1, options)?;
            Ok(vec![records_value.clone()])
        }
        _ => Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "records_path must point to an array or object",
        )),
    }
}
