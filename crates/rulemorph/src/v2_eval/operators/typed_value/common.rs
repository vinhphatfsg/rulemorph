use serde_json::{Map as JsonMap, Value as JsonValue};

use super::options::{CodecOptions, Hint, HintType, OnMissing, PathPart};
use super::{DYNAMODB_NAME_MAX_BYTES, MAX_TYPED_VALUE_INPUT_STRING_BYTES};
use crate::error::{TransformError, TransformErrorKind};

mod numeric;
mod provider;
mod resource;

pub(super) use numeric::*;
pub(super) use provider::*;
pub(super) use resource::ResourceGuard;

#[derive(Debug, Clone, Copy)]
pub(super) enum PathElem<'a> {
    Key(&'a str),
    Index,
}

pub(super) fn best_hint<'a>(options: &'a CodecOptions, path: &[PathElem<'_>]) -> Option<&'a Hint> {
    let mut best: Option<(&Hint, usize)> = None;
    for hint in &options.hints {
        if hint.path.0.len() != path.len() {
            continue;
        }
        let mut exact = 0usize;
        let mut matches = true;
        for (part, elem) in hint.path.0.iter().zip(path.iter()) {
            match (part, elem) {
                (PathPart::Key(a), PathElem::Key(b)) if a == b => exact += 1,
                (PathPart::AnyIndex, PathElem::Index) => {}
                _ => {
                    matches = false;
                    break;
                }
            }
        }
        if matches {
            if let Some((_, best_exact)) = best {
                if exact > best_exact {
                    best = Some((hint, exact));
                }
            } else {
                best = Some((hint, exact));
            }
        }
    }
    best.map(|(hint, _)| hint)
}

pub(super) fn decode_hint_type(options: &CodecOptions, path: &[PathElem<'_>]) -> Option<HintType> {
    if path.is_empty() {
        options
            .root_type
            .or_else(|| best_hint(options, path).map(|hint| hint.ty))
    } else {
        best_hint(options, path).map(|hint| hint.ty)
    }
}

pub(super) fn check_required_hints(
    input: &JsonValue,
    options: &CodecOptions,
    path: &str,
) -> Result<(), TransformError> {
    for hint in &options.hints {
        if !logical_path_satisfied(input, &hint.path.0) {
            match hint.on_missing {
                OnMissing::Error => {
                    return Err(expr_error("field type path is missing", path));
                }
                OnMissing::Propagate | OnMissing::Ignore => {}
            }
        }
    }
    Ok(())
}

pub(super) fn logical_path_satisfied(value: &JsonValue, parts: &[PathPart]) -> bool {
    let Some((first, rest)) = parts.split_first() else {
        return true;
    };
    match first {
        PathPart::Key(key) => value
            .as_object()
            .and_then(|map| map.get(key))
            .is_some_and(|next| logical_path_satisfied(next, rest)),
        PathPart::AnyIndex => {
            let Some(items) = value.as_array() else {
                return false;
            };
            for item in items {
                if !logical_path_satisfied(item, rest) {
                    return false;
                }
            }
            true
        }
    }
}

pub(super) fn single_key(key: &str, value: JsonValue) -> Result<JsonValue, TransformError> {
    let mut map = JsonMap::new();
    map.insert(key.to_string(), value);
    Ok(JsonValue::Object(map))
}

pub(super) fn wrapper(key: &str, value: JsonValue) -> Result<JsonValue, TransformError> {
    single_key(key, value)
}

pub(super) fn expect_string(
    value: &JsonValue,
    message: &str,
    path: &str,
) -> Result<String, TransformError> {
    let value = value.as_str().ok_or_else(|| expr_error(message, path))?;
    validate_input_string_bytes(value, path)?;
    Ok(value.to_owned())
}

pub(super) fn array_of_strings(
    value: &JsonValue,
    message: &str,
    path: &str,
) -> Result<Vec<String>, TransformError> {
    let values = value.as_array().ok_or_else(|| expr_error(message, path))?;
    values
        .iter()
        .map(|item| {
            let value = item.as_str().ok_or_else(|| expr_error(message, path))?;
            validate_input_string_bytes(value, path)?;
            Ok(value.to_owned())
        })
        .collect()
}

pub(super) fn validate_json_value_input_string_bytes(
    value: &JsonValue,
    path: &str,
) -> Result<(), TransformError> {
    if let Some(value) = value.as_str() {
        validate_input_string_bytes(value, path)?;
    }
    Ok(())
}

pub(super) fn validate_input_string_bytes(value: &str, path: &str) -> Result<(), TransformError> {
    if value.len() > MAX_TYPED_VALUE_INPUT_STRING_BYTES {
        return Err(expr_error(
            "typed value input string bytes exceed configured limit",
            path,
        ));
    }
    Ok(())
}

pub(super) fn validate_dynamodb_key(key: &str, path: &str) -> Result<(), TransformError> {
    if key.is_empty() || key.len() > DYNAMODB_NAME_MAX_BYTES {
        return Err(expr_error(
            "DynamoDB attribute name violates length constraint",
            path,
        ));
    }
    Ok(())
}

pub(super) fn expr_error(message: impl Into<String>, path: &str) -> TransformError {
    TransformError::new(TransformErrorKind::ExprError, message).with_path(path)
}
