use super::*;

pub(in crate::transform) fn sort_key_from_value(
    value: &JsonValue,
    path: &str,
) -> Result<SortKey, TransformError> {
    match value {
        JsonValue::Number(number) => number.as_f64().map(SortKey::Number).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "sort key must be a finite number",
            )
            .with_path(path)
        }),
        JsonValue::String(value) => Ok(SortKey::String(value.clone())),
        JsonValue::Bool(value) => Ok(SortKey::Bool(*value)),
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "sort key must be string/number/bool",
        )
        .with_path(path)),
    }
}
