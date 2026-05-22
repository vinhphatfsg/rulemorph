use serde_json::{Map, Value};

pub(crate) fn get_optional_string(
    args: &Map<String, Value>,
    key: &str,
) -> Result<Option<String>, String> {
    match args.get(key) {
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(format!("{} must be a string", key)),
        None => Ok(None),
    }
}

pub(crate) fn get_optional_bool(
    args: &Map<String, Value>,
    key: &str,
) -> Result<Option<bool>, String> {
    match args.get(key) {
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(format!("{} must be a boolean", key)),
        None => Ok(None),
    }
}

pub(crate) fn get_optional_usize(
    args: &Map<String, Value>,
    key: &str,
) -> Result<Option<usize>, String> {
    match args.get(key) {
        Some(Value::Number(value)) => value
            .as_u64()
            .and_then(|value| {
                if value > 0 {
                    Some(value as usize)
                } else {
                    None
                }
            })
            .ok_or_else(|| format!("{} must be a positive integer", key))
            .map(Some),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(format!("{} must be a positive integer", key)),
        None => Ok(None),
    }
}

pub(crate) fn get_optional_json_value(
    args: &Map<String, Value>,
    key: &str,
) -> Result<Option<Value>, String> {
    match args.get(key) {
        Some(Value::Array(_)) | Some(Value::Object(_)) => Ok(args.get(key).cloned()),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(format!("{} must be an object or array", key)),
        None => Ok(None),
    }
}

pub(crate) fn get_optional_object(
    args: &Map<String, Value>,
    key: &str,
) -> Result<Option<Value>, String> {
    match args.get(key) {
        Some(Value::Object(_)) => Ok(args.get(key).cloned()),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(format!("{} must be an object", key)),
        None => Ok(None),
    }
}
