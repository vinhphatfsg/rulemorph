use serde_json::{Value, json};

pub(crate) enum CallError {
    InvalidParams(String),
    Tool {
        message: String,
        errors: Option<Vec<Value>>,
    },
}

pub(crate) fn tool_error_result(message: &str, errors: Option<Vec<Value>>) -> Value {
    let mut result = json!({
        "content": [
            {
                "type": "text",
                "text": message
            }
        ],
        "isError": true
    });

    if let Some(errors) = errors {
        result["meta"] = json!({ "errors": errors });
    }

    result
}

pub(crate) fn io_error_json(message: &str, path: Option<&str>) -> Value {
    let mut value = json!({
        "type": "io",
        "message": message
    });
    if let Some(path) = path {
        value["path"] = json!(path);
    }
    value
}
