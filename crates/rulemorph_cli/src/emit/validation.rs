use rulemorph::RuleError;
use serde_json::json;

use super::super::ErrorFormat;

pub(crate) fn emit_validation_errors(errors: &[RuleError], format: ErrorFormat) {
    match format {
        ErrorFormat::Text => {
            for err in errors {
                emit_validation_text(err);
            }
        }
        ErrorFormat::Json => {
            let values: Vec<_> = errors
                .iter()
                .map(|err| validation_error_json(err))
                .collect();
            eprintln!("{}", serde_json::to_string(&values).unwrap_or_default());
        }
    }
}

fn emit_validation_text(err: &RuleError) {
    let mut parts = Vec::new();
    parts.push(format!("E {}", err.code.as_str()));
    if let Some(path) = &err.path {
        parts.push(format!("path={}", path));
    }
    if let Some(location) = &err.location {
        parts.push(format!("line={}", location.line));
        parts.push(format!("col={}", location.column));
    }
    parts.push(format!("msg=\"{}\"", err.message));
    eprintln!("{}", parts.join(" "));
}

fn validation_error_json(err: &RuleError) -> serde_json::Value {
    let mut value = json!({
        "type": "validation",
        "code": err.code.as_str(),
        "message": err.message,
    });

    if let Some(path) = &err.path {
        value["path"] = json!(path);
    }
    if let Some(location) = &err.location {
        value["line"] = json!(location.line);
        value["column"] = json!(location.column);
    }

    value
}
