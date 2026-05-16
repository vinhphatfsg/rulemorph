use rulemorph::{RuleError, TransformError, TransformErrorKind, TransformWarning};
use serde_json::json;

use super::ErrorFormat;

pub(super) fn emit_validation_errors(errors: &[RuleError], format: ErrorFormat) {
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

#[cfg(feature = "server")]
pub(super) fn emit_rules_dir_errors(
    errors: &rulemorph_server::RulesDirErrors,
    format: ErrorFormat,
) {
    match format {
        ErrorFormat::Text => {
            eprintln!("{}", errors);
        }
        ErrorFormat::Json => {
            let values: Vec<_> = errors
                .errors
                .iter()
                .map(|err| rules_dir_error_json(err))
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

#[cfg(feature = "server")]
fn rules_dir_error_json(err: &rulemorph_server::RulesDirError) -> serde_json::Value {
    let mut value = json!({
        "type": "rules_dir",
        "code": err.code,
        "message": err.message,
        "file": err.file.to_string_lossy(),
    });
    if let Some(path) = &err.path {
        value["path"] = json!(path);
    }
    if let Some(line) = err.line {
        value["line"] = json!(line);
    }
    if let Some(column) = err.column {
        value["column"] = json!(column);
    }
    value
}

pub(super) fn emit_transform_error(err: &TransformError, format: ErrorFormat) {
    match format {
        ErrorFormat::Text => {
            let mut parts = Vec::new();
            parts.push(format!("E {}", transform_kind_to_str(&err.kind)));
            if let Some(path) = &err.path {
                parts.push(format!("path={}", path));
            }
            parts.push(format!("msg=\"{}\"", err.message));
            eprintln!("{}", parts.join(" "));
        }
        ErrorFormat::Json => {
            let mut value = json!({
                "type": "transform",
                "kind": transform_kind_to_str(&err.kind),
                "message": err.message,
            });
            if let Some(path) = &err.path {
                value["path"] = json!(path);
            }
            eprintln!(
                "{}",
                serde_json::to_string(&vec![value]).unwrap_or_default()
            );
        }
    }
}

pub(super) fn emit_transform_warnings(warnings: &[TransformWarning], format: ErrorFormat) {
    if warnings.is_empty() {
        return;
    }

    match format {
        ErrorFormat::Text => {
            for warning in warnings {
                let mut parts = Vec::new();
                parts.push(format!("W {}", transform_kind_to_str(&warning.kind)));
                if let Some(path) = &warning.path {
                    parts.push(format!("path={}", path));
                }
                parts.push(format!("msg=\"{}\"", warning.message));
                eprintln!("{}", parts.join(" "));
            }
        }
        ErrorFormat::Json => {
            let values: Vec<_> = warnings
                .iter()
                .map(|warning| transform_warning_json(warning))
                .collect();
            eprintln!("{}", serde_json::to_string(&values).unwrap_or_default());
        }
    }
}

fn transform_warning_json(warning: &TransformWarning) -> serde_json::Value {
    let mut value = json!({
        "type": "warning",
        "kind": transform_kind_to_str(&warning.kind),
        "message": warning.message,
    });
    if let Some(path) = &warning.path {
        value["path"] = json!(path);
    }
    value
}

fn transform_kind_to_str(kind: &TransformErrorKind) -> &'static str {
    match kind {
        TransformErrorKind::InvalidInput => "InvalidInput",
        TransformErrorKind::InvalidRecordsPath => "InvalidRecordsPath",
        TransformErrorKind::InvalidRef => "InvalidRef",
        TransformErrorKind::InvalidTarget => "InvalidTarget",
        TransformErrorKind::MissingRequired => "MissingRequired",
        TransformErrorKind::TypeCastFailed => "TypeCastFailed",
        TransformErrorKind::ExprError => "ExprError",
        TransformErrorKind::AssertionFailed => "AssertionFailed",
    }
}
