use rulemorph::{TransformError, TransformErrorKind, TransformWarning};
use serde_json::json;

use super::super::ErrorFormat;

pub(crate) fn emit_transform_error(err: &TransformError, format: ErrorFormat) {
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

pub(crate) fn emit_transform_warnings(warnings: &[TransformWarning], format: ErrorFormat) {
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
