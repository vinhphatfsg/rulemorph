use std::path::Path;

use rulemorph::{
    Expr, ExprChain, ExprOp, InputData, RuleError, RuleFile, TransformError, TransformErrorKind,
    TransformWarning, transform_stream_input, transform_stream_input_with_base_dir,
};
use serde_json::{Value, json};

use crate::errors::CallError;

pub(crate) fn transform_to_ndjson(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&serde_json::Value>,
    base_dir: Option<&Path>,
) -> Result<(String, Vec<TransformWarning>), CallError> {
    let stream = match base_dir {
        Some(base_dir) => transform_stream_input_with_base_dir(rule, input, context, base_dir),
        None => transform_stream_input(rule, input, context),
    }
    .map_err(|err| CallError::Tool {
        message: transform_error_to_text(&err),
        errors: Some(vec![transform_error_json(&err)]),
    })?;
    let mut output = String::new();
    let mut warnings = Vec::new();

    for item in stream {
        let item = item.map_err(|err| CallError::Tool {
            message: transform_error_to_text(&err),
            errors: Some(vec![transform_error_json(&err)]),
        })?;
        warnings.extend(item.warnings);
        let output_value = match item.output {
            Some(output_value) => output_value,
            None => continue,
        };
        let line = serde_json::to_string(&output_value).map_err(|err| {
            let message = format!("failed to serialize output JSON: {}", err);
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![parse_error_json(&message, None)]),
            }
        })?;
        output.push_str(&line);
        output.push('\n');
    }

    Ok((output, warnings))
}

pub(crate) struct RuleWarning {
    code: &'static str,
    message: String,
    path: Option<String>,
}

pub(crate) fn collect_rule_warnings(rule: &RuleFile) -> Vec<RuleWarning> {
    let mut warnings = Vec::new();
    if let Some(expr) = &rule.record_when {
        collect_expr_warnings(expr, "record_when", &mut warnings);
    }
    for (index, mapping) in rule.mappings.iter().enumerate() {
        let base_path = format!("mappings[{}]", index);
        if let Some(expr) = &mapping.expr {
            collect_expr_warnings(expr, &format!("{}.expr", base_path), &mut warnings);
        }
        if let Some(expr) = &mapping.when {
            collect_expr_warnings(expr, &format!("{}.when", base_path), &mut warnings);
        }
    }
    warnings
}

fn collect_expr_warnings(expr: &Expr, path: &str, warnings: &mut Vec<RuleWarning>) {
    match expr {
        Expr::Ref(_) | Expr::Literal(_) => {}
        Expr::Op(expr_op) => collect_op_warnings(expr_op, path, false, warnings),
        Expr::Chain(chain) => collect_chain_warnings(chain, path, warnings),
    }
}

fn collect_chain_warnings(chain: &ExprChain, path: &str, warnings: &mut Vec<RuleWarning>) {
    for (index, step) in chain.chain.iter().enumerate() {
        let step_path = format!("{}.chain[{}]", path, index);
        if index == 0 {
            collect_expr_warnings(step, &step_path, warnings);
            continue;
        }

        match step {
            Expr::Op(expr_op) => collect_op_warnings(expr_op, &step_path, true, warnings),
            _ => collect_expr_warnings(step, &step_path, warnings),
        }
    }
}

fn collect_op_warnings(
    expr_op: &ExprOp,
    path: &str,
    chain_step: bool,
    warnings: &mut Vec<RuleWarning>,
) {
    if expr_op.op == "date_format" {
        warn_date_format_missing_input_format(expr_op, path, chain_step, warnings);
    } else if expr_op.op == "to_unixtime" {
        warnings.push(RuleWarning {
            code: "to_unixtime_auto_parse",
            message: "to_unixtime relies on heuristic date parsing; consider normalizing with date_format + input_format.".to_string(),
            path: Some(path.to_string()),
        });
    }

    for (index, arg) in expr_op.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", path, index);
        collect_expr_warnings(arg, &arg_path, warnings);
    }
}

fn warn_date_format_missing_input_format(
    expr_op: &ExprOp,
    path: &str,
    chain_step: bool,
    warnings: &mut Vec<RuleWarning>,
) {
    let input_index = if chain_step { 1 } else { 2 };
    if expr_op.args.len() <= input_index {
        warnings.push(RuleWarning {
            code: "date_format_missing_input_format",
            message: "date_format without input_format relies on heuristic parsing; consider providing input_format.".to_string(),
            path: Some(format!("{}.args", path)),
        });
        return;
    }

    if expr_looks_like_timezone(&expr_op.args[input_index]) {
        warnings.push(RuleWarning {
            code: "date_format_missing_input_format",
            message: "date_format without input_format relies on heuristic parsing; consider providing input_format.".to_string(),
            path: Some(format!("{}.args[{}]", path, input_index)),
        });
    }
}

fn expr_looks_like_timezone(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(Value::String(value)) => looks_like_timezone(value),
        _ => false,
    }
}

fn looks_like_timezone(value: &str) -> bool {
    if value.eq_ignore_ascii_case("utc") || value == "Z" {
        return true;
    }
    matches!(value.chars().next(), Some('+') | Some('-'))
}

pub(crate) fn validation_errors_to_text(errors: &[RuleError]) -> String {
    let values = validation_errors_to_values(errors);
    serde_json::to_string(&values).unwrap_or_else(|_| "validation error".to_string())
}

pub(crate) fn validation_errors_to_values(errors: &[RuleError]) -> Vec<Value> {
    errors.iter().map(validation_error_json).collect()
}

fn validation_error_json(err: &RuleError) -> Value {
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

pub(crate) fn rule_warnings_to_json(warnings: &[RuleWarning]) -> Value {
    let values: Vec<_> = warnings.iter().map(rule_warning_json).collect();
    Value::Array(values)
}

fn rule_warning_json(warning: &RuleWarning) -> Value {
    let mut value = json!({
        "type": "warning",
        "code": warning.code,
        "message": warning.message,
    });
    if let Some(path) = &warning.path {
        value["path"] = json!(path);
    }
    value
}

pub(crate) fn parse_error_json(message: &str, path: Option<&str>) -> Value {
    let mut value = json!({
        "type": "parse",
        "message": message,
    });
    if let Some(path) = path {
        value["path"] = json!(path);
    }
    value
}

pub(crate) fn truncate_to_bytes(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

pub(crate) fn preview_ndjson(text: &str, max_rows: usize) -> String {
    let mut preview = String::new();
    for (index, line) in text.split_terminator('\n').enumerate() {
        if index >= max_rows {
            break;
        }
        preview.push_str(line);
        preview.push('\n');
    }
    preview
}

pub(crate) fn transform_error_to_text(err: &TransformError) -> String {
    let value = transform_error_json(err);
    serde_json::to_string(&vec![value]).unwrap_or_else(|_| err.message.clone())
}

pub(crate) fn transform_error_json(err: &TransformError) -> Value {
    let mut value = json!({
        "type": "transform",
        "kind": transform_kind_to_str(&err.kind),
        "message": err.message,
    });
    if let Some(path) = &err.path {
        value["path"] = json!(path);
    }
    value
}

pub(crate) fn warnings_to_json(warnings: &[TransformWarning]) -> Value {
    let values: Vec<_> = warnings.iter().map(transform_warning_json).collect();
    Value::Array(values)
}

fn transform_warning_json(warning: &TransformWarning) -> Value {
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
