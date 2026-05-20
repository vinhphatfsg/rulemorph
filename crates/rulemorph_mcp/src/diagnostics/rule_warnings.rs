use rulemorph::{Expr, ExprChain, ExprOp, RuleFile};
use serde_json::{Value, json};

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
