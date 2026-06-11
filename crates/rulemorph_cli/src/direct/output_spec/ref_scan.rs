use rulemorph::{
    Expr, ExprChain, ExprOp, ExprRef,
    v2_model::{
        V2CallArg, V2Comparison, V2Condition, V2Expr, V2ObjectFieldValue, V2Pipe, V2Ref, V2Start,
        V2Step,
    },
    v2_parser::parse_v2_expr,
};

pub(in crate::direct) fn expr_has_evaluated_root_ref(expr: &serde_json::Value, root: &str) -> bool {
    scan_expr_refs(expr, root)
}

fn scan_expr_refs(expr: &serde_json::Value, root: &str) -> bool {
    if let Some(expr) = parse_v1_expr_ref_shape(expr) {
        return scan_v1_expr_refs(&expr, root);
    }
    parse_v2_expr(expr)
        .ok()
        .is_some_and(|expr| scan_v2_expr_refs(&expr, root))
}

fn parse_v1_expr_ref_shape(expr: &serde_json::Value) -> Option<Expr> {
    match expr {
        serde_json::Value::Object(values)
            if values.contains_key("ref")
                || values.contains_key("op")
                || values.contains_key("chain") =>
        {
            serde_json::from_value(expr.clone()).ok()
        }
        _ => None,
    }
}

fn scan_v1_expr_refs(expr: &Expr, root: &str) -> bool {
    match expr {
        Expr::Ref(ExprRef { ref_path }) => is_v1_root_ref(ref_path, root),
        Expr::Op(ExprOp { args, .. }) => args.iter().any(|expr| scan_v1_expr_refs(expr, root)),
        Expr::Chain(ExprChain { chain }) => chain.iter().any(|expr| scan_v1_expr_refs(expr, root)),
        Expr::Literal(_) => false,
    }
}

fn scan_v2_expr_refs(expr: &V2Expr, root: &str) -> bool {
    match expr {
        V2Expr::Pipe(pipe) => scan_v2_pipe_refs(pipe, root),
        V2Expr::V1Fallback(expr) => scan_v1_expr_refs(expr, root),
    }
}

fn scan_v2_pipe_refs(pipe: &V2Pipe, root: &str) -> bool {
    scan_v2_start_refs(&pipe.start, root)
        || pipe.steps.iter().any(|step| scan_v2_step_refs(step, root))
}

fn scan_v2_start_refs(start: &V2Start, root: &str) -> bool {
    match start {
        V2Start::Ref(reference) => is_v2_root_ref(reference, root),
        V2Start::V1Expr(expr) => scan_v1_expr_refs(expr, root),
        V2Start::PipeValue | V2Start::ImplicitPipeValue | V2Start::Literal(_) => false,
    }
}

fn scan_v2_step_refs(step: &V2Step, root: &str) -> bool {
    match step {
        V2Step::Ref(reference) => is_v2_root_ref(reference, root),
        V2Step::Op(op) => op.args.iter().any(|expr| scan_v2_expr_refs(expr, root)),
        V2Step::Object(object) => object.fields.iter().any(|field| match &field.value {
            V2ObjectFieldValue::Expr(expr) => scan_v2_expr_refs(expr, root),
            V2ObjectFieldValue::Value(_) => false,
        }),
        V2Step::CustomCall(call) => call.with.as_ref().is_some_and(|args| {
            args.iter().any(|(_, arg)| match arg {
                V2CallArg::Expr(expr) => scan_v2_expr_refs(expr, root),
                V2CallArg::Value(_) => false,
            })
        }),
        V2Step::Let(let_step) => let_step
            .bindings
            .iter()
            .any(|(_, expr)| scan_v2_expr_refs(expr, root)),
        V2Step::If(if_step) => {
            scan_v2_condition_refs(&if_step.cond, root)
                || scan_v2_pipe_refs(&if_step.then_branch, root)
                || if_step
                    .else_branch
                    .as_ref()
                    .is_some_and(|pipe| scan_v2_pipe_refs(pipe, root))
        }
        V2Step::Map(map_step) => map_step
            .steps
            .iter()
            .any(|step| scan_v2_step_refs(step, root)),
    }
}

fn scan_v2_condition_refs(condition: &V2Condition, root: &str) -> bool {
    match condition {
        V2Condition::All(conditions) | V2Condition::Any(conditions) => conditions
            .iter()
            .any(|condition| scan_v2_condition_refs(condition, root)),
        V2Condition::Comparison(V2Comparison { args, .. }) => {
            args.iter().any(|expr| scan_v2_expr_refs(expr, root))
        }
        V2Condition::Expr(expr) => scan_v2_expr_refs(expr, root),
    }
}

fn is_v2_root_ref(reference: &V2Ref, root: &str) -> bool {
    match (reference, root) {
        (V2Ref::Input(path), "input") => is_canonical_numeric_root_path(path),
        (V2Ref::Out(_), "out") => true,
        (V2Ref::Context(_), "context") => true,
        _ => false,
    }
}

fn is_v1_root_ref(value: &str, root: &str) -> bool {
    if value == root {
        return root != "input";
    }
    let prefix = format!("{}.", root);
    let Some(rest) = value.strip_prefix(&prefix) else {
        return false;
    };
    if root == "input" {
        return is_canonical_numeric_root_path(rest);
    }
    !rest.is_empty()
}

fn is_canonical_numeric_root_path(rest: &str) -> bool {
    let digit_count = rest
        .as_bytes()
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if digit_count == 0 {
        return false;
    }
    let digits = &rest[..digit_count];
    if digits.len() > 1 && digits.starts_with('0') {
        return false;
    }
    matches!(
        rest.as_bytes().get(digit_count),
        None | Some(b'.') | Some(b'[')
    )
}
