use rulemorph::Expr;
use rulemorph::v2_parser::{is_literal_escape, is_pipe_value, is_v2_ref};
use serde_json::{Map as JsonMap, Value as JsonValue, json};

pub(in crate::endpoint_engine::trace_graph) fn expr_to_json_for_v2_pipe(
    expr: &Expr,
) -> Option<JsonValue> {
    match expr {
        Expr::Literal(JsonValue::Array(arr)) => Some(JsonValue::Array(arr.clone())),
        Expr::Literal(JsonValue::String(value)) => {
            if is_v2_ref(value) || is_pipe_value(value) || is_literal_escape(value) {
                Some(JsonValue::String(value.clone()))
            } else {
                None
            }
        }
        Expr::Ref(expr_ref)
            if is_v2_ref(&expr_ref.ref_path)
                || is_pipe_value(&expr_ref.ref_path)
                || is_literal_escape(&expr_ref.ref_path) =>
        {
            Some(JsonValue::Array(vec![JsonValue::String(
                expr_ref.ref_path.clone(),
            )]))
        }
        Expr::Chain(chain) => {
            if let Some(first) = chain.chain.first()
                && expr_starts_v2_pipe(first)
            {
                let items: Vec<JsonValue> = chain.chain.iter().map(expr_to_json_value).collect();
                return Some(JsonValue::Array(items));
            }
            None
        }
        _ => None,
    }
}

pub(in crate::endpoint_engine::trace_graph) fn expr_to_json_for_v2_condition(
    expr: &Expr,
) -> Option<JsonValue> {
    match expr {
        Expr::Literal(value) => Some(value.clone()),
        Expr::Ref(reference)
            if is_v2_ref(&reference.ref_path)
                || is_pipe_value(&reference.ref_path)
                || is_literal_escape(&reference.ref_path) =>
        {
            Some(JsonValue::String(reference.ref_path.clone()))
        }
        Expr::Chain(chain) => {
            if let Some(first) = chain.chain.first()
                && expr_starts_v2_pipe(first)
            {
                let items: Vec<JsonValue> = chain
                    .chain
                    .iter()
                    .map(expr_to_json_value_for_condition)
                    .collect();
                return Some(JsonValue::Array(items));
            }
            None
        }
        _ => None,
    }
}

fn expr_to_json_value_for_condition(expr: &Expr) -> JsonValue {
    match expr {
        Expr::Ref(reference) => JsonValue::String(reference.ref_path.clone()),
        Expr::Literal(value) => value.clone(),
        Expr::Op(op) => {
            let args: Vec<JsonValue> = op
                .args
                .iter()
                .map(expr_to_json_value_for_condition)
                .collect();
            let mut obj = JsonMap::new();
            obj.insert(op.op.clone(), JsonValue::Array(args));
            JsonValue::Object(obj)
        }
        Expr::Chain(chain) => {
            let items: Vec<JsonValue> = chain
                .chain
                .iter()
                .map(expr_to_json_value_for_condition)
                .collect();
            JsonValue::Array(items)
        }
    }
}

pub(in crate::endpoint_engine::trace_graph) fn expr_to_json_value(expr: &Expr) -> JsonValue {
    match expr {
        Expr::Ref(reference) => json!({ "ref": reference.ref_path }),
        Expr::Op(op) => {
            let args: Vec<JsonValue> = op.args.iter().map(expr_to_json_value).collect();
            json!({ "op": op.op, "args": args })
        }
        Expr::Chain(chain) => {
            let items: Vec<JsonValue> = chain.chain.iter().map(expr_to_json_value).collect();
            JsonValue::Array(items)
        }
        Expr::Literal(value) => value.clone(),
    }
}

fn expr_starts_v2_pipe(expr: &Expr) -> bool {
    match expr {
        Expr::Ref(reference) => {
            is_v2_ref(&reference.ref_path)
                || is_pipe_value(&reference.ref_path)
                || is_literal_escape(&reference.ref_path)
        }
        Expr::Literal(JsonValue::String(value)) => {
            is_v2_ref(value) || is_pipe_value(value) || is_literal_escape(value)
        }
        _ => false,
    }
}
