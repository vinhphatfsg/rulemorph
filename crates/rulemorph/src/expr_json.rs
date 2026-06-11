use serde_json::{Map, Value as JsonValue};

use crate::model::Expr;
use crate::v2_parser::{is_literal_escape, is_pipe_value, is_v2_ref};

pub(crate) fn literal_string(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(value) => value.as_str(),
        _ => None,
    }
}

/// Convert an Expr to JSON value for v2 pipe parsing.
/// Returns Some if the expr looks like a v2 pipe expression:
/// - Literal(Array) -> direct array
/// - Ref where ref_path starts with @ -> single element array
/// - Chain where first element starts with @ -> convert to array
///
/// Returns None if it looks like v1 expression and should be handled by v1 eval.
pub(crate) fn expr_to_json_for_v2_pipe(expr: &Expr) -> Option<JsonValue> {
    match expr {
        Expr::Literal(JsonValue::Array(arr)) => {
            // Direct array - v2 pipe
            Some(JsonValue::Array(arr.clone()))
        }
        Expr::Literal(JsonValue::String(s)) => {
            if is_v2_ref(s) || is_pipe_value(s) || is_literal_escape(s) {
                Some(JsonValue::String(s.clone()))
            } else {
                None
            }
        }
        Expr::Ref(expr_ref)
            if is_v2_ref(&expr_ref.ref_path)
                || is_pipe_value(&expr_ref.ref_path)
                || is_literal_escape(&expr_ref.ref_path) =>
        {
            // Single v2 reference or literal escape (serde collapsed 1-element array)
            // Wrap it as single-element array
            Some(JsonValue::Array(vec![JsonValue::String(
                expr_ref.ref_path.clone(),
            )]))
        }
        Expr::Chain(chain) => {
            // Check if first element is a v2 start value. serde may decode
            // single-element arrays like ["$"] through ExprChain.
            if let Some(first) = chain.chain.first()
                && expr_starts_v2_pipe(first)
            {
                // Convert chain to array
                let arr: Vec<JsonValue> = chain.chain.iter().map(expr_to_json_value).collect();
                return Some(JsonValue::Array(arr));
            }
            None
        }
        _ => None,
    }
}

/// Convert an Expr to JSON value for v2 condition parsing.
/// Accepts literal values and v2-looking refs/chains while avoiding v1-only forms.
pub(crate) fn expr_to_json_for_v2_condition(expr: &Expr) -> Option<JsonValue> {
    match expr {
        Expr::Literal(value) => Some(value.clone()),
        Expr::Ref(ref_expr)
            if is_v2_ref(&ref_expr.ref_path)
                || is_pipe_value(&ref_expr.ref_path)
                || is_literal_escape(&ref_expr.ref_path) =>
        {
            Some(JsonValue::String(ref_expr.ref_path.clone()))
        }
        Expr::Chain(chain) => {
            if let Some(first) = chain.chain.first()
                && expr_starts_v2_pipe(first)
            {
                let arr: Vec<JsonValue> = chain.chain.iter().map(expr_to_json_value).collect();
                return Some(JsonValue::Array(arr));
            }
            None
        }
        _ => None,
    }
}

/// Helper to convert Expr to JsonValue (for Chain conversion).
fn expr_to_json_value(expr: &Expr) -> JsonValue {
    match expr {
        Expr::Ref(r) => JsonValue::String(r.ref_path.clone()),
        Expr::Literal(v) => v.clone(),
        Expr::Op(op) => {
            let mut obj = Map::new();
            let args: Vec<JsonValue> = op.args.iter().map(expr_to_json_value).collect();
            obj.insert(op.op.clone(), JsonValue::Array(args));
            JsonValue::Object(obj)
        }
        Expr::Chain(chain) => {
            let arr: Vec<JsonValue> = chain.chain.iter().map(expr_to_json_value).collect();
            JsonValue::Array(arr)
        }
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
