use crate::v2_model::{V2Expr, V2Pipe, V2Start, V2Step};
use crate::v2_validator::is_valid_op;
use serde_json::Value as JsonValue;

use super::{
    V2ParseError, extract_literal, is_literal_escape, is_pipe_value, is_v2_ref, parse_v2_ref,
    parse_v2_step,
};

/// Parse a V2Start from a serde_json::Value
pub fn parse_v2_start(value: &JsonValue) -> Result<V2Start, V2ParseError> {
    match value {
        JsonValue::String(s) => {
            // Check for pipe value ($)
            if is_pipe_value(s) {
                return Ok(V2Start::PipeValue);
            }
            // Check for literal escape (lit:...)
            if let Some(lit) = extract_literal(s) {
                return Ok(V2Start::Literal(JsonValue::String(lit.to_string())));
            }
            // Check for v2 reference (@...)
            if let Some(v2_ref) = parse_v2_ref(s) {
                return Ok(V2Start::Ref(v2_ref));
            }
            if is_v2_ref(s) {
                return Err(V2ParseError::InvalidStart(format!(
                    "invalid v2 reference: {}",
                    s
                )));
            }
            // Otherwise, treat as literal string
            Ok(V2Start::Literal(value.clone()))
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) => {
            Ok(V2Start::Literal(value.clone()))
        }
        JsonValue::Array(_) | JsonValue::Object(_) => {
            // Arrays and objects as start values are treated as literals
            Ok(V2Start::Literal(value.clone()))
        }
    }
}

/// Parse V2Expr arguments from an array value
pub(super) fn parse_v2_expr_args(value: &JsonValue) -> Result<Vec<V2Expr>, V2ParseError> {
    match value {
        JsonValue::Array(arr) => arr.iter().map(parse_v2_expr).collect(),
        _ => Err(V2ParseError::InvalidArgs(
            "args must be an array".to_string(),
        )),
    }
}

/// Parse a V2Pipe from a JSON array value
pub fn parse_v2_pipe_from_value(value: &JsonValue) -> Result<V2Pipe, V2ParseError> {
    match value {
        JsonValue::Array(arr) => parse_v2_pipe(arr),
        JsonValue::String(_) => {
            // Single string can be treated as a pipe with just a start
            let start = parse_v2_start(value)?;
            Ok(V2Pipe {
                start,
                steps: vec![],
            })
        }
        _ => {
            // Other values become a single-element pipe
            let start = parse_v2_start(value)?;
            Ok(V2Pipe {
                start,
                steps: vec![],
            })
        }
    }
}

/// Parse a V2Pipe from a JSON array
/// Format: [start_value, step1, step2, ...]
pub fn parse_v2_pipe(arr: &[JsonValue]) -> Result<V2Pipe, V2ParseError> {
    if arr.is_empty() {
        return Err(V2ParseError::EmptyPipe);
    }

    if arr.len() == 1 && looks_like_step(&arr[0]) {
        // Single-step pipe can omit explicit `$` start.
        let steps: Result<Vec<V2Step>, _> = arr.iter().map(parse_v2_step).collect();
        return Ok(V2Pipe {
            start: V2Start::PipeValue,
            steps: steps?,
        });
    }

    // First element is the start value
    let start = parse_v2_start(&arr[0])?;

    // Remaining elements are steps
    let steps: Result<Vec<V2Step>, _> = arr[1..].iter().map(parse_v2_step).collect();

    Ok(V2Pipe {
        start,
        steps: steps?,
    })
}

/// Check if a JSON value looks like a step rather than a start value
fn looks_like_step(value: &JsonValue) -> bool {
    match value {
        JsonValue::Object(obj) => {
            // Check for explicit step keywords
            if obj.contains_key("op")
                || obj.contains_key("let")
                || obj.contains_key("if")
                || obj.contains_key("map")
            {
                return true;
            }
            // Check for op shorthand: single key that's not a reserved keyword
            if obj.len() == 1 {
                let key = obj.keys().next().unwrap();
                // Skip values that are likely starts (plain objects)
                if !["op", "let", "if", "map", "then", "else", "cond", "ref"]
                    .contains(&key.as_str())
                {
                    // Only treat as step when the key matches a known v2 op name.
                    return is_valid_op(key);
                }
            }
            false
        }
        JsonValue::String(_) => {
            // Strings as first element should always be treated as start values, not steps
            // This includes op-like strings like "trim" - they should only be steps
            // when appearing after the first element in a pipe array
            false
        }
        _ => false,
    }
}

/// Parse a V2Expr from a JSON value
pub fn parse_v2_expr(value: &JsonValue) -> Result<V2Expr, V2ParseError> {
    match value {
        JsonValue::Array(arr) => {
            // Array is a pipe expression
            let pipe = parse_v2_pipe(arr)?;
            Ok(V2Expr::Pipe(pipe))
        }
        JsonValue::String(s) => {
            // String can be a reference, pipe value, literal escape, or plain literal
            if is_pipe_value(s) {
                Ok(V2Expr::Pipe(V2Pipe {
                    start: V2Start::PipeValue,
                    steps: vec![],
                }))
            } else if let Some(lit) = extract_literal(s) {
                Ok(V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(JsonValue::String(lit.to_string())),
                    steps: vec![],
                }))
            } else if let Some(v2_ref) = parse_v2_ref(s) {
                Ok(V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(v2_ref),
                    steps: vec![],
                }))
            } else if is_v2_ref(s) {
                Err(V2ParseError::InvalidStart(format!(
                    "invalid v2 reference: {}",
                    s
                )))
            } else {
                Ok(V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(value.clone()),
                    steps: vec![],
                }))
            }
        }
        _ => {
            // Other values become single-element pipes with literals
            Ok(V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(value.clone()),
                steps: vec![],
            }))
        }
    }
}

/// Detect if a JSON value uses v2 expression syntax
/// v2 syntax: pipe arrays or @-prefixed references
/// v1 syntax: { ref: "..." } or { op: "...", args: [...] }
pub fn is_v2_expr(value: &JsonValue) -> bool {
    match value {
        JsonValue::Array(_) => {
            // Any array is treated as v2 pipe syntax, even with literal/object starts.
            true
        }
        JsonValue::String(s) => {
            // String with @ prefix is v2 reference
            is_v2_ref(s) || is_pipe_value(s) || is_literal_escape(s)
        }
        JsonValue::Object(obj) => {
            // v1 uses { ref: ... } or { op: ..., args: ... }
            // v2 condition uses { all: ... } or { any: ... } or { eq: ... }
            // We consider condition syntax as v2-like
            !(obj.contains_key("ref") || (obj.contains_key("op") && !obj.contains_key("if")))
        }
        _ => false,
    }
}

#[cfg(test)]
mod v2_pipe_parser_tests {
    use super::*;
    use crate::v2_model::V2Ref;
    use serde_json::{Value as JsonValue, json};

    #[test]
    fn test_parse_simple_pipe() {
        // ["@input.name", "trim"]
        let arr = vec![json!("@input.name"), json!("trim")];
        let pipe = parse_v2_pipe(&arr).unwrap();

        assert_eq!(pipe.start, V2Start::Ref(V2Ref::Input("name".to_string())));
        assert_eq!(pipe.steps.len(), 1);
        if let V2Step::Op(op) = &pipe.steps[0] {
            assert_eq!(op.op, "trim");
            assert!(op.args.is_empty());
        } else {
            panic!("Expected Op step");
        }
    }

    #[test]
    fn test_parse_pipe_with_multiple_steps() {
        // ["@input.name", "trim", "uppercase"]
        let arr = vec![json!("@input.name"), json!("trim"), json!("uppercase")];
        let pipe = parse_v2_pipe(&arr).unwrap();

        assert_eq!(pipe.steps.len(), 2);
    }

    #[test]
    fn test_parse_pipe_with_op_object() {
        // ["@input.value", { "op": "add", "args": [10] }]
        let arr = vec![json!("@input.value"), json!({ "op": "add", "args": [10] })];
        let pipe = parse_v2_pipe(&arr).unwrap();

        if let V2Step::Op(op) = &pipe.steps[0] {
            assert_eq!(op.op, "add");
            assert_eq!(op.args.len(), 1);
        } else {
            panic!("Expected Op step");
        }
    }

    #[test]
    fn test_parse_pipe_with_pipe_value_start() {
        // ["$", "trim"]
        let arr = vec![json!("$"), json!("trim")];
        let pipe = parse_v2_pipe(&arr).unwrap();

        assert_eq!(pipe.start, V2Start::PipeValue);
    }

    #[test]
    fn test_parse_pipe_with_literal_start() {
        // [42, { "op": "multiply", "args": [2] }]
        let arr = vec![json!(42), json!({ "op": "multiply", "args": [2] })];
        let pipe = parse_v2_pipe(&arr).unwrap();

        assert_eq!(pipe.start, V2Start::Literal(json!(42)));
    }

    #[test]
    fn test_parse_pipe_with_literal_escape() {
        // ["lit:@input.name", "trim"]
        let arr = vec![json!("lit:@input.name"), json!("trim")];
        let pipe = parse_v2_pipe(&arr).unwrap();

        assert_eq!(pipe.start, V2Start::Literal(json!("@input.name")));
    }

    #[test]
    fn test_parse_empty_pipe_error() {
        let arr: Vec<JsonValue> = vec![];
        let result = parse_v2_pipe(&arr);
        assert_eq!(result, Err(V2ParseError::EmptyPipe));
    }

    #[test]
    fn test_parse_v2_start_ref() {
        let result = parse_v2_start(&json!("@input.name")).unwrap();
        assert_eq!(result, V2Start::Ref(V2Ref::Input("name".to_string())));
    }

    #[test]
    fn test_parse_v2_start_pipe_value() {
        let result = parse_v2_start(&json!("$")).unwrap();
        assert_eq!(result, V2Start::PipeValue);
    }

    #[test]
    fn test_parse_v2_start_literal() {
        let result = parse_v2_start(&json!(123)).unwrap();
        assert_eq!(result, V2Start::Literal(json!(123)));

        let result = parse_v2_start(&json!(true)).unwrap();
        assert_eq!(result, V2Start::Literal(json!(true)));

        let result = parse_v2_start(&json!(null)).unwrap();
        assert_eq!(result, V2Start::Literal(json!(null)));
    }

    #[test]
    fn test_parse_v2_start_invalid_at_ref_error() {
        let invalid_refs = [json!("@"), json!("@foo-bar"), json!("@123invalid")];
        for value in invalid_refs {
            let err = parse_v2_start(&value).unwrap_err();
            assert!(matches!(err, V2ParseError::InvalidStart(_)));
        }
    }
}

#[cfg(test)]
mod v2_rulefile_parser_tests {
    use super::*;
    use crate::v2_model::{V2ComparisonOp, V2Condition, V2Ref};
    use serde_json::json;

    #[test]
    fn test_parse_v2_expr_from_yaml_array() {
        // Test that parse_v2_expr can handle YAML-parsed array representing v2 expr
        let value = json!(["@input.name", "trim", "uppercase"]);
        let expr = parse_v2_expr(&value).unwrap();

        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.start, V2Start::Ref(V2Ref::Input("name".to_string())));
            assert_eq!(pipe.steps.len(), 2);
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_expr_with_op_args() {
        // v2 expr with op that has arguments
        let value = json!(["@input.price", { "op": "multiply", "args": [0.9] }]);
        let expr = parse_v2_expr(&value).unwrap();

        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.steps.len(), 1);
            if let V2Step::Op(op) = &pipe.steps[0] {
                assert_eq!(op.op, "multiply");
                assert_eq!(op.args.len(), 1);
            } else {
                panic!("Expected Op step");
            }
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_expr_literal_object_start_pipe() {
        // Pipe start can be a literal object followed by steps
        let value = json!([{"foo": 1}, "keys"]);
        let expr = parse_v2_expr(&value).unwrap();

        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.start, V2Start::Literal(json!({"foo": 1})));
            assert_eq!(pipe.steps.len(), 1);
            if let V2Step::Op(op) = &pipe.steps[0] {
                assert_eq!(op.op, "keys");
            } else {
                panic!("Expected Op step");
            }
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_expr_literal_object_with_op_key_start_pipe() {
        // Literal object starts should not be coerced into implicit steps.
        let value = json!([{"op": "x"}, "keys"]);
        let expr = parse_v2_expr(&value).unwrap();

        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.start, V2Start::Literal(json!({"op": "x"})));
            assert_eq!(pipe.steps.len(), 1);
            if let V2Step::Op(op) = &pipe.steps[0] {
                assert_eq!(op.op, "keys");
            } else {
                panic!("Expected Op step");
            }
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_condition_from_record_when() {
        // Test parsing conditions as used in record_when
        let value = json!({
            "all": [
                { "gt": ["@input.score", 0] },
                { "eq": ["@input.active", true] }
            ]
        });

        let cond = super::super::parse_v2_condition(&value).unwrap();
        if let V2Condition::All(conditions) = cond {
            assert_eq!(conditions.len(), 2);
        } else {
            panic!("Expected All condition");
        }
    }

    #[test]
    fn test_parse_v2_expr_single_ref() {
        // Single reference without steps
        let value = json!("@input.name");
        let expr = parse_v2_expr(&value).unwrap();

        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.start, V2Start::Ref(V2Ref::Input("name".to_string())));
            assert!(pipe.steps.is_empty());
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_expr_invalid_at_ref_error() {
        let value = json!("@foo-bar");
        let err = parse_v2_expr(&value).unwrap_err();
        assert!(matches!(err, V2ParseError::InvalidStart(_)));
    }

    #[test]
    fn test_parse_v2_expr_v1_fallback_op() {
        // v1 style op within v2 pipe: { op: "uppercase", args: [] }
        let value = json!(["@input.name", { "op": "uppercase", "args": [] }]);
        let expr = parse_v2_expr(&value).unwrap();

        if let V2Expr::Pipe(pipe) = expr {
            if let V2Step::Op(op) = &pipe.steps[0] {
                assert_eq!(op.op, "uppercase");
            } else {
                panic!("Expected Op step");
            }
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_expr_single_step_comparison_alias() {
        // Single-step pipe should treat alias comparison op as a step.
        let value = json!([{ "gt": 80 }]);
        let expr = parse_v2_expr(&value).unwrap();

        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.start, V2Start::PipeValue);
            assert_eq!(pipe.steps.len(), 1);
            if let V2Step::Op(op) = &pipe.steps[0] {
                assert_eq!(op.op, "gt");
                assert_eq!(op.args.len(), 1);
            } else {
                panic!("Expected Op step");
            }
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_mapping_when_condition() {
        // mapping.when with v2 condition syntax
        let value = json!({ "eq": ["@input.role", "admin"] });
        let cond = super::super::parse_v2_condition(&value).unwrap();

        if let V2Condition::Comparison(comp) = cond {
            assert_eq!(comp.op, V2ComparisonOp::Eq);
            assert_eq!(comp.args.len(), 2);
        } else {
            panic!("Expected Comparison condition");
        }
    }

    #[test]
    fn test_parse_v2_expr_with_let_step() {
        // v2 expr with let binding
        let value = json!([
            "@input.price",
            { "let": { "base": "$" } },
            { "op": "add", "args": [100] }
        ]);

        let expr = parse_v2_expr(&value).unwrap();
        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.steps.len(), 2);
            assert!(matches!(pipe.steps[0], V2Step::Let(_)));
            assert!(matches!(pipe.steps[1], V2Step::Op(_)));
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_expr_with_if_step() {
        // v2 expr with if step
        let value = json!([
            "@input.amount",
            {
                "if": { "gt": ["$", 10000] },
                "then": [{ "op": "multiply", "args": [0.9] }],
                "else": ["$"]
            }
        ]);

        let expr = parse_v2_expr(&value).unwrap();
        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.steps.len(), 1);
            assert!(matches!(pipe.steps[0], V2Step::If(_)));
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_expr_with_map_step() {
        // v2 expr with map step for array processing
        let value = json!([
            "@input.items",
            {
                "map": [
                    { "op": "get", "args": ["name"] }
                ]
            }
        ]);

        let expr = parse_v2_expr(&value).unwrap();
        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.steps.len(), 1);
            assert!(matches!(pipe.steps[0], V2Step::Map(_)));
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_is_v2_expr_pipe_array() {
        // Helper function to detect v2 syntax
        assert!(is_v2_expr(&json!(["@input.name", "trim"])));
        assert!(is_v2_expr(&json!([])));
        assert!(is_v2_expr(&json!(["hello", "trim"])));
        assert!(is_v2_expr(&json!([{"lookup_first": []}, "trim"])));
        assert!(is_v2_expr(&json!("@input.name")));
        assert!(is_v2_expr(&json!("lit:@input.name")));
        assert!(!is_v2_expr(&json!({ "ref": "input.name" })));
        assert!(!is_v2_expr(&json!({ "op": "uppercase", "args": [] })));
    }
}
