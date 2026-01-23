//! v2 Expression Parser for rulemorph v2.0
//!
//! This module parses v2 expression syntax including:
//! - `@input.*`, `@context.*`, `@out.*` namespace references
//! - `@item.*`, `@acc.*` iteration references
//! - `@localVar` local variable references
//! - `$` pipe value
//! - `lit:` escape prefix for literals
//! - Pipe arrays: `[start_value, step1, step2, ...]`

use serde_json::Value as JsonValue;
use crate::v2_model::{
    V2Ref, V2Expr, V2Pipe, V2Start, V2Step, V2OpStep,
    V2Condition, V2Comparison, V2ComparisonOp,
    V2LetStep, V2IfStep, V2MapStep,
};
// Note: V2Step::Ref variant is used for reference steps like "@doubled"

/// Parse a v2 reference string into V2Ref
///
/// Supported formats:
/// - `@input.path.to.field` -> V2Ref::Input("path.to.field")
/// - `@context.data[0].id` -> V2Ref::Context("data[0].id")
/// - `@out.previous_field` -> V2Ref::Out("previous_field")
/// - `@item.value` -> V2Ref::Item("value")
/// - `@acc.total` -> V2Ref::Acc("total")
/// - `@myVar` -> V2Ref::Local("myVar")
pub fn parse_v2_ref(s: &str) -> Option<V2Ref> {
    if !s.starts_with('@') {
        return None;
    }

    let rest = &s[1..]; // Remove '@' prefix

    // Check for namespace prefixes
    if let Some(path) = rest.strip_prefix("input.") {
        return Some(V2Ref::Input(path.to_string()));
    }
    if let Some(path) = rest.strip_prefix("context.") {
        return Some(V2Ref::Context(path.to_string()));
    }
    if let Some(path) = rest.strip_prefix("out.") {
        return Some(V2Ref::Out(path.to_string()));
    }
    if let Some(path) = rest.strip_prefix("item.") {
        return Some(V2Ref::Item(path.to_string()));
    }
    if let Some(path) = rest.strip_prefix("item") {
        if path.is_empty() {
            return Some(V2Ref::Item(String::new()));
        }
    }
    if let Some(path) = rest.strip_prefix("acc.") {
        return Some(V2Ref::Acc(path.to_string()));
    }
    if let Some(path) = rest.strip_prefix("acc") {
        if path.is_empty() {
            return Some(V2Ref::Acc(String::new()));
        }
    }

    // Check for reserved namespaces that should not be local variables
    if rest == "input" || rest == "context" || rest == "out" {
        return None; // These require a path after the dot
    }

    // Otherwise, it's a local variable reference
    if is_valid_identifier(rest) {
        return Some(V2Ref::Local(rest.to_string()));
    }

    None
}

/// Check if a string is a valid identifier (for local variable names)
fn is_valid_identifier(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let mut chars = s.chars();
    // First character must be letter or underscore
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    // Rest can be alphanumeric or underscore
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Check if a string represents a pipe value ($)
pub fn is_pipe_value(s: &str) -> bool {
    s == "$"
}

/// Check if a string is a literal escape (lit:...)
pub fn is_literal_escape(s: &str) -> bool {
    s.starts_with("lit:")
}

/// Extract the literal value from a lit: escaped string
pub fn extract_literal(s: &str) -> Option<&str> {
    s.strip_prefix("lit:")
}

/// Check if a string looks like a v2 reference (starts with @)
pub fn is_v2_ref(s: &str) -> bool {
    s.starts_with('@')
}

// =============================================================================
// v2 Pipe Parser
// =============================================================================

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

/// Parse a V2Step from a serde_json::Value (expects an object with op: key)
pub fn parse_v2_step(value: &JsonValue) -> Result<V2Step, V2ParseError> {
    match value {
        JsonValue::Object(obj) => {
            // Check for op step: { op: "name", args: [...] }
            if let Some(op_name) = obj.get("op").and_then(|v| v.as_str()) {
                let args = if let Some(args_val) = obj.get("args") {
                    parse_v2_expr_args(args_val)?
                } else {
                    vec![]
                };
                return Ok(V2Step::Op(V2OpStep {
                    op: op_name.to_string(),
                    args,
                }));
            }

            // Check for let step: { let: { varName: expr, ... } }
            if let Some(let_bindings) = obj.get("let") {
                return parse_let_step(let_bindings);
            }

            // Check for if step: { if: condition, then: pipe, else: pipe }
            if obj.contains_key("if") {
                return parse_if_step(obj);
            }

            // Check for map step: { map: [...steps...] }
            if let Some(map_steps) = obj.get("map") {
                return parse_map_step(map_steps);
            }

            // Check for shorthand op format: { opName: [args] } or { opName: arg }
            // This handles cases like { multiply: [1.1] }, { concat: ["@out.name"] }, etc.
            if obj.len() == 1 {
                let (op_name, args_val) = obj.iter().next().unwrap();
                // Skip reserved keywords
                if !["op", "let", "if", "map", "then", "else", "cond"].contains(&op_name.as_str()) {
                    let args = match args_val {
                        JsonValue::Array(arr) => {
                            arr.iter().map(parse_v2_expr).collect::<Result<Vec<_>, _>>()?
                        }
                        // Single value (non-array) becomes single arg
                        other => vec![parse_v2_expr(other)?],
                    };
                    return Ok(V2Step::Op(V2OpStep {
                        op: op_name.clone(),
                        args,
                    }));
                }
            }

            Err(V2ParseError::InvalidStep("unknown step type".to_string()))
        }
        JsonValue::String(s) => {
            // Check if it's a v2 reference (starts with @)
            if let Some(v2_ref) = parse_v2_ref(s) {
                return Ok(V2Step::Ref(v2_ref));
            }
            // Check for pipe value ($)
            if is_pipe_value(s) {
                // $ as a step means "return current pipe value"
                // This is essentially a no-op, but we represent it as a PipeValue reference
                return Err(V2ParseError::InvalidStep(
                    "$ as a step is not valid, use it as start or in expressions".to_string(),
                ));
            }
            // Shorthand for simple operations: "trim" -> { op: "trim" }
            Ok(V2Step::Op(V2OpStep {
                op: s.clone(),
                args: vec![],
            }))
        }
        _ => Err(V2ParseError::InvalidStep(
            "step must be object or string".to_string(),
        )),
    }
}

/// Parse V2Expr arguments from an array value
fn parse_v2_expr_args(value: &JsonValue) -> Result<Vec<V2Expr>, V2ParseError> {
    match value {
        JsonValue::Array(arr) => {
            arr.iter().map(parse_v2_expr).collect()
        }
        _ => Err(V2ParseError::InvalidArgs("args must be an array".to_string())),
    }
}

/// Parse a let step from its bindings
fn parse_let_step(bindings: &JsonValue) -> Result<V2Step, V2ParseError> {
    match bindings {
        JsonValue::Object(obj) => {
            let mut result = Vec::new();
            for (key, value) in obj {
                let expr = parse_v2_expr(value)?;
                result.push((key.clone(), expr));
            }
            Ok(V2Step::Let(V2LetStep { bindings: result }))
        }
        _ => Err(V2ParseError::InvalidStep(
            "let bindings must be an object".to_string(),
        )),
    }
}

/// Parse an if step
/// Supports two formats:
/// 1. `{ if: condition, then: pipe, else: pipe }` - condition directly in if value
/// 2. `{ if: { cond: condition, then: pipe, else: pipe } }` - nested object format
fn parse_if_step(obj: &serde_json::Map<String, JsonValue>) -> Result<V2Step, V2ParseError> {
    let if_val = obj.get("if")
        .ok_or_else(|| V2ParseError::InvalidStep("if step missing 'if' key".to_string()))?;

    // Check if `if` value is an object with cond/then/else (nested format)
    if let JsonValue::Object(inner_obj) = if_val {
        if inner_obj.contains_key("cond") || inner_obj.contains_key("then") {
            // Nested format: { if: { cond: ..., then: ..., else: ... } }
            let cond_val = inner_obj.get("cond")
                .ok_or_else(|| V2ParseError::InvalidStep("if step missing 'cond'".to_string()))?;
            let then_val = inner_obj.get("then")
                .ok_or_else(|| V2ParseError::InvalidStep("if step missing 'then' branch".to_string()))?;

            let condition = parse_v2_condition(cond_val)?;
            let then_branch = parse_v2_pipe_from_value(then_val)?;
            let else_branch = if let Some(else_val) = inner_obj.get("else") {
                Some(parse_v2_pipe_from_value(else_val)?)
            } else {
                None
            };

            return Ok(V2Step::If(V2IfStep {
                cond: condition,
                then_branch,
                else_branch,
            }));
        }
    }

    // Original format: { if: condition, then: pipe, else: pipe }
    let then_val = obj.get("then")
        .ok_or_else(|| V2ParseError::InvalidStep("if step missing then branch".to_string()))?;

    let condition = parse_v2_condition(if_val)?;
    let then_branch = parse_v2_pipe_from_value(then_val)?;
    let else_branch = if let Some(else_val) = obj.get("else") {
        Some(parse_v2_pipe_from_value(else_val)?)
    } else {
        None
    };

    Ok(V2Step::If(V2IfStep {
        cond: condition,
        then_branch,
        else_branch,
    }))
}

/// Parse a map step
fn parse_map_step(steps: &JsonValue) -> Result<V2Step, V2ParseError> {
    match steps {
        JsonValue::Array(arr) => {
            let parsed_steps: Result<Vec<V2Step>, _> = arr.iter().map(parse_v2_step).collect();
            Ok(V2Step::Map(V2MapStep { steps: parsed_steps? }))
        }
        _ => Err(V2ParseError::InvalidStep(
            "map steps must be an array".to_string(),
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
            Ok(V2Pipe { start, steps: vec![] })
        }
        _ => {
            // Other values become a single-element pipe
            let start = parse_v2_start(value)?;
            Ok(V2Pipe { start, steps: vec![] })
        }
    }
}

/// Parse a V2Pipe from a JSON array
/// Format: [start_value, step1, step2, ...]
/// If first element looks like a step (op shorthand object), use implicit `$` as start
pub fn parse_v2_pipe(arr: &[JsonValue]) -> Result<V2Pipe, V2ParseError> {
    if arr.is_empty() {
        return Err(V2ParseError::EmptyPipe);
    }

    // Check if first element looks like a step rather than a start value
    if looks_like_step(&arr[0]) {
        // Use implicit pipe value ($) as start, all elements are steps
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
            if obj.contains_key("op") || obj.contains_key("let") ||
               obj.contains_key("if") || obj.contains_key("map") {
                return true;
            }
            // Check for op shorthand: single key that's not a reserved keyword
            if obj.len() == 1 {
                let key = obj.keys().next().unwrap();
                // Skip values that are likely starts (plain objects)
                if !["op", "let", "if", "map", "then", "else", "cond", "ref"].contains(&key.as_str()) {
                    // If the key is a known operation name or ends with common patterns, it's a step
                    // This includes: lookup_first, lookup, multiply, concat, trim, etc.
                    return true;
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

// =============================================================================
// v2 Condition Parser
// =============================================================================

/// Parse a V2Condition from a JSON value
pub fn parse_v2_condition(value: &JsonValue) -> Result<V2Condition, V2ParseError> {
    match value {
        JsonValue::Object(obj) => {
            // Check for all: [...]
            if let Some(all_arr) = obj.get("all") {
                return parse_condition_array(all_arr, |conds| V2Condition::All(conds));
            }
            // Check for any: [...]
            if let Some(any_arr) = obj.get("any") {
                return parse_condition_array(any_arr, |conds| V2Condition::Any(conds));
            }
            // Check for comparison operators: eq, ne, gt, gte, lt, lte, match
            if let Some(comp) = parse_comparison_from_object(obj)? {
                return Ok(V2Condition::Comparison(comp));
            }
            // Otherwise, treat as expression condition
            let expr = parse_v2_expr(value)?;
            Ok(V2Condition::Expr(expr))
        }
        JsonValue::Array(_) => {
            // Array treated as expression
            let expr = parse_v2_expr(value)?;
            Ok(V2Condition::Expr(expr))
        }
        _ => {
            // Other values treated as expression
            let expr = parse_v2_expr(value)?;
            Ok(V2Condition::Expr(expr))
        }
    }
}

fn parse_condition_array<F>(value: &JsonValue, constructor: F) -> Result<V2Condition, V2ParseError>
where
    F: FnOnce(Vec<V2Condition>) -> V2Condition,
{
    match value {
        JsonValue::Array(arr) => {
            let conditions: Result<Vec<V2Condition>, _> =
                arr.iter().map(parse_v2_condition).collect();
            Ok(constructor(conditions?))
        }
        _ => Err(V2ParseError::InvalidCondition(
            "all/any must contain an array".to_string(),
        )),
    }
}

fn parse_comparison_from_object(
    obj: &serde_json::Map<String, JsonValue>,
) -> Result<Option<V2Comparison>, V2ParseError> {
    let ops = [
        ("eq", V2ComparisonOp::Eq),
        ("ne", V2ComparisonOp::Ne),
        ("gt", V2ComparisonOp::Gt),
        ("gte", V2ComparisonOp::Gte),
        ("lt", V2ComparisonOp::Lt),
        ("lte", V2ComparisonOp::Lte),
        ("match", V2ComparisonOp::Match),
    ];

    for (key, op) in ops.iter() {
        if let Some(args_val) = obj.get(*key) {
            let args = parse_v2_expr_args(args_val)?;
            return Ok(Some(V2Comparison {
                op: *op,
                args,
            }));
        }
    }

    Ok(None)
}

// =============================================================================
// Parse Errors
// =============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum V2ParseError {
    EmptyPipe,
    InvalidStart(String),
    InvalidStep(String),
    InvalidArgs(String),
    InvalidCondition(String),
}

impl std::fmt::Display for V2ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            V2ParseError::EmptyPipe => write!(f, "pipe array cannot be empty"),
            V2ParseError::InvalidStart(msg) => write!(f, "invalid start value: {}", msg),
            V2ParseError::InvalidStep(msg) => write!(f, "invalid step: {}", msg),
            V2ParseError::InvalidArgs(msg) => write!(f, "invalid args: {}", msg),
            V2ParseError::InvalidCondition(msg) => write!(f, "invalid condition: {}", msg),
        }
    }
}

impl std::error::Error for V2ParseError {}

// =============================================================================
// v2 Parser Tests
// =============================================================================

#[cfg(test)]
mod v2_ref_parser_tests {
    use super::*;

    #[test]
    fn test_parse_input_ref() {
        assert_eq!(
            parse_v2_ref("@input.name"),
            Some(V2Ref::Input("name".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@input.user.profile.name"),
            Some(V2Ref::Input("user.profile.name".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@input.items[0].id"),
            Some(V2Ref::Input("items[0].id".to_string()))
        );
    }

    #[test]
    fn test_parse_context_ref() {
        assert_eq!(
            parse_v2_ref("@context.config"),
            Some(V2Ref::Context("config".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@context.users[0].id"),
            Some(V2Ref::Context("users[0].id".to_string()))
        );
    }

    #[test]
    fn test_parse_out_ref() {
        assert_eq!(
            parse_v2_ref("@out.user_id"),
            Some(V2Ref::Out("user_id".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@out.computed_field"),
            Some(V2Ref::Out("computed_field".to_string()))
        );
    }

    #[test]
    fn test_parse_item_ref() {
        assert_eq!(
            parse_v2_ref("@item.value"),
            Some(V2Ref::Item("value".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@item"),
            Some(V2Ref::Item(String::new()))
        );
    }

    #[test]
    fn test_parse_acc_ref() {
        assert_eq!(
            parse_v2_ref("@acc.total"),
            Some(V2Ref::Acc("total".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@acc"),
            Some(V2Ref::Acc(String::new()))
        );
    }

    #[test]
    fn test_parse_local_ref() {
        assert_eq!(
            parse_v2_ref("@myVar"),
            Some(V2Ref::Local("myVar".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@price"),
            Some(V2Ref::Local("price".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@_temp"),
            Some(V2Ref::Local("_temp".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@var123"),
            Some(V2Ref::Local("var123".to_string()))
        );
    }

    #[test]
    fn test_invalid_refs() {
        // No @ prefix
        assert_eq!(parse_v2_ref("input.name"), None);
        // Empty after @
        assert_eq!(parse_v2_ref("@"), None);
        // Reserved names without path
        assert_eq!(parse_v2_ref("@input"), None);
        assert_eq!(parse_v2_ref("@context"), None);
        assert_eq!(parse_v2_ref("@out"), None);
        // Invalid identifier
        assert_eq!(parse_v2_ref("@123invalid"), None);
    }

    #[test]
    fn test_is_pipe_value() {
        assert!(is_pipe_value("$"));
        assert!(!is_pipe_value("$$"));
        assert!(!is_pipe_value("@input.name"));
        assert!(!is_pipe_value(""));
    }

    #[test]
    fn test_is_literal_escape() {
        assert!(is_literal_escape("lit:@input.name"));
        assert!(is_literal_escape("lit:$"));
        assert!(is_literal_escape("lit:"));
        assert!(!is_literal_escape("@input.name"));
        assert!(!is_literal_escape("literal:"));
    }

    #[test]
    fn test_extract_literal() {
        assert_eq!(extract_literal("lit:@input.name"), Some("@input.name"));
        assert_eq!(extract_literal("lit:$"), Some("$"));
        assert_eq!(extract_literal("lit:"), Some(""));
        assert_eq!(extract_literal("@input.name"), None);
    }

    #[test]
    fn test_is_v2_ref() {
        assert!(is_v2_ref("@input.name"));
        assert!(is_v2_ref("@myVar"));
        assert!(!is_v2_ref("input.name"));
        assert!(!is_v2_ref("$"));
        assert!(!is_v2_ref("lit:@input"));
    }
}

// =============================================================================
// v2 Pipe Parser Tests (T04)
// =============================================================================

#[cfg(test)]
mod v2_pipe_parser_tests {
    use super::*;
    use serde_json::json;

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
        let arr = vec![
            json!("@input.value"),
            json!({ "op": "add", "args": [10] }),
        ];
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

        assert_eq!(
            pipe.start,
            V2Start::Literal(json!("@input.name"))
        );
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
}

// =============================================================================
// v2 Condition Parser Tests (T05)
// =============================================================================

#[cfg(test)]
mod v2_condition_parser_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_condition_all() {
        let value = json!({
            "all": [
                { "eq": ["@input.status", "active"] },
                { "gt": ["@input.age", 18] }
            ]
        });

        let cond = parse_v2_condition(&value).unwrap();
        if let V2Condition::All(conditions) = cond {
            assert_eq!(conditions.len(), 2);
        } else {
            panic!("Expected All condition");
        }
    }

    #[test]
    fn test_parse_condition_any() {
        let value = json!({
            "any": [
                { "eq": ["@input.role", "admin"] },
                { "eq": ["@input.role", "moderator"] }
            ]
        });

        let cond = parse_v2_condition(&value).unwrap();
        if let V2Condition::Any(conditions) = cond {
            assert_eq!(conditions.len(), 2);
        } else {
            panic!("Expected Any condition");
        }
    }

    #[test]
    fn test_parse_condition_eq() {
        let value = json!({ "eq": ["@input.name", "John"] });
        let cond = parse_v2_condition(&value).unwrap();

        if let V2Condition::Comparison(comp) = cond {
            assert_eq!(comp.op, V2ComparisonOp::Eq);
            assert_eq!(comp.args.len(), 2);
        } else {
            panic!("Expected Comparison condition");
        }
    }

    #[test]
    fn test_parse_condition_ne() {
        let value = json!({ "ne": ["@input.status", "deleted"] });
        let cond = parse_v2_condition(&value).unwrap();

        if let V2Condition::Comparison(comp) = cond {
            assert_eq!(comp.op, V2ComparisonOp::Ne);
        } else {
            panic!("Expected Comparison condition");
        }
    }

    #[test]
    fn test_parse_condition_gt() {
        let value = json!({ "gt": ["@input.age", 18] });
        let cond = parse_v2_condition(&value).unwrap();

        if let V2Condition::Comparison(comp) = cond {
            assert_eq!(comp.op, V2ComparisonOp::Gt);
        } else {
            panic!("Expected Comparison condition");
        }
    }

    #[test]
    fn test_parse_condition_gte() {
        let value = json!({ "gte": ["@input.score", 60] });
        let cond = parse_v2_condition(&value).unwrap();

        if let V2Condition::Comparison(comp) = cond {
            assert_eq!(comp.op, V2ComparisonOp::Gte);
        } else {
            panic!("Expected Comparison condition");
        }
    }

    #[test]
    fn test_parse_condition_lt() {
        let value = json!({ "lt": ["@input.count", 100] });
        let cond = parse_v2_condition(&value).unwrap();

        if let V2Condition::Comparison(comp) = cond {
            assert_eq!(comp.op, V2ComparisonOp::Lt);
        } else {
            panic!("Expected Comparison condition");
        }
    }

    #[test]
    fn test_parse_condition_lte() {
        let value = json!({ "lte": ["@input.retries", 3] });
        let cond = parse_v2_condition(&value).unwrap();

        if let V2Condition::Comparison(comp) = cond {
            assert_eq!(comp.op, V2ComparisonOp::Lte);
        } else {
            panic!("Expected Comparison condition");
        }
    }

    #[test]
    fn test_parse_condition_match() {
        let value = json!({ "match": ["@input.email", "^[a-z]+@"] });
        let cond = parse_v2_condition(&value).unwrap();

        if let V2Condition::Comparison(comp) = cond {
            assert_eq!(comp.op, V2ComparisonOp::Match);
        } else {
            panic!("Expected Comparison condition");
        }
    }

    #[test]
    fn test_parse_nested_conditions() {
        let value = json!({
            "all": [
                { "any": [
                    { "eq": ["@input.type", "A"] },
                    { "eq": ["@input.type", "B"] }
                ]},
                { "gt": ["@input.value", 0] }
            ]
        });

        let cond = parse_v2_condition(&value).unwrap();
        if let V2Condition::All(conditions) = cond {
            assert_eq!(conditions.len(), 2);
            assert!(matches!(conditions[0], V2Condition::Any(_)));
        } else {
            panic!("Expected All condition");
        }
    }
}

// =============================================================================
// v2 Step Parser Tests (T06)
// =============================================================================

#[cfg(test)]
mod v2_step_parser_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_let_step() {
        let value = json!({
            "let": {
                "x": "@input.value",
                "y": 10
            }
        });

        let step = parse_v2_step(&value).unwrap();
        if let V2Step::Let(let_step) = step {
            assert_eq!(let_step.bindings.len(), 2);
        } else {
            panic!("Expected Let step");
        }
    }

    #[test]
    fn test_parse_if_step() {
        let value = json!({
            "if": { "gt": ["@input.age", 18] },
            "then": ["adult"],
            "else": ["minor"]
        });

        let step = parse_v2_step(&value).unwrap();
        if let V2Step::If(if_step) = step {
            assert!(matches!(if_step.cond, V2Condition::Comparison(_)));
            assert!(if_step.else_branch.is_some());
        } else {
            panic!("Expected If step");
        }
    }

    #[test]
    fn test_parse_if_step_without_else() {
        let value = json!({
            "if": { "eq": ["@input.enabled", true] },
            "then": ["process"]
        });

        let step = parse_v2_step(&value).unwrap();
        if let V2Step::If(if_step) = step {
            assert!(if_step.else_branch.is_none());
        } else {
            panic!("Expected If step");
        }
    }

    #[test]
    fn test_parse_map_step() {
        let value = json!({
            "map": [
                { "op": "multiply", "args": [2] }
            ]
        });

        let step = parse_v2_step(&value).unwrap();
        if let V2Step::Map(map_step) = step {
            assert_eq!(map_step.steps.len(), 1);
        } else {
            panic!("Expected Map step");
        }
    }

    #[test]
    fn test_parse_op_step_shorthand() {
        // String shorthand for simple operations
        let step = parse_v2_step(&json!("trim")).unwrap();
        if let V2Step::Op(op) = step {
            assert_eq!(op.op, "trim");
            assert!(op.args.is_empty());
        } else {
            panic!("Expected Op step");
        }
    }

    #[test]
    fn test_parse_op_step_with_args() {
        let value = json!({
            "op": "concat",
            "args": ["@input.first", " ", "@input.last"]
        });

        let step = parse_v2_step(&value).unwrap();
        if let V2Step::Op(op) = step {
            assert_eq!(op.op, "concat");
            assert_eq!(op.args.len(), 3);
        } else {
            panic!("Expected Op step");
        }
    }

    #[test]
    fn test_parse_complex_pipe_with_steps() {
        // Complex pipe with multiple step types
        let arr = vec![
            json!("@input.items"),
            json!({ "let": { "threshold": 100 } }),
            json!({ "map": [
                { "if": { "gt": ["@item.value", "@threshold"] },
                  "then": ["@item.value"],
                  "else": [0]
                }
            ]}),
        ];

        let pipe = parse_v2_pipe(&arr).unwrap();
        assert_eq!(pipe.steps.len(), 2);
        assert!(matches!(pipe.steps[0], V2Step::Let(_)));
        assert!(matches!(pipe.steps[1], V2Step::Map(_)));
    }
}

// =============================================================================
// v2 RuleFile Parser Tests (T07)
// =============================================================================

#[cfg(test)]
mod v2_rulefile_parser_tests {
    use super::*;
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
    fn test_parse_v2_condition_from_record_when() {
        // Test parsing conditions as used in record_when
        let value = json!({
            "all": [
                { "gt": ["@input.score", 0] },
                { "eq": ["@input.active", true] }
            ]
        });

        let cond = parse_v2_condition(&value).unwrap();
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
    fn test_parse_v2_mapping_when_condition() {
        // mapping.when with v2 condition syntax
        let value = json!({ "eq": ["@input.role", "admin"] });
        let cond = parse_v2_condition(&value).unwrap();

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
