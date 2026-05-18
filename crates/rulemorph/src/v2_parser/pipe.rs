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
mod tests;
