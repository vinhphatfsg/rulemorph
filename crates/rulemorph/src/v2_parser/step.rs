use crate::v2_model::{
    V2CallArg, V2CustomCallStep, V2IfStep, V2LetStep, V2MapStep, V2OpStep, V2Step,
};
use crate::v2_operator::is_valid_operator;
use serde_json::Value as JsonValue;

use super::{
    V2ParseError, is_pipe_value, parse_v2_condition, parse_v2_expr, parse_v2_expr_args,
    parse_v2_pipe_from_value, parse_v2_ref,
};

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
                    if !is_valid_operator(op_name)
                        && let Some(call) = parse_custom_call_step(op_name, args_val)?
                    {
                        return Ok(V2Step::CustomCall(call));
                    }
                    let args = match args_val {
                        JsonValue::Array(arr) => arr
                            .iter()
                            .map(parse_v2_expr)
                            .collect::<Result<Vec<_>, _>>()?,
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

pub(crate) fn parse_custom_call_step(
    op_name: &str,
    value: &JsonValue,
) -> Result<Option<V2CustomCallStep>, V2ParseError> {
    let JsonValue::Array(options) = value else {
        return Ok(None);
    };
    if options.is_empty() {
        return Ok(None);
    }
    let mut with = None;
    let mut saw_call_option = false;
    for option in options {
        let JsonValue::Object(option_obj) = option else {
            return Ok(None);
        };
        if option_obj.len() != 1 {
            return Ok(None);
        }
        let (key, value) = option_obj.iter().next().unwrap();
        if key != "with" {
            if saw_call_option {
                return Err(V2ParseError::InvalidStep(format!(
                    "unknown call option '{}'",
                    key
                )));
            }
            return Ok(None);
        }
        saw_call_option = true;
        if with.is_some() {
            return Err(V2ParseError::InvalidStep(
                "duplicate call option 'with'".to_string(),
            ));
        }
        with = Some(parse_with_args(value)?);
    }
    if !saw_call_option {
        return Ok(None);
    }
    Ok(Some(V2CustomCallStep {
        op: op_name.to_string(),
        with,
    }))
}

pub(crate) fn custom_call_step_candidate(value: &JsonValue) -> Option<(&str, &JsonValue)> {
    let JsonValue::Object(obj) = value else {
        return None;
    };
    if obj.len() != 1 {
        return None;
    }
    let (op_name, args_val) = obj.iter().next().unwrap();
    if ["op", "let", "if", "map", "then", "else", "cond", "ref"].contains(&op_name.as_str()) {
        return None;
    }
    Some((op_name.as_str(), args_val))
}

fn parse_with_args(value: &JsonValue) -> Result<Vec<(String, V2CallArg)>, V2ParseError> {
    let JsonValue::Object(map) = value else {
        return Err(V2ParseError::InvalidStep(
            "with option must be an object".to_string(),
        ));
    };
    let mut args = Vec::new();
    for (key, value) in map {
        args.push((key.clone(), parse_call_arg(value)?));
    }
    Ok(args)
}

fn parse_call_arg(value: &JsonValue) -> Result<V2CallArg, V2ParseError> {
    if let JsonValue::Object(map) = value
        && map.len() == 1
    {
        if let Some(expr) = map.get("expr") {
            return Ok(V2CallArg::Expr(parse_v2_expr(expr)?));
        }
        if let Some(value) = map.get("value") {
            return Ok(V2CallArg::Value(value.clone()));
        }
    }
    Ok(V2CallArg::Expr(parse_v2_expr(value)?))
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
    let if_val = obj
        .get("if")
        .ok_or_else(|| V2ParseError::InvalidStep("if step missing 'if' key".to_string()))?;

    // Check if `if` value is an object with cond/then/else (nested format)
    if let JsonValue::Object(inner_obj) = if_val {
        if inner_obj.contains_key("cond") || inner_obj.contains_key("then") {
            // Nested format: { if: { cond: ..., then: ..., else: ... } }
            let cond_val = inner_obj
                .get("cond")
                .ok_or_else(|| V2ParseError::InvalidStep("if step missing 'cond'".to_string()))?;
            let then_val = inner_obj.get("then").ok_or_else(|| {
                V2ParseError::InvalidStep("if step missing 'then' branch".to_string())
            })?;

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
    let then_val = obj
        .get("then")
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
            Ok(V2Step::Map(V2MapStep {
                steps: parsed_steps?,
            }))
        }
        _ => Err(V2ParseError::InvalidStep(
            "map steps must be an array".to_string(),
        )),
    }
}
