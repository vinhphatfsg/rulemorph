use serde_json::Value as JsonValue;

use crate::v2_model::{V2Expr, V2Pipe, V2Start, V2Step};

/// Inferred types for v2 expressions
#[derive(Debug, Clone, PartialEq)]
pub enum V2Type {
    Unknown,
    Null,
    Bool,
    Number,
    String,
    Array(Box<V2Type>),
    Object,
    Any,
}

impl V2Type {
    /// Check if this type is compatible with another type
    pub fn is_compatible_with(&self, other: &V2Type) -> bool {
        matches!(
            (self, other),
            (V2Type::Unknown, _)
                | (_, V2Type::Unknown)
                | (V2Type::Any, _)
                | (_, V2Type::Any)
                | (V2Type::Null, V2Type::Null)
                | (V2Type::Bool, V2Type::Bool)
                | (V2Type::Number, V2Type::Number)
                | (V2Type::String, V2Type::String)
                | (V2Type::Object, V2Type::Object)
                | (V2Type::Array(_), V2Type::Array(_))
        )
    }

    /// Check if this type is definitely boolean
    pub fn is_bool(&self) -> bool {
        matches!(self, V2Type::Bool)
    }

    /// Check if this type cannot be boolean
    pub fn is_definitely_not_bool(&self) -> bool {
        matches!(
            self,
            V2Type::Null | V2Type::Number | V2Type::String | V2Type::Array(_) | V2Type::Object
        )
    }
}

/// Infer the type of a v2 expression
pub fn infer_v2_expr_type(expr: &V2Expr) -> V2Type {
    match expr {
        V2Expr::Pipe(pipe) => infer_pipe_type(pipe),
        V2Expr::V1Fallback(_) => V2Type::Unknown,
    }
}

/// Infer the type of a pipe
fn infer_pipe_type(pipe: &V2Pipe) -> V2Type {
    let mut current_type = infer_start_type(&pipe.start);
    for step in &pipe.steps {
        current_type = infer_step_result_type(step, &current_type);
    }
    current_type
}

/// Infer the type of a pipe start value
fn infer_start_type(start: &V2Start) -> V2Type {
    match start {
        V2Start::Literal(value) => infer_json_type(value),
        V2Start::Ref(_) => V2Type::Unknown,
        V2Start::PipeValue => V2Type::Unknown,
        V2Start::V1Expr(_) => V2Type::Unknown,
    }
}

/// Infer the type of a JSON value
fn infer_json_type(value: &JsonValue) -> V2Type {
    match value {
        JsonValue::Null => V2Type::Null,
        JsonValue::Bool(_) => V2Type::Bool,
        JsonValue::Number(_) => V2Type::Number,
        JsonValue::String(_) => V2Type::String,
        JsonValue::Array(_) => V2Type::Array(Box::new(V2Type::Unknown)),
        JsonValue::Object(_) => V2Type::Object,
    }
}

/// Infer the result type of a step
fn infer_step_result_type(step: &V2Step, _input_type: &V2Type) -> V2Type {
    match step {
        V2Step::Op(op_step) => infer_op_result_type(&op_step.op),
        V2Step::Let(_) => V2Type::Unknown, // Let returns last expression or input
        V2Step::If(_) => V2Type::Unknown,  // Could be either branch
        V2Step::Map(_) => V2Type::Array(Box::new(V2Type::Unknown)),
        V2Step::Ref(_) => V2Type::Unknown, // Reference returns unknown type
    }
}

/// Infer the result type of an operation
fn infer_op_result_type(op: &str) -> V2Type {
    match op {
        // String operations
        "trim" | "lowercase" | "uppercase" | "concat" | "to_string" => V2Type::String,

        // Numeric operations
        "+" | "-" | "*" | "/" | "add" | "subtract" | "multiply" | "divide" => V2Type::Number,

        // Lookup returns arrays of matches
        "lookup" => V2Type::Array(Box::new(V2Type::Unknown)),

        // Coalesce and lookup_first return unknown (could be any type)
        "coalesce" | "lookup_first" => V2Type::Unknown,

        // Default to unknown
        _ => V2Type::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_type_is_bool() {
        assert!(V2Type::Bool.is_bool());
        assert!(!V2Type::String.is_bool());
        assert!(!V2Type::Unknown.is_bool());
    }

    #[test]
    fn test_type_is_definitely_not_bool() {
        assert!(V2Type::String.is_definitely_not_bool());
        assert!(V2Type::Number.is_definitely_not_bool());
        assert!(V2Type::Null.is_definitely_not_bool());
        assert!(!V2Type::Bool.is_definitely_not_bool());
        assert!(!V2Type::Unknown.is_definitely_not_bool());
    }

    #[test]
    fn test_infer_json_type() {
        assert_eq!(infer_json_type(&json!(null)), V2Type::Null);
        assert_eq!(infer_json_type(&json!(true)), V2Type::Bool);
        assert_eq!(infer_json_type(&json!(42)), V2Type::Number);
        assert_eq!(infer_json_type(&json!("hello")), V2Type::String);
        assert!(matches!(infer_json_type(&json!([1, 2])), V2Type::Array(_)));
        assert_eq!(infer_json_type(&json!({"a": 1})), V2Type::Object);
    }
}
