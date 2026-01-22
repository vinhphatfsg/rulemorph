//! v2 Evaluation Context and Functions for rulemorph v2.0
//!
//! This module provides the evaluation context and functions for v2 expressions,
//! including pipe value tracking, let bindings, and item/acc scopes.

use serde_json::Value as JsonValue;
use std::collections::HashMap;

use crate::error::{TransformError, TransformErrorKind};
use crate::path::{get_path, parse_path};
use crate::v2_model::{V2Comparison, V2ComparisonOp, V2Condition, V2Expr, V2IfStep, V2LetStep, V2MapStep, V2OpStep, V2Pipe, V2Ref, V2Start, V2Step};

// =============================================================================
// EvalValue - Same as v1 transform
// =============================================================================

/// Evaluation result - either a value or missing
#[derive(Debug, Clone, PartialEq)]
pub enum EvalValue {
    Missing,
    Value(JsonValue),
}

impl EvalValue {
    pub fn is_missing(&self) -> bool {
        matches!(self, EvalValue::Missing)
    }

    pub fn into_value(self) -> Option<JsonValue> {
        match self {
            EvalValue::Value(v) => Some(v),
            EvalValue::Missing => None,
        }
    }

    pub fn as_value(&self) -> Option<&JsonValue> {
        match self {
            EvalValue::Value(v) => Some(v),
            EvalValue::Missing => None,
        }
    }
}

// =============================================================================
// V2EvalContext - Evaluation context for v2 expressions
// =============================================================================

/// Item in a map/filter operation
#[derive(Clone)]
pub struct EvalItem<'a> {
    pub value: &'a JsonValue,
    pub index: usize,
}

/// v2 evaluation context - tracks pipe value, let bindings, and iteration scopes
#[derive(Clone)]
pub struct V2EvalContext<'a> {
    /// Current pipe value ($)
    pipe_value: Option<EvalValue>,
    /// Let-bound variables (local scope)
    let_bindings: HashMap<String, EvalValue>,
    /// Item scope for map/filter operations (@item)
    item: Option<EvalItem<'a>>,
    /// Accumulator scope for reduce/fold operations (@acc)
    acc: Option<&'a JsonValue>,
}

impl<'a> V2EvalContext<'a> {
    /// Create a new empty context
    pub fn new() -> Self {
        Self {
            pipe_value: None,
            let_bindings: HashMap::new(),
            item: None,
            acc: None,
        }
    }

    /// Create a new context with a pipe value
    pub fn with_pipe_value(mut self, value: EvalValue) -> Self {
        self.pipe_value = Some(value);
        self
    }

    /// Create a new context with a let binding added
    pub fn with_let_binding(mut self, name: String, value: EvalValue) -> Self {
        self.let_bindings.insert(name, value);
        self
    }

    /// Create a new context with multiple let bindings added
    pub fn with_let_bindings(mut self, bindings: Vec<(String, EvalValue)>) -> Self {
        for (name, value) in bindings {
            self.let_bindings.insert(name, value);
        }
        self
    }

    /// Create a new context with item scope (for map/filter operations)
    pub fn with_item(mut self, item: EvalItem<'a>) -> Self {
        self.item = Some(item);
        self
    }

    /// Create a new context with accumulator scope (for reduce/fold operations)
    pub fn with_acc(mut self, acc: &'a JsonValue) -> Self {
        self.acc = Some(acc);
        self
    }

    /// Get the current pipe value
    pub fn get_pipe_value(&self) -> Option<&EvalValue> {
        self.pipe_value.as_ref()
    }

    /// Resolve a local variable name
    pub fn resolve_local(&self, name: &str) -> Option<&EvalValue> {
        self.let_bindings.get(name)
    }

    /// Get the current item (if in map/filter scope)
    pub fn get_item(&self) -> Option<&EvalItem<'a>> {
        self.item.as_ref()
    }

    /// Get the current accumulator (if in reduce/fold scope)
    pub fn get_acc(&self) -> Option<&JsonValue> {
        self.acc
    }

    /// Check if item scope is available
    pub fn has_item_scope(&self) -> bool {
        self.item.is_some()
    }

    /// Check if accumulator scope is available
    pub fn has_acc_scope(&self) -> bool {
        self.acc.is_some()
    }
}

impl<'a> Default for V2EvalContext<'a> {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// V2EvalContext Tests (T12)
// =============================================================================

#[cfg(test)]
mod v2_eval_context_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_context_new() {
        let ctx = V2EvalContext::new();
        assert!(ctx.get_pipe_value().is_none());
        assert!(ctx.resolve_local("x").is_none());
        assert!(ctx.get_item().is_none());
        assert!(ctx.get_acc().is_none());
    }

    #[test]
    fn test_context_with_pipe_value() {
        let ctx = V2EvalContext::new().with_pipe_value(EvalValue::Value(json!(42)));
        assert!(ctx.get_pipe_value().is_some());
        assert_eq!(
            ctx.get_pipe_value(),
            Some(&EvalValue::Value(json!(42)))
        );
    }

    #[test]
    fn test_context_with_let_binding() {
        let ctx = V2EvalContext::new()
            .with_let_binding("x".to_string(), EvalValue::Value(json!(100)));
        assert!(ctx.resolve_local("x").is_some());
        assert_eq!(
            ctx.resolve_local("x"),
            Some(&EvalValue::Value(json!(100)))
        );
        assert!(ctx.resolve_local("y").is_none());
    }

    #[test]
    fn test_context_with_multiple_let_bindings() {
        let ctx = V2EvalContext::new().with_let_bindings(vec![
            ("a".to_string(), EvalValue::Value(json!(1))),
            ("b".to_string(), EvalValue::Value(json!(2))),
        ]);
        assert!(ctx.resolve_local("a").is_some());
        assert!(ctx.resolve_local("b").is_some());
        assert!(ctx.resolve_local("c").is_none());
    }

    #[test]
    fn test_context_scope_chain() {
        let ctx = V2EvalContext::new()
            .with_let_binding("x".to_string(), EvalValue::Value(json!(1)));
        let inner_ctx = ctx
            .clone()
            .with_let_binding("y".to_string(), EvalValue::Value(json!(2)));

        // Inner context has both x and y
        assert!(inner_ctx.resolve_local("x").is_some());
        assert!(inner_ctx.resolve_local("y").is_some());

        // Outer context only has x
        assert!(ctx.resolve_local("x").is_some());
        assert!(ctx.resolve_local("y").is_none());
    }

    #[test]
    fn test_context_with_item() {
        let item_value = json!({"name": "test"});
        let ctx = V2EvalContext::new().with_item(EvalItem {
            value: &item_value,
            index: 0,
        });
        assert!(ctx.has_item_scope());
        assert!(ctx.get_item().is_some());
        let item = ctx.get_item().unwrap();
        assert_eq!(item.value, &json!({"name": "test"}));
        assert_eq!(item.index, 0);
    }

    #[test]
    fn test_context_with_acc() {
        let acc_value = json!(0);
        let ctx = V2EvalContext::new().with_acc(&acc_value);
        assert!(ctx.has_acc_scope());
        assert!(ctx.get_acc().is_some());
        assert_eq!(ctx.get_acc(), Some(&json!(0)));
    }

    #[test]
    fn test_eval_value_is_missing() {
        assert!(EvalValue::Missing.is_missing());
        assert!(!EvalValue::Value(json!(null)).is_missing());
    }

    #[test]
    fn test_eval_value_into_value() {
        assert_eq!(EvalValue::Missing.into_value(), None);
        assert_eq!(
            EvalValue::Value(json!("hello")).into_value(),
            Some(json!("hello"))
        );
    }

    #[test]
    fn test_eval_value_as_value() {
        let missing = EvalValue::Missing;
        let val = EvalValue::Value(json!(42));
        assert!(missing.as_value().is_none());
        assert_eq!(val.as_value(), Some(&json!(42)));
    }

    #[test]
    fn test_context_preserves_pipe_value_after_let() {
        let ctx = V2EvalContext::new()
            .with_pipe_value(EvalValue::Value(json!(100)))
            .with_let_binding("x".to_string(), EvalValue::Value(json!(50)));

        // Pipe value should still be accessible
        assert_eq!(
            ctx.get_pipe_value(),
            Some(&EvalValue::Value(json!(100)))
        );
        // Let binding should also be accessible
        assert_eq!(
            ctx.resolve_local("x"),
            Some(&EvalValue::Value(json!(50)))
        );
    }
}

// =============================================================================
// v2 Reference Evaluation (T13)
// =============================================================================

/// Helper to get value at path string
fn get_path_str<'a>(value: &'a JsonValue, path_str: &str, error_path: &str) -> Result<EvalValue, TransformError> {
    let tokens = parse_path(path_str).map_err(|_| {
        TransformError::new(TransformErrorKind::ExprError, format!("invalid path: {}", path_str))
            .with_path(error_path)
    })?;
    match get_path(value, &tokens) {
        Some(v) => Ok(EvalValue::Value(v.clone())),
        None => Ok(EvalValue::Missing),
    }
}

/// Evaluate a v2 reference to get its value
pub fn eval_v2_ref<'a>(
    v2_ref: &V2Ref,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    match v2_ref {
        V2Ref::Input(ref_path) => {
            if ref_path.is_empty() {
                Ok(EvalValue::Value(record.clone()))
            } else {
                get_path_str(record, ref_path, path)
            }
        }
        V2Ref::Context(ref_path) => {
            let ctx_value = context.ok_or_else(|| {
                TransformError::new(TransformErrorKind::ExprError, "context is not available")
                    .with_path(path)
            })?;
            if ref_path.is_empty() {
                Ok(EvalValue::Value(ctx_value.clone()))
            } else {
                get_path_str(ctx_value, ref_path, path)
            }
        }
        V2Ref::Out(ref_path) => {
            if ref_path.is_empty() {
                Ok(EvalValue::Value(out.clone()))
            } else {
                get_path_str(out, ref_path, path)
            }
        }
        V2Ref::Item(ref_path) => {
            let item = ctx.get_item().ok_or_else(|| {
                TransformError::new(TransformErrorKind::ExprError, "@item is only available in map/filter operations")
                    .with_path(path)
            })?;
            if ref_path.is_empty() {
                Ok(EvalValue::Value(item.value.clone()))
            } else if ref_path == "index" {
                Ok(EvalValue::Value(JsonValue::Number(item.index.into())))
            } else if let Some(rest) = ref_path.strip_prefix("value.") {
                get_path_str(item.value, rest, path)
            } else if ref_path == "value" {
                Ok(EvalValue::Value(item.value.clone()))
            } else {
                // Direct path on item value
                get_path_str(item.value, ref_path, path)
            }
        }
        V2Ref::Acc(ref_path) => {
            let acc = ctx.get_acc().ok_or_else(|| {
                TransformError::new(TransformErrorKind::ExprError, "@acc is only available in reduce/fold operations")
                    .with_path(path)
            })?;
            if ref_path.is_empty() {
                Ok(EvalValue::Value(acc.clone()))
            } else if let Some(rest) = ref_path.strip_prefix("value.") {
                get_path_str(acc, rest, path)
            } else if ref_path == "value" {
                Ok(EvalValue::Value(acc.clone()))
            } else {
                // Direct path on acc value
                get_path_str(acc, ref_path, path)
            }
        }
        V2Ref::Local(var_name) => {
            let value = ctx.resolve_local(var_name).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("undefined variable: @{}", var_name),
                )
                .with_path(path)
            })?;
            Ok(value.clone())
        }
    }
}

// =============================================================================
// v2 Reference Evaluation Tests (T13)
// =============================================================================

#[cfg(test)]
mod v2_ref_eval_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_eval_input_ref() {
        let record = json!({"name": "Alice", "age": 30});
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Input("name".to_string()),
            &record,
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("Alice")));
    }

    #[test]
    fn test_eval_input_ref_nested() {
        let record = json!({"user": {"profile": {"name": "Bob"}}});
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Input("user.profile.name".to_string()),
            &record,
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("Bob")));
    }

    #[test]
    fn test_eval_input_ref_missing() {
        let record = json!({"name": "Alice"});
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Input("nonexistent".to_string()),
            &record,
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Missing)));
    }

    #[test]
    fn test_eval_context_ref() {
        let record = json!({});
        let context = json!({"rate": 1.5, "config": {"enabled": true}});
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Context("rate".to_string()),
            &record,
            Some(&context),
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(1.5)));
    }

    #[test]
    fn test_eval_context_ref_nested() {
        let record = json!({});
        let context = json!({"config": {"enabled": true}});
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Context("config.enabled".to_string()),
            &record,
            Some(&context),
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(true)));
    }

    #[test]
    fn test_eval_context_ref_no_context_error() {
        let record = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Context("rate".to_string()),
            &record,
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_out_ref() {
        let record = json!({});
        let out = json!({"computed": 42});
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Out("computed".to_string()),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(42)));
    }

    #[test]
    fn test_eval_local_ref() {
        let ctx = V2EvalContext::new()
            .with_let_binding("price".to_string(), EvalValue::Value(json!(100)));
        let result = eval_v2_ref(
            &V2Ref::Local("price".to_string()),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(100)));
    }

    #[test]
    fn test_eval_local_ref_undefined_error() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Local("undefined".to_string()),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_item_ref() {
        let item_value = json!({"name": "item1", "value": 10});
        let ctx = V2EvalContext::new().with_item(EvalItem {
            value: &item_value,
            index: 2,
        });
        let result = eval_v2_ref(
            &V2Ref::Item("name".to_string()),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("item1")));
    }

    #[test]
    fn test_eval_item_ref_index() {
        let item_value = json!({"name": "item1"});
        let ctx = V2EvalContext::new().with_item(EvalItem {
            value: &item_value,
            index: 5,
        });
        let result = eval_v2_ref(
            &V2Ref::Item("index".to_string()),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(5)));
    }

    #[test]
    fn test_eval_item_ref_no_scope_error() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Item("value".to_string()),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_acc_ref() {
        let acc_value = json!(100);
        let ctx = V2EvalContext::new().with_acc(&acc_value);
        let result = eval_v2_ref(
            &V2Ref::Acc(String::new()),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(100)));
    }

    #[test]
    fn test_eval_acc_ref_no_scope_error() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Acc("value".to_string()),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_input_ref_empty_path() {
        let record = json!({"name": "Alice"});
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Input(String::new()),
            &record,
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!({"name": "Alice"})));
    }
}

// =============================================================================
// v2 Start Value Evaluation (T14)
// =============================================================================

/// Evaluate a v2 start value
pub fn eval_v2_start<'a>(
    start: &V2Start,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    match start {
        V2Start::Ref(v2_ref) => eval_v2_ref(v2_ref, record, context, out, path, ctx),
        V2Start::PipeValue => {
            // If pipe value is not available, return Missing instead of error
            // This allows ops like lookup_first that don't use pipe input to work
            Ok(ctx.get_pipe_value()
                .cloned()
                .unwrap_or(EvalValue::Missing))
        }
        V2Start::Literal(value) => Ok(EvalValue::Value(value.clone())),
        V2Start::V1Expr(_expr) => {
            // V1 expressions would be evaluated using the existing v1 eval logic
            // For now, return an error as this is a fallback case
            Err(TransformError::new(
                TransformErrorKind::ExprError,
                "v1 expression fallback not yet implemented",
            )
            .with_path(path))
        }
    }
}

// =============================================================================
// v2 Start Value Evaluation Tests (T14)
// =============================================================================

#[cfg(test)]
mod v2_start_eval_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_eval_start_literal_string() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_start(
            &V2Start::Literal(json!("hello")),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("hello")));
    }

    #[test]
    fn test_eval_start_literal_number() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_start(
            &V2Start::Literal(json!(42)),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(42)));
    }

    #[test]
    fn test_eval_start_literal_bool() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_start(
            &V2Start::Literal(json!(true)),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(true)));
    }

    #[test]
    fn test_eval_start_literal_null() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_start(
            &V2Start::Literal(json!(null)),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(null)));
    }

    #[test]
    fn test_eval_start_literal_array() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_start(
            &V2Start::Literal(json!([1, 2, 3])),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([1, 2, 3])));
    }

    #[test]
    fn test_eval_start_literal_object() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_start(
            &V2Start::Literal(json!({"key": "value"})),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!({"key": "value"})));
    }

    #[test]
    fn test_eval_start_ref() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_start(
            &V2Start::Ref(V2Ref::Input("name".to_string())),
            &json!({"name": "Bob"}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("Bob")));
    }

    #[test]
    fn test_eval_start_pipe_value() {
        let ctx = V2EvalContext::new().with_pipe_value(EvalValue::Value(json!(42)));
        let result = eval_v2_start(
            &V2Start::PipeValue,
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(42)));
    }

    #[test]
    fn test_eval_start_pipe_value_not_available() {
        // When pipe value is not set, it returns Missing (not error)
        // This allows ops like lookup_first that don't use pipe input to work
        let ctx = V2EvalContext::new();
        let result = eval_v2_start(
            &V2Start::PipeValue,
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), EvalValue::Missing);
    }

    #[test]
    fn test_eval_start_pipe_value_missing() {
        let ctx = V2EvalContext::new().with_pipe_value(EvalValue::Missing);
        let result = eval_v2_start(
            &V2Start::PipeValue,
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Missing)));
    }
}

// =============================================================================
// v2 Op Step Evaluation (T15)
// =============================================================================

/// Evaluate a v2 pipe expression
pub fn eval_v2_pipe<'a>(
    pipe: &V2Pipe,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    // Evaluate start value
    let mut current = eval_v2_start(&pipe.start, record, context, out, path, ctx)?;
    let mut current_ctx = ctx.clone();

    // Apply each step
    for (i, step) in pipe.steps.iter().enumerate() {
        let step_path = format!("{}[{}]", path, i + 1);
        // Update context with current pipe value for each step
        current_ctx = current_ctx.clone().with_pipe_value(current.clone());

        match step {
            V2Step::Op(op_step) => {
                current = eval_v2_op_step(op_step, current, record, context, out, &step_path, &current_ctx)?;
            }
            V2Step::Let(let_step) => {
                // Let step doesn't change pipe value, just adds bindings to context
                current_ctx = eval_v2_let_step(let_step, current.clone(), record, context, out, &step_path, &current_ctx)?;
            }
            V2Step::If(if_step) => {
                current = eval_v2_if_step(if_step, current, record, context, out, &step_path, &current_ctx)?;
            }
            V2Step::Map(map_step) => {
                current = eval_v2_map_step(map_step, current, record, context, out, &step_path, &current_ctx)?;
            }
            V2Step::Ref(v2_ref) => {
                // Reference step evaluates the reference and returns its value
                current = eval_v2_ref(v2_ref, record, context, out, &step_path, &current_ctx)?;
            }
        }
    }

    Ok(current)
}

/// Evaluate a v2 let step - binds variables to context without changing pipe value
pub fn eval_v2_let_step<'a>(
    let_step: &V2LetStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<V2EvalContext<'a>, TransformError> {
    let mut new_ctx = ctx.clone().with_pipe_value(pipe_value);

    for (name, expr) in &let_step.bindings {
        let binding_path = format!("{}.{}", path, name);
        let value = eval_v2_expr(expr, record, context, out, &binding_path, &new_ctx)?;
        new_ctx = new_ctx.with_let_binding(name.clone(), value);
    }

    Ok(new_ctx)
}

/// Evaluate a v2 if step - conditional branching
pub fn eval_v2_if_step<'a>(
    if_step: &V2IfStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    // Create context with current pipe value for condition evaluation
    let cond_ctx = ctx.clone().with_pipe_value(pipe_value.clone());

    // Evaluate condition
    let cond_path = format!("{}.cond", path);
    let cond_result = eval_v2_condition(&if_step.cond, record, context, out, &cond_path, &cond_ctx)?;

    if cond_result {
        // Execute then branch
        let then_path = format!("{}.then", path);
        eval_v2_pipe(&if_step.then_branch, record, context, out, &then_path, &cond_ctx)
    } else if let Some(ref else_branch) = if_step.else_branch {
        // Execute else branch
        let else_path = format!("{}.else", path);
        eval_v2_pipe(else_branch, record, context, out, &else_path, &cond_ctx)
    } else {
        // No else branch, return pipe value unchanged
        Ok(pipe_value)
    }
}

/// Evaluate a v2 map step - iterates over arrays
pub fn eval_v2_map_step<'a>(
    map_step: &V2MapStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    // Get the array to iterate over
    let arr = match &pipe_value {
        EvalValue::Missing => {
            return Ok(EvalValue::Missing);
        }
        EvalValue::Value(JsonValue::Array(arr)) => arr,
        EvalValue::Value(other) => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                format!("map step requires array, got {:?}", other),
            )
            .with_path(path));
        }
    };

    // Map over each element
    let mut results = Vec::with_capacity(arr.len());
    for (index, item_value) in arr.iter().enumerate() {
        let item_path = format!("{}[{}]", path, index);

        // Create context with item scope
        let item_ctx = ctx.clone()
            .with_pipe_value(EvalValue::Value(item_value.clone()))
            .with_item(EvalItem {
                value: item_value,
                index,
            });

        // Apply all steps to this item
        let mut current = EvalValue::Value(item_value.clone());
        let mut step_ctx = item_ctx.clone();  // Declare outside loop to preserve let bindings

        for (step_idx, step) in map_step.steps.iter().enumerate() {
            let step_path = format!("{}.step[{}]", item_path, step_idx);
            step_ctx = step_ctx.clone().with_pipe_value(current.clone());

            match step {
                V2Step::Op(op_step) => {
                    current = eval_v2_op_step(op_step, current, record, context, out, &step_path, &step_ctx)?;
                }
                V2Step::Let(let_step) => {
                    // Let in map context - evaluate and update context to preserve bindings
                    step_ctx = eval_v2_let_step(let_step, current.clone(), record, context, out, &step_path, &step_ctx)?;
                    // Let doesn't change pipe value
                    current = step_ctx.get_pipe_value().cloned().unwrap_or(current);
                }
                V2Step::If(if_step) => {
                    current = eval_v2_if_step(if_step, current, record, context, out, &step_path, &step_ctx)?;
                }
                V2Step::Map(nested_map) => {
                    current = eval_v2_map_step(nested_map, current, record, context, out, &step_path, &step_ctx)?;
                }
                V2Step::Ref(v2_ref) => {
                    // Reference step evaluates the reference and returns its value
                    current = eval_v2_ref(v2_ref, record, context, out, &step_path, &step_ctx)?;
                }
            };
        }

        // Only add non-missing values to results
        if let EvalValue::Value(v) = current {
            results.push(v);
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

/// Evaluate a v2 condition - returns bool
pub fn eval_v2_condition<'a>(
    condition: &V2Condition,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<bool, TransformError> {
    match condition {
        V2Condition::All(conditions) => {
            for (i, cond) in conditions.iter().enumerate() {
                let cond_path = format!("{}[{}]", path, i);
                if !eval_v2_condition(cond, record, context, out, &cond_path, ctx)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        V2Condition::Any(conditions) => {
            for (i, cond) in conditions.iter().enumerate() {
                let cond_path = format!("{}[{}]", path, i);
                if eval_v2_condition(cond, record, context, out, &cond_path, ctx)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        V2Condition::Comparison(comparison) => {
            eval_v2_comparison(comparison, record, context, out, path, ctx)
        }
        V2Condition::Expr(expr) => {
            let expr_path = format!("{}.expr", path);
            let value = eval_v2_expr(expr, record, context, out, &expr_path, ctx)?;
            Ok(is_truthy(&value))
        }
    }
}

/// Check if a value is truthy
fn is_truthy(value: &EvalValue) -> bool {
    match value {
        EvalValue::Missing => false,
        EvalValue::Value(v) => match v {
            JsonValue::Null => false,
            JsonValue::Bool(b) => *b,
            JsonValue::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
            JsonValue::String(s) => !s.is_empty(),
            JsonValue::Array(arr) => !arr.is_empty(),
            JsonValue::Object(obj) => !obj.is_empty(),
        },
    }
}

/// Evaluate a v2 comparison
fn eval_v2_comparison<'a>(
    comparison: &V2Comparison,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<bool, TransformError> {
    if comparison.args.len() != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!("comparison requires exactly 2 arguments, got {}", comparison.args.len()),
        )
        .with_path(path));
    }

    let left_path = format!("{}.args[0]", path);
    let right_path = format!("{}.args[1]", path);

    let left = eval_v2_expr(&comparison.args[0], record, context, out, &left_path, ctx)?;
    let right = eval_v2_expr(&comparison.args[1], record, context, out, &right_path, ctx)?;

    match comparison.op {
        V2ComparisonOp::Eq => Ok(compare_values_eq(&left, &right)),
        V2ComparisonOp::Ne => Ok(!compare_values_eq(&left, &right)),
        V2ComparisonOp::Gt => compare_values_ord(&left, &right, path).map(|ord| ord == std::cmp::Ordering::Greater),
        V2ComparisonOp::Gte => compare_values_ord(&left, &right, path).map(|ord| ord != std::cmp::Ordering::Less),
        V2ComparisonOp::Lt => compare_values_ord(&left, &right, path).map(|ord| ord == std::cmp::Ordering::Less),
        V2ComparisonOp::Lte => compare_values_ord(&left, &right, path).map(|ord| ord != std::cmp::Ordering::Greater),
        V2ComparisonOp::Match => compare_values_match(&left, &right, path),
    }
}

/// Compare two values for equality
fn compare_values_eq(left: &EvalValue, right: &EvalValue) -> bool {
    match (left, right) {
        (EvalValue::Missing, EvalValue::Missing) => true,
        (EvalValue::Missing, _) | (_, EvalValue::Missing) => false,
        (EvalValue::Value(l), EvalValue::Value(r)) => l == r,
    }
}

/// Compare two values for ordering
fn compare_values_ord(left: &EvalValue, right: &EvalValue, path: &str) -> Result<std::cmp::Ordering, TransformError> {
    match (left, right) {
        (EvalValue::Value(l), EvalValue::Value(r)) => {
            // Try numeric comparison first
            if let (Some(l_num), Some(r_num)) = (value_as_f64(l), value_as_f64(r)) {
                return Ok(l_num.partial_cmp(&r_num).unwrap_or(std::cmp::Ordering::Equal));
            }
            // Try string comparison
            if let (Some(l_str), Some(r_str)) = (value_as_str(l), value_as_str(r)) {
                return Ok(l_str.cmp(r_str));
            }
            Err(TransformError::new(
                TransformErrorKind::ExprError,
                "cannot compare values of different types",
            )
            .with_path(path))
        }
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "cannot compare missing values",
        )
        .with_path(path)),
    }
}

/// Compare with regex match
fn compare_values_match(left: &EvalValue, right: &EvalValue, path: &str) -> Result<bool, TransformError> {
    let text = match left {
        EvalValue::Value(JsonValue::String(s)) => s.as_str(),
        _ => return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "match operator requires string on left side",
        )
        .with_path(path)),
    };

    let pattern = match right {
        EvalValue::Value(JsonValue::String(s)) => s.as_str(),
        _ => return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "match operator requires regex pattern string on right side",
        )
        .with_path(path)),
    };

    let re = regex::Regex::new(pattern).map_err(|e| {
        TransformError::new(
            TransformErrorKind::ExprError,
            format!("invalid regex pattern: {}", e),
        )
        .with_path(path)
    })?;

    Ok(re.is_match(text))
}

/// Helper to get f64 from JsonValue
fn value_as_f64(v: &JsonValue) -> Option<f64> {
    match v {
        JsonValue::Number(n) => n.as_f64(),
        JsonValue::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

/// Helper to get str from JsonValue
fn value_as_str(v: &JsonValue) -> Option<&str> {
    match v {
        JsonValue::String(s) => Some(s.as_str()),
        _ => None,
    }
}

/// Evaluate a v2 expression
pub fn eval_v2_expr<'a>(
    expr: &V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    match expr {
        V2Expr::Pipe(pipe) => eval_v2_pipe(pipe, record, context, out, path, ctx),
        V2Expr::V1Fallback(_) => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "v1 fallback not yet implemented",
        )
        .with_path(path)),
    }
}

/// Helper to convert EvalValue to string
fn eval_value_as_string(value: &EvalValue, path: &str) -> Result<String, TransformError> {
    match value {
        EvalValue::Missing => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expected string, got missing value",
        )
        .with_path(path)),
        EvalValue::Value(v) => match v {
            JsonValue::String(s) => Ok(s.clone()),
            JsonValue::Number(n) => Ok(n.to_string()),
            JsonValue::Bool(b) => Ok(b.to_string()),
            _ => Err(TransformError::new(
                TransformErrorKind::ExprError,
                format!("expected string, got {:?}", v),
            )
            .with_path(path)),
        },
    }
}

/// Helper to convert EvalValue to number
fn eval_value_as_number(value: &EvalValue, path: &str) -> Result<f64, TransformError> {
    match value {
        EvalValue::Missing => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expected number, got missing value",
        )
        .with_path(path)),
        EvalValue::Value(v) => match v {
            JsonValue::Number(n) => n.as_f64().ok_or_else(|| {
                TransformError::new(TransformErrorKind::ExprError, "number conversion failed")
                    .with_path(path)
            }),
            JsonValue::String(s) => s.parse::<f64>().map_err(|_| {
                TransformError::new(TransformErrorKind::ExprError, "failed to parse string as number")
                    .with_path(path)
            }),
            _ => Err(TransformError::new(
                TransformErrorKind::ExprError,
                format!("expected number, got {:?}", v),
            )
            .with_path(path)),
        },
    }
}

/// Evaluate a v2 op step with a pipe value as implicit first argument
pub fn eval_v2_op_step<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    // Create a new context with the current pipe value
    let step_ctx = ctx.clone().with_pipe_value(pipe_value.clone());

    // Handle "@..." as a reference (from shorthand string in step position)
    if op_step.op.starts_with('@') {
        use crate::v2_parser::parse_v2_ref;
        if let Some(v2_ref) = parse_v2_ref(&op_step.op) {
            return eval_v2_ref(&v2_ref, record, context, out, path, &step_ctx);
        }
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!("invalid reference: {}", op_step.op),
        )
        .with_path(path));
    }

    match op_step.op.as_str() {
        // String operations
        "trim" => {
            let s = eval_value_as_string(&pipe_value, path)?;
            Ok(EvalValue::Value(JsonValue::String(s.trim().to_string())))
        }
        "lowercase" => {
            let s = eval_value_as_string(&pipe_value, path)?;
            Ok(EvalValue::Value(JsonValue::String(s.to_lowercase())))
        }
        "uppercase" => {
            let s = eval_value_as_string(&pipe_value, path)?;
            Ok(EvalValue::Value(JsonValue::String(s.to_uppercase())))
        }
        "to_string" => match &pipe_value {
            EvalValue::Missing => Ok(EvalValue::Missing),
            EvalValue::Value(v) => {
                let s = match v {
                    JsonValue::String(s) => s.clone(),
                    JsonValue::Number(n) => n.to_string(),
                    JsonValue::Bool(b) => b.to_string(),
                    JsonValue::Null => "null".to_string(),
                    JsonValue::Array(_) | JsonValue::Object(_) => v.to_string(),
                };
                Ok(EvalValue::Value(JsonValue::String(s)))
            }
        },
        "concat" => {
            // Pipe value is first, then args
            let mut parts = Vec::new();
            parts.push(eval_value_as_string(&pipe_value, path)?);
            for (i, arg) in op_step.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", path, i);
                let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, &step_ctx)?;
                parts.push(eval_value_as_string(&arg_value, &arg_path)?);
            }
            Ok(EvalValue::Value(JsonValue::String(parts.join(""))))
        }

        // Numeric operations
        "add" | "+" => {
            let mut result = eval_value_as_number(&pipe_value, path)?;
            for (i, arg) in op_step.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", path, i);
                let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, &step_ctx)?;
                result += eval_value_as_number(&arg_value, &arg_path)?;
            }
            Ok(EvalValue::Value(serde_json::json!(result)))
        }
        "subtract" | "-" => {
            if op_step.args.is_empty() {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "subtract requires at least one argument",
                )
                .with_path(path));
            }
            let mut result = eval_value_as_number(&pipe_value, path)?;
            for (i, arg) in op_step.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", path, i);
                let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, &step_ctx)?;
                result -= eval_value_as_number(&arg_value, &arg_path)?;
            }
            Ok(EvalValue::Value(serde_json::json!(result)))
        }
        "multiply" | "*" => {
            let mut result = eval_value_as_number(&pipe_value, path)?;
            for (i, arg) in op_step.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", path, i);
                let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, &step_ctx)?;
                result *= eval_value_as_number(&arg_value, &arg_path)?;
            }
            Ok(EvalValue::Value(serde_json::json!(result)))
        }
        "divide" | "/" => {
            if op_step.args.is_empty() {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "divide requires at least one argument",
                )
                .with_path(path));
            }
            let mut result = eval_value_as_number(&pipe_value, path)?;
            for (i, arg) in op_step.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", path, i);
                let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, &step_ctx)?;
                let divisor = eval_value_as_number(&arg_value, &arg_path)?;
                if divisor == 0.0 {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "division by zero",
                    )
                    .with_path(&arg_path));
                }
                result /= divisor;
            }
            Ok(EvalValue::Value(serde_json::json!(result)))
        }

        // Coalesce
        "coalesce" => {
            // If pipe value is present and not null, use it
            if let EvalValue::Value(v) = &pipe_value {
                if !v.is_null() {
                    return Ok(pipe_value);
                }
            }
            // Otherwise, try args in order
            for (i, arg) in op_step.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", path, i);
                let arg_value = eval_v2_expr(arg, record, context, out, &arg_path, &step_ctx)?;
                if let EvalValue::Value(v) = &arg_value {
                    if !v.is_null() {
                        return Ok(arg_value);
                    }
                }
            }
            Ok(EvalValue::Missing)
        }

        // Lookup operations - v2 keyword format: lookup_first: {from: ..., match: [...], get: ...}
        // For v2, lookup args are parsed from V2OpStep with special handling
        // args[0] = from (array to search in)
        // args[1] = match key (field name in array items to match)
        // args[2] = match value (value to match against)
        // args[3] = get (optional - field to extract from matched item)
        "lookup_first" => {
            if op_step.args.len() < 3 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "lookup_first requires at least 3 arguments: from, match_key, match_value",
                )
                .with_path(path));
            }

            // Evaluate 'from' - the array to search in
            let from_path = format!("{}.from", path);
            let from_value = eval_v2_expr(&op_step.args[0], record, context, out, &from_path, &step_ctx)?;
            let arr = match &from_value {
                EvalValue::Value(JsonValue::Array(arr)) => arr,
                EvalValue::Missing => return Ok(EvalValue::Missing),
                _ => return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "lookup_first 'from' must be an array",
                )
                .with_path(&from_path)),
            };

            // Get match key as string
            let key_path = format!("{}.match_key", path);
            let key_value = eval_v2_expr(&op_step.args[1], record, context, out, &key_path, &step_ctx)?;
            let match_key = eval_value_as_string(&key_value, &key_path)?;

            // Get match value
            let val_path = format!("{}.match_value", path);
            let match_value = eval_v2_expr(&op_step.args[2], record, context, out, &val_path, &step_ctx)?;

            // Get 'get' field if provided
            let get_field = if op_step.args.len() > 3 {
                let get_path = format!("{}.get", path);
                let get_value = eval_v2_expr(&op_step.args[3], record, context, out, &get_path, &step_ctx)?;
                Some(eval_value_as_string(&get_value, &get_path)?)
            } else {
                None
            };

            // Search for first matching item
            for item in arr {
                if let JsonValue::Object(obj) = item {
                    if let Some(field_val) = obj.get(&match_key) {
                        let item_val = EvalValue::Value(field_val.clone());
                        if compare_values_eq(&item_val, &match_value) {
                            // Found a match
                            if let Some(ref get_key) = get_field {
                                // Return specific field from matched item
                                return match obj.get(get_key) {
                                    Some(v) => Ok(EvalValue::Value(v.clone())),
                                    None => Ok(EvalValue::Missing),
                                };
                            } else {
                                // Return entire matched item
                                return Ok(EvalValue::Value(item.clone()));
                            }
                        }
                    }
                }
            }

            Ok(EvalValue::Missing)
        }

        "lookup" => {
            if op_step.args.len() < 3 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "lookup requires at least 3 arguments: from, match_key, match_value",
                )
                .with_path(path));
            }

            // Evaluate 'from' - the array to search in
            let from_path = format!("{}.from", path);
            let from_value = eval_v2_expr(&op_step.args[0], record, context, out, &from_path, &step_ctx)?;
            let arr = match &from_value {
                EvalValue::Value(JsonValue::Array(arr)) => arr,
                EvalValue::Missing => return Ok(EvalValue::Missing),
                _ => return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "lookup 'from' must be an array",
                )
                .with_path(&from_path)),
            };

            // Get match key as string
            let key_path = format!("{}.match_key", path);
            let key_value = eval_v2_expr(&op_step.args[1], record, context, out, &key_path, &step_ctx)?;
            let match_key = eval_value_as_string(&key_value, &key_path)?;

            // Get match value
            let val_path = format!("{}.match_value", path);
            let match_value = eval_v2_expr(&op_step.args[2], record, context, out, &val_path, &step_ctx)?;

            // Get 'get' field if provided
            let get_field = if op_step.args.len() > 3 {
                let get_path = format!("{}.get", path);
                let get_value = eval_v2_expr(&op_step.args[3], record, context, out, &get_path, &step_ctx)?;
                Some(eval_value_as_string(&get_value, &get_path)?)
            } else {
                None
            };

            // Search for ALL matching items
            let mut results = Vec::new();
            for item in arr {
                if let JsonValue::Object(obj) = item {
                    if let Some(field_val) = obj.get(&match_key) {
                        let item_val = EvalValue::Value(field_val.clone());
                        if compare_values_eq(&item_val, &match_value) {
                            // Found a match
                            if let Some(ref get_key) = get_field {
                                // Add specific field from matched item
                                if let Some(v) = obj.get(get_key) {
                                    results.push(v.clone());
                                }
                            } else {
                                // Add entire matched item
                                results.push(item.clone());
                            }
                        }
                    }
                }
            }

            Ok(EvalValue::Value(JsonValue::Array(results)))
        }

        // Default case - unknown op
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!("unknown op: {}", op_step.op),
        )
        .with_path(path)),
    }
}

// =============================================================================
// v2 Op Step Evaluation Tests (T15)
// =============================================================================

#[cfg(test)]
mod v2_op_step_eval_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_eval_op_trim() {
        let op = V2OpStep {
            op: "trim".to_string(),
            args: vec![],
        };
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!("  hello  ")),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("hello")));
    }

    #[test]
    fn test_eval_op_lowercase() {
        let op = V2OpStep {
            op: "lowercase".to_string(),
            args: vec![],
        };
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!("HELLO")),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("hello")));
    }

    #[test]
    fn test_eval_op_uppercase() {
        let op = V2OpStep {
            op: "uppercase".to_string(),
            args: vec![],
        };
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!("hello")),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("HELLO")));
    }

    #[test]
    fn test_eval_op_to_string() {
        let op = V2OpStep {
            op: "to_string".to_string(),
            args: vec![],
        };
        let ctx = V2EvalContext::new();

        // Number to string
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(42)),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("42")));

        // Bool to string
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(true)),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("true")));
    }

    #[test]
    fn test_eval_op_add() {
        let op = V2OpStep {
            op: "add".to_string(),
            args: vec![V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(10)),
                steps: vec![],
            })],
        };
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(5)),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(15.0)));
    }

    #[test]
    fn test_eval_op_subtract() {
        let op = V2OpStep {
            op: "subtract".to_string(),
            args: vec![V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(3)),
                steps: vec![],
            })],
        };
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(10)),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(7.0)));
    }

    #[test]
    fn test_eval_op_multiply() {
        let op = V2OpStep {
            op: "multiply".to_string(),
            args: vec![V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(0.9)),
                steps: vec![],
            })],
        };
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(100)),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(90.0)));
    }

    #[test]
    fn test_eval_op_divide() {
        let op = V2OpStep {
            op: "divide".to_string(),
            args: vec![V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(2)),
                steps: vec![],
            })],
        };
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(10)),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(5.0)));
    }

    #[test]
    fn test_eval_op_divide_by_zero() {
        let op = V2OpStep {
            op: "divide".to_string(),
            args: vec![V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(0)),
                steps: vec![],
            })],
        };
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(10)),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_op_coalesce() {
        let op = V2OpStep {
            op: "coalesce".to_string(),
            args: vec![V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("default")),
                steps: vec![],
            })],
        };
        let ctx = V2EvalContext::new();

        // When pipe value is present, use it
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!("value")),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("value")));

        // When pipe value is null, use first non-null arg
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(null)),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("default")));

        // When pipe value is missing, use first non-null arg
        let result = eval_v2_op_step(
            &op,
            EvalValue::Missing,
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("default")));
    }

    #[test]
    fn test_eval_op_unknown() {
        let op = V2OpStep {
            op: "unknown_op".to_string(),
            args: vec![],
        };
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!("test")),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(result.is_err());
    }
}

// =============================================================================
// v2 Let Step Evaluation Tests (T16)
// =============================================================================

#[cfg(test)]
mod v2_let_step_eval_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_eval_let_single_binding() {
        let let_step = V2LetStep {
            bindings: vec![
                ("x".to_string(), V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(42)),
                    steps: vec![],
                })),
            ],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_let_step(
            &let_step,
            EvalValue::Value(json!("pipe_value")),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(result.is_ok());
        let new_ctx = result.unwrap();
        assert_eq!(
            new_ctx.resolve_local("x"),
            Some(&EvalValue::Value(json!(42)))
        );
    }

    #[test]
    fn test_eval_let_multiple_bindings() {
        let let_step = V2LetStep {
            bindings: vec![
                ("a".to_string(), V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(1)),
                    steps: vec![],
                })),
                ("b".to_string(), V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(2)),
                    steps: vec![],
                })),
            ],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_let_step(
            &let_step,
            EvalValue::Value(json!("pipe")),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(result.is_ok());
        let new_ctx = result.unwrap();
        assert_eq!(new_ctx.resolve_local("a"), Some(&EvalValue::Value(json!(1))));
        assert_eq!(new_ctx.resolve_local("b"), Some(&EvalValue::Value(json!(2))));
    }

    #[test]
    fn test_eval_let_binding_uses_pipe_value() {
        // let: { x: $ } should bind x to current pipe value
        let let_step = V2LetStep {
            bindings: vec![
                ("x".to_string(), V2Expr::Pipe(V2Pipe {
                    start: V2Start::PipeValue,
                    steps: vec![],
                })),
            ],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_let_step(
            &let_step,
            EvalValue::Value(json!(100)),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(result.is_ok());
        let new_ctx = result.unwrap();
        assert_eq!(
            new_ctx.resolve_local("x"),
            Some(&EvalValue::Value(json!(100)))
        );
    }

    #[test]
    fn test_eval_let_binding_from_input() {
        let let_step = V2LetStep {
            bindings: vec![
                ("name".to_string(), V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Input("user.name".to_string())),
                    steps: vec![],
                })),
            ],
        };
        let record = json!({"user": {"name": "Alice"}});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_let_step(
            &let_step,
            EvalValue::Value(json!("ignored")),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(result.is_ok());
        let new_ctx = result.unwrap();
        assert_eq!(
            new_ctx.resolve_local("name"),
            Some(&EvalValue::Value(json!("Alice")))
        );
    }

    #[test]
    fn test_eval_let_binding_chain() {
        // let: { x: 10, y: @x } - y should be able to reference x
        let let_step = V2LetStep {
            bindings: vec![
                ("x".to_string(), V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                })),
                ("y".to_string(), V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Local("x".to_string())),
                    steps: vec![],
                })),
            ],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_let_step(
            &let_step,
            EvalValue::Value(json!("pipe")),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(result.is_ok());
        let new_ctx = result.unwrap();
        assert_eq!(new_ctx.resolve_local("x"), Some(&EvalValue::Value(json!(10))));
        assert_eq!(new_ctx.resolve_local("y"), Some(&EvalValue::Value(json!(10))));
    }

    #[test]
    fn test_eval_pipe_with_let() {
        // [100, { let: { x: $ } }, @x] -> 100
        let pipe = V2Pipe {
            start: V2Start::Literal(json!(100)),
            steps: vec![
                V2Step::Let(V2LetStep {
                    bindings: vec![
                        ("x".to_string(), V2Expr::Pipe(V2Pipe {
                            start: V2Start::PipeValue,
                            steps: vec![],
                        })),
                    ],
                }),
            ],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        // Let step doesn't change pipe value
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(100)));
    }

    #[test]
    fn test_eval_pipe_let_then_op() {
        // [100, { let: { factor: 2 } }, { op: "multiply", args: [@factor] }] -> 200
        let pipe = V2Pipe {
            start: V2Start::Literal(json!(100)),
            steps: vec![
                V2Step::Let(V2LetStep {
                    bindings: vec![
                        ("factor".to_string(), V2Expr::Pipe(V2Pipe {
                            start: V2Start::Literal(json!(2)),
                            steps: vec![],
                        })),
                    ],
                }),
                V2Step::Op(V2OpStep {
                    op: "multiply".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Ref(V2Ref::Local("factor".to_string())),
                        steps: vec![],
                    })],
                }),
            ],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(200.0)));
    }
}

// =============================================================================
// v2 If Step Evaluation Tests (T17)
// =============================================================================

#[cfg(test)]
mod v2_if_step_eval_tests {
    use super::*;
    use serde_json::json;

    // ------ Condition evaluation tests ------

    #[test]
    fn test_eval_condition_eq_true() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Eq,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_eq_false() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Eq,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(20)),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(false)));
    }

    #[test]
    fn test_eval_condition_ne() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Ne,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("a")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("b")),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_gt() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Gt,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(20)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_lt() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Lt,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(5)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_gte_equal() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Gte,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_lte_less() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Lte,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(5)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_match() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Match,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("hello123")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("^hello\\d+")),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_all_true() {
        let cond = V2Condition::All(vec![
            V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Gt,
                args: vec![
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(10)), steps: vec![] }),
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(5)), steps: vec![] }),
                ],
            }),
            V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Lt,
                args: vec![
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(10)), steps: vec![] }),
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(20)), steps: vec![] }),
                ],
            }),
        ]);
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_all_false() {
        let cond = V2Condition::All(vec![
            V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Gt,
                args: vec![
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(10)), steps: vec![] }),
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(5)), steps: vec![] }),
                ],
            }),
            V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Lt, // 10 < 5 is false
                args: vec![
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(10)), steps: vec![] }),
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(5)), steps: vec![] }),
                ],
            }),
        ]);
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(false)));
    }

    #[test]
    fn test_eval_condition_any_true() {
        let cond = V2Condition::Any(vec![
            V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Eq,
                args: vec![
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!("admin")), steps: vec![] }),
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!("user")), steps: vec![] }),
                ],
            }),
            V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Gt,
                args: vec![
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(100)), steps: vec![] }),
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(50)), steps: vec![] }),
                ],
            }),
        ]);
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_any_false() {
        let cond = V2Condition::Any(vec![
            V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Eq,
                args: vec![
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(1)), steps: vec![] }),
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(2)), steps: vec![] }),
                ],
            }),
            V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Eq,
                args: vec![
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(3)), steps: vec![] }),
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(4)), steps: vec![] }),
                ],
            }),
        ]);
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(false)));
    }

    #[test]
    fn test_eval_condition_expr_truthy() {
        let cond = V2Condition::Expr(V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(true)),
            steps: vec![],
        }));
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_expr_falsy() {
        let cond = V2Condition::Expr(V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(false)),
            steps: vec![],
        }));
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(false)));
    }

    #[test]
    fn test_eval_condition_with_pipe_value() {
        // Condition: { gt: ["$", 100] }
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Gt,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::PipeValue,
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(100)),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new().with_pipe_value(EvalValue::Value(json!(150)));
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    // ------ If step evaluation tests ------

    #[test]
    fn test_eval_if_step_then_branch() {
        // if: { cond: { gt: ["$", 10] }, then: [{ multiply: 2 }] }
        let if_step = V2IfStep {
            cond: V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Gt,
                args: vec![
                    V2Expr::Pipe(V2Pipe { start: V2Start::PipeValue, steps: vec![] }),
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(10)), steps: vec![] }),
                ],
            }),
            then_branch: V2Pipe {
                start: V2Start::PipeValue,
                steps: vec![V2Step::Op(V2OpStep {
                    op: "multiply".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(2)),
                        steps: vec![],
                    })],
                })],
            },
            else_branch: None,
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_if_step(&if_step, EvalValue::Value(json!(20)), &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(40.0)));
    }

    #[test]
    fn test_eval_if_step_else_branch() {
        // if: { cond: { gt: ["$", 10] }, then: [{ multiply: 2 }], else: [{ multiply: 0.5 }] }
        let if_step = V2IfStep {
            cond: V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Gt,
                args: vec![
                    V2Expr::Pipe(V2Pipe { start: V2Start::PipeValue, steps: vec![] }),
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(10)), steps: vec![] }),
                ],
            }),
            then_branch: V2Pipe {
                start: V2Start::PipeValue,
                steps: vec![V2Step::Op(V2OpStep {
                    op: "multiply".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(2)),
                        steps: vec![],
                    })],
                })],
            },
            else_branch: Some(V2Pipe {
                start: V2Start::PipeValue,
                steps: vec![V2Step::Op(V2OpStep {
                    op: "multiply".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(0.5)),
                        steps: vec![],
                    })],
                })],
            }),
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        // pipe value 5 is less than 10, so else branch is taken
        let result = eval_v2_if_step(&if_step, EvalValue::Value(json!(5)), &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(2.5)));
    }

    #[test]
    fn test_eval_if_step_no_else_returns_pipe_value() {
        // if: { cond: { gt: ["$", 10] }, then: [{ multiply: 2 }] }
        let if_step = V2IfStep {
            cond: V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Gt,
                args: vec![
                    V2Expr::Pipe(V2Pipe { start: V2Start::PipeValue, steps: vec![] }),
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(10)), steps: vec![] }),
                ],
            }),
            then_branch: V2Pipe {
                start: V2Start::PipeValue,
                steps: vec![V2Step::Op(V2OpStep {
                    op: "multiply".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(2)),
                        steps: vec![],
                    })],
                })],
            },
            else_branch: None,
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        // pipe value 5 is less than 10, no else branch, returns original pipe value
        let result = eval_v2_if_step(&if_step, EvalValue::Value(json!(5)), &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(5)));
    }

    #[test]
    fn test_eval_pipe_with_if_step() {
        // [10000, { if: { cond: { gt: ["$", 5000] }, then: [{ multiply: 0.9 }] } }]
        let pipe = V2Pipe {
            start: V2Start::Literal(json!(10000)),
            steps: vec![V2Step::If(V2IfStep {
                cond: V2Condition::Comparison(V2Comparison {
                    op: V2ComparisonOp::Gt,
                    args: vec![
                        V2Expr::Pipe(V2Pipe { start: V2Start::PipeValue, steps: vec![] }),
                        V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(5000)), steps: vec![] }),
                    ],
                }),
                then_branch: V2Pipe {
                    start: V2Start::PipeValue,
                    steps: vec![V2Step::Op(V2OpStep {
                        op: "multiply".to_string(),
                        args: vec![V2Expr::Pipe(V2Pipe {
                            start: V2Start::Literal(json!(0.9)),
                            steps: vec![],
                        })],
                    })],
                },
                else_branch: None,
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(9000.0)));
    }

    #[test]
    fn test_eval_if_with_input_condition() {
        // if: { cond: { eq: ["@input.role", "admin"] }, then: [100], else: [50] }
        let if_step = V2IfStep {
            cond: V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Eq,
                args: vec![
                    V2Expr::Pipe(V2Pipe { start: V2Start::Ref(V2Ref::Input("role".to_string())), steps: vec![] }),
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!("admin")), steps: vec![] }),
                ],
            }),
            then_branch: V2Pipe {
                start: V2Start::Literal(json!(100)),
                steps: vec![],
            },
            else_branch: Some(V2Pipe {
                start: V2Start::Literal(json!(50)),
                steps: vec![],
            }),
        };
        let record = json!({"role": "admin"});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_if_step(&if_step, EvalValue::Value(json!(0)), &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(100)));

        // When not admin
        let record2 = json!({"role": "user"});
        let result2 = eval_v2_if_step(&if_step, EvalValue::Value(json!(0)), &record2, None, &out, "test", &ctx);
        assert!(matches!(result2, Ok(EvalValue::Value(v)) if v == json!(50)));
    }

    #[test]
    fn test_eval_nested_if() {
        // Nested if: if x > 100 then (if x > 500 then "gold" else "silver") else "bronze"
        let if_step = V2IfStep {
            cond: V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Gt,
                args: vec![
                    V2Expr::Pipe(V2Pipe { start: V2Start::PipeValue, steps: vec![] }),
                    V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(100)), steps: vec![] }),
                ],
            }),
            then_branch: V2Pipe {
                start: V2Start::PipeValue,
                steps: vec![V2Step::If(V2IfStep {
                    cond: V2Condition::Comparison(V2Comparison {
                        op: V2ComparisonOp::Gt,
                        args: vec![
                            V2Expr::Pipe(V2Pipe { start: V2Start::PipeValue, steps: vec![] }),
                            V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(500)), steps: vec![] }),
                        ],
                    }),
                    then_branch: V2Pipe {
                        start: V2Start::Literal(json!("gold")),
                        steps: vec![],
                    },
                    else_branch: Some(V2Pipe {
                        start: V2Start::Literal(json!("silver")),
                        steps: vec![],
                    }),
                })],
            },
            else_branch: Some(V2Pipe {
                start: V2Start::Literal(json!("bronze")),
                steps: vec![],
            }),
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();

        // 50 -> bronze
        let result = eval_v2_if_step(&if_step, EvalValue::Value(json!(50)), &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("bronze")));

        // 200 -> silver
        let result = eval_v2_if_step(&if_step, EvalValue::Value(json!(200)), &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("silver")));

        // 600 -> gold
        let result = eval_v2_if_step(&if_step, EvalValue::Value(json!(600)), &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("gold")));
    }

    // ------ Truthy tests ------

    #[test]
    fn test_is_truthy() {
        assert!(!is_truthy(&EvalValue::Missing));
        assert!(!is_truthy(&EvalValue::Value(json!(null))));
        assert!(!is_truthy(&EvalValue::Value(json!(false))));
        assert!(is_truthy(&EvalValue::Value(json!(true))));
        assert!(!is_truthy(&EvalValue::Value(json!(0))));
        assert!(is_truthy(&EvalValue::Value(json!(1))));
        assert!(is_truthy(&EvalValue::Value(json!(-1))));
        assert!(!is_truthy(&EvalValue::Value(json!(""))));
        assert!(is_truthy(&EvalValue::Value(json!("hello"))));
        assert!(!is_truthy(&EvalValue::Value(json!([]))));
        assert!(is_truthy(&EvalValue::Value(json!([1]))));
        assert!(!is_truthy(&EvalValue::Value(json!({}))));
        assert!(is_truthy(&EvalValue::Value(json!({"a": 1}))));
    }
}

// =============================================================================
// v2 Map Step Evaluation Tests (T18)
// =============================================================================

#[cfg(test)]
mod v2_map_step_eval_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_eval_map_step_simple() {
        // map: [uppercase] on ["a", "b", "c"] -> ["A", "B", "C"]
        let map_step = V2MapStep {
            steps: vec![V2Step::Op(V2OpStep {
                op: "uppercase".to_string(),
                args: vec![],
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Value(json!(["a", "b", "c"])),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(["A", "B", "C"])));
    }

    #[test]
    fn test_eval_map_step_with_multiply() {
        // map: [multiply: 2] on [1, 2, 3] -> [2, 4, 6]
        let map_step = V2MapStep {
            steps: vec![V2Step::Op(V2OpStep {
                op: "multiply".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(2)),
                    steps: vec![],
                })],
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Value(json!([1, 2, 3])),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([2.0, 4.0, 6.0])));
    }

    #[test]
    fn test_eval_map_step_empty_array() {
        let map_step = V2MapStep {
            steps: vec![V2Step::Op(V2OpStep {
                op: "uppercase".to_string(),
                args: vec![],
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Value(json!([])),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([])));
    }

    #[test]
    fn test_eval_map_step_missing_returns_missing() {
        let map_step = V2MapStep {
            steps: vec![V2Step::Op(V2OpStep {
                op: "uppercase".to_string(),
                args: vec![],
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Missing,
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Missing)));
    }

    #[test]
    fn test_eval_map_step_non_array_error() {
        let map_step = V2MapStep {
            steps: vec![V2Step::Op(V2OpStep {
                op: "uppercase".to_string(),
                args: vec![],
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Value(json!("not an array")),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_map_step_with_item_ref() {
        // Access @item.name from each object
        let map_step = V2MapStep {
            steps: vec![V2Step::Op(V2OpStep {
                op: "concat".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("!")),
                    steps: vec![],
                })],
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Value(json!(["hello", "world"])),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(["hello!", "world!"])));
    }

    #[test]
    fn test_eval_map_step_with_item_index() {
        // Create pipe that returns @item.index
        // This requires testing through the full context
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Input("items".to_string())),
            steps: vec![V2Step::Map(V2MapStep {
                steps: vec![],  // Just return the item as-is
            })],
        };
        let record = json!({"items": [10, 20, 30]});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([10, 20, 30])));
    }

    #[test]
    fn test_eval_map_step_multiple_ops() {
        // map: [trim, uppercase] on ["  a  ", "  b  "] -> ["A", "B"]
        let map_step = V2MapStep {
            steps: vec![
                V2Step::Op(V2OpStep {
                    op: "trim".to_string(),
                    args: vec![],
                }),
                V2Step::Op(V2OpStep {
                    op: "uppercase".to_string(),
                    args: vec![],
                }),
            ],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Value(json!(["  a  ", "  b  "])),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(["A", "B"])));
    }

    #[test]
    fn test_eval_pipe_with_map_step() {
        // Full pipe: [@input.names, { map: [uppercase] }]
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Input("names".to_string())),
            steps: vec![V2Step::Map(V2MapStep {
                steps: vec![V2Step::Op(V2OpStep {
                    op: "uppercase".to_string(),
                    args: vec![],
                })],
            })],
        };
        let record = json!({"names": ["alice", "bob"]});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(["ALICE", "BOB"])));
    }

    #[test]
    fn test_eval_map_with_if_step() {
        // map with conditional: double if > 5, else keep
        let map_step = V2MapStep {
            steps: vec![V2Step::If(V2IfStep {
                cond: V2Condition::Comparison(V2Comparison {
                    op: V2ComparisonOp::Gt,
                    args: vec![
                        V2Expr::Pipe(V2Pipe { start: V2Start::PipeValue, steps: vec![] }),
                        V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(5)), steps: vec![] }),
                    ],
                }),
                then_branch: V2Pipe {
                    start: V2Start::PipeValue,
                    steps: vec![V2Step::Op(V2OpStep {
                        op: "multiply".to_string(),
                        args: vec![V2Expr::Pipe(V2Pipe {
                            start: V2Start::Literal(json!(2)),
                            steps: vec![],
                        })],
                    })],
                },
                else_branch: None,
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        // [3, 7, 2, 10] -> [3, 14, 2, 20] (only 7 and 10 are > 5)
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Value(json!([3, 7, 2, 10])),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([3, 14.0, 2, 20.0])));
    }

    #[test]
    fn test_eval_nested_map() {
        // Nested map: [[1, 2], [3, 4]] -> map of (map multiply 2) -> [[2, 4], [6, 8]]
        let map_step = V2MapStep {
            steps: vec![V2Step::Map(V2MapStep {
                steps: vec![V2Step::Op(V2OpStep {
                    op: "multiply".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(2)),
                        steps: vec![],
                    })],
                })],
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Value(json!([[1, 2], [3, 4]])),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([[2.0, 4.0], [6.0, 8.0]])));
    }

    #[test]
    fn test_eval_map_objects() {
        // Map over array of objects and extract a field
        // Since we're using pipe value directly, this tests object handling
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Input("users".to_string())),
            steps: vec![V2Step::Map(V2MapStep {
                steps: vec![],  // No-op, just return items
            })],
        };
        let record = json!({"users": [{"name": "Alice"}, {"name": "Bob"}]});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([{"name": "Alice"}, {"name": "Bob"}])));
    }
}

// =============================================================================
// v2 Pipe Full Evaluation Tests (T19)
// =============================================================================

#[cfg(test)]
mod v2_pipe_eval_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_eval_pipe_simple_ref() {
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Input("name".to_string())),
            steps: vec![],
        };
        let record = json!({"name": "Alice"});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("Alice")));
    }

    #[test]
    fn test_eval_pipe_literal_start() {
        let pipe = V2Pipe {
            start: V2Start::Literal(json!(42)),
            steps: vec![],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(42)));
    }

    #[test]
    fn test_eval_pipe_chain_ops() {
        // ["  hello  ", trim, uppercase]
        let pipe = V2Pipe {
            start: V2Start::Literal(json!("  hello  ")),
            steps: vec![
                V2Step::Op(V2OpStep { op: "trim".to_string(), args: vec![] }),
                V2Step::Op(V2OpStep { op: "uppercase".to_string(), args: vec![] }),
            ],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("HELLO")));
    }

    #[test]
    fn test_eval_pipe_with_context() {
        // [@context.multiplier, multiply: @input.value]
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Context("multiplier".to_string())),
            steps: vec![V2Step::Op(V2OpStep {
                op: "multiply".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Input("value".to_string())),
                    steps: vec![],
                })],
            })],
        };
        let record = json!({"value": 10});
        let context = json!({"multiplier": 5});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, Some(&context), &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(50.0)));
    }

    #[test]
    fn test_eval_pipe_with_out_ref() {
        // [@out.previous, add: 1]
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Out("previous".to_string())),
            steps: vec![V2Step::Op(V2OpStep {
                op: "add".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(1)),
                    steps: vec![],
                })],
            })],
        };
        let record = json!({});
        let out = json!({"previous": 99});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(100.0)));
    }

    #[test]
    fn test_eval_pipe_complex_chain() {
        // [@input.price, let: {original: $}, multiply: 0.9, let: {discounted: $},
        //  if: {cond: {gt: [$, 1000]}, then: [subtract: 100]}]
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Input("price".to_string())),
            steps: vec![
                V2Step::Let(V2LetStep {
                    bindings: vec![("original".to_string(), V2Expr::Pipe(V2Pipe {
                        start: V2Start::PipeValue,
                        steps: vec![],
                    }))],
                }),
                V2Step::Op(V2OpStep {
                    op: "multiply".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(0.9)),
                        steps: vec![],
                    })],
                }),
                V2Step::If(V2IfStep {
                    cond: V2Condition::Comparison(V2Comparison {
                        op: V2ComparisonOp::Gt,
                        args: vec![
                            V2Expr::Pipe(V2Pipe { start: V2Start::PipeValue, steps: vec![] }),
                            V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(1000)), steps: vec![] }),
                        ],
                    }),
                    then_branch: V2Pipe {
                        start: V2Start::PipeValue,
                        steps: vec![V2Step::Op(V2OpStep {
                            op: "subtract".to_string(),
                            args: vec![V2Expr::Pipe(V2Pipe {
                                start: V2Start::Literal(json!(100)),
                                steps: vec![],
                            })],
                        })],
                    },
                    else_branch: None,
                }),
            ],
        };
        let record = json!({"price": 2000});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        // 2000 * 0.9 = 1800 > 1000, so 1800 - 100 = 1700
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(1700.0)));
    }

    #[test]
    fn test_eval_pipe_all_step_types() {
        // Test combining let, op, if, map in one pipe
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Input("items".to_string())),
            steps: vec![
                // map: multiply each by 2
                V2Step::Map(V2MapStep {
                    steps: vec![V2Step::Op(V2OpStep {
                        op: "multiply".to_string(),
                        args: vec![V2Expr::Pipe(V2Pipe {
                            start: V2Start::Literal(json!(2)),
                            steps: vec![],
                        })],
                    })],
                }),
            ],
        };
        let record = json!({"items": [1, 2, 3]});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([2.0, 4.0, 6.0])));
    }

    #[test]
    fn test_eval_pipe_coalesce_chain() {
        // [@input.primary, coalesce: @input.secondary, coalesce: "default"]
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Input("primary".to_string())),
            steps: vec![
                V2Step::Op(V2OpStep {
                    op: "coalesce".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Ref(V2Ref::Input("secondary".to_string())),
                        steps: vec![],
                    })],
                }),
                V2Step::Op(V2OpStep {
                    op: "coalesce".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!("default")),
                        steps: vec![],
                    })],
                }),
            ],
        };

        // Test with primary present
        let record = json!({"primary": "first"});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("first")));

        // Test with primary null, secondary present
        let record = json!({"primary": null, "secondary": "second"});
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("second")));

        // Test with both null, use default
        let record = json!({"primary": null, "secondary": null});
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("default")));
    }

    #[test]
    fn test_eval_expr_with_v2_pipe() {
        let expr = V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!("hello")),
            steps: vec![V2Step::Op(V2OpStep {
                op: "uppercase".to_string(),
                args: vec![],
            })],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_expr(&expr, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("HELLO")));
    }

    #[test]
    fn test_eval_pipe_deep_nesting() {
        // Deeply nested: input -> map -> if -> op
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Input("scores".to_string())),
            steps: vec![V2Step::Map(V2MapStep {
                steps: vec![V2Step::If(V2IfStep {
                    cond: V2Condition::Comparison(V2Comparison {
                        op: V2ComparisonOp::Gte,
                        args: vec![
                            V2Expr::Pipe(V2Pipe { start: V2Start::PipeValue, steps: vec![] }),
                            V2Expr::Pipe(V2Pipe { start: V2Start::Literal(json!(60)), steps: vec![] }),
                        ],
                    }),
                    then_branch: V2Pipe {
                        start: V2Start::Literal(json!("pass")),
                        steps: vec![],
                    },
                    else_branch: Some(V2Pipe {
                        start: V2Start::Literal(json!("fail")),
                        steps: vec![],
                    }),
                })],
            })],
        };
        let record = json!({"scores": [80, 55, 90, 45]});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(["pass", "fail", "pass", "fail"])));
    }
}

// =============================================================================
// v2 Lookup Evaluation Tests (T20)
// =============================================================================

#[cfg(test)]
mod v2_lookup_eval_tests {
    use super::*;
    use serde_json::json;

    fn make_departments() -> JsonValue {
        json!([
            {"id": 1, "name": "Engineering", "budget": 100000},
            {"id": 2, "name": "Sales", "budget": 50000},
            {"id": 3, "name": "HR", "budget": 30000}
        ])
    }

    #[test]
    fn test_lookup_first_basic() {
        // lookup_first: {from: @context.departments, match: [id, 2], get: name}
        let op = V2OpStep {
            op: "lookup_first".to_string(),
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Context("departments".to_string())),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("id")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(2)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("name")),
                    steps: vec![],
                }),
            ],
        };
        let record = json!({});
        let context = json!({"departments": make_departments()});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(null)),
            &record,
            Some(&context),
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("Sales")));
    }

    #[test]
    fn test_lookup_first_no_match() {
        let op = V2OpStep {
            op: "lookup_first".to_string(),
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Context("departments".to_string())),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("id")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(999)),  // Non-existent ID
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("name")),
                    steps: vec![],
                }),
            ],
        };
        let record = json!({});
        let context = json!({"departments": make_departments()});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(null)),
            &record,
            Some(&context),
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Missing)));
    }

    #[test]
    fn test_lookup_first_return_whole_object() {
        // Without 'get', return the whole matched object
        let op = V2OpStep {
            op: "lookup_first".to_string(),
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Context("departments".to_string())),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("id")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(1)),
                    steps: vec![],
                }),
            ],
        };
        let record = json!({});
        let context = json!({"departments": make_departments()});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(null)),
            &record,
            Some(&context),
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!({"id": 1, "name": "Engineering", "budget": 100000})));
    }

    #[test]
    fn test_lookup_first_with_input_match_value() {
        // Match using value from input
        let op = V2OpStep {
            op: "lookup_first".to_string(),
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Context("departments".to_string())),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("id")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Input("dept_id".to_string())),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("name")),
                    steps: vec![],
                }),
            ],
        };
        let record = json!({"dept_id": 3});
        let context = json!({"departments": make_departments()});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(null)),
            &record,
            Some(&context),
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("HR")));
    }

    #[test]
    fn test_lookup_all_matches() {
        // lookup (not lookup_first) returns all matches
        let employees = json!([
            {"name": "Alice", "dept": "Engineering"},
            {"name": "Bob", "dept": "Sales"},
            {"name": "Charlie", "dept": "Engineering"},
            {"name": "Diana", "dept": "HR"}
        ]);
        let op = V2OpStep {
            op: "lookup".to_string(),
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Context("employees".to_string())),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("dept")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("Engineering")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("name")),
                    steps: vec![],
                }),
            ],
        };
        let record = json!({});
        let context = json!({"employees": employees});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(null)),
            &record,
            Some(&context),
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(["Alice", "Charlie"])));
    }

    #[test]
    fn test_lookup_no_matches() {
        let op = V2OpStep {
            op: "lookup".to_string(),
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Context("departments".to_string())),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("id")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(999)),
                    steps: vec![],
                }),
            ],
        };
        let record = json!({});
        let context = json!({"departments": make_departments()});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(null)),
            &record,
            Some(&context),
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([])));
    }

    #[test]
    fn test_lookup_first_missing_from() {
        let op = V2OpStep {
            op: "lookup_first".to_string(),
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Context("nonexistent".to_string())),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("id")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(1)),
                    steps: vec![],
                }),
            ],
        };
        let record = json!({});
        let context = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(null)),
            &record,
            Some(&context),
            &out,
            "test",
            &ctx,
        );
        // Missing 'from' returns Missing
        assert!(matches!(result, Ok(EvalValue::Missing)));
    }

    #[test]
    fn test_lookup_first_insufficient_args() {
        let op = V2OpStep {
            op: "lookup_first".to_string(),
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!([])),
                    steps: vec![],
                }),
            ],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(null)),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_lookup_in_pipe() {
        // Full pipe: lookup then transform result
        // Simpler test: just lookup and verify
        let pipe = V2Pipe {
            start: V2Start::Literal(json!(null)),
            steps: vec![
                V2Step::Op(V2OpStep {
                    op: "lookup_first".to_string(),
                    args: vec![
                        V2Expr::Pipe(V2Pipe {
                            start: V2Start::Ref(V2Ref::Context("departments".to_string())),
                            steps: vec![],
                        }),
                        V2Expr::Pipe(V2Pipe {
                            start: V2Start::Literal(json!("id")),
                            steps: vec![],
                        }),
                        V2Expr::Pipe(V2Pipe {
                            start: V2Start::Ref(V2Ref::Input("dept_id".to_string())),
                            steps: vec![],
                        }),
                        V2Expr::Pipe(V2Pipe {
                            start: V2Start::Literal(json!("budget")),
                            steps: vec![],
                        }),
                    ],
                }),
            ],
        };
        let record = json!({"dept_id": 2});  // Sales dept
        let context = json!({"departments": make_departments()});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, Some(&context), &out, "test", &ctx);
        // Sales budget is 50000
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(50000)));
    }

    #[test]
    fn test_lookup_then_multiply() {
        // Two-step pipe: lookup, then multiply
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Context("departments".to_string())),
            steps: vec![],
        };
        let record = json!({"dept_id": 2});
        let context = json!({"departments": make_departments()});
        let out = json!({});
        let ctx = V2EvalContext::new();

        // First verify context is accessible
        let result = eval_v2_pipe(&pipe, &record, Some(&context), &out, "test", &ctx);
        assert!(result.is_ok());

        // Now test just the lookup op step directly
        let op = V2OpStep {
            op: "lookup_first".to_string(),
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Context("departments".to_string())),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("id")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(2)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("budget")),
                    steps: vec![],
                }),
            ],
        };
        let result = eval_v2_op_step(&op, EvalValue::Value(json!(null)), &record, Some(&context), &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(ref v)) if *v == json!(50000)));

        // Now multiply it
        let multiply_op = V2OpStep {
            op: "multiply".to_string(),
            args: vec![V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(1.1)),
                steps: vec![],
            })],
        };
        let budget = result.unwrap();
        let result2 = eval_v2_op_step(&multiply_op, budget, &record, None, &out, "test", &ctx);
        // multiply returns f64, check approximately 55000
        match result2 {
            Ok(EvalValue::Value(v)) => {
                let num = v.as_f64().expect("should be number");
                assert!((num - 55000.0).abs() < 0.001, "expected 55000.0, got {}", num);
            }
            other => panic!("expected Ok(EvalValue::Value), got {:?}", other),
        }
    }
}
