use serde_json::Value as JsonValue;
use std::collections::HashMap;

use crate::transform::EvalLimits;

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
    /// Trace-mode precomputed operator args, keyed by the operator rule path.
    precomputed_op_args: Option<(String, Vec<EvalValue>)>,
    limits: EvalLimits,
}

impl<'a> V2EvalContext<'a> {
    /// Create a new empty context
    pub fn new() -> Self {
        Self {
            pipe_value: None,
            let_bindings: HashMap::new(),
            item: None,
            acc: None,
            precomputed_op_args: None,
            limits: EvalLimits::default(),
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

    pub(crate) fn with_precomputed_op_args(
        mut self,
        base_path: impl Into<String>,
        values: Vec<EvalValue>,
    ) -> Self {
        self.precomputed_op_args = Some((base_path.into(), values));
        self
    }

    pub(crate) fn with_limits(mut self, limits: EvalLimits) -> Self {
        self.limits = limits;
        self
    }

    pub(crate) fn limits(&self) -> EvalLimits {
        self.limits
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

    pub(super) fn precomputed_arg_for_path(&self, path: &str) -> Option<EvalValue> {
        let (base_path, values) = self.precomputed_op_args.as_ref()?;
        let suffix = path.strip_prefix(base_path)?;
        let index_text = suffix.strip_prefix(".args[")?.strip_suffix(']')?;
        if index_text.contains(|ch: char| !ch.is_ascii_digit()) {
            return None;
        }
        let index = index_text.parse::<usize>().ok()?;
        values.get(index).cloned()
    }

    pub(super) fn let_bindings(&self) -> impl Iterator<Item = (&String, &EvalValue)> {
        self.let_bindings.iter()
    }
}

impl<'a> Default for V2EvalContext<'a> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    include!("context/tests.rs");
}
