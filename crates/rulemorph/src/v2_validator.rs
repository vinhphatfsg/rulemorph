//! v2 Expression Static Validator
//!
//! This module provides compile-time validation for v2 expressions,
//! catching errors that previously only occurred at runtime.

use std::collections::{HashMap, HashSet};

use serde_json::Value as JsonValue;

use crate::error::{ErrorCode, RuleError};
use crate::locator::YamlLocator;
use crate::path::{parse_path, PathToken};
use crate::v2_model::{
    V2Comparison, V2Condition, V2Expr, V2IfStep, V2LetStep, V2MapStep, V2OpStep, V2Pipe, V2Ref,
    V2Start, V2Step,
};

// =============================================================================
// Type System
// =============================================================================

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

// =============================================================================
// Scope Management (Lexical Scoping)
// =============================================================================

/// Scope tracking for lexical scoping of let bindings
#[derive(Debug, Clone)]
pub struct V2Scope {
    /// Let-bound variable names in current scope
    let_bindings: HashSet<String>,
    /// Whether @item/@item.index is available
    item_available: bool,
    /// Whether @acc is available
    acc_available: bool,
    /// Parent scope (for lexical scoping)
    parent: Option<Box<V2Scope>>,
}

impl V2Scope {
    /// Create a new empty scope
    pub fn new() -> Self {
        Self {
            let_bindings: HashSet::new(),
            item_available: false,
            acc_available: false,
            parent: None,
        }
    }

    /// Create a new child scope inheriting from parent
    pub fn with_parent(parent: &V2Scope) -> Self {
        Self {
            let_bindings: HashSet::new(),
            item_available: parent.item_available,
            acc_available: parent.acc_available,
            parent: Some(Box::new(parent.clone())),
        }
    }

    /// Enable @item references in this scope
    pub fn with_item(mut self) -> Self {
        self.item_available = true;
        self
    }

    /// Enable @acc references in this scope
    pub fn with_acc(mut self) -> Self {
        self.acc_available = true;
        self
    }

    /// Add a let binding to the current scope
    pub fn add_binding(&mut self, name: String) {
        self.let_bindings.insert(name);
    }

    /// Check if a let binding exists in scope chain
    pub fn has_binding(&self, name: &str) -> bool {
        if self.let_bindings.contains(name) {
            return true;
        }
        if let Some(ref parent) = self.parent {
            return parent.has_binding(name);
        }
        false
    }

    /// Check if @item is available
    pub fn allows_item(&self) -> bool {
        self.item_available
    }

    /// Check if @acc is available
    pub fn allows_acc(&self) -> bool {
        self.acc_available
    }
}

impl Default for V2Scope {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Validation Context
// =============================================================================

/// Context for v2 validation
pub struct V2ValidationCtx<'a> {
    /// YAML source locator for error positions
    locator: Option<&'a YamlLocator>,
    /// Accumulated errors
    errors: Vec<RuleError>,
    /// Previously computed output targets (for @out forward reference check)
    produced_targets: HashSet<Vec<PathToken>>,
    /// Whether @out forward references are allowed
    allow_any_out_ref: bool,
    /// Whether @context was referenced (for informational purposes)
    pub context_referenced: bool,
}

impl<'a> V2ValidationCtx<'a> {
    /// Create a new validation context
    pub fn new(locator: Option<&'a YamlLocator>) -> Self {
        Self {
            locator,
            errors: Vec::new(),
            produced_targets: HashSet::new(),
            allow_any_out_ref: false,
            context_referenced: false,
        }
    }

    /// Create context with existing produced targets
    pub fn with_produced_targets(
        locator: Option<&'a YamlLocator>,
        produced_targets: HashSet<Vec<PathToken>>,
        allow_any_out_ref: bool,
    ) -> Self {
        Self {
            locator,
            errors: Vec::new(),
            produced_targets,
            allow_any_out_ref,
            context_referenced: false,
        }
    }

    /// Push an error with path
    pub fn push_error(&mut self, code: ErrorCode, message: impl Into<String>, path: &str) {
        let mut err = RuleError::new(code, message).with_path(path);
        if let Some(locator) = self.locator {
            if let Some(location) = locator.location_for(path) {
                err = err.with_location(location.line, location.column);
            }
        }
        self.errors.push(err);
    }

    /// Add a produced target
    pub fn add_produced_target(&mut self, tokens: Vec<PathToken>) {
        self.produced_targets.insert(tokens);
    }

    /// Get reference to produced targets
    pub fn produced_targets(&self) -> &HashSet<Vec<PathToken>> {
        &self.produced_targets
    }

    /// Consume context and return errors if any
    pub fn finish(self) -> Result<(), Vec<RuleError>> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(self.errors)
        }
    }

    /// Check if there are errors
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Get collected errors
    pub fn errors(&self) -> &[RuleError] {
        &self.errors
    }
}

// =============================================================================
// Type Inference
// =============================================================================

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

// =============================================================================
// Reference Validation
// =============================================================================

/// Validate a v2 reference
pub fn validate_v2_ref(
    v2_ref: &V2Ref,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    match v2_ref {
        V2Ref::Input(path) => {
            validate_path_syntax(path, base_path, ctx);
        }
        V2Ref::Context(path) => {
            validate_path_syntax(path, base_path, ctx);
            ctx.context_referenced = true;
        }
        V2Ref::Out(path) => {
            validate_path_syntax(path, base_path, ctx);
            validate_out_not_forward(path, base_path, ctx);
        }
        V2Ref::Item(path) => {
            if !scope.allows_item() {
                ctx.push_error(
                    ErrorCode::InvalidItemRef,
                    "@item is only valid inside map/filter operations",
                    base_path,
                );
            } else {
                validate_item_path(path, base_path, ctx);
            }
        }
        V2Ref::Acc(path) => {
            if !scope.allows_acc() {
                ctx.push_error(
                    ErrorCode::InvalidAccRef,
                    "@acc is only valid inside reduce/fold operations",
                    base_path,
                );
            } else if !path.is_empty() {
                validate_path_syntax(path, base_path, ctx);
            }
        }
        V2Ref::Local(name) => {
            if !scope.has_binding(name) {
                ctx.push_error(
                    ErrorCode::UndefinedVariable,
                    format!("undefined variable: @{}", name),
                    base_path,
                );
            }
        }
    }
}

/// Validate path syntax
fn validate_path_syntax(path: &str, base_path: &str, ctx: &mut V2ValidationCtx<'_>) {
    if path.is_empty() {
        return; // Empty path is valid (returns entire namespace)
    }
    if parse_path(path).is_err() {
        ctx.push_error(ErrorCode::InvalidPath, "invalid path syntax", base_path);
    }
}

/// Validate @item path (supports @item, @item.path, @item.index)
fn validate_item_path(path: &str, base_path: &str, ctx: &mut V2ValidationCtx<'_>) {
    if path.is_empty() {
        return; // @item with no path is valid
    }
    if path == "index" || path == "value" {
        return; // @item.index / @item.value are valid
    }
    // Direct field access on item value is also valid
    validate_path_syntax(path, base_path, ctx);
}

/// Validate @out reference is not a forward reference
fn validate_out_not_forward(path: &str, base_path: &str, ctx: &mut V2ValidationCtx<'_>) {
    if ctx.allow_any_out_ref {
        return;
    }
    if path.is_empty() {
        return;
    }

    let tokens = match parse_path(path) {
        Ok(t) => t,
        Err(_) => return, // Path syntax error handled elsewhere
    };

    // Extract key tokens (ignore array indices for forward reference check)
    let key_tokens: Vec<PathToken> = tokens
        .iter()
        .filter_map(|t| match t {
            PathToken::Key(k) => Some(PathToken::Key(k.clone())),
            PathToken::Index(_) => None,
        })
        .collect();

    if key_tokens.is_empty() {
        ctx.push_error(
            ErrorCode::ForwardOutReference,
            "out reference must have at least one key",
            base_path,
        );
        return;
    }

    // Check if any prefix of the path has been produced
    for end in (1..=key_tokens.len()).rev() {
        if ctx.produced_targets.contains(&key_tokens[..end].to_vec()) {
            return; // Found a matching prefix
        }
    }

    ctx.push_error(
        ErrorCode::ForwardOutReference,
        "out reference must point to previous mappings",
        base_path,
    );
}

// =============================================================================
// Step Validation
// =============================================================================

/// Validate a v2 expression
pub fn validate_v2_expr(
    expr: &V2Expr,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    match expr {
        V2Expr::Pipe(pipe) => validate_v2_pipe(pipe, base_path, scope, ctx),
        V2Expr::V1Fallback(_) => {
            // V1 expressions are validated by the existing v1 validator
        }
    }
}

/// Validate a v2 pipe
pub fn validate_v2_pipe(
    pipe: &V2Pipe,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    // Validate start value (at index 0 in the array)
    validate_v2_start(&pipe.start, &format!("{}[0]", base_path), scope, ctx);

    // Validate each step with proper scope management
    // Steps start at index 1 in the pipe array (after start value)
    let mut current_scope = scope.clone();
    for (i, step) in pipe.steps.iter().enumerate() {
        let step_path = format!("{}[{}]", base_path, i + 1);
        validate_v2_step(step, &step_path, &mut current_scope, ctx);
    }
}

/// Validate a v2 start value
fn validate_v2_start(
    start: &V2Start,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    match start {
        V2Start::Ref(v2_ref) => validate_v2_ref(v2_ref, base_path, scope, ctx),
        V2Start::PipeValue => {} // $ is always valid
        V2Start::Literal(_) => {} // Literals are always valid
        V2Start::V1Expr(_) => {} // V1 expressions are validated elsewhere
    }
}

/// Validate a v2 step
fn validate_v2_step(
    step: &V2Step,
    base_path: &str,
    scope: &mut V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    match step {
        V2Step::Op(op_step) => validate_v2_op_step(op_step, base_path, scope, ctx),
        V2Step::Let(let_step) => validate_v2_let_step(let_step, base_path, scope, ctx),
        V2Step::If(if_step) => validate_v2_if_step(if_step, base_path, scope, ctx),
        V2Step::Map(map_step) => validate_v2_map_step(map_step, base_path, scope, ctx),
        V2Step::Ref(v2_ref) => validate_v2_ref(v2_ref, base_path, scope, ctx),
    }
}

/// Validate a v2 op step
fn validate_v2_op_step(
    op_step: &V2OpStep,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    // Check if op is known
    if !is_valid_op(&op_step.op) {
        ctx.push_error(
            ErrorCode::UnknownOp,
            format!("unknown operation: {}", op_step.op),
            base_path,
        );
    }

    // Validate argument count
    validate_op_args_count(&op_step.op, op_step.args.len(), base_path, ctx);

    // Validate each argument expression
    for (i, arg) in op_step.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", base_path, i);
        let arg_scope = if op_step.op == "zip_with" && i == op_step.args.len().saturating_sub(1) {
            V2Scope::with_parent(scope).with_item()
        } else {
            get_arg_scope_for_op(&op_step.op, i, scope)
        };
        validate_v2_expr(arg, &arg_path, &arg_scope, ctx);
    }
}

/// Validate a v2 let step (adds bindings to scope)
fn validate_v2_let_step(
    let_step: &V2LetStep,
    base_path: &str,
    scope: &mut V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    for (name, expr) in &let_step.bindings {
        let binding_path = format!("{}.let.{}", base_path, name);

        // Validate the binding expression with current scope
        validate_v2_expr(expr, &binding_path, scope, ctx);

        // Add binding to scope for subsequent steps
        scope.add_binding(name.clone());
    }
}

/// Validate a v2 if step
fn validate_v2_if_step(
    if_step: &V2IfStep,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    // Validate condition
    let cond_path = format!("{}.if.cond", base_path);
    validate_v2_condition(&if_step.cond, &cond_path, scope, ctx);

    // Validate then branch (creates new child scope)
    let then_path = format!("{}.if.then", base_path);
    let then_scope = V2Scope::with_parent(scope);
    validate_v2_pipe(&if_step.then_branch, &then_path, &then_scope, ctx);

    // Validate else branch if present
    if let Some(ref else_branch) = if_step.else_branch {
        let else_path = format!("{}.if.else", base_path);
        let else_scope = V2Scope::with_parent(scope);
        validate_v2_pipe(else_branch, &else_path, &else_scope, ctx);
    }
}

/// Validate a v2 map step
fn validate_v2_map_step(
    map_step: &V2MapStep,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    // Create new scope with @item available
    let mut map_scope = V2Scope::with_parent(scope).with_item();

    for (i, step) in map_step.steps.iter().enumerate() {
        let step_path = format!("{}.map[{}]", base_path, i);
        validate_v2_step(step, &step_path, &mut map_scope, ctx);
    }
}

// =============================================================================
// Condition Validation
// =============================================================================

/// Validate a v2 condition
pub fn validate_v2_condition(
    cond: &V2Condition,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    match cond {
        V2Condition::All(conditions) => {
            for (i, c) in conditions.iter().enumerate() {
                let path = format!("{}.all[{}]", base_path, i);
                validate_v2_condition(c, &path, scope, ctx);
            }
        }
        V2Condition::Any(conditions) => {
            for (i, c) in conditions.iter().enumerate() {
                let path = format!("{}.any[{}]", base_path, i);
                validate_v2_condition(c, &path, scope, ctx);
            }
        }
        V2Condition::Comparison(comp) => {
            validate_v2_comparison(comp, base_path, scope, ctx);
        }
        V2Condition::Expr(expr) => {
            validate_v2_expr(expr, base_path, scope, ctx);
            // Type check: must be bool or unknown
            let typ = infer_v2_expr_type(expr);
            if typ.is_definitely_not_bool() {
                ctx.push_error(
                    ErrorCode::InvalidWhenType,
                    "condition must evaluate to boolean",
                    base_path,
                );
            }
        }
    }
}

/// Validate a v2 comparison
fn validate_v2_comparison(
    comp: &V2Comparison,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    // Comparisons need exactly 2 arguments
    if comp.args.len() != 2 {
        ctx.push_error(
            ErrorCode::InvalidArgs,
            format!(
                "comparison requires exactly 2 arguments, got {}",
                comp.args.len()
            ),
            base_path,
        );
    }

    // Validate each argument
    for (i, arg) in comp.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", base_path, i);
        validate_v2_expr(arg, &arg_path, scope, ctx);
    }
}

// =============================================================================
// Operation Validation
// =============================================================================

/// Check if an operation name is valid
pub(crate) fn is_valid_op(op: &str) -> bool {
    matches!(
        op,
        // String operations
        "concat"
            | "to_string"
            | "trim"
            | "lowercase"
            | "uppercase"
            | "replace"
            | "split"
            | "pad_start"
            | "pad_end"
            // Null handling
            | "coalesce"
            // Lookup
            | "lookup"
            | "lookup_first"
            // Arithmetic
            | "+"
            | "-"
            | "*"
            | "/"
            | "multiply"
            | "add"
            | "subtract"
            | "divide"
            | "round"
            | "to_base"
            // Date
            | "date_format"
            | "to_unixtime"
            // Logical
            | "and"
            | "or"
            | "not"
            // Comparison
            | "=="
            | "!="
            | "<"
            | "<="
            | ">"
            | ">="
            | "~="
            | "eq"
            | "ne"
            | "lt"
            | "lte"
            | "gt"
            | "gte"
            | "match"
            // JSON
            | "merge"
            | "deep_merge"
            | "get"
            | "pick"
            | "omit"
            | "keys"
            | "values"
            | "entries"
            | "len"
            | "from_entries"
            | "object_flatten"
            | "object_unflatten"
            // Array
            | "map"
            | "filter"
            | "flat_map"
            | "flatten"
            | "take"
            | "drop"
            | "slice"
            | "chunk"
            | "zip"
            | "zip_with"
            | "unzip"
            | "group_by"
            | "key_by"
            | "partition"
            | "unique"
            | "distinct_by"
            | "sort_by"
            | "find"
            | "find_index"
            | "index_of"
            | "contains"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "reduce"
            | "fold"
            | "first"
            | "last"
            // Type casts
            | "string"
            | "int"
            | "float"
            | "bool"
    )
}

/// Get the appropriate scope for an operation argument
fn get_arg_scope_for_op(op: &str, arg_index: usize, parent_scope: &V2Scope) -> V2Scope {
    match op {
        "map" | "filter" | "flat_map" | "group_by" | "key_by" | "partition" | "distinct_by"
        | "sort_by" | "find" | "find_index"
            if arg_index == 0 =>
        {
            V2Scope::with_parent(parent_scope).with_item()
        }
        "reduce" if arg_index == 0 => V2Scope::with_parent(parent_scope).with_item().with_acc(),
        "fold" if arg_index == 1 => V2Scope::with_parent(parent_scope).with_item().with_acc(),
        _ => parent_scope.clone(),
    }
}

/// Validate operation argument count
fn validate_op_args_count(op: &str, count: usize, base_path: &str, ctx: &mut V2ValidationCtx<'_>) {
    let (min, max) = get_op_arg_range(op);

    if count < min {
        ctx.push_error(
            ErrorCode::InvalidArgs,
            format!("{} requires at least {} argument(s), got {}", op, min, count),
            base_path,
        );
    } else if let Some(max_val) = max {
        if count > max_val {
            ctx.push_error(
                ErrorCode::InvalidArgs,
                format!(
                    "{} accepts at most {} argument(s), got {}",
                    op, max_val, count
                ),
                base_path,
            );
        }
    }
}

/// Get the valid argument count range for an operation
/// Returns (min, max) where max is None for unlimited
fn get_op_arg_range(op: &str) -> (usize, Option<usize>) {
    match op {
        // No arguments
        "trim" | "lowercase" | "uppercase" | "to_string" | "keys" | "values" | "entries"
        | "unique" | "unzip" | "first" | "last" | "len" | "sum" | "avg" | "min"
        | "max" | "not" | "string" | "int" | "float" | "bool" => (0, Some(0)),

        // Optional one argument
        "round" | "flatten" => (0, Some(1)),

        // Exactly 1 argument
        "take" | "drop" | "get" | "object_flatten" | "object_unflatten" | "chunk" | "map"
        | "filter" | "flat_map" | "group_by" | "key_by" | "distinct_by" | "find"
        | "find_index" | "index_of" | "contains" | "partition" | "split" | "reduce" | "to_base" => {
            (1, Some(1))
        }

        // One or two arguments
        "sort_by" => (1, Some(2)),

        // One or two arguments
        "pad_start" | "pad_end" | "slice" => (1, Some(2)),

        // Exactly 2 arguments
        "fold" => (2, Some(2)),

        // Two or three arguments
        "replace" => (2, Some(3)),

        // Date/Time
        "date_format" => (1, Some(3)),
        "to_unixtime" => (0, Some(2)),

        // Variable arguments (at least 1)
        "concat" | "coalesce" | "merge" | "deep_merge" | "and" | "or" | "pick" | "omit"
        | "from_entries" | "add" | "subtract" | "multiply" | "divide" | "zip" => (1, None),

        // Variable arguments (at least 2)
        "zip_with" => (2, None),

        // Comparison operators (exactly 1 argument for pipe context)
        "==" | "!=" | "<" | "<=" | ">" | ">=" | "~="
        | "eq" | "ne" | "lt" | "lte" | "gt" | "gte" | "match" => (1, Some(1)),

        // Arithmetic (at least 1 argument for pipe context)
        "+" | "-" | "*" | "/" => (1, None),

        // Lookup operations (2-4 arguments: match_key, match_value, get? or from, match_key, match_value, get?)
        "lookup" | "lookup_first" => (2, Some(4)),

        // Default for unknown ops
        _ => (0, None),
    }
}

// =============================================================================
// Cyclic Dependency Detection
// =============================================================================

/// Collect all @out references from a v2 expression
pub fn collect_out_references(expr: &V2Expr) -> HashSet<String> {
    let mut refs = HashSet::new();
    collect_out_refs_recursive(expr, &mut refs);
    refs
}

fn collect_out_refs_recursive(expr: &V2Expr, refs: &mut HashSet<String>) {
    match expr {
        V2Expr::Pipe(pipe) => {
            collect_out_refs_from_start(&pipe.start, refs);
            for step in &pipe.steps {
                collect_out_refs_from_step(step, refs);
            }
        }
        V2Expr::V1Fallback(_) => {}
    }
}

fn collect_out_refs_from_start(start: &V2Start, refs: &mut HashSet<String>) {
    match start {
        V2Start::Ref(V2Ref::Out(path)) => {
            if !path.is_empty() {
                refs.insert(path.clone());
            }
        }
        _ => {}
    }
}

fn collect_out_refs_from_step(step: &V2Step, refs: &mut HashSet<String>) {
    match step {
        V2Step::Op(op_step) => {
            for arg in &op_step.args {
                collect_out_refs_recursive(arg, refs);
            }
        }
        V2Step::Let(let_step) => {
            for (_, expr) in &let_step.bindings {
                collect_out_refs_recursive(expr, refs);
            }
        }
        V2Step::If(if_step) => {
            collect_out_refs_from_condition(&if_step.cond, refs);
            collect_out_refs_from_pipe(&if_step.then_branch, refs);
            if let Some(ref else_branch) = if_step.else_branch {
                collect_out_refs_from_pipe(else_branch, refs);
            }
        }
        V2Step::Map(map_step) => {
            for step in &map_step.steps {
                collect_out_refs_from_step(step, refs);
            }
        }
        V2Step::Ref(V2Ref::Out(path)) => {
            if !path.is_empty() {
                refs.insert(path.clone());
            }
        }
        V2Step::Ref(_) => {} // Non-out refs don't contribute to cyclic dependencies
    }
}

fn collect_out_refs_from_pipe(pipe: &V2Pipe, refs: &mut HashSet<String>) {
    collect_out_refs_from_start(&pipe.start, refs);
    for step in &pipe.steps {
        collect_out_refs_from_step(step, refs);
    }
}

fn collect_out_refs_from_condition(cond: &V2Condition, refs: &mut HashSet<String>) {
    match cond {
        V2Condition::All(conditions) | V2Condition::Any(conditions) => {
            for c in conditions {
                collect_out_refs_from_condition(c, refs);
            }
        }
        V2Condition::Comparison(comp) => {
            for arg in &comp.args {
                collect_out_refs_recursive(arg, refs);
            }
        }
        V2Condition::Expr(expr) => {
            collect_out_refs_recursive(expr, refs);
        }
    }
}

/// Check for cyclic dependencies among mappings
pub fn validate_no_cyclic_dependencies(
    targets_with_deps: &[(String, HashSet<String>)],
    base_path: &str,
    ctx: &mut V2ValidationCtx<'_>,
) {
    // Build adjacency list: target -> depends on targets
    let graph: HashMap<String, HashSet<String>> = targets_with_deps
        .iter()
        .cloned()
        .collect();

    // Detect cycles using DFS
    let mut visited: HashSet<String> = HashSet::new();
    let mut rec_stack: HashSet<String> = HashSet::new();

    for (target, _) in targets_with_deps {
        if has_cycle(target, &graph, &mut visited, &mut rec_stack) {
            ctx.push_error(
                ErrorCode::CyclicDependency,
                format!("cyclic dependency detected involving target: {}", target),
                &format!("{}.{}", base_path, target),
            );
        }
    }
}

fn has_cycle(
    node: &str,
    graph: &HashMap<String, HashSet<String>>,
    visited: &mut HashSet<String>,
    rec_stack: &mut HashSet<String>,
) -> bool {
    if rec_stack.contains(node) {
        return true;
    }
    if visited.contains(node) {
        return false;
    }

    visited.insert(node.to_string());
    rec_stack.insert(node.to_string());

    if let Some(deps) = graph.get(node) {
        for dep in deps {
            if has_cycle(dep, graph, visited, rec_stack) {
                return true;
            }
        }
    }

    rec_stack.remove(node);
    false
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Scope tests
    #[test]
    fn test_scope_new() {
        let scope = V2Scope::new();
        assert!(!scope.allows_item());
        assert!(!scope.allows_acc());
        assert!(!scope.has_binding("x"));
    }

    #[test]
    fn test_scope_with_item() {
        let scope = V2Scope::new().with_item();
        assert!(scope.allows_item());
        assert!(!scope.allows_acc());
    }

    #[test]
    fn test_scope_with_acc() {
        let scope = V2Scope::new().with_acc();
        assert!(!scope.allows_item());
        assert!(scope.allows_acc());
    }

    #[test]
    fn test_scope_let_binding() {
        let mut scope = V2Scope::new();
        assert!(!scope.has_binding("x"));
        scope.add_binding("x".to_string());
        assert!(scope.has_binding("x"));
        assert!(!scope.has_binding("y"));
    }

    #[test]
    fn test_scope_lexical_parent() {
        let mut parent = V2Scope::new();
        parent.add_binding("x".to_string());

        let child = V2Scope::with_parent(&parent);
        assert!(child.has_binding("x")); // Inherited from parent
        assert!(!child.has_binding("y"));
    }

    #[test]
    fn test_scope_child_binding_not_in_parent() {
        let parent = V2Scope::new();
        let mut child = V2Scope::with_parent(&parent);
        child.add_binding("y".to_string());

        assert!(child.has_binding("y"));
        assert!(!parent.has_binding("y")); // Child binding not in parent
    }

    // Type tests
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

    // Op validation tests
    #[test]
    fn test_is_valid_op() {
        assert!(is_valid_op("trim"));
        assert!(is_valid_op("concat"));
        assert!(is_valid_op("coalesce"));
        assert!(is_valid_op("lookup_first"));
        assert!(is_valid_op("add"));
        assert!(is_valid_op("subtract"));
        assert!(is_valid_op("multiply"));
        assert!(is_valid_op("divide"));
        assert!(is_valid_op("+"));
        assert!(is_valid_op("replace"));
        assert!(is_valid_op("split"));
        assert!(is_valid_op("pad_start"));
        assert!(is_valid_op("merge"));
        assert!(is_valid_op("map"));
        assert!(is_valid_op("filter"));
        assert!(is_valid_op("round"));
        assert!(is_valid_op("to_base"));
        assert!(is_valid_op("date_format"));
        assert!(is_valid_op("to_unixtime"));
        assert!(is_valid_op("string"));
        assert!(is_valid_op("gt"));
        assert!(is_valid_op("gte"));
        assert!(is_valid_op("lt"));
        assert!(is_valid_op("lte"));
        assert!(is_valid_op("eq"));
        assert!(is_valid_op("ne"));
        assert!(is_valid_op("match"));
        assert!(!is_valid_op("nonexistent_op"));
    }

    #[test]
    fn test_op_arg_range() {
        assert_eq!(get_op_arg_range("trim"), (0, Some(0)));
        assert_eq!(get_op_arg_range("multiply"), (1, None));
        assert_eq!(get_op_arg_range("subtract"), (1, None));
        assert_eq!(get_op_arg_range("divide"), (1, None));
        assert_eq!(get_op_arg_range("concat"), (1, None));
        assert_eq!(get_op_arg_range("lookup_first"), (2, Some(4)));
        assert_eq!(get_op_arg_range("split"), (1, Some(1)));
        assert_eq!(get_op_arg_range("pad_start"), (1, Some(2)));
        assert_eq!(get_op_arg_range("round"), (0, Some(1)));
        assert_eq!(get_op_arg_range("zip"), (1, None));
        assert_eq!(get_op_arg_range("gt"), (1, Some(1)));
        assert_eq!(get_op_arg_range("gte"), (1, Some(1)));
        assert_eq!(get_op_arg_range("lt"), (1, Some(1)));
        assert_eq!(get_op_arg_range("lte"), (1, Some(1)));
        assert_eq!(get_op_arg_range("eq"), (1, Some(1)));
        assert_eq!(get_op_arg_range("ne"), (1, Some(1)));
        assert_eq!(get_op_arg_range("match"), (1, Some(1)));
        assert_eq!(get_op_arg_range("zip_with"), (2, None));
        assert_eq!(get_op_arg_range("reduce"), (1, Some(1)));
        assert_eq!(get_op_arg_range("fold"), (2, Some(2)));
        assert_eq!(get_op_arg_range("to_unixtime"), (0, Some(2)));
    }

    #[test]
    fn test_validate_sort_by_order_arg_allowed() {
        let expr = V2Expr::Pipe(V2Pipe {
            start: V2Start::Ref(V2Ref::Input("items".to_string())),
            steps: vec![V2Step::Op(V2OpStep {
                op: "sort_by".to_string(),
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Ref(V2Ref::Item("value".to_string())),
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!("desc")),
                        steps: vec![],
                    }),
                ],
            })],
        });
        let scope = V2Scope::new();
        let mut ctx = V2ValidationCtx::new(None);

        validate_v2_expr(&expr, "test", &scope, &mut ctx);

        assert!(
            ctx.errors().is_empty(),
            "expected no errors, got: {:?}",
            ctx.errors()
        );
    }

    #[test]
    fn test_validate_zip_with_item_scope_allowed() {
        let expr = V2Expr::Pipe(V2Pipe {
            start: V2Start::Ref(V2Ref::Input("left".to_string())),
            steps: vec![V2Step::Op(V2OpStep {
                op: "zip_with".to_string(),
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Ref(V2Ref::Input("right".to_string())),
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Ref(V2Ref::Item(String::new())),
                        steps: vec![],
                    }),
                ],
            })],
        });
        let scope = V2Scope::new();
        let mut ctx = V2ValidationCtx::new(None);

        validate_v2_expr(&expr, "test", &scope, &mut ctx);

        assert!(
            ctx.errors().is_empty(),
            "expected no errors, got: {:?}",
            ctx.errors()
        );
    }

    #[test]
    fn test_validate_v2_expr_rejects_unimplemented_op() {
        let expr = V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!("hello")),
            steps: vec![V2Step::Op(V2OpStep {
                op: "nonexistent_op".to_string(),
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
            })],
        });
        let scope = V2Scope::new();
        let mut ctx = V2ValidationCtx::new(None);

        validate_v2_expr(&expr, "test", &scope, &mut ctx);

        assert!(ctx.errors().iter().any(|err| err.code == ErrorCode::UnknownOp));
    }

    // Reference validation tests
    #[test]
    fn test_validate_item_ref_outside_map() {
        let mut ctx = V2ValidationCtx::new(None);
        let scope = V2Scope::new(); // No @item scope
        let v2_ref = V2Ref::Item("value".to_string());

        validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);

        assert!(ctx.has_errors());
        assert_eq!(ctx.errors()[0].code, ErrorCode::InvalidItemRef);
    }

    #[test]
    fn test_validate_item_ref_inside_map() {
        let mut ctx = V2ValidationCtx::new(None);
        let scope = V2Scope::new().with_item();
        let v2_ref = V2Ref::Item("value".to_string());

        validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);

        assert!(!ctx.has_errors());
    }

    #[test]
    fn test_validate_item_index() {
        let mut ctx = V2ValidationCtx::new(None);
        let scope = V2Scope::new().with_item();
        let v2_ref = V2Ref::Item("index".to_string());

        validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);

        assert!(!ctx.has_errors());
    }

    #[test]
    fn test_validate_item_ref_invalid_subpath() {
        let scope = V2Scope::new().with_item();

        let mut ctx = V2ValidationCtx::new(None);
        let v2_ref = V2Ref::Item("value..foo".to_string());
        validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);
        assert!(ctx.errors().iter().any(|err| err.code == ErrorCode::InvalidPath));

        let mut ctx = V2ValidationCtx::new(None);
        let v2_ref = V2Ref::Item("index..foo".to_string());
        validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);
        assert!(ctx.errors().iter().any(|err| err.code == ErrorCode::InvalidPath));
    }

    #[test]
    fn test_validate_undefined_local() {
        let mut ctx = V2ValidationCtx::new(None);
        let scope = V2Scope::new();
        let v2_ref = V2Ref::Local("undefined_var".to_string());

        validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);

        assert!(ctx.has_errors());
        assert_eq!(ctx.errors()[0].code, ErrorCode::UndefinedVariable);
    }

    #[test]
    fn test_validate_defined_local() {
        let mut ctx = V2ValidationCtx::new(None);
        let mut scope = V2Scope::new();
        scope.add_binding("x".to_string());
        let v2_ref = V2Ref::Local("x".to_string());

        validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);

        assert!(!ctx.has_errors());
    }

    // Cyclic dependency tests
    #[test]
    fn test_no_cycle() {
        let mut ctx = V2ValidationCtx::new(None);
        let targets = vec![
            ("a".to_string(), HashSet::new()),
            ("b".to_string(), ["a".to_string()].into_iter().collect()),
            ("c".to_string(), ["b".to_string()].into_iter().collect()),
        ];

        validate_no_cyclic_dependencies(&targets, "mappings", &mut ctx);

        assert!(!ctx.has_errors());
    }

    #[test]
    fn test_self_reference_cycle() {
        let mut ctx = V2ValidationCtx::new(None);
        let targets = vec![(
            "a".to_string(),
            ["a".to_string()].into_iter().collect(), // Self-reference
        )];

        validate_no_cyclic_dependencies(&targets, "mappings", &mut ctx);

        assert!(ctx.has_errors());
        assert_eq!(ctx.errors()[0].code, ErrorCode::CyclicDependency);
    }

    #[test]
    fn test_indirect_cycle() {
        let mut ctx = V2ValidationCtx::new(None);
        let targets = vec![
            ("a".to_string(), ["b".to_string()].into_iter().collect()),
            ("b".to_string(), ["a".to_string()].into_iter().collect()), // a -> b -> a
        ];

        validate_no_cyclic_dependencies(&targets, "mappings", &mut ctx);

        assert!(ctx.has_errors());
        assert_eq!(ctx.errors()[0].code, ErrorCode::CyclicDependency);
    }
}
