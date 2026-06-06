//! v2 Expression Types for rulemorph v2.0
//!
//! This module defines the data structures for the v2 expression syntax,
//! which uses `@input.*`, `@context.*`, `@out.*` references and pipe-based
//! transformations.

use crate::model::Expr;
use serde_json::Value as JsonValue;

// =============================================================================
// v2 Expression Types
// =============================================================================

/// v2 expression - either a Pipe (new v2 syntax) or V1Fallback (legacy syntax)
#[derive(Debug, Clone, PartialEq)]
pub enum V2Expr {
    Pipe(V2Pipe),
    V1Fallback(Expr),
}

/// v2 Pipe - a start value followed by transformation steps
#[derive(Debug, Clone, PartialEq)]
pub struct V2Pipe {
    pub start: V2Start,
    pub steps: Vec<V2Step>,
}

/// v2 Start - the starting value of a pipe
#[derive(Debug, Clone, PartialEq)]
pub enum V2Start {
    Ref(V2Ref),
    PipeValue,
    ImplicitPipeValue,
    Literal(JsonValue),
    V1Expr(Box<Expr>),
}

/// v2 Reference - namespace-qualified references with @ prefix
/// Examples: @input.name, @context.users[0].id, @out.user_id, @item.value, @acc.total, @myVar
#[derive(Debug, Clone, PartialEq)]
pub enum V2Ref {
    Input(String),   // @input.path
    Context(String), // @context.path
    Out(String),     // @out.path
    Pipe(String),    // $.path
    Item(String),    // @item.path (in map)
    Acc(String),     // @acc.path (in reduce)
    Local(String),   // @varName (let-bound)
}

/// v2 Step - a transformation step in a pipe
#[derive(Debug, Clone, PartialEq)]
pub enum V2Step {
    Op(V2OpStep),
    Object(V2ObjectStep),
    CustomCall(V2CustomCallStep),
    Let(V2LetStep),
    If(V2IfStep),
    Map(V2MapStep),
    /// Reference step - returns a reference value (e.g., "@doubled" to return a let-bound variable)
    Ref(V2Ref),
}

/// v2 Op Step - a named operation with arguments
#[derive(Debug, Clone, PartialEq)]
pub struct V2OpStep {
    pub op: String,
    pub args: Vec<V2Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct V2ObjectStep {
    pub fields: Vec<V2ObjectField>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct V2ObjectField {
    pub key: String,
    pub value: V2ObjectFieldValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum V2ObjectFieldValue {
    Expr(V2Expr),
    Value(JsonValue),
}

#[derive(Debug, Clone, PartialEq)]
pub struct V2CustomCallStep {
    pub op: String,
    pub with: Option<Vec<(String, V2CallArg)>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum V2CallArg {
    Expr(V2Expr),
    Value(JsonValue),
}

/// v2 Let Step - variable bindings
#[derive(Debug, Clone, PartialEq)]
pub struct V2LetStep {
    pub bindings: Vec<(String, V2Expr)>,
}

/// v2 If Step - conditional branching
#[derive(Debug, Clone, PartialEq)]
pub struct V2IfStep {
    pub cond: V2Condition,
    pub then_branch: V2Pipe,
    pub else_branch: Option<V2Pipe>,
}

/// v2 Map Step - array iteration
#[derive(Debug, Clone, PartialEq)]
pub struct V2MapStep {
    pub steps: Vec<V2Step>,
}

/// v2 Condition - logical conditions for if/when
#[derive(Debug, Clone, PartialEq)]
pub enum V2Condition {
    All(Vec<V2Condition>),
    Any(Vec<V2Condition>),
    Comparison(V2Comparison),
    Expr(V2Expr),
}

/// v2 Comparison - comparison operations
#[derive(Debug, Clone, PartialEq)]
pub struct V2Comparison {
    pub op: V2ComparisonOp,
    pub args: Vec<V2Expr>,
}

/// v2 Comparison Operator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V2ComparisonOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Match,
}

pub(crate) fn object_field_rule_path(base_path: &str, key: &str) -> String {
    if is_simple_object_field_key(key) {
        format!("{}.object.{}", base_path, key)
    } else {
        let quoted = serde_json::to_string(key).unwrap_or_else(|_| "\"<invalid>\"".to_string());
        format!("{}.object[{}]", base_path, quoted)
    }
}

fn is_simple_object_field_key(key: &str) -> bool {
    let mut chars = key.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

// =============================================================================
// v2 Model Tests
// =============================================================================

#[cfg(test)]
#[path = "v2_model/tests.rs"]
mod v2_model_tests;
