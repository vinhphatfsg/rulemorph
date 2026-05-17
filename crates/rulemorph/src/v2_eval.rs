//! v2 Evaluation Context and Functions for rulemorph v2.0
//!
//! This module provides the evaluation context and functions for v2 expressions,
//! including pipe value tracking, let bindings, and item/acc scopes.

#[cfg(test)]
use serde_json::Value as JsonValue;

#[cfg(test)]
use crate::error::TransformErrorKind;
#[cfg(test)]
use crate::v2_model::{V2Expr, V2OpStep, V2Pipe, V2Start};

mod cast;
mod collection;
mod comparison;
mod condition;
mod context;
mod control;
mod lookup;
mod operators;
mod reference;
#[cfg(test)]
mod tests;
mod v1_bridge;
mod value;

use cast::eval_type_cast;
use collection::eval_collection_op;
use comparison::eval_comparison_op;
pub use condition::eval_v2_condition;
pub use context::{EvalItem, EvalValue, V2EvalContext};
pub use control::{
    eval_v2_expr, eval_v2_if_step, eval_v2_let_step, eval_v2_map_step, eval_v2_pipe,
};
use lookup::eval_lookup_op;
pub use operators::eval_v2_op_step;
pub use reference::{eval_v2_ref, eval_v2_start};
use v1_bridge::eval_v2_op_with_v1_fallback;
use value::{
    eval_v2_expr_or_null, eval_value_as_number, eval_value_as_string, value_as_bool,
    value_to_string,
};
