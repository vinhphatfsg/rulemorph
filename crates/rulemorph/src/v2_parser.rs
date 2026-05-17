//! v2 Expression Parser for rulemorph v2.0
//!
//! This module parses v2 expression syntax including:
//! - `@input.*`, `@context.*`, `@out.*` namespace references
//! - `@item.*`, `@acc.*` iteration references
//! - `@localVar` local variable references
//! - `$` pipe value
//! - `lit:` escape prefix for literals
//! - Pipe arrays: `[start_value, step1, step2, ...]`

mod condition;
mod error;
mod pipe;
mod ref_parse;
mod step;
#[cfg(test)]
mod tests;

pub use condition::parse_v2_condition;
pub use error::V2ParseError;
use pipe::parse_v2_expr_args;
pub use pipe::{
    is_v2_expr, parse_v2_expr, parse_v2_pipe, parse_v2_pipe_from_value, parse_v2_start,
};
pub use ref_parse::{extract_literal, is_literal_escape, is_pipe_value, is_v2_ref, parse_v2_ref};
pub use step::parse_v2_step;
