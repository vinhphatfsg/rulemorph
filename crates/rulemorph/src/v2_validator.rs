//! v2 Expression Static Validator
//!
//! This module provides compile-time validation for v2 expressions,
//! catching errors that previously only occurred at runtime.

mod conditions;
mod context;
mod dependencies;
mod operators;
mod references;
mod steps;
#[cfg(test)]
mod tests;
mod types;

pub use self::conditions::validate_v2_condition;
pub use self::context::{V2Scope, V2ValidationCtx};
pub use self::dependencies::{collect_out_references, validate_no_cyclic_dependencies};
pub(crate) use self::operators::is_valid_op;
pub use self::references::validate_v2_ref;
pub use self::steps::{validate_v2_expr, validate_v2_pipe};
pub use self::types::{V2Type, infer_v2_expr_type};
