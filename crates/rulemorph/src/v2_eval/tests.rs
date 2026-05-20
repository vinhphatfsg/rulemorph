use super::*;
use crate::error::TransformErrorKind;
use crate::v2_model::{
    V2Comparison, V2ComparisonOp, V2Condition, V2Expr, V2IfStep, V2LetStep, V2MapStep, V2OpStep,
    V2Pipe, V2Ref, V2Start, V2Step,
};

#[cfg(test)]
#[path = "tests/ref_eval.rs"]
mod v2_ref_eval_tests;

#[cfg(test)]
#[path = "tests/start_eval.rs"]
mod v2_start_eval_tests;

#[cfg(test)]
#[path = "tests/op_step.rs"]
mod v2_op_step_eval_tests;

#[cfg(test)]
#[path = "tests/let_step.rs"]
mod v2_let_step_eval_tests;

#[cfg(test)]
#[path = "tests/if_step.rs"]
mod v2_if_step_eval_tests;

#[cfg(test)]
#[path = "tests/map_step.rs"]
mod v2_map_step_eval_tests;

#[cfg(test)]
#[path = "tests/lookup.rs"]
mod v2_lookup_eval_tests;

#[cfg(test)]
#[path = "tests/pipe.rs"]
mod v2_pipe_eval_tests;
