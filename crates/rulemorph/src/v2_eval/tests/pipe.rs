use super::*;
use crate::v2_model::{
    V2Comparison, V2ComparisonOp, V2Condition, V2Expr, V2IfStep, V2LetStep, V2MapStep, V2OpStep,
    V2Pipe, V2Ref, V2Start, V2Step,
};
use serde_json::json;

include!("pipe/basic.rs");
include!("pipe/context.rs");
include!("pipe/composed.rs");
