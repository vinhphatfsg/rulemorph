use super::*;
use serde_json::{Value as JsonValue, json};

fn lit(value: JsonValue) -> V2Expr {
    V2Expr::Pipe(V2Pipe {
        start: V2Start::Literal(value),
        steps: vec![],
    })
}

include!("op_step/string.rs");
include!("op_step/number.rs");
include!("op_step/json.rs");
include!("op_step/array.rs");
include!("op_step/cast.rs");
include!("op_step/logical.rs");
include!("op_step/comparison.rs");
include!("op_step/misc.rs");
