use super::super::*;

mod chunk;
mod slice;
mod take_drop;

pub(in crate::transform::operators) use self::chunk::eval_array_chunk;
pub(in crate::transform::operators) use self::slice::eval_array_slice;
pub(in crate::transform::operators) use self::take_drop::{eval_array_drop, eval_array_take};
