use super::*;

mod window;
mod zip;

pub(in crate::transform::operators) use window::{
    eval_array_chunk, eval_array_drop, eval_array_flatten, eval_array_slice, eval_array_take,
};
pub(in crate::transform::operators) use zip::{
    eval_array_unzip, eval_array_zip, eval_array_zip_with,
};
