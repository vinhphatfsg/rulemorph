use serde_json::Value as JsonValue;
use std::collections::HashMap;

use super::{EvalValue, V2EvalContext, eval_v2_expr};
use crate::error::TransformError;
use crate::model::{Expr, ExprOp, ExprRef};
use crate::transform::{
    EvalItem as V1EvalItem, EvalLocals as V1EvalLocals, EvalValue as V1EvalValue,
    eval_op as eval_v1_op,
};
use crate::v2_model::V2OpStep;

fn v2_eval_to_v1_eval(value: &EvalValue) -> V1EvalValue {
    match value {
        EvalValue::Missing => V1EvalValue::Missing,
        EvalValue::Value(v) => V1EvalValue::Value(v.clone()),
    }
}

fn v1_eval_to_v2_eval(value: V1EvalValue) -> EvalValue {
    match value {
        V1EvalValue::Missing => EvalValue::Missing,
        V1EvalValue::Value(v) => EvalValue::Value(v),
    }
}

fn map_v2_op_name(op: &str) -> &str {
    match op {
        "add" => "+",
        "subtract" => "-",
        "multiply" => "*",
        "divide" => "/",
        _ => op,
    }
}

pub(super) fn eval_v2_op_with_v1_fallback<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    let mut v1_locals_map: HashMap<String, V1EvalValue> = ctx
        .let_bindings()
        .map(|(k, v)| (k.clone(), v2_eval_to_v1_eval(v)))
        .collect();
    let mut arg_refs = Vec::with_capacity(op_step.args.len());
    for (index, arg) in op_step.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", path, index);
        let value = eval_v2_expr(arg, record, context, out, &arg_path, ctx)?;
        let mut key = format!("__v2_arg{}", index);
        if v1_locals_map.contains_key(&key) {
            let mut suffix = 1usize;
            while v1_locals_map.contains_key(&format!("{}{}", key, suffix)) {
                suffix += 1;
            }
            key = format!("{}{}", key, suffix);
        }
        v1_locals_map.insert(key.clone(), v2_eval_to_v1_eval(&value));
        arg_refs.push(Expr::Ref(ExprRef {
            ref_path: format!("local.{}", key),
        }));
    }

    let expr_op = ExprOp {
        op: map_v2_op_name(&op_step.op).to_string(),
        args: arg_refs,
    };

    let v1_pipe = v2_eval_to_v1_eval(&pipe_value);
    let v1_item = ctx.get_item().map(|item| V1EvalItem {
        value: item.value,
        index: item.index,
    });
    let v1_locals = V1EvalLocals {
        item: v1_item,
        acc: ctx.get_acc(),
        pipe: Some(&v1_pipe),
        locals: Some(&v1_locals_map),
        precomputed_op_args: None,
    };

    let result = eval_v1_op(
        &expr_op,
        record,
        context,
        out,
        path,
        Some(&v1_pipe),
        Some(&v1_locals),
    )?;

    Ok(v1_eval_to_v2_eval(result))
}
