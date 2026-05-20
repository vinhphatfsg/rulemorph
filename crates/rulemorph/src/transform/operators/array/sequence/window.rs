use super::*;

mod slicing;

pub(in crate::transform::operators) use self::slicing::{
    eval_array_chunk, eval_array_drop, eval_array_slice, eval_array_take,
};

fn flatten_value(value: &JsonValue, depth: usize, out: &mut Vec<JsonValue>) {
    if depth == 0 {
        out.push(value.clone());
        return;
    }

    if let JsonValue::Array(items) = value {
        for item in items {
            flatten_value(item, depth - 1, out);
        }
    } else {
        out.push(value.clone());
    }
}

pub(in crate::transform::operators) fn eval_array_flatten(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(1..=2).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain one or two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let depth = if total_len == 2 {
        let depth_path = format!("{}.args[1]", base_path);
        let depth_value =
            match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if depth_value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(depth_path));
        }
        let depth = value_to_i64(
            &depth_value,
            &depth_path,
            "depth must be a non-negative integer",
        )?;
        if depth < 0 {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "depth must be a non-negative integer",
            )
            .with_path(depth_path));
        }
        usize::try_from(depth).map_err(|_| {
            TransformError::new(TransformErrorKind::ExprError, "depth is too large")
                .with_path(depth_path)
        })?
    } else {
        1
    };

    let mut results = Vec::new();
    for item in &array {
        flatten_value(item, depth, &mut results);
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}
