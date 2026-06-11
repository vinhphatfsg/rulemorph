use super::*;

mod aggregate;
mod equality;
mod scoped;
mod sequence;

pub(super) use self::aggregate::{eval_array_avg, eval_array_max, eval_array_min, eval_array_sum};
pub(super) use self::equality::{eval_array_contains, eval_array_index_of, eval_array_unique};
pub(super) use self::scoped::predicate::{
    eval_array_filter, eval_array_find, eval_array_find_index, eval_array_partition,
};
pub(super) use self::scoped::{
    eval_array_distinct_by, eval_array_flat_map, eval_array_fold, eval_array_group_by,
    eval_array_key_by, eval_array_map, eval_array_reduce, eval_array_sort_by,
};
pub(super) use self::sequence::{
    eval_array_chunk, eval_array_drop, eval_array_flatten, eval_array_slice, eval_array_take,
    eval_array_unzip, eval_array_zip, eval_array_zip_with,
};
#[expect(
    clippy::too_many_arguments,
    reason = "operator eval helpers keep the shared v1 expression call shape until a wider evaluator context rewrite"
)]
fn eval_array_arg(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<Vec<JsonValue>, TransformError> {
    let arg_path = format!("{}.args[{}]", base_path, index);
    match eval_expr_at_index(
        index, args, injected, record, context, out, base_path, locals,
    )? {
        EvalValue::Missing => Ok(Vec::new()),
        EvalValue::Value(value) => {
            if value.is_null() {
                Ok(Vec::new())
            } else if let JsonValue::Array(items) = value {
                Ok(items)
            } else {
                Err(
                    TransformError::new(TransformErrorKind::ExprError, "expr arg must be an array")
                        .with_path(arg_path),
                )
            }
        }
    }
}

fn eval_expr_or_null(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<JsonValue, TransformError> {
    match eval_expr(expr, record, context, out, base_path, locals)? {
        EvalValue::Missing => Ok(JsonValue::Null),
        EvalValue::Value(value) => Ok(value),
    }
}

fn eval_predicate_expr(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<bool, TransformError> {
    match eval_expr(expr, record, context, out, base_path, locals)? {
        EvalValue::Missing => Ok(false),
        EvalValue::Value(value) => {
            if value.is_null() {
                return Ok(false);
            }
            let flag = value_as_bool(&value, base_path)?;
            Ok(flag)
        }
    }
}

fn eval_key_expr_string(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<String, TransformError> {
    let value = match eval_expr(expr, record, context, out, base_path, locals)? {
        EvalValue::Missing => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be missing",
            )
            .with_path(base_path));
        }
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(base_path));
    }
    value_to_string(&value, base_path)
}

fn ensure_eq_compatible(value: &JsonValue, path: &str) -> Result<(), TransformError> {
    if value.is_null() {
        return Ok(());
    }
    if value_to_string_optional(value).is_some() {
        return Ok(());
    }
    Err(expr_type_error(
        "value must be string/number/bool or null",
        path,
    ))
}

fn eval_sort_key(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<SortKey, TransformError> {
    let value = match eval_expr(expr, record, context, out, base_path, locals)? {
        EvalValue::Missing => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be missing",
            )
            .with_path(base_path));
        }
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(base_path));
    }

    match value {
        JsonValue::Number(number) => {
            let value = number
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or_else(|| expr_type_error("sort_by key must be a finite number", base_path))?;
            Ok(SortKey::Number(value))
        }
        JsonValue::String(value) => Ok(SortKey::String(value)),
        JsonValue::Bool(value) => Ok(SortKey::Bool(value)),
        _ => Err(expr_type_error(
            "sort_by key must be string/number/bool",
            base_path,
        )),
    }
}
