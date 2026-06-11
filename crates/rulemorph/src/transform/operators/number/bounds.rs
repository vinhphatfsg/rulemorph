use super::args::eval_number_arg_at;
use super::*;

pub(in crate::transform::operators) fn eval_clamp(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 3 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "clamp requires exactly three arguments",
        )
        .with_path(format!("{}.args", base_path)));
    }
    let value =
        match eval_number_arg_at(0, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let min = match eval_number_arg_at(1, args, injected, record, context, out, base_path, locals)?
    {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let max = match eval_number_arg_at(2, args, injected, record, context, out, base_path, locals)?
    {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    if min.0 > max.0 {
        return Err(expr_type_error(
            "clamp min must be less than or equal to max",
            &min.1,
        ));
    }
    Ok(EvalValue::Value(json_number_from_f64(
        value.0.clamp(min.0, max.0),
        base_path,
    )?))
}
