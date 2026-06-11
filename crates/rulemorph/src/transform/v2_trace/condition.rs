use super::*;

pub(in crate::transform) fn eval_v2_condition_traced<'a>(
    condition: &V2Condition,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<bool, TransformError> {
    let _eval_scope = ctx.enter_eval_scope();
    match condition {
        V2Condition::All(conditions) => {
            for (index, cond) in conditions.iter().enumerate() {
                let cond_path = format!("{}[{}]", path, index);
                if !eval_v2_condition_traced(
                    cond, record, context, out, &cond_path, ctx, collector,
                )? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        V2Condition::Any(conditions) => {
            for (index, cond) in conditions.iter().enumerate() {
                let cond_path = format!("{}[{}]", path, index);
                if eval_v2_condition_traced(cond, record, context, out, &cond_path, ctx, collector)?
                {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        V2Condition::Comparison(comparison) => {
            eval_v2_comparison_traced(comparison, record, context, out, path, ctx, collector)
        }
        V2Condition::Expr(expr) => {
            let expr_path = format!("{}.expr", path);
            let value =
                eval_v2_expr_traced(expr, record, context, out, &expr_path, ctx, collector)?;
            match value {
                V2EvalValue::Value(JsonValue::Bool(flag)) => Ok(flag),
                V2EvalValue::Missing => Ok(false),
                V2EvalValue::Value(_) => Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "when/record_when must evaluate to boolean",
                )
                .with_path(&expr_path)),
            }
        }
    }
}

fn eval_v2_comparison_traced<'a>(
    comparison: &crate::v2_model::V2Comparison,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<bool, TransformError> {
    if comparison.args.len() != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!(
                "comparison requires exactly 2 arguments, got {}",
                comparison.args.len()
            ),
        )
        .with_path(path));
    }

    let operator = v2_comparison_operator_name(comparison.op);
    collector
        .start_span(TraceEventKind::OpStart, TracePhase::Start)
        .rule_path(path)
        .operator(operator)
        .attr_count("arg_count", 2)
        .finish(collector);

    let result =
        (|| {
            let left_path = format!("{}.args[0]", path);
            let right_path = format!("{}.args[1]", path);
            let left = eval_v2_expr_traced(
                &comparison.args[0],
                record,
                context,
                out,
                &left_path,
                ctx,
                collector,
            )?;
            emit_v2_arg_eval(collector, &left_path, 0, operator, &left);
            let right = eval_v2_expr_traced(
                &comparison.args[1],
                record,
                context,
                out,
                &right_path,
                ctx,
                collector,
            )?;
            emit_v2_arg_eval(collector, &right_path, 1, operator, &right);

            match comparison.op {
                V2ComparisonOp::Eq => Ok(compare_v2_eval_eq(&left, &right)),
                V2ComparisonOp::Ne => Ok(!compare_v2_eval_eq(&left, &right)),
                V2ComparisonOp::Gt => compare_v2_eval_ord(&left, &right, path)
                    .map(|ordering| ordering == Ordering::Greater),
                V2ComparisonOp::Gte => compare_v2_eval_ord(&left, &right, path)
                    .map(|ordering| ordering != Ordering::Less),
                V2ComparisonOp::Lt => compare_v2_eval_ord(&left, &right, path)
                    .map(|ordering| ordering == Ordering::Less),
                V2ComparisonOp::Lte => compare_v2_eval_ord(&left, &right, path)
                    .map(|ordering| ordering != Ordering::Greater),
                V2ComparisonOp::Match => compare_v2_eval_match(&left, &right, path),
            }
        })();

    match result {
        Ok(flag) => {
            collector
                .end_span(TraceEventKind::OpEnd, TracePhase::End)
                .rule_path(path)
                .operator(operator)
                .finish_with_output(collector, &JsonValue::Bool(flag), None);
            Ok(flag)
        }
        Err(error) => {
            collector
                .error_span(TraceEventKind::OpError, "OP_ERROR", "operator failed")
                .rule_path(path)
                .operator(operator)
                .finish(collector);
            Err(error)
        }
    }
}

fn v2_comparison_operator_name(op: V2ComparisonOp) -> &'static str {
    match op {
        V2ComparisonOp::Eq => "eq",
        V2ComparisonOp::Ne => "ne",
        V2ComparisonOp::Gt => "gt",
        V2ComparisonOp::Gte => "gte",
        V2ComparisonOp::Lt => "lt",
        V2ComparisonOp::Lte => "lte",
        V2ComparisonOp::Match => "match",
    }
}

fn compare_v2_eval_eq(left: &V2EvalValue, right: &V2EvalValue) -> bool {
    match (left, right) {
        (V2EvalValue::Value(left), V2EvalValue::Value(right)) => left == right,
        (V2EvalValue::Missing, V2EvalValue::Missing) => true,
        (V2EvalValue::Missing, V2EvalValue::Value(right)) => right.is_null(),
        (V2EvalValue::Value(left), V2EvalValue::Missing) => left.is_null(),
    }
}

fn compare_v2_eval_ord(
    left: &V2EvalValue,
    right: &V2EvalValue,
    path: &str,
) -> Result<Ordering, TransformError> {
    match (left, right) {
        (V2EvalValue::Value(left), V2EvalValue::Value(right)) => {
            if let (Some(left), Some(right)) = (json_value_as_f64(left), json_value_as_f64(right)) {
                return Ok(left.partial_cmp(&right).unwrap_or(Ordering::Equal));
            }
            if let (Some(left), Some(right)) = (left.as_str(), right.as_str()) {
                return Ok(left.cmp(right));
            }
            Err(TransformError::new(
                TransformErrorKind::ExprError,
                "cannot compare values of different types",
            )
            .with_path(path))
        }
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "cannot compare missing values",
        )
        .with_path(path)),
    }
}

fn compare_v2_eval_match(
    left: &V2EvalValue,
    right: &V2EvalValue,
    path: &str,
) -> Result<bool, TransformError> {
    let text = match left {
        V2EvalValue::Value(JsonValue::String(value)) => value,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "match operator requires string on left side",
            )
            .with_path(path));
        }
    };
    let pattern = match right {
        V2EvalValue::Value(JsonValue::String(value)) => value,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "match operator requires regex pattern string on right side",
            )
            .with_path(path));
        }
    };
    Regex::new(pattern)
        .map_err(|err| {
            TransformError::new(
                TransformErrorKind::ExprError,
                format!("invalid regex pattern: {}", err),
            )
            .with_path(path)
        })
        .map(|regex| regex.is_match(text))
}

fn json_value_as_f64(value: &JsonValue) -> Option<f64> {
    match value {
        JsonValue::Number(number) => number.as_f64(),
        JsonValue::String(value) => value.parse::<f64>().ok(),
        _ => None,
    }
}
