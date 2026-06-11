use super::*;

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(super) fn eval_v2_predicate_collection_traced<'a>(
    op_step: &crate::v2_model::V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    step_ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    let operator = op_step.op.as_str();
    if op_step.args.len() != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!("{operator} requires exactly one argument"),
        )
        .with_path(path));
    }
    let array = v2_eval_array_from_value(pipe_value, path)?;
    let arg_path = format!("{}.args[0]", path);
    let mut kept = Vec::new();
    let mut rejected = Vec::new();
    for (index, item) in array.iter().enumerate() {
        let item_path = format!("{}[{}]", path, index);
        emit_v2_collection_item_start(collector, &item_path, operator, index, item);
        let item_ctx = step_ctx
            .clone()
            .with_pipe_value(V2EvalValue::Value(item.clone()))
            .with_item(V2EvalItem { value: item, index });
        let matches = eval_v2_predicate_expr_traced(
            &op_step.args[0],
            record,
            context,
            out,
            &arg_path,
            &item_ctx,
            collector,
        )?;
        let match_value = V2EvalValue::Value(JsonValue::Bool(matches));
        emit_v2_arg_eval(collector, &arg_path, 0, operator, &match_value);
        finish_v2_collection_item(
            collector,
            &item_path,
            operator,
            index,
            &match_value,
            Some(("matched", matches)),
        );
        match operator {
            "filter" => {
                if matches {
                    kept.push(item.clone());
                }
            }
            "partition" => {
                if matches {
                    kept.push(item.clone());
                } else {
                    rejected.push(item.clone());
                }
            }
            "find" if matches => return Ok(V2EvalValue::Value(item.clone())),
            "find_index" if matches => {
                return Ok(V2EvalValue::Value(JsonValue::Number((index as i64).into())));
            }
            _ => {}
        }
    }
    match operator {
        "filter" => Ok(V2EvalValue::Value(JsonValue::Array(kept))),
        "partition" => Ok(V2EvalValue::Value(JsonValue::Array(vec![
            JsonValue::Array(kept),
            JsonValue::Array(rejected),
        ]))),
        "find" => Ok(V2EvalValue::Value(JsonValue::Null)),
        "find_index" => Ok(V2EvalValue::Value(JsonValue::Number((-1).into()))),
        _ => unreachable!(),
    }
}
