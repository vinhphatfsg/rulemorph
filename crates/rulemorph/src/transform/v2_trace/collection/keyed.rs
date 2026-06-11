use std::collections::HashSet;

use super::*;

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(super) fn eval_v2_keyed_collection_traced<'a>(
    op_step: &crate::v2_model::V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
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
    let mut grouped = serde_json::Map::new();
    let mut keyed = serde_json::Map::new();
    let mut distinct = Vec::new();
    let mut seen = HashSet::new();
    for (index, item) in array.iter().enumerate() {
        let item_path = format!("{}[{}]", path, index);
        emit_v2_collection_item_start(collector, &item_path, operator, index, item);
        let item_ctx = ctx
            .clone()
            .with_pipe_value(V2EvalValue::Value(item.clone()))
            .with_item(V2EvalItem { value: item, index });
        let key = eval_v2_key_expr_string_traced(
            &op_step.args[0],
            record,
            context,
            out,
            &arg_path,
            &item_ctx,
            collector,
        )?;
        let key_output = V2EvalValue::Value(JsonValue::String(key.clone()));
        emit_v2_arg_eval(collector, &arg_path, 0, operator, &key_output);
        let selected = match operator {
            "group_by" => {
                let entry = grouped
                    .entry(key)
                    .or_insert_with(|| JsonValue::Array(Vec::new()));
                if let JsonValue::Array(items) = entry {
                    items.push(item.clone());
                }
                true
            }
            "key_by" => {
                keyed.insert(key, item.clone());
                true
            }
            "distinct_by" => {
                if seen.insert(key) {
                    distinct.push(item.clone());
                    true
                } else {
                    false
                }
            }
            _ => unreachable!(),
        };
        finish_v2_collection_item(
            collector,
            &item_path,
            operator,
            index,
            &key_output,
            Some(("selected", selected)),
        );
    }
    match operator {
        "group_by" => Ok(V2EvalValue::Value(JsonValue::Object(grouped))),
        "key_by" => Ok(V2EvalValue::Value(JsonValue::Object(keyed))),
        "distinct_by" => Ok(V2EvalValue::Value(JsonValue::Array(distinct))),
        _ => unreachable!(),
    }
}
