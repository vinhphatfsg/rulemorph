use rulemorph::v2_eval::{
    EvalValue, V2EvalContext, eval_v2_if_step, eval_v2_let_step, eval_v2_map_step, eval_v2_op_step,
    eval_v2_pipe, eval_v2_ref, eval_v2_start,
};
use rulemorph::v2_model::V2Step;
use rulemorph::{PathToken, get_path, parse_path};
use serde_json::{Map as JsonMap, Value as JsonValue, json};

mod expr_json;
mod labels;
pub(super) use self::expr_json::{
    expr_to_json_for_v2_condition, expr_to_json_for_v2_pipe, expr_to_json_value,
};
use self::labels::{v2_start_label, v2_step_label};

pub(super) fn build_pipe_steps(
    pipe: &rulemorph::v2_model::V2Pipe,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    ctx: &V2EvalContext,
) -> Vec<JsonValue> {
    let mut steps = Vec::new();
    let start_value = eval_v2_start(&pipe.start, record, context, out, "trace", ctx).ok();
    let start_output = start_value.clone().and_then(eval_value_to_json);
    steps.push(json!({
        "index": 0,
        "label": v2_start_label(&pipe.start),
        "input": JsonValue::Null,
        "output": start_output
    }));

    let mut current = match start_value {
        Some(value) => value,
        None => return steps,
    };
    let mut current_ctx = ctx.clone();

    for (index, step) in pipe.steps.iter().enumerate() {
        let step_input = eval_value_to_json(current.clone());
        current_ctx = current_ctx.clone().with_pipe_value(current.clone());
        let step_path = format!("trace[{}]", index + 1);
        match step {
            V2Step::Op(op_step) => {
                if let Ok(next) = eval_v2_op_step(
                    op_step,
                    current.clone(),
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                ) {
                    current = next;
                }
            }
            V2Step::Let(let_step) => {
                if let Ok(next_ctx) = eval_v2_let_step(
                    let_step,
                    current.clone(),
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                ) {
                    current_ctx = next_ctx;
                }
            }
            V2Step::If(if_step) => {
                if let Ok(next) = eval_v2_if_step(
                    if_step,
                    current.clone(),
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                ) {
                    current = next;
                }
            }
            V2Step::Map(map_step) => {
                if let Ok(next) = eval_v2_map_step(
                    map_step,
                    current.clone(),
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                ) {
                    current = next;
                }
            }
            V2Step::Ref(v2_ref) => {
                if let Ok(next) =
                    eval_v2_ref(v2_ref, record, context, out, &step_path, &current_ctx)
                {
                    current = next;
                }
            }
        }

        steps.push(json!({
            "index": index + 1,
            "label": v2_step_label(step),
            "input": step_input,
            "output": eval_value_to_json(current.clone())
        }));
    }

    steps
}

pub(super) fn eval_v2_start_value(
    start: &rulemorph::v2_model::V2Start,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    ctx: &V2EvalContext,
) -> Option<JsonValue> {
    eval_v2_start(start, record, context, out, "trace", ctx)
        .ok()
        .and_then(eval_value_to_json)
}

pub(super) fn eval_v2_pipe_value(
    pipe: &rulemorph::v2_model::V2Pipe,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    ctx: &V2EvalContext,
) -> Option<JsonValue> {
    eval_v2_pipe(pipe, record, context, out, "trace", ctx)
        .ok()
        .and_then(eval_value_to_json)
}

fn eval_value_to_json(value: EvalValue) -> Option<JsonValue> {
    match value {
        EvalValue::Missing => None,
        EvalValue::Value(value) => Some(value),
    }
}

pub(super) fn resolve_source_value(
    source: &str,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
) -> Option<JsonValue> {
    let trimmed = source.strip_prefix('@').unwrap_or(source);
    let (prefix, path) = trimmed.split_once('.').unwrap_or(("input", trimmed));
    if path.is_empty() {
        return None;
    }
    let target = match prefix {
        "input" => Some(record),
        "context" => context,
        "out" => Some(out),
        _ => Some(record),
    }?;
    let tokens = parse_path(path).ok()?;
    get_path(target, &tokens).cloned()
}

pub(super) fn set_path_value(root: &mut JsonValue, path: &str, value: JsonValue) -> Result<(), ()> {
    let tokens = parse_path(path).map_err(|_| ())?;
    if tokens.is_empty() {
        return Err(());
    }
    let mut current = root;
    for (index, token) in tokens.iter().enumerate() {
        let is_last = index == tokens.len() - 1;
        let key = match token {
            PathToken::Key(key) => key,
            PathToken::Index(_) => return Err(()),
        };

        if is_last {
            match current {
                JsonValue::Object(map) => {
                    map.insert(key.to_string(), value);
                }
                _ => {
                    let mut map = JsonMap::new();
                    map.insert(key.to_string(), value);
                    *current = JsonValue::Object(map);
                }
            }
            return Ok(());
        }

        let next = match current {
            JsonValue::Object(map) => map
                .entry(key.to_string())
                .or_insert_with(|| JsonValue::Object(JsonMap::new())),
            _ => {
                *current = JsonValue::Object(JsonMap::new());
                if let JsonValue::Object(map) = current {
                    map.entry(key.to_string())
                        .or_insert_with(|| JsonValue::Object(JsonMap::new()))
                } else {
                    return Err(());
                }
            }
        };
        current = next;
    }
    Err(())
}
