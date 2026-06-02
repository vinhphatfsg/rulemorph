use rulemorph::v2_eval::{
    EvalValue, V2EvalContext, eval_v2_if_step, eval_v2_let_step, eval_v2_map_step, eval_v2_op_step,
    eval_v2_pipe, eval_v2_ref, eval_v2_start,
};
use rulemorph::v2_model::{V2Pipe, V2Start, V2Step};
use rulemorph::{PathToken, TransformError, get_path, parse_path};
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
    let literal_custom_label = literal_start_custom_call_label(&pipe.start, ctx);
    let start_result = if literal_custom_label.is_some() {
        let start_pipe = V2Pipe {
            start: pipe.start.clone(),
            steps: Vec::new(),
        };
        eval_v2_pipe(&start_pipe, record, context, out, "trace[0]", ctx)
    } else {
        eval_v2_start(&pipe.start, record, context, out, "trace", ctx)
    };
    let start_value = match start_result {
        Ok(value) => value,
        Err(err) => {
            steps.push(json!({
                "index": 0,
                "label": literal_custom_label.unwrap_or_else(|| v2_start_label(&pipe.start)),
                "status": "error",
                "input": JsonValue::Null,
                "output": JsonValue::Null,
                "error": transform_error_json(&err)
            }));
            return steps;
        }
    };
    let start_output = eval_value_to_json(start_value.clone());
    steps.push(json!({
        "index": 0,
        "label": literal_custom_label.unwrap_or_else(|| v2_start_label(&pipe.start)),
        "status": "ok",
        "input": JsonValue::Null,
        "output": start_output
    }));

    let mut current = start_value;
    let mut current_ctx = ctx.clone();

    for (index, step) in pipe.steps.iter().enumerate() {
        let step_input = eval_value_to_json(current.clone());
        current_ctx = current_ctx.clone().with_pipe_value(current.clone());
        let step_path = format!("trace[{}]", index + 1);
        let step_result = match step {
            V2Step::Op(op_step) => eval_v2_op_step(
                op_step,
                current.clone(),
                record,
                context,
                out,
                &step_path,
                &current_ctx,
            ),
            V2Step::CustomCall(_) => {
                let single_step_pipe = V2Pipe {
                    start: V2Start::PipeValue,
                    steps: vec![step.clone()],
                };
                eval_v2_pipe(
                    &single_step_pipe,
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                )
            }
            V2Step::Let(let_step) => {
                match eval_v2_let_step(
                    let_step,
                    current.clone(),
                    record,
                    context,
                    out,
                    &step_path,
                    &current_ctx,
                ) {
                    Ok(next_ctx) => {
                        current_ctx = next_ctx;
                        Ok(current.clone())
                    }
                    Err(err) => Err(err),
                }
            }
            V2Step::If(if_step) => eval_v2_if_step(
                if_step,
                current.clone(),
                record,
                context,
                out,
                &step_path,
                &current_ctx,
            ),
            V2Step::Map(map_step) => eval_v2_map_step(
                map_step,
                current.clone(),
                record,
                context,
                out,
                &step_path,
                &current_ctx,
            ),
            V2Step::Ref(v2_ref) => {
                eval_v2_ref(v2_ref, record, context, out, &step_path, &current_ctx)
            }
        };

        let next = match step_result {
            Ok(next) => next,
            Err(err) => {
                steps.push(json!({
                    "index": index + 1,
                    "label": v2_step_label(step),
                    "status": "error",
                    "input": step_input,
                    "output": JsonValue::Null,
                    "error": transform_error_json(&err)
                }));
                return steps;
            }
        };
        current = next;
        steps.push(json!({
            "index": index + 1,
            "label": v2_step_label(step),
            "status": "ok",
            "input": step_input,
            "output": eval_value_to_json(current.clone())
        }));
    }

    steps
}

fn transform_error_json(err: &TransformError) -> JsonValue {
    json!({
        "code": format!("{:?}", err.kind),
        "message": err.message,
        "path": err.path,
    })
}

fn literal_start_custom_call_label(start: &V2Start, ctx: &V2EvalContext) -> Option<String> {
    let V2Start::Literal(JsonValue::Object(map)) = start else {
        return None;
    };
    if map.len() != 1 {
        return None;
    }
    let (op_name, args) = map.iter().next().expect("one entry");
    if !ctx.has_custom_op(op_name) || !looks_like_with_call_options(args) {
        return None;
    }
    Some(op_name.clone())
}

fn looks_like_with_call_options(value: &JsonValue) -> bool {
    let JsonValue::Array(options) = value else {
        return false;
    };
    !options.is_empty()
        && options.iter().all(|option| {
            let JsonValue::Object(option_map) = option else {
                return false;
            };
            option_map.len() == 1 && option_map.contains_key("with")
        })
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
