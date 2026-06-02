use std::time::Instant;

use rulemorph::v2_eval::V2EvalContext;
use rulemorph::v2_parser::parse_v2_pipe_from_value;
use rulemorph::{Mapping, RuleFile};
use serde_json::{Map as JsonMap, Value as JsonValue, json};

use super::v2_helpers::{
    build_pipe_steps, eval_v2_pipe_value, eval_v2_start_value, expr_to_json_for_v2_pipe,
    expr_to_json_value, resolve_source_value, set_path_value,
};

pub(in crate::endpoint_engine) fn build_mapping_ops_with_values(
    rule: Option<&RuleFile>,
    mappings: &[Mapping],
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &mut JsonValue,
    rule_version: u8,
    step_index: usize,
    trace_ctx: Option<&V2EvalContext<'_>>,
) -> Vec<JsonValue> {
    let mut ops = Vec::new();
    for (index, mapping) in mappings.iter().enumerate() {
        let op_started = Instant::now();
        let mut args = JsonMap::new();
        args.insert(
            "target".to_string(),
            JsonValue::String(mapping.target.clone()),
        );
        if let Some(source) = &mapping.source {
            args.insert("source".to_string(), JsonValue::String(source.clone()));
        }
        if let Some(value) = &mapping.value {
            args.insert("value".to_string(), value.clone());
        }
        if let Some(expr) = &mapping.expr {
            args.insert("expr".to_string(), expr_to_json_value(expr));
        }
        if let Some(when) = &mapping.when {
            args.insert("when".to_string(), expr_to_json_value(when));
        }
        if let Some(value_type) = &mapping.value_type {
            args.insert("type".to_string(), JsonValue::String(value_type.clone()));
        }
        if mapping.required {
            args.insert("required".to_string(), JsonValue::Bool(true));
        }
        if let Some(default) = &mapping.default {
            args.insert("default".to_string(), default.clone());
        }

        let mut input_value = None;
        let mut output_value = None;
        let mut pipe_value = None;
        let mut pipe_steps: Option<Vec<JsonValue>> = None;
        if let Some(expr) = &mapping.expr {
            if rule_version >= 2 {
                if let Some(raw) = expr_to_json_for_v2_pipe(expr) {
                    pipe_value = Some(raw.clone());
                    if let Ok(pipe) = parse_v2_pipe_from_value(&raw) {
                        let ctx = trace_ctx
                            .cloned()
                            .unwrap_or_else(|| fresh_trace_eval_ctx(rule));
                        input_value = eval_v2_start_value(&pipe.start, record, context, out, &ctx);
                        output_value = eval_v2_pipe_value(&pipe, record, context, out, &ctx);
                        pipe_steps = Some(build_pipe_steps(&pipe, record, context, out, &ctx));
                    }
                }
            }
        } else if let Some(source) = &mapping.source {
            input_value = resolve_source_value(source, record, context, out);
            output_value = input_value.clone();
            pipe_steps = Some(vec![json!({
                "index": 0,
                "label": "source",
                "input": input_value,
                "output": output_value
            })]);
        } else if let Some(value) = &mapping.value {
            input_value = Some(value.clone());
            output_value = Some(value.clone());
            pipe_steps = Some(vec![json!({
                "index": 0,
                "label": "value",
                "input": input_value,
                "output": output_value
            })]);
        }

        if let Some(value) = output_value.clone() {
            let _ = set_path_value(out, &mapping.target, value);
        }

        let duration_us = op_started.elapsed().as_micros() as u64;
        ops.push(json!({
            "id": format!("op-{}-{}", step_index, index),
            "kind": "op",
            "label": mapping.target,
            "status": "ok",
            "input": input_value,
            "pipe_value": pipe_value,
            "pipe_steps": pipe_steps,
            "args": JsonValue::Object(args),
            "output": output_value,
            "duration_us": duration_us,
            "meta": { "op": "mapping" }
        }));
    }
    ops
}

fn fresh_trace_eval_ctx<'a>(rule: Option<&'a RuleFile>) -> V2EvalContext<'a> {
    match rule {
        Some(rule) => V2EvalContext::new()
            .with_rule(rule)
            .with_shared_custom_op_counter(),
        None => V2EvalContext::new(),
    }
}
