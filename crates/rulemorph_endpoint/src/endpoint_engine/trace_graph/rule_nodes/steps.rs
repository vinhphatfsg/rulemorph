use std::path::Path;

use rulemorph::v2_eval::V2EvalContext;
use rulemorph::{RuleFile, TransformError, TransformErrorKind};
use serde_json::{Map as JsonMap, Value as JsonValue};

use self::conditions::{
    StepConditionContext, StepMetaState, apply_asserts_meta, apply_branch_meta,
    apply_record_when_meta,
};
use self::node::{StepNodeInput, build_step_node, step_kind, step_label};
use super::step_outputs::collect_step_outputs;
use super::transform_error_to_trace;
use crate::endpoint_engine::trace_graph::mapping_ops::{
    MappingOpsInput, build_mapping_ops_with_values,
};

mod conditions;
mod node;

pub(super) fn build_step_nodes(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    base_dir: &Path,
    trace_ctx: &V2EvalContext<'_>,
) -> Vec<JsonValue> {
    let Some(steps) = rule.steps.as_deref() else {
        return Vec::new();
    };
    let step_outputs = collect_step_outputs(rule, record, context, base_dir);

    let mut nodes = Vec::new();
    let mut prev_output = JsonValue::Object(JsonMap::new());
    let mut halted = false;
    let mut prev_elapsed = 0u64;
    for (index, step) in steps.iter().enumerate() {
        let label = step_label(rule, index);
        let kind = step_kind(rule, index);

        let step_input = prev_output.clone();
        let mut status = "ok".to_string();
        let mut output_value: Option<JsonValue> = None;
        let mut error: Option<JsonValue> = None;
        let mut meta = JsonMap::new();

        let step_active = !halted;
        let (step_result, elapsed_total) = match step_outputs.get(index) {
            Some((result, elapsed)) => (result.clone(), *elapsed),
            None => (
                Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    "missing step output",
                )),
                0,
            ),
        };
        let step_duration_us = elapsed_total.saturating_sub(prev_elapsed);
        prev_elapsed = elapsed_total;

        if halted {
            status = "skipped".to_string();
        } else {
            match step_result {
                Ok(Some(out)) => {
                    prev_output = out.clone();
                    output_value = Some(out.clone());
                }
                Ok(None) => {
                    status = "skipped".to_string();
                    output_value = Some(JsonValue::Null);
                    halted = true;
                }
                Err(err) => {
                    status = "error".to_string();
                    error = Some(transform_error_to_trace(&err));
                    halted = true;
                }
            }
        }

        let condition_context = StepConditionContext {
            rule,
            step_index: index,
            record,
            context,
            base_dir,
            step_input: &step_input,
            step_active,
            trace_ctx,
        };
        let child_trace = {
            let mut meta_state = StepMetaState {
                status: &mut status,
                error: &mut error,
                halted: &mut halted,
                meta: &mut meta,
            };
            apply_record_when_meta(&condition_context, &mut meta_state);
            apply_asserts_meta(&condition_context, &mut meta_state);
            apply_branch_meta(&condition_context, &mut meta_state)
        };

        let children = if status == "ok" {
            if let Some(mappings) = step.mappings.as_deref() {
                let mut mapping_out = step_input.clone();
                build_mapping_ops_with_values(MappingOpsInput {
                    rule: Some(rule),
                    mappings,
                    record,
                    context,
                    out: &mut mapping_out,
                    rule_version: rule.version,
                    step_index: index,
                    trace_ctx: Some(trace_ctx),
                })
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        nodes.push(build_step_node(StepNodeInput {
            step_index: index,
            kind,
            label,
            status,
            input: step_input,
            output: output_value,
            duration_us: step_duration_us,
            error,
            child_trace,
            meta,
            children,
        }));
    }
    nodes
}
