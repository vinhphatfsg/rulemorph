use std::path::Path;
use std::time::Instant;

use chrono::Utc;
use rulemorph::v2_eval::{EvalValue, V2EvalContext, eval_v2_condition, eval_v2_expr};
use rulemorph::v2_parser::{parse_v2_condition, parse_v2_expr};
use rulemorph::{
    Expr, RuleFile, TransformError, TransformErrorKind, transform_record_with_base_dir,
};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use uuid::Uuid;

mod finalize;
mod mapping_ops;
mod network_nodes;
mod v2_helpers;

use self::finalize::build_finalize_trace;
pub(super) use self::mapping_ops::build_mapping_ops_with_values;
pub(super) use self::network_nodes::build_network_nodes_with_timing;
use self::v2_helpers::{expr_to_json_for_v2_condition, expr_to_json_for_v2_pipe};
use super::rule_loader::{RuleKind, load_rule_kind, yaml_source_to_json};
use super::{
    empty_object, resolve_rule_path, rule_display_name, rule_ref_from_path, rule_ref_from_rule,
};

pub(super) fn build_rule_trace(
    rule_type: &str,
    name: String,
    path: String,
    version: u8,
    rule_source: JsonValue,
    input: JsonValue,
    output: JsonValue,
    nodes: Vec<JsonValue>,
    finalize: Option<JsonValue>,
    duration_us: u64,
    status: &str,
) -> JsonValue {
    let trace_id = Uuid::new_v4().to_string();
    let now = Utc::now();
    let record = json!({
        "index": 0,
        "status": status,
        "duration_us": duration_us,
        "input": input,
        "output": output,
        "nodes": nodes,
    });
    let mut trace = json!({
        "trace_id": trace_id,
        "timestamp": now.to_rfc3339(),
        "rule": {
            "type": rule_type,
            "name": name,
            "path": path,
            "version": version
        },
        "input_format": "json",
        "rule_source": rule_source,
        "records": [record],
        "summary": {
            "record_total": 1,
            "record_success": if status == "ok" { 1 } else { 0 },
            "record_failed": if status == "ok" { 0 } else { 1 },
            "duration_us": duration_us
        }
    });
    if let Some(finalize) = finalize {
        if let Some(obj) = trace.as_object_mut() {
            obj.insert("finalize".to_string(), finalize);
        }
    }
    trace
}

pub(super) struct RuleTraceNodes {
    pub(super) nodes: Vec<JsonValue>,
    pub(super) finalize: Option<JsonValue>,
    pub(super) pre_finalize_output: Option<JsonValue>,
    pub(super) duration_us: u64,
}

pub(super) fn build_rule_nodes_from_rule(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    base_dir: &Path,
) -> RuleTraceNodes {
    let mut nodes = Vec::new();
    let mut finalize_trace: Option<JsonValue> = None;
    let mut pre_finalize_output: Option<JsonValue> = None;
    if let Some(steps) = &rule.steps {
        let mut step_outputs = Vec::with_capacity(steps.len());
        for index in 0..steps.len() {
            let mut partial_rule = rule.clone();
            partial_rule.steps = Some(steps[..=index].to_vec());
            partial_rule.finalize = None;
            let started = Instant::now();
            let result = transform_record_with_base_dir(&partial_rule, record, context, base_dir);
            let duration_us = started.elapsed().as_micros() as u64;
            step_outputs.push((result, duration_us));
        }

        let mut prev_output = JsonValue::Object(JsonMap::new());
        let mut halted = false;
        let mut prev_elapsed = 0u64;
        for (index, step) in steps.iter().enumerate() {
            let label = step
                .name
                .clone()
                .unwrap_or_else(|| format!("step-{}", index + 1));
            let kind = if step.branch.is_some() {
                "branch"
            } else if step.record_when.is_some() {
                "record_when"
            } else if step.asserts.is_some() {
                "asserts"
            } else if step.mappings.is_some() {
                "mappings"
            } else {
                "step"
            };

            let step_input = prev_output.clone();
            let mut status = "ok".to_string();
            let mut output_value: Option<JsonValue> = None;
            let mut error: Option<JsonValue> = None;
            let mut child_trace: Option<JsonValue> = None;
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

            if step_active && status != "error" {
                if let Some(expr) = step.record_when.as_ref() {
                    match eval_trace_condition(
                        expr,
                        record,
                        context,
                        &step_input,
                        "record_when",
                        rule.version,
                    ) {
                        Ok(flag) => {
                            meta.insert("record_when".to_string(), JsonValue::Bool(flag));
                        }
                        Err(err) => {
                            status = "error".to_string();
                            error = Some(transform_error_to_trace(&err));
                            halted = true;
                        }
                    }
                }
            }

            if step_active && status != "error" {
                if let Some(asserts) = step.asserts.as_ref() {
                    let mut asserts_ok = true;
                    for (assert_index, assert) in asserts.iter().enumerate() {
                        let assert_path =
                            format!("steps[{}].asserts[{}].when", index, assert_index);
                        match eval_trace_condition(
                            &assert.when,
                            record,
                            context,
                            &step_input,
                            &assert_path,
                            rule.version,
                        ) {
                            Ok(true) => {}
                            Ok(false) => {
                                asserts_ok = false;
                                let err = TransformError::new(
                                    TransformErrorKind::AssertionFailed,
                                    format!(
                                        "assert failed: {}: {}",
                                        assert.error.code, assert.error.message
                                    ),
                                )
                                .with_path(format!("steps[{}].asserts[{}]", index, assert_index));
                                status = "error".to_string();
                                error = Some(transform_error_to_trace(&err));
                                halted = true;
                                break;
                            }
                            Err(err) => {
                                asserts_ok = false;
                                status = "error".to_string();
                                error = Some(transform_error_to_trace(&err));
                                halted = true;
                                break;
                            }
                        }
                    }
                    meta.insert("asserts_ok".to_string(), JsonValue::Bool(asserts_ok));
                }
            }
            if step.asserts.is_some() && !meta.contains_key("asserts_ok") {
                meta.insert("asserts_ok".to_string(), JsonValue::Bool(false));
            }

            if step_active && status != "error" {
                if let Some(branch) = step.branch.as_ref() {
                    let mut refs = Vec::new();
                    let mut labels = Vec::new();
                    let then_ref = rule_ref_from_rule(base_dir, &branch.then);
                    refs.push(then_ref.clone());
                    labels.push("branch: then".to_string());
                    let else_ref = branch
                        .r#else
                        .as_ref()
                        .map(|other| rule_ref_from_rule(base_dir, other));
                    if let Some(other_ref) = else_ref.as_ref() {
                        refs.push(other_ref.clone());
                        labels.push("branch: else".to_string());
                    }

                    let branch_taken = match eval_trace_condition(
                        &branch.when,
                        record,
                        context,
                        &step_input,
                        "branch.when",
                        rule.version,
                    ) {
                        Ok(true) => "then",
                        Ok(false) => {
                            if branch.r#else.is_some() {
                                "else"
                            } else {
                                "none"
                            }
                        }
                        Err(err) => {
                            status = "error".to_string();
                            error = Some(transform_error_to_trace(&err));
                            halted = true;
                            "none"
                        }
                    };
                    meta.insert(
                        "branch_taken".to_string(),
                        JsonValue::String(branch_taken.to_string()),
                    );
                    meta.insert(
                        "rule_refs".to_string(),
                        JsonValue::Array(refs.iter().cloned().map(JsonValue::String).collect()),
                    );
                    meta.insert(
                        "rule_ref_labels".to_string(),
                        JsonValue::Array(labels.iter().cloned().map(JsonValue::String).collect()),
                    );
                    if branch.return_ && branch_taken != "none" {
                        halted = true;
                    }

                    let taken_ref = match branch_taken {
                        "then" => Some((branch.then.as_str(), then_ref)),
                        "else" => branch
                            .r#else
                            .as_deref()
                            .and_then(|path| else_ref.map(|label| (path, label))),
                        _ => None,
                    };
                    if let Some((target_path, ref_label)) = taken_ref {
                        meta.insert("rule_ref".to_string(), JsonValue::String(ref_label.clone()));
                        meta.insert(
                            "rule_ref_label".to_string(),
                            JsonValue::String(format!("branch: {}", branch_taken)),
                        );
                        let resolved = resolve_rule_path(base_dir, target_path);
                        if let Ok(RuleKind::Normal(loaded)) = load_rule_kind(&resolved) {
                            let rule_source = std::fs::read_to_string(&resolved)
                                .ok()
                                .and_then(|source| yaml_source_to_json(&source))
                                .unwrap_or_else(|| json!({}));
                            let child_rule_trace = build_rule_nodes_from_rule(
                                &loaded.rule,
                                &step_input,
                                context,
                                &loaded.base_dir,
                            );
                            let child_duration_us = child_rule_trace.duration_us;
                            let child_output = transform_record_with_base_dir(
                                &loaded.rule,
                                &step_input,
                                context,
                                &loaded.base_dir,
                            )
                            .ok()
                            .and_then(|value| value)
                            .unwrap_or_else(empty_object);
                            let trace_output = child_rule_trace
                                .pre_finalize_output
                                .clone()
                                .unwrap_or_else(|| child_output.clone());
                            child_trace = Some(build_rule_trace(
                                "normal",
                                rule_display_name(&resolved),
                                rule_ref_from_path(base_dir, &resolved),
                                loaded.rule.version,
                                rule_source,
                                step_input.clone(),
                                trace_output,
                                child_rule_trace.nodes,
                                child_rule_trace.finalize,
                                child_duration_us,
                                "ok",
                            ));
                        }
                    }
                }
            }

            let children = if status == "ok" {
                if let Some(mappings) = step.mappings.as_deref() {
                    let mut mapping_out = step_input.clone();
                    build_mapping_ops_with_values(
                        mappings,
                        record,
                        context,
                        &mut mapping_out,
                        rule.version,
                        index,
                    )
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };

            let mut node = json!({
                "id": format!("step-{}", index),
                "kind": kind,
                "label": label,
                "status": status,
                "input": step_input,
                "output": output_value,
                "duration_us": step_duration_us,
            });
            if let Some(err) = error {
                if let Some(obj) = node.as_object_mut() {
                    obj.insert("error".to_string(), err);
                }
            }
            if let Some(trace) = child_trace {
                if let Some(obj) = node.as_object_mut() {
                    obj.insert("child_trace".to_string(), trace);
                }
            }
            if !meta.is_empty() {
                if let Some(obj) = node.as_object_mut() {
                    obj.insert("meta".to_string(), JsonValue::Object(meta));
                }
            }
            if !children.is_empty() {
                if let Some(obj) = node.as_object_mut() {
                    obj.insert("children".to_string(), JsonValue::Array(children));
                }
            }
            nodes.push(node);
        }
    } else {
        let started = Instant::now();
        let mut out = JsonValue::Object(JsonMap::new());
        let children = build_mapping_ops_with_values(
            &rule.mappings,
            record,
            context,
            &mut out,
            rule.version,
            0,
        );
        let duration_us = started.elapsed().as_micros() as u64;
        let mut node = json!({
            "id": "step-0",
            "kind": "mapping",
            "label": "mappings",
            "status": "ok",
            "input": record,
            "output": out,
            "duration_us": duration_us,
        });
        if !children.is_empty() {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("children".to_string(), JsonValue::Array(children));
            }
        }
        nodes.push(node);
    }

    if let Some(finalize) = build_finalize_trace(rule, record, context, base_dir) {
        pre_finalize_output = finalize.pre_finalize_output;
        finalize_trace = Some(finalize.trace);
    }

    let duration_us = sum_rule_trace_duration_us(&nodes, finalize_trace.as_ref());

    RuleTraceNodes {
        nodes,
        finalize: finalize_trace,
        pre_finalize_output,
        duration_us,
    }
}

fn sum_node_duration_us(nodes: &[JsonValue]) -> u64 {
    nodes
        .iter()
        .filter_map(|node| node.get("duration_us").and_then(|value| value.as_u64()))
        .sum()
}

pub(super) fn sum_rule_trace_duration_us(nodes: &[JsonValue], finalize: Option<&JsonValue>) -> u64 {
    sum_node_duration_us(nodes).saturating_add(
        finalize
            .and_then(|trace| trace.get("duration_us"))
            .and_then(|value| value.as_u64())
            .unwrap_or(0),
    )
}

fn transform_error_to_trace(err: &TransformError) -> JsonValue {
    json!({
        "code": format!("{:?}", err.kind),
        "message": err.message,
        "path": err.path,
    })
}

fn eval_trace_condition(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    path: &str,
    rule_version: u8,
) -> Result<bool, TransformError> {
    if rule_version >= 2 {
        if let Some(raw_value) = expr_to_json_for_v2_condition(expr) {
            if let Ok(condition) = parse_v2_condition(&raw_value) {
                let ctx = V2EvalContext::new();
                return eval_v2_condition(&condition, record, context, out, path, &ctx);
            }
            if let Ok(v2_expr) = parse_v2_expr(&raw_value) {
                let ctx = V2EvalContext::new();
                let value = eval_v2_expr(&v2_expr, record, context, out, path, &ctx)?;
                return match value {
                    EvalValue::Missing => Ok(false),
                    EvalValue::Value(JsonValue::Bool(flag)) => Ok(flag),
                    EvalValue::Value(_) => Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "when/record_when must evaluate to boolean",
                    )
                    .with_path(path)),
                };
            }
        }
        if let Some(raw_value) = expr_to_json_for_v2_pipe(expr) {
            let v2_expr = parse_v2_expr(&raw_value).map_err(|err| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("invalid v2 condition: {}", err),
                )
                .with_path(path)
            })?;
            let ctx = V2EvalContext::new();
            let value = eval_v2_expr(&v2_expr, record, context, out, path, &ctx)?;
            return match value {
                EvalValue::Missing => Ok(false),
                EvalValue::Value(JsonValue::Bool(flag)) => Ok(flag),
                EvalValue::Value(_) => Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "when/record_when must evaluate to boolean",
                )
                .with_path(path)),
            };
        }
    }

    Err(TransformError::new(
        TransformErrorKind::ExprError,
        "when/record_when must evaluate to boolean",
    )
    .with_path(path))
}
