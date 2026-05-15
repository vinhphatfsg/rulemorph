use std::path::Path;
use std::time::Instant;

use chrono::Utc;
use rulemorph::v2_eval::{
    EvalValue, V2EvalContext, eval_v2_condition, eval_v2_expr, eval_v2_if_step, eval_v2_let_step,
    eval_v2_map_step, eval_v2_op_step, eval_v2_pipe, eval_v2_ref, eval_v2_start,
};
use rulemorph::v2_model::{V2Ref, V2Start, V2Step};
use rulemorph::v2_parser::{
    is_literal_escape, is_pipe_value, is_v2_ref, parse_v2_condition, parse_v2_expr,
    parse_v2_pipe_from_value,
};
use rulemorph::{
    Expr, Mapping, PathToken, RuleFile, TransformError, TransformErrorKind, get_path, parse_path,
    transform_record_with_base_dir,
};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use uuid::Uuid;

use super::rule_loader::{RuleKind, load_rule_kind, yaml_source_to_json};
use super::{
    CompiledNetworkRule, NetworkExecution, empty_object, resolve_rule_path, rule_display_name,
    rule_ref_from_path, rule_ref_from_rule,
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

    if let Some(finalize) = &rule.finalize {
        let mut base_rule = rule.clone();
        base_rule.finalize = None;
        let base_started = Instant::now();
        let pre_finalize = transform_record_with_base_dir(&base_rule, record, context, base_dir)
            .ok()
            .and_then(|value| value);
        let base_duration_us = base_started.elapsed().as_micros() as u64;
        pre_finalize_output = pre_finalize.clone();
        let finalize_input = match pre_finalize {
            Some(value) => JsonValue::Array(vec![value]),
            None => JsonValue::Array(Vec::new()),
        };
        let finalize_started = Instant::now();
        let finalize_result = transform_record_with_base_dir(rule, record, context, base_dir);
        let total_duration_us = finalize_started.elapsed().as_micros() as u64;
        let finalize_duration_us = total_duration_us.saturating_sub(base_duration_us);
        let mut finalize_status = "ok";
        let mut finalize_output: Option<JsonValue> = None;
        let mut finalize_error: Option<JsonValue> = None;
        match finalize_result {
            Ok(Some(value)) => {
                finalize_output = Some(value);
            }
            Ok(None) => {
                finalize_output = Some(JsonValue::Null);
            }
            Err(err) => {
                finalize_status = "error";
                finalize_error = Some(transform_error_to_trace(&err));
            }
        }
        let mut children = Vec::new();
        if let Some(filter) = &finalize.filter {
            children.push(json!({
                "id": "op-filter",
                "kind": "op",
                "label": "filter",
                "status": "ok",
                "meta": { "op": "filter" },
                "args": { "expr": expr_to_json_value(filter) }
            }));
        }
        if let Some(sort) = &finalize.sort {
            children.push(json!({
                "id": "op-sort",
                "kind": "op",
                "label": "sort",
                "status": "ok",
                "meta": { "op": "sort" },
                "args": { "by": sort.by, "order": sort.order }
            }));
        }
        if let Some(limit) = finalize.limit {
            children.push(json!({
                "id": "op-limit",
                "kind": "op",
                "label": "limit",
                "status": "ok",
                "meta": { "op": "limit" },
                "args": { "limit": limit }
            }));
        }
        if let Some(offset) = finalize.offset {
            children.push(json!({
                "id": "op-offset",
                "kind": "op",
                "label": "offset",
                "status": "ok",
                "meta": { "op": "offset" },
                "args": { "offset": offset }
            }));
        }
        if let Some(wrap) = &finalize.wrap {
            children.push(json!({
                "id": "op-wrap",
                "kind": "op",
                "label": "wrap",
                "status": "ok",
                "meta": { "op": "wrap" },
                "args": { "wrap": wrap }
            }));
        }

        let mut finalize = json!({
            "status": finalize_status,
            "input": finalize_input,
            "output": finalize_output,
            "duration_us": finalize_duration_us,
            "nodes": children,
        });
        if let Some(err) = finalize_error {
            if let Some(obj) = finalize.as_object_mut() {
                obj.insert("error".to_string(), err);
            }
        }
        finalize_trace = Some(finalize);
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

pub(super) fn build_network_nodes_with_timing(
    rule: &CompiledNetworkRule,
    timing: &NetworkExecution,
) -> Vec<JsonValue> {
    let mut children = Vec::new();
    let mut request_args = JsonMap::new();
    request_args.insert(
        "method".to_string(),
        JsonValue::String(rule.request.method.to_string()),
    );
    request_args.insert(
        "url".to_string(),
        JsonValue::String(format!("{:?}", rule.request.url)),
    );
    if !rule.request.headers.is_empty() {
        let mut headers = JsonMap::new();
        for (key, expr) in &rule.request.headers {
            headers.insert(key.to_string(), JsonValue::String(format!("{:?}", expr)));
        }
        request_args.insert("headers".to_string(), JsonValue::Object(headers));
    }
    children.push(json!({
        "id": "op-request",
        "kind": "op",
        "label": "request",
        "status": "ok",
        "duration_us": timing.request_us,
        "meta": { "op": "request" },
        "args": JsonValue::Object(request_args)
    }));

    if let Some(body) = &rule.body {
        children.push(json!({
            "id": "op-body",
            "kind": "op",
            "label": "body",
            "status": "ok",
            "meta": { "op": "body" },
            "args": { "expr": format!("{:?}", body) }
        }));
    }
    if let Some(body_map) = &rule.body_map {
        let mut out = JsonValue::Object(JsonMap::new());
        let empty = JsonValue::Object(JsonMap::new());
        let ops = build_mapping_ops_with_values(body_map, &empty, None, &mut out, 2, 0);
        children.extend(ops);
    }
    if rule.body_rule.is_some() {
        children.push(json!({
            "id": "op-body-rule",
            "kind": "op",
            "label": "body_rule",
            "status": "ok",
            "meta": { "op": "body_rule" }
        }));
    }
    if let Some(select) = &rule.select {
        children.push(json!({
            "id": "op-select",
            "kind": "op",
            "label": "select",
            "status": "ok",
            "meta": { "op": "select" },
            "args": { "path": select }
        }));
    }
    if let Some(retry) = &rule.retry {
        children.push(json!({
            "id": "op-retry",
            "kind": "op",
            "label": "retry",
            "status": "ok",
            "meta": { "op": "retry" },
            "args": {
                "max": retry.max,
                "backoff": format!("{:?}", retry.backoff),
                "initial_delay_ms": retry.initial_delay.as_millis()
            }
        }));
    }

    let mut node = json!({
        "id": "step-0",
        "kind": "network",
        "label": "request",
        "status": "ok",
        "duration_us": timing.total_us,
    });
    if let Some(rule_ref) = rule.body_rule_ref.as_ref() {
        if let Some(obj) = node.as_object_mut() {
            obj.insert(
                "meta".to_string(),
                json!({
                    "rule_ref": rule_ref,
                    "rule_ref_label": "body_rule"
                }),
            );
        }
    }
    if let Some(trace) = timing.body_rule_trace.as_ref() {
        if let Some(obj) = node.as_object_mut() {
            obj.insert("child_trace".to_string(), trace.clone());
        }
    }
    if let Some(obj) = node.as_object_mut() {
        obj.insert("children".to_string(), JsonValue::Array(children));
    }
    vec![node]
}

pub(super) fn build_mapping_ops_with_values(
    mappings: &[Mapping],
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &mut JsonValue,
    rule_version: u8,
    step_index: usize,
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
                        let ctx = V2EvalContext::new();
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

fn expr_to_json_for_v2_pipe(expr: &Expr) -> Option<JsonValue> {
    match expr {
        Expr::Literal(JsonValue::Array(arr)) => Some(JsonValue::Array(arr.clone())),
        Expr::Literal(JsonValue::String(value)) => {
            if is_v2_ref(value) || is_pipe_value(value) || is_literal_escape(value) {
                Some(JsonValue::String(value.clone()))
            } else {
                None
            }
        }
        Expr::Ref(expr_ref)
            if expr_ref.ref_path.starts_with('@') || is_literal_escape(&expr_ref.ref_path) =>
        {
            Some(JsonValue::Array(vec![JsonValue::String(
                expr_ref.ref_path.clone(),
            )]))
        }
        Expr::Chain(chain) => {
            if let Some(first) = chain.chain.first() {
                if let Expr::Ref(reference) = first {
                    if reference.ref_path.starts_with('@') {
                        let items: Vec<JsonValue> =
                            chain.chain.iter().map(expr_to_json_value).collect();
                        return Some(JsonValue::Array(items));
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn expr_to_json_for_v2_condition(expr: &Expr) -> Option<JsonValue> {
    match expr {
        Expr::Literal(value) => Some(value.clone()),
        Expr::Ref(reference)
            if reference.ref_path.starts_with('@') || is_literal_escape(&reference.ref_path) =>
        {
            Some(JsonValue::String(reference.ref_path.clone()))
        }
        Expr::Chain(chain) => {
            if let Some(first) = chain.chain.first() {
                if let Expr::Ref(reference) = first {
                    if reference.ref_path.starts_with('@') {
                        let items: Vec<JsonValue> = chain
                            .chain
                            .iter()
                            .map(expr_to_json_value_for_condition)
                            .collect();
                        return Some(JsonValue::Array(items));
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn expr_to_json_value_for_condition(expr: &Expr) -> JsonValue {
    match expr {
        Expr::Ref(reference) => JsonValue::String(reference.ref_path.clone()),
        Expr::Literal(value) => value.clone(),
        Expr::Op(op) => {
            let args: Vec<JsonValue> = op
                .args
                .iter()
                .map(expr_to_json_value_for_condition)
                .collect();
            let mut obj = JsonMap::new();
            obj.insert(op.op.clone(), JsonValue::Array(args));
            JsonValue::Object(obj)
        }
        Expr::Chain(chain) => {
            let items: Vec<JsonValue> = chain
                .chain
                .iter()
                .map(expr_to_json_value_for_condition)
                .collect();
            JsonValue::Array(items)
        }
    }
}

fn build_pipe_steps(
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

fn v2_start_label(start: &V2Start) -> String {
    match start {
        V2Start::Ref(reference) => v2_ref_label(reference),
        V2Start::PipeValue => "$".to_string(),
        V2Start::Literal(value) => value.to_string(),
        V2Start::V1Expr(_) => "v1_expr".to_string(),
    }
}

fn v2_step_label(step: &V2Step) -> String {
    match step {
        V2Step::Op(op) => op.op.clone(),
        V2Step::Let(let_step) => format!(
            "let {}",
            let_step
                .bindings
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        V2Step::If(_) => "if".to_string(),
        V2Step::Map(_) => "map".to_string(),
        V2Step::Ref(reference) => v2_ref_label(reference),
    }
}

fn v2_ref_label(reference: &V2Ref) -> String {
    match reference {
        V2Ref::Input(path) => format!("@input.{}", path),
        V2Ref::Context(path) => format!("@context.{}", path),
        V2Ref::Out(path) => format!("@out.{}", path),
        V2Ref::Item(path) => format!("@item.{}", path),
        V2Ref::Acc(path) => format!("@acc.{}", path),
        V2Ref::Local(name) => format!("@{}", name),
    }
}

fn eval_v2_start_value(
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

fn eval_v2_pipe_value(
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

fn resolve_source_value(
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

fn set_path_value(root: &mut JsonValue, path: &str, value: JsonValue) -> Result<(), ()> {
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

fn expr_to_json_value(expr: &Expr) -> JsonValue {
    match expr {
        Expr::Ref(reference) => json!({ "ref": reference.ref_path }),
        Expr::Op(op) => {
            let args: Vec<JsonValue> = op.args.iter().map(expr_to_json_value).collect();
            json!({ "op": op.op, "args": args })
        }
        Expr::Chain(chain) => {
            let items: Vec<JsonValue> = chain.chain.iter().map(expr_to_json_value).collect();
            JsonValue::Array(items)
        }
        Expr::Literal(value) => value.clone(),
    }
}
