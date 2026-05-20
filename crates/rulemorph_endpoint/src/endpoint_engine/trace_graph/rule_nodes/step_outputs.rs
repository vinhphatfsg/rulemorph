use std::path::Path;
use std::time::Instant;

use rulemorph::{RuleFile, TransformError, transform_record_with_base_dir};
use serde_json::Value as JsonValue;

pub(super) fn collect_step_outputs(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    base_dir: &Path,
) -> Vec<(Result<Option<JsonValue>, TransformError>, u64)> {
    let Some(steps) = rule.steps.as_deref() else {
        return Vec::new();
    };
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
    step_outputs
}
