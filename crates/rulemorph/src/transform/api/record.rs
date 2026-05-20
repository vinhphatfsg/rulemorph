use super::*;

pub fn transform_record(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
) -> Result<Option<JsonValue>, TransformError> {
    let (output, _warnings) = transform_record_with_warnings(rule, record, context)?;
    Ok(output)
}

pub fn transform_record_with_base_dir(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    base_dir: &Path,
) -> Result<Option<JsonValue>, TransformError> {
    let (output, _warnings) =
        transform_record_with_warnings_with_base_dir(rule, record, context, base_dir)?;
    Ok(output)
}

pub fn transform_record_with_warnings(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
) -> Result<(Option<JsonValue>, Vec<TransformWarning>), TransformError> {
    let mut branch_context = BranchContext::default();
    transform_record_with_warnings_inner(rule, record, context, None, &mut branch_context)
}

pub fn transform_record_with_warnings_with_base_dir(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    base_dir: &Path,
) -> Result<(Option<JsonValue>, Vec<TransformWarning>), TransformError> {
    let mut branch_context = BranchContext::default();
    transform_record_with_warnings_inner(rule, record, context, Some(base_dir), &mut branch_context)
}

pub(in crate::transform) fn transform_record_with_warnings_inner(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    base_dir: Option<&Path>,
    branch_context: &mut BranchContext,
) -> Result<(Option<JsonValue>, Vec<TransformWarning>), TransformError> {
    let mut warnings = Vec::new();
    let output = apply_rule_to_record(
        rule,
        record,
        context,
        &mut warnings,
        base_dir,
        branch_context,
    )?;
    if output.is_none() {
        return Ok((None, warnings));
    }
    if let Some(finalize) = &rule.finalize {
        let mut records = Vec::new();
        if let Some(value) = output {
            records.push(value);
        }
        let finalized = apply_finalize(finalize, JsonValue::Array(records), context)?;
        return Ok((Some(finalized), warnings));
    }
    Ok((output, warnings))
}
