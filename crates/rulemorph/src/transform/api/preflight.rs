use super::*;

pub fn preflight_validate(
    rule: &RuleFile,
    input: &str,
    context: Option<&JsonValue>,
) -> Result<(), TransformError> {
    preflight_validate_with_warnings(rule, input, context).map(|_| ())
}

pub fn preflight_validate_input(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
) -> Result<(), TransformError> {
    preflight_validate_input_with_warnings(rule, input, context).map(|_| ())
}

pub fn preflight_validate_with_base_dir(
    rule: &RuleFile,
    input: &str,
    context: Option<&JsonValue>,
    base_dir: &Path,
) -> Result<(), TransformError> {
    preflight_validate_with_warnings_with_base_dir(rule, input, context, base_dir).map(|_| ())
}

pub fn preflight_validate_input_with_base_dir(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
    base_dir: &Path,
) -> Result<(), TransformError> {
    preflight_validate_input_with_warnings_with_base_dir(rule, input, context, base_dir).map(|_| ())
}

pub fn preflight_validate_with_warnings(
    rule: &RuleFile,
    input: &str,
    context: Option<&JsonValue>,
) -> Result<Vec<TransformWarning>, TransformError> {
    preflight_validate_input_with_warnings(rule, InputData::Text(input), context)
}

pub fn preflight_validate_input_with_warnings(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
) -> Result<Vec<TransformWarning>, TransformError> {
    preflight_validate_input_with_warnings_inner(
        rule,
        input,
        context,
        None,
        &NormalizationOptions::default(),
    )
}

pub fn preflight_validate_with_warnings_with_base_dir(
    rule: &RuleFile,
    input: &str,
    context: Option<&JsonValue>,
    base_dir: &Path,
) -> Result<Vec<TransformWarning>, TransformError> {
    preflight_validate_input_with_warnings_with_base_dir(
        rule,
        InputData::Text(input),
        context,
        base_dir,
    )
}

pub fn preflight_validate_input_with_warnings_with_base_dir(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
    base_dir: &Path,
) -> Result<Vec<TransformWarning>, TransformError> {
    preflight_validate_input_with_warnings_with_base_dir_and_options(
        rule,
        input,
        context,
        base_dir,
        &NormalizationOptions::default(),
    )
}

pub fn preflight_validate_input_with_warnings_with_base_dir_and_options(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
    base_dir: &Path,
    options: &NormalizationOptions,
) -> Result<Vec<TransformWarning>, TransformError> {
    preflight_validate_input_with_warnings_inner(rule, input, context, Some(base_dir), options)
}

fn preflight_validate_input_with_warnings_inner(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
    base_dir: Option<&Path>,
    options: &NormalizationOptions,
) -> Result<Vec<TransformWarning>, TransformError> {
    let mut warnings = Vec::new();
    if let Some(warning) = crate::legacy_v1_rule_warning(rule) {
        warnings.push(warning);
    }
    let limits = EvalLimits::from(options);
    if rule.finalize.is_some() {
        let mut output_records = Vec::new();
        let records = input_records_iter_with_options(rule, input, options)?;
        for record in records {
            let record = record?;
            let mut record_warnings = Vec::new();
            let mut branch_context = BranchContext::default();
            if let Some(output) = apply_rule_to_record(RuleRecordInput {
                rule,
                record: &record,
                context,
                warnings: &mut record_warnings,
                base_dir,
                branch_context: &mut branch_context,
                limits,
                compiled_rule: None,
            })? {
                output_records.push(output);
            }
            warnings.extend(record_warnings);
        }
        if let Some(finalize) = &rule.finalize {
            let _ = apply_finalize(
                rule,
                finalize,
                JsonValue::Array(output_records),
                context,
                limits,
            )?;
        }
    } else {
        let stream = match base_dir {
            Some(base_dir) => transform_stream_input_with_base_dir_and_options(
                rule, input, context, base_dir, options,
            )?
            .suppress_legacy_v1_warning(),
            None => transform_stream_input_with_options(rule, input, context, options)?
                .suppress_legacy_v1_warning(),
        };
        for item in stream {
            let item = item?;
            warnings.extend(item.warnings);
        }
    }
    Ok(warnings)
}
