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
    if rule.finalize.is_some() {
        let mut output_records = Vec::new();
        let mut records = input_records_iter_with_options(rule, input, options)?;
        while let Some(record) = records.next() {
            let record = record?;
            let mut record_warnings = Vec::new();
            let mut branch_context = BranchContext::default();
            if let Some(output) = apply_rule_to_record(
                rule,
                &record,
                context,
                &mut record_warnings,
                base_dir,
                &mut branch_context,
            )? {
                output_records.push(output);
            }
            warnings.extend(record_warnings);
        }
        if let Some(finalize) = &rule.finalize {
            let _ = apply_finalize(finalize, JsonValue::Array(output_records), context)?;
        }
    } else {
        let stream = match base_dir {
            Some(base_dir) => transform_stream_input_with_base_dir_and_options(
                rule, input, context, base_dir, options,
            )?,
            None => transform_stream_input_with_options(rule, input, context, options)?,
        };
        for item in stream {
            let item = item?;
            warnings.extend(item.warnings);
        }
    }
    Ok(warnings)
}
