use super::*;

pub(super) fn transform_with_warnings_inner(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
    base_dir: Option<&Path>,
    options: &NormalizationOptions,
) -> Result<(JsonValue, Vec<TransformWarning>), TransformError> {
    let mut warnings = Vec::new();
    let mut output_records = Vec::new();
    if rule.finalize.is_some() {
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
            if let Some(output) = item.output {
                output_records.push(output);
            }
        }
    }

    let mut output = JsonValue::Array(output_records);
    if let Some(finalize) = &rule.finalize {
        output = apply_finalize(finalize, output, context)?;
    }

    Ok((output, warnings))
}
