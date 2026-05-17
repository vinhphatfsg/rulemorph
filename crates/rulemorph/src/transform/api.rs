use serde_json::Value as JsonValue;
use std::path::Path;

use crate::error::{TransformError, TransformWarning};
use crate::model::RuleFile;
use crate::normalization::{InputData, NormalizationOptions};

use super::records::input_records_iter_with_options;
use super::stream::{
    transform_stream_input_with_base_dir_and_options, transform_stream_input_with_options,
};
use super::{BranchContext, apply_finalize, apply_rule_to_record};

mod trace;

pub use trace::{
    transform_input_with_trace, transform_input_with_trace_with_base_dir_and_options,
    transform_record_with_trace,
};

pub fn transform(
    rule: &RuleFile,
    input: &str,
    context: Option<&JsonValue>,
) -> Result<JsonValue, TransformError> {
    transform_with_warnings(rule, input, context).map(|(output, _)| output)
}

pub fn transform_input(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
) -> Result<JsonValue, TransformError> {
    transform_input_with_warnings(rule, input, context).map(|(output, _)| output)
}

pub fn transform_with_options(
    rule: &RuleFile,
    input: &str,
    context: Option<&JsonValue>,
    options: &NormalizationOptions,
) -> Result<JsonValue, TransformError> {
    transform_with_warnings_with_options(rule, input, context, options).map(|(output, _)| output)
}

pub fn transform_input_with_options(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
    options: &NormalizationOptions,
) -> Result<JsonValue, TransformError> {
    transform_input_with_warnings_with_options(rule, input, context, options)
        .map(|(output, _)| output)
}

pub fn transform_with_base_dir(
    rule: &RuleFile,
    input: &str,
    context: Option<&JsonValue>,
    base_dir: &Path,
) -> Result<JsonValue, TransformError> {
    transform_with_warnings_with_base_dir(rule, input, context, base_dir).map(|(output, _)| output)
}

pub fn transform_input_with_base_dir(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
    base_dir: &Path,
) -> Result<JsonValue, TransformError> {
    transform_input_with_warnings_with_base_dir(rule, input, context, base_dir)
        .map(|(output, _)| output)
}

pub fn transform_input_with_base_dir_and_options(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
    base_dir: &Path,
    options: &NormalizationOptions,
) -> Result<JsonValue, TransformError> {
    transform_input_with_warnings_with_base_dir_and_options(rule, input, context, base_dir, options)
        .map(|(output, _)| output)
}

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

pub fn transform_with_warnings(
    rule: &RuleFile,
    input: &str,
    context: Option<&JsonValue>,
) -> Result<(JsonValue, Vec<TransformWarning>), TransformError> {
    transform_input_with_warnings(rule, InputData::Text(input), context)
}

pub fn transform_input_with_warnings(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
) -> Result<(JsonValue, Vec<TransformWarning>), TransformError> {
    transform_with_warnings_inner(rule, input, context, None, &NormalizationOptions::default())
}

pub fn transform_with_warnings_with_options(
    rule: &RuleFile,
    input: &str,
    context: Option<&JsonValue>,
    options: &NormalizationOptions,
) -> Result<(JsonValue, Vec<TransformWarning>), TransformError> {
    transform_input_with_warnings_with_options(rule, InputData::Text(input), context, options)
}

pub fn transform_input_with_warnings_with_options(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
    options: &NormalizationOptions,
) -> Result<(JsonValue, Vec<TransformWarning>), TransformError> {
    transform_with_warnings_inner(rule, input, context, None, options)
}

pub fn transform_with_warnings_with_base_dir(
    rule: &RuleFile,
    input: &str,
    context: Option<&JsonValue>,
    base_dir: &Path,
) -> Result<(JsonValue, Vec<TransformWarning>), TransformError> {
    transform_with_warnings_with_base_dir_and_options(
        rule,
        input,
        context,
        base_dir,
        &NormalizationOptions::default(),
    )
}

pub fn transform_input_with_warnings_with_base_dir(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
    base_dir: &Path,
) -> Result<(JsonValue, Vec<TransformWarning>), TransformError> {
    transform_input_with_warnings_with_base_dir_and_options(
        rule,
        input,
        context,
        base_dir,
        &NormalizationOptions::default(),
    )
}

pub fn transform_with_warnings_with_base_dir_and_options(
    rule: &RuleFile,
    input: &str,
    context: Option<&JsonValue>,
    base_dir: &Path,
    options: &NormalizationOptions,
) -> Result<(JsonValue, Vec<TransformWarning>), TransformError> {
    transform_input_with_warnings_with_base_dir_and_options(
        rule,
        InputData::Text(input),
        context,
        base_dir,
        options,
    )
}

pub fn transform_input_with_warnings_with_base_dir_and_options(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
    base_dir: &Path,
    options: &NormalizationOptions,
) -> Result<(JsonValue, Vec<TransformWarning>), TransformError> {
    transform_with_warnings_inner(rule, input, context, Some(base_dir), options)
}

fn transform_with_warnings_inner(
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

pub(super) fn transform_record_with_warnings_inner(
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
