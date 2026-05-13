use serde_json::Value as JsonValue;
use std::path::Path;

use crate::error::{TransformError, TransformWarning};
use crate::model::RuleFile;
use crate::normalization::{InputData, NormalizationOptions};
use crate::trace::{
    TraceCollector, TraceEventKind, TracePhase, TransformRecordTraceResult, TransformTraceError,
    TransformTraceOptions, TransformTraceResult,
};

use super::records::input_records_iter_with_options;
use super::stream::{
    transform_stream_input_with_base_dir_and_options, transform_stream_input_with_options,
};
use super::{
    BranchContext, apply_finalize, apply_finalize_traced, apply_rule_to_record,
    apply_rule_to_record_traced,
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

pub fn transform_input_with_trace(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
    trace_options: &TransformTraceOptions,
) -> Result<TransformTraceResult, TransformTraceError> {
    transform_input_with_trace_with_base_dir_and_options(
        rule,
        input,
        context,
        None,
        &NormalizationOptions::default(),
        trace_options,
    )
}

pub fn transform_input_with_trace_with_base_dir_and_options(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
    base_dir: Option<&Path>,
    options: &NormalizationOptions,
    trace_options: &TransformTraceOptions,
) -> Result<TransformTraceResult, TransformTraceError> {
    let mut collector = TraceCollector::new(trace_options.clone());
    match transform_with_warnings_inner_traced(
        rule,
        input,
        context,
        base_dir,
        options,
        &mut collector,
    ) {
        Ok((output, warnings)) => Ok(TransformTraceResult {
            output,
            warnings,
            trace: collector.finish(),
        }),
        Err((error, warnings)) => Err(TransformTraceError {
            error,
            warnings,
            trace: collector.finish(),
        }),
    }
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

fn transform_with_warnings_inner_traced(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&JsonValue>,
    base_dir: Option<&Path>,
    options: &NormalizationOptions,
    collector: &mut TraceCollector,
) -> Result<(JsonValue, Vec<TransformWarning>), (TransformError, Vec<TransformWarning>)> {
    let mut warnings = Vec::new();
    let mut output_records = Vec::new();
    let mut records = input_records_iter_with_options(rule, input, options)
        .map_err(|error| (error, warnings.clone()))?;
    let mut record_index = 0usize;
    while let Some(record) = records.next() {
        let record = record.map_err(|error| (error, warnings.clone()))?;
        collector.start_record(record_index, &record);
        record_index += 1;
        let mut record_warnings = Vec::new();
        let mut branch_context = BranchContext::default();
        match apply_rule_to_record_traced(
            rule,
            &record,
            context,
            &mut record_warnings,
            base_dir,
            &mut branch_context,
            collector,
        ) {
            Ok(Some(output)) => output_records.push(output),
            Ok(None) => {}
            Err(error) => {
                warnings.extend(record_warnings);
                return Err((error, warnings));
            }
        }
        warnings.extend(record_warnings);
    }

    let mut output = JsonValue::Array(output_records);
    if let Some(finalize) = &rule.finalize {
        collector.start_finalize(&output);
        match apply_finalize_traced(finalize, output, context, collector) {
            Ok(finalized) => {
                output = finalized;
                collector
                    .end_span(TraceEventKind::FinalizeEnd, TracePhase::End)
                    .finish(collector);
            }
            Err(error) => {
                collector
                    .error_span(TraceEventKind::Error, "FINALIZE_ERROR", "finalize failed")
                    .finish(collector);
                return Err((error, warnings));
            }
        }
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

pub fn transform_record_with_trace(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    trace_options: &TransformTraceOptions,
) -> Result<TransformRecordTraceResult, TransformTraceError> {
    let mut collector = TraceCollector::new(trace_options.clone());
    collector.start_record(0, record);
    let mut warnings = Vec::new();
    let mut branch_context = BranchContext::default();
    let result = apply_rule_to_record_traced(
        rule,
        record,
        context,
        &mut warnings,
        None,
        &mut branch_context,
        &mut collector,
    );
    match result {
        Ok(output) => {
            let output = if let Some(finalize) = &rule.finalize {
                let Some(value) = output else {
                    return Ok(TransformRecordTraceResult {
                        output: None,
                        warnings,
                        trace: collector.finish(),
                    });
                };
                let mut records = Vec::new();
                records.push(value);
                let array = JsonValue::Array(records);
                collector.start_finalize(&array);
                match apply_finalize_traced(finalize, array, context, &mut collector) {
                    Ok(finalized) => {
                        collector
                            .end_span(TraceEventKind::FinalizeEnd, TracePhase::End)
                            .finish(&mut collector);
                        Some(finalized)
                    }
                    Err(error) => {
                        collector
                            .error_span(TraceEventKind::Error, "FINALIZE_ERROR", "finalize failed")
                            .finish(&mut collector);
                        return Err(TransformTraceError {
                            error,
                            warnings,
                            trace: collector.finish(),
                        });
                    }
                }
            } else {
                output
            };
            Ok(TransformRecordTraceResult {
                output,
                warnings,
                trace: collector.finish(),
            })
        }
        Err(error) => Err(TransformTraceError {
            error,
            warnings,
            trace: collector.finish(),
        }),
    }
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
