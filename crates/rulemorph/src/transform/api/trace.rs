use serde_json::Value as JsonValue;
use std::path::Path;

use crate::error::{TransformError, TransformWarning};
use crate::model::RuleFile;
use crate::normalization::{InputData, NormalizationOptions};
use crate::trace::{
    TraceCollector, TraceEventKind, TracePhase, TransformRecordTraceResult, TransformTraceError,
    TransformTraceOptions, TransformTraceResult,
};

use super::super::{
    BranchContext, EvalLimits, apply_finalize_traced, apply_rule_to_record_traced,
    records::input_records_iter_with_options,
};

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
    let limits = EvalLimits::from(options);
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
            limits,
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
        match apply_finalize_traced(finalize, output, context, limits, collector) {
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
        EvalLimits::default(),
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
                match apply_finalize_traced(
                    finalize,
                    array,
                    context,
                    EvalLimits::default(),
                    &mut collector,
                ) {
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
