use chrono::offset::TimeZone;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime};
use regex::Regex;
use serde_json::{Map, Value as JsonValue};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use crate::cache::LruCache;
use crate::error::{TransformError, TransformErrorKind, TransformWarning};
use crate::model::{Expr, ExprChain, ExprOp, ExprRef, FinalizeSpec, Mapping, RuleFile, V2RuleStep};
use crate::normalization::{
    InputData, NormalizationOptions, NormalizedRecords, normalize_records_with_options,
};
use crate::path::{PathToken, get_path, parse_path};
use crate::trace::{
    TraceCollector, TraceEventKind, TracePhase, TransformRecordTraceResult, TransformTraceError,
    TransformTraceOptions, TransformTraceResult, canonical_acc_path, canonical_context_path,
    canonical_input_path, canonical_item_path, canonical_out_path, canonical_output_path,
};
use crate::v2_eval::{
    EvalItem as V2EvalItem, EvalValue as V2EvalValue, V2EvalContext, eval_v2_condition,
    eval_v2_expr, eval_v2_let_step, eval_v2_op_step, eval_v2_pipe, eval_v2_ref, eval_v2_start,
};
use crate::v2_model::{V2ComparisonOp, V2Condition, V2Pipe, V2Ref, V2Start, V2Step};
use crate::v2_parser::{
    is_literal_escape, is_pipe_value, is_v2_ref, parse_v2_condition, parse_v2_expr,
    parse_v2_pipe_from_value,
};

const REGEX_CACHE_CAPACITY: usize = 128;
const BRANCH_MAX_DEPTH: usize = 64;

#[cfg(test)]
pub(crate) const TRACE_GENERIC_V2_OPERATORS: &[&str] = &[
    "lookup",
    "lookup_first",
    "to_string",
    "pad_start",
    "pad_end",
    "+",
    "-",
    "*",
    "/",
    "multiply",
    "add",
    "subtract",
    "divide",
    "round",
    "to_base",
    "date_format",
    "to_unixtime",
    "and",
    "or",
    "not",
    "==",
    "!=",
    "<",
    "<=",
    ">",
    ">=",
    "~=",
    "eq",
    "ne",
    "lt",
    "lte",
    "gt",
    "gte",
    "match",
    "merge",
    "deep_merge",
    "get",
    "pick",
    "omit",
    "keys",
    "values",
    "entries",
    "len",
    "from_entries",
    "object_flatten",
    "object_unflatten",
    "map",
    "filter",
    "flat_map",
    "flatten",
    "take",
    "drop",
    "slice",
    "chunk",
    "zip",
    "zip_with",
    "unzip",
    "group_by",
    "key_by",
    "partition",
    "unique",
    "distinct_by",
    "sort_by",
    "find",
    "find_index",
    "index_of",
    "contains",
    "sum",
    "avg",
    "min",
    "max",
    "reduce",
    "fold",
    "first",
    "last",
    "string",
    "int",
    "float",
    "bool",
    "trim",
    "uppercase",
    "lowercase",
    "split",
    "replace",
    "concat",
    "coalesce",
];

fn regex_cache() -> &'static Mutex<LruCache<String, Regex>> {
    static REGEX_CACHE: OnceLock<Mutex<LruCache<String, Regex>>> = OnceLock::new();
    REGEX_CACHE.get_or_init(|| Mutex::new(LruCache::new(REGEX_CACHE_CAPACITY)))
}

fn cached_regex(pattern: &str, path: &str) -> Result<Regex, TransformError> {
    let key = pattern.to_string();
    if let Some(regex) = {
        let mut cache = regex_cache().lock().unwrap_or_else(|err| err.into_inner());
        cache.get_cloned(&key)
    } {
        return Ok(regex);
    }

    let regex = Regex::new(pattern).map_err(|_| {
        TransformError::new(TransformErrorKind::ExprError, "regex pattern is invalid")
            .with_path(path)
    })?;
    {
        let mut cache = regex_cache().lock().unwrap_or_else(|err| err.into_inner());
        cache.insert(key, regex.clone());
    }
    Ok(regex)
}

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

#[derive(Debug)]
pub struct TransformStreamItem {
    pub output: Option<JsonValue>,
    pub warnings: Vec<TransformWarning>,
}

/// Output iterator for `transform_stream*` APIs.
///
/// The iterator emits transformed records incrementally, but input normalization
/// is bounded by `NormalizationOptions` and may materialize records internally
/// for formats that require whole-document parsing.
pub struct TransformStream<'a> {
    rule: &'a RuleFile,
    context: Option<&'a JsonValue>,
    records: InputRecordsIter<'a>,
    base_dir: Option<&'a Path>,
    done: bool,
}

impl<'a> TransformStream<'a> {
    fn new(
        rule: &'a RuleFile,
        input: &'a str,
        context: Option<&'a JsonValue>,
        base_dir: Option<&'a Path>,
    ) -> Result<Self, TransformError> {
        Self::new_with_input_and_options(
            rule,
            InputData::Text(input),
            context,
            base_dir,
            &NormalizationOptions::default(),
        )
    }

    fn new_with_options(
        rule: &'a RuleFile,
        input: &'a str,
        context: Option<&'a JsonValue>,
        base_dir: Option<&'a Path>,
        options: &NormalizationOptions,
    ) -> Result<Self, TransformError> {
        Self::new_with_input_and_options(rule, InputData::Text(input), context, base_dir, options)
    }

    fn new_with_input_and_options(
        rule: &'a RuleFile,
        input: InputData<'a>,
        context: Option<&'a JsonValue>,
        base_dir: Option<&'a Path>,
        options: &NormalizationOptions,
    ) -> Result<Self, TransformError> {
        let records = input_records_iter_with_options(rule, input, options)?;
        Ok(Self {
            rule,
            context,
            records,
            base_dir,
            done: false,
        })
    }
}

impl<'a> Iterator for TransformStream<'a> {
    type Item = Result<TransformStreamItem, TransformError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            let record = match self.records.next() {
                None => {
                    self.done = true;
                    return None;
                }
                Some(Ok(record)) => record,
                Some(Err(err)) => {
                    self.done = true;
                    return Some(Err(err));
                }
            };

            let mut warnings = Vec::new();
            let mut branch_context = BranchContext::default();
            match apply_rule_to_record(
                self.rule,
                &record,
                self.context,
                &mut warnings,
                self.base_dir,
                &mut branch_context,
            ) {
                Ok(output) => {
                    if output.is_none() && warnings.is_empty() {
                        continue;
                    }
                    return Some(Ok(TransformStreamItem { output, warnings }));
                }
                Err(err) => {
                    self.done = true;
                    return Some(Err(err));
                }
            }
        }
    }
}

pub fn transform_stream<'a>(
    rule: &'a RuleFile,
    input: &'a str,
    context: Option<&'a JsonValue>,
) -> Result<TransformStream<'a>, TransformError> {
    if rule.finalize.is_some() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "finalize is not supported in stream mode",
        ));
    }
    TransformStream::new(rule, input, context, None)
}

pub fn transform_stream_input<'a>(
    rule: &'a RuleFile,
    input: InputData<'a>,
    context: Option<&'a JsonValue>,
) -> Result<TransformStream<'a>, TransformError> {
    if rule.finalize.is_some() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "finalize is not supported in stream mode",
        ));
    }
    TransformStream::new_with_input_and_options(
        rule,
        input,
        context,
        None,
        &NormalizationOptions::default(),
    )
}

pub fn transform_stream_with_base_dir<'a>(
    rule: &'a RuleFile,
    input: &'a str,
    context: Option<&'a JsonValue>,
    base_dir: &'a Path,
) -> Result<TransformStream<'a>, TransformError> {
    if rule.finalize.is_some() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "finalize is not supported in stream mode",
        ));
    }
    TransformStream::new(rule, input, context, Some(base_dir))
}

pub fn transform_stream_input_with_base_dir<'a>(
    rule: &'a RuleFile,
    input: InputData<'a>,
    context: Option<&'a JsonValue>,
    base_dir: &'a Path,
) -> Result<TransformStream<'a>, TransformError> {
    if rule.finalize.is_some() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "finalize is not supported in stream mode",
        ));
    }
    TransformStream::new_with_input_and_options(
        rule,
        input,
        context,
        Some(base_dir),
        &NormalizationOptions::default(),
    )
}

pub fn transform_stream_with_options<'a>(
    rule: &'a RuleFile,
    input: &'a str,
    context: Option<&'a JsonValue>,
    options: &NormalizationOptions,
) -> Result<TransformStream<'a>, TransformError> {
    if rule.finalize.is_some() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "finalize is not supported in stream mode",
        ));
    }
    TransformStream::new_with_options(rule, input, context, None, options)
}

pub fn transform_stream_input_with_options<'a>(
    rule: &'a RuleFile,
    input: InputData<'a>,
    context: Option<&'a JsonValue>,
    options: &NormalizationOptions,
) -> Result<TransformStream<'a>, TransformError> {
    if rule.finalize.is_some() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "finalize is not supported in stream mode",
        ));
    }
    TransformStream::new_with_input_and_options(rule, input, context, None, options)
}

pub fn transform_stream_with_base_dir_and_options<'a>(
    rule: &'a RuleFile,
    input: &'a str,
    context: Option<&'a JsonValue>,
    base_dir: &'a Path,
    options: &NormalizationOptions,
) -> Result<TransformStream<'a>, TransformError> {
    if rule.finalize.is_some() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "finalize is not supported in stream mode",
        ));
    }
    TransformStream::new_with_options(rule, input, context, Some(base_dir), options)
}

pub fn transform_stream_input_with_base_dir_and_options<'a>(
    rule: &'a RuleFile,
    input: InputData<'a>,
    context: Option<&'a JsonValue>,
    base_dir: &'a Path,
    options: &NormalizationOptions,
) -> Result<TransformStream<'a>, TransformError> {
    if rule.finalize.is_some() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "finalize is not supported in stream mode",
        ));
    }
    TransformStream::new_with_input_and_options(rule, input, context, Some(base_dir), options)
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

fn transform_record_with_warnings_inner(
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

fn apply_mappings(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
) -> Result<JsonValue, TransformError> {
    let mut out = JsonValue::Object(Map::new());
    apply_mappings_into(
        &rule.mappings,
        record,
        context,
        &mut out,
        warnings,
        rule.version,
        "mappings",
    )?;
    Ok(out)
}

fn apply_mappings_into(
    mappings: &[Mapping],
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &mut JsonValue,
    warnings: &mut Vec<TransformWarning>,
    rule_version: u8,
    base_path: &str,
) -> Result<(), TransformError> {
    for (index, mapping) in mappings.iter().enumerate() {
        let mapping_path = format!("{}[{}]", base_path, index);
        if !eval_when(
            mapping,
            record,
            context,
            out,
            &mapping_path,
            warnings,
            rule_version,
        ) {
            continue;
        }
        let value = eval_mapping(mapping, record, context, out, &mapping_path, rule_version)?;
        if let Some(value) = value {
            set_path(out, &mapping.target, value, &mapping_path)?;
        }
    }
    Ok(())
}

fn apply_mappings_traced(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    collector: &mut TraceCollector,
) -> Result<JsonValue, TransformError> {
    let mut out = JsonValue::Object(Map::new());
    apply_mappings_into_traced(
        &rule.mappings,
        record,
        context,
        &mut out,
        warnings,
        rule.version,
        "mappings",
        collector,
    )?;
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn apply_mappings_into_traced(
    mappings: &[Mapping],
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &mut JsonValue,
    warnings: &mut Vec<TransformWarning>,
    rule_version: u8,
    base_path: &str,
    collector: &mut TraceCollector,
) -> Result<(), TransformError> {
    for (index, mapping) in mappings.iter().enumerate() {
        let mapping_path = format!("{}[{}]", base_path, index);
        collector
            .start_span(TraceEventKind::MappingStart, TracePhase::Start)
            .rule_path(&mapping_path)
            .attr_index("mapping_index", index)
            .finish(collector);

        let applied = if mapping.when.is_some() {
            let when_path = format!("{}.when", mapping_path);
            collector
                .start_span(TraceEventKind::MappingWhenStart, TracePhase::Start)
                .rule_path(&when_path)
                .finish(collector);
            let flag = eval_when_traced(
                mapping,
                record,
                context,
                out,
                &mapping_path,
                warnings,
                rule_version,
                collector,
            );
            collector
                .end_span(TraceEventKind::MappingWhenEnd, TracePhase::End)
                .rule_path(&when_path)
                .finish_with_output(collector, &JsonValue::Bool(flag), None);
            flag
        } else {
            true
        };

        collector
            .emit(TraceEventKind::MappingDecision, TracePhase::Instant)
            .rule_path(&mapping_path)
            .attr_bool("applied", applied)
            .attr_enum("skip_reason", if applied { "none" } else { "when_false" })
            .finish(collector);

        if !applied {
            collector
                .end_span(TraceEventKind::MappingEnd, TracePhase::End)
                .rule_path(&mapping_path)
                .finish(collector);
            continue;
        }

        let value = match eval_mapping_traced(
            mapping,
            record,
            context,
            out,
            &mapping_path,
            rule_version,
            collector,
        ) {
            Ok(value) => value,
            Err(error) => {
                collector
                    .error_span(TraceEventKind::Error, "MAPPING_ERROR", "mapping failed")
                    .rule_path(&mapping_path)
                    .finish(collector);
                return Err(error);
            }
        };

        if let Some(value) = value {
            if let Err(error) = set_path(out, &mapping.target, value.clone(), &mapping_path) {
                collector
                    .error_span(TraceEventKind::Error, "MAPPING_ERROR", "mapping failed")
                    .rule_path(&mapping_path)
                    .finish(collector);
                return Err(error);
            }
            let output_redaction_hint = mapping_output_redaction_hint(mapping);
            collector
                .emit(TraceEventKind::OutputWrite, TracePhase::Instant)
                .rule_path(format!("{}.target", mapping_path))
                .output_path(canonical_output_path(&mapping.target))
                .attr_path("target_path", canonical_output_path(&mapping.target))
                .finish_with_output(collector, &value, Some(&output_redaction_hint));
        }

        collector
            .end_span(TraceEventKind::MappingEnd, TracePhase::End)
            .rule_path(&mapping_path)
            .finish(collector);
    }
    Ok(())
}

fn mapping_output_redaction_hint(mapping: &Mapping) -> String {
    let mut hint = mapping.target.clone();
    if let Some(source) = &mapping.source {
        hint.push(' ');
        hint.push_str(source);
    }
    if let Some(expr) = &mapping.expr {
        collect_expr_redaction_hints(expr, &mut hint);
    }
    hint
}

fn collect_expr_redaction_hints(expr: &Expr, hint: &mut String) {
    match expr {
        Expr::Ref(expr_ref) => {
            hint.push(' ');
            hint.push_str(&expr_ref.ref_path);
        }
        Expr::Op(expr_op) => {
            for arg in &expr_op.args {
                collect_expr_redaction_hints(arg, hint);
            }
        }
        Expr::Chain(expr_chain) => {
            for part in &expr_chain.chain {
                collect_expr_redaction_hints(part, hint);
            }
        }
        Expr::Literal(value) => collect_json_redaction_hints(value, hint),
    }
}

fn collect_json_redaction_hints(value: &JsonValue, hint: &mut String) {
    match value {
        JsonValue::String(value) if value.starts_with('@') => {
            hint.push(' ');
            hint.push_str(value);
        }
        JsonValue::Array(values) => {
            for value in values {
                collect_json_redaction_hints(value, hint);
            }
        }
        JsonValue::Object(values) => {
            for value in values.values() {
                collect_json_redaction_hints(value, hint);
            }
        }
        _ => {}
    }
}

fn apply_rule_to_record_traced(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    base_dir: Option<&Path>,
    branch_context: &mut BranchContext,
    collector: &mut TraceCollector,
) -> Result<Option<JsonValue>, TransformError> {
    if let Some(steps) = &rule.steps {
        return apply_steps_traced(
            steps,
            record,
            context,
            warnings,
            rule.version,
            base_dir,
            branch_context,
            collector,
        );
    }

    if rule.record_when.is_some() {
        collector
            .start_span(TraceEventKind::RecordWhenStart, TracePhase::Start)
            .rule_path("record_when")
            .finish(collector);
    }
    let keep = eval_record_when_traced(rule, record, context, warnings, collector);
    if rule.record_when.is_some() {
        collector
            .end_span(TraceEventKind::RecordWhenEnd, TracePhase::End)
            .rule_path("record_when")
            .finish_with_output(collector, &JsonValue::Bool(keep), None);
    }
    collector
        .emit(TraceEventKind::RecordDecision, TracePhase::Instant)
        .attr_bool("kept", keep)
        .finish(collector);
    if !keep {
        return Ok(None);
    }

    let output = apply_mappings_traced(rule, record, context, warnings, collector)?;
    Ok(Some(output))
}

#[allow(clippy::too_many_arguments)]
fn transform_record_with_warnings_inner_traced(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    base_dir: Option<&Path>,
    branch_context: &mut BranchContext,
    collector: &mut TraceCollector,
) -> Result<(Option<JsonValue>, Vec<TransformWarning>), TransformError> {
    let mut warnings = Vec::new();
    let output = apply_rule_to_record_traced(
        rule,
        record,
        context,
        &mut warnings,
        base_dir,
        branch_context,
        collector,
    )?;
    let Some(output) = output else {
        return Ok((None, warnings));
    };

    if let Some(finalize) = &rule.finalize {
        let array = JsonValue::Array(vec![output]);
        collector
            .start_span(TraceEventKind::FinalizeStart, TracePhase::Start)
            .rule_path("finalize")
            .finish_with_output(collector, &array, None);
        match apply_finalize_traced(finalize, array, context, collector) {
            Ok(finalized) => {
                collector
                    .end_span(TraceEventKind::FinalizeEnd, TracePhase::End)
                    .rule_path("finalize")
                    .finish(collector);
                return Ok((Some(finalized), warnings));
            }
            Err(error) => {
                collector
                    .error_span(TraceEventKind::Error, "FINALIZE_ERROR", "finalize failed")
                    .rule_path("finalize")
                    .finish(collector);
                return Err(error);
            }
        }
    }

    Ok((Some(output), warnings))
}

fn apply_rule_to_record(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    base_dir: Option<&Path>,
    branch_context: &mut BranchContext,
) -> Result<Option<JsonValue>, TransformError> {
    if let Some(steps) = &rule.steps {
        return apply_steps(
            steps,
            record,
            context,
            warnings,
            rule.version,
            base_dir,
            branch_context,
        );
    }

    if !eval_record_when(rule, record, context, warnings) {
        return Ok(None);
    }

    let output = apply_mappings(rule, record, context, warnings)?;
    Ok(Some(output))
}

fn apply_steps(
    steps: &[V2RuleStep],
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    rule_version: u8,
    base_dir: Option<&Path>,
    branch_context: &mut BranchContext,
) -> Result<Option<JsonValue>, TransformError> {
    let mut out = JsonValue::Object(Map::new());

    for (step_index, step) in steps.iter().enumerate() {
        let base_path = format!("steps[{}]", step_index);

        if let Some(mappings) = &step.mappings {
            apply_mappings_into(
                mappings,
                record,
                context,
                &mut out,
                warnings,
                rule_version,
                &format!("{}.mappings", base_path),
            )?;
            continue;
        }

        if let Some(expr) = &step.record_when {
            let when_path = format!("{}.record_when", base_path);
            let keep = eval_when_expr(expr, record, context, &out, &when_path, rule_version)?;
            if !keep {
                return Ok(None);
            }
            continue;
        }

        if let Some(asserts) = &step.asserts {
            for (assert_index, assert) in asserts.iter().enumerate() {
                let assert_path = format!("{}.asserts[{}]", base_path, assert_index);
                let ok = eval_when_expr(
                    &assert.when,
                    record,
                    context,
                    &out,
                    &format!("{}.when", assert_path),
                    rule_version,
                )?;
                if !ok {
                    return Err(TransformError::new(
                        TransformErrorKind::AssertionFailed,
                        format!(
                            "assert failed: {}: {}",
                            assert.error.code, assert.error.message
                        ),
                    )
                    .with_path(assert_path));
                }
            }
            continue;
        }

        if let Some(branch) = &step.branch {
            let branch_path = format!("{}.branch", base_path);
            let take = eval_when_expr(
                &branch.when,
                record,
                context,
                &out,
                &format!("{}.when", branch_path),
                rule_version,
            )?;
            let (target, target_field) = if take {
                (Some(branch.then.as_str()), "then")
            } else {
                (branch.r#else.as_deref(), "else")
            };
            if let Some(target) = target {
                let branch_path_guard = branch_context
                    .enter(base_dir, target)
                    .map_err(|err| err.with_path(format!("{}.{}", branch_path, target_field)))?;
                let (branch_rule, branch_base_dir) =
                    load_rule_from_path(base_dir, target, branch_context.allowed_root()).map_err(
                        |err| err.with_path(format!("{}.{}", branch_path, target_field)),
                    )?;
                let branch_input = out.clone();
                let (branch_output, branch_warnings) = transform_record_with_warnings_inner(
                    &branch_rule,
                    &branch_input,
                    context,
                    Some(&branch_base_dir),
                    branch_context,
                )?;
                branch_context.exit(branch_path_guard);
                warnings.extend(branch_warnings);
                let Some(branch_output) = branch_output else {
                    return Ok(None);
                };

                if branch.return_ {
                    return Ok(Some(branch_output));
                }
                merge_branch_output(&mut out, &branch_output, &branch_path)?;
            }
            continue;
        }
    }

    Ok(Some(out))
}

enum TracedStepOutcome {
    Continue,
    DropRecord,
    Return(JsonValue),
}

#[allow(clippy::too_many_arguments)]
fn apply_steps_traced(
    steps: &[V2RuleStep],
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    rule_version: u8,
    base_dir: Option<&Path>,
    branch_context: &mut BranchContext,
    collector: &mut TraceCollector,
) -> Result<Option<JsonValue>, TransformError> {
    let mut out = JsonValue::Object(Map::new());

    for (step_index, step) in steps.iter().enumerate() {
        let base_path = format!("steps[{}]", step_index);
        collector
            .start_span(TraceEventKind::StepStart, TracePhase::Start)
            .rule_path(&base_path)
            .attr_index("step_index", step_index)
            .finish(collector);

        let step_result = (|| -> Result<TracedStepOutcome, TransformError> {
            if let Some(mappings) = &step.mappings {
                apply_mappings_into_traced(
                    mappings,
                    record,
                    context,
                    &mut out,
                    warnings,
                    rule_version,
                    &format!("{}.mappings", base_path),
                    collector,
                )?;
                return Ok(TracedStepOutcome::Continue);
            }

            if let Some(expr) = &step.record_when {
                let when_path = format!("{}.record_when", base_path);
                collector
                    .start_span(TraceEventKind::RecordWhenStart, TracePhase::Start)
                    .rule_path(&when_path)
                    .finish(collector);
                let keep = match eval_when_expr_traced(
                    expr,
                    record,
                    context,
                    &out,
                    &when_path,
                    rule_version,
                    collector,
                ) {
                    Ok(keep) => keep,
                    Err(error) => {
                        collector
                            .error_span(
                                TraceEventKind::Error,
                                "RECORD_WHEN_ERROR",
                                "record_when failed",
                            )
                            .rule_path(&when_path)
                            .finish(collector);
                        return Err(error);
                    }
                };
                collector
                    .end_span(TraceEventKind::RecordWhenEnd, TracePhase::End)
                    .rule_path(&when_path)
                    .finish_with_output(collector, &JsonValue::Bool(keep), None);
                collector
                    .emit(TraceEventKind::RecordDecision, TracePhase::Instant)
                    .rule_path(&when_path)
                    .attr_bool("kept", keep)
                    .finish(collector);
                if !keep {
                    return Ok(TracedStepOutcome::DropRecord);
                }
                return Ok(TracedStepOutcome::Continue);
            }

            if let Some(asserts) = &step.asserts {
                for (assert_index, assert) in asserts.iter().enumerate() {
                    let assert_path = format!("{}.asserts[{}]", base_path, assert_index);
                    let ok = eval_when_expr(
                        &assert.when,
                        record,
                        context,
                        &out,
                        &format!("{}.when", assert_path),
                        rule_version,
                    )?;
                    collector
                        .emit(TraceEventKind::AssertEval, TracePhase::Instant)
                        .rule_path(&assert_path)
                        .attr_index("assert_index", assert_index)
                        .finish_with_output(collector, &JsonValue::Bool(ok), None);
                    if !ok {
                        return Err(TransformError::new(
                            TransformErrorKind::AssertionFailed,
                            format!(
                                "assert failed: {}: {}",
                                assert.error.code, assert.error.message
                            ),
                        )
                        .with_path(assert_path));
                    }
                }
                return Ok(TracedStepOutcome::Continue);
            }

            if let Some(branch) = &step.branch {
                let branch_path = format!("{}.branch", base_path);
                let take = eval_when_expr(
                    &branch.when,
                    record,
                    context,
                    &out,
                    &format!("{}.when", branch_path),
                    rule_version,
                )?;
                collector
                    .emit(TraceEventKind::BranchEval, TracePhase::Instant)
                    .rule_path(format!("{}.when", branch_path))
                    .finish_with_output(collector, &JsonValue::Bool(take), None);
                let (target, target_field) = if take {
                    (Some(branch.then.as_str()), "then")
                } else {
                    (branch.r#else.as_deref(), "else")
                };
                if let Some(target) = target {
                    collector
                        .start_span(TraceEventKind::BranchTaken, TracePhase::Start)
                        .rule_path(&branch_path)
                        .attr_enum("selected_branch", target_field)
                        .finish(collector);
                    let branch_path_guard = match branch_context.enter(base_dir, target) {
                        Ok(guard) => guard,
                        Err(err) => {
                            collector
                                .error_span(TraceEventKind::Error, "BRANCH_ERROR", "branch failed")
                                .rule_path(&branch_path)
                                .finish(collector);
                            return Err(err.with_path(format!("{}.{}", branch_path, target_field)));
                        }
                    };
                    let branch_result = (|| {
                        let (branch_rule, branch_base_dir) =
                            load_rule_from_path(base_dir, target, branch_context.allowed_root())
                                .map_err(|err| {
                                    err.with_path(format!("{}.{}", branch_path, target_field))
                                })?;
                        let branch_input = out.clone();
                        transform_record_with_warnings_inner_traced(
                            &branch_rule,
                            &branch_input,
                            context,
                            Some(&branch_base_dir),
                            branch_context,
                            collector,
                        )
                    })();
                    branch_context.exit(branch_path_guard);
                    let (branch_output, branch_warnings) = match branch_result {
                        Ok(output) => output,
                        Err(error) => {
                            collector
                                .error_span(TraceEventKind::Error, "BRANCH_ERROR", "branch failed")
                                .rule_path(&branch_path)
                                .finish(collector);
                            return Err(error);
                        }
                    };
                    warnings.extend(branch_warnings);
                    let Some(branch_output) = branch_output else {
                        collector
                            .end_span(TraceEventKind::BranchTaken, TracePhase::End)
                            .rule_path(&branch_path)
                            .finish(collector);
                        return Ok(TracedStepOutcome::DropRecord);
                    };

                    if branch.return_ {
                        collector
                            .end_span(TraceEventKind::BranchTaken, TracePhase::End)
                            .rule_path(&branch_path)
                            .finish_with_output(collector, &branch_output, None);
                        return Ok(TracedStepOutcome::Return(branch_output));
                    }
                    if let Err(error) = merge_branch_output(&mut out, &branch_output, &branch_path)
                    {
                        collector
                            .error_span(TraceEventKind::Error, "BRANCH_ERROR", "branch failed")
                            .rule_path(&branch_path)
                            .finish(collector);
                        return Err(error);
                    }
                    collector
                        .emit(TraceEventKind::BranchMerge, TracePhase::Instant)
                        .rule_path(&branch_path)
                        .finish_with_output(collector, &out, None);
                    collector
                        .end_span(TraceEventKind::BranchTaken, TracePhase::End)
                        .rule_path(&branch_path)
                        .finish(collector);
                }
                return Ok(TracedStepOutcome::Continue);
            }

            Ok(TracedStepOutcome::Continue)
        })();

        match step_result {
            Ok(TracedStepOutcome::DropRecord) => {
                collector
                    .end_span(TraceEventKind::StepStart, TracePhase::End)
                    .rule_path(&base_path)
                    .finish(collector);
                return Ok(None);
            }
            Ok(TracedStepOutcome::Return(value)) => {
                collector
                    .end_span(TraceEventKind::StepStart, TracePhase::End)
                    .rule_path(&base_path)
                    .finish_with_output(collector, &value, None);
                return Ok(Some(value));
            }
            Ok(TracedStepOutcome::Continue) => {
                collector
                    .end_span(TraceEventKind::StepStart, TracePhase::End)
                    .rule_path(&base_path)
                    .finish(collector);
            }
            Err(error) => {
                collector
                    .error_span(TraceEventKind::Error, "STEP_ERROR", "step failed")
                    .rule_path(&base_path)
                    .finish(collector);
                return Err(error);
            }
        }
    }

    Ok(Some(out))
}

fn merge_branch_output(
    out: &mut JsonValue,
    other: &JsonValue,
    path: &str,
) -> Result<(), TransformError> {
    let out_map = out.as_object_mut().ok_or_else(|| {
        TransformError::new(TransformErrorKind::InvalidTarget, "output must be object")
            .with_path(path)
    })?;
    let other_map = other.as_object().ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::InvalidTarget,
            "branch output must be object",
        )
        .with_path(path)
    })?;
    merge_object_maps(out_map, other_map);
    Ok(())
}

#[derive(Default)]
struct BranchContext {
    stack: Vec<PathBuf>,
    allowed_root: Option<PathBuf>,
}

impl BranchContext {
    fn enter(
        &mut self,
        base_dir: Option<&Path>,
        target: &str,
    ) -> Result<BranchPathGuard, TransformError> {
        if self.stack.len() >= BRANCH_MAX_DEPTH {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "branch rule depth limit exceeded",
            ));
        }
        let resolved = resolve_rule_path(base_dir, target);
        let canonical = resolved.canonicalize().map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to resolve branch rule: {}", err),
            )
        })?;
        let allowed_root = match (&self.allowed_root, base_dir) {
            (Some(root), _) => Some(root.clone()),
            (None, Some(base_dir)) => Some(base_dir.canonicalize().map_err(|err| {
                TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to resolve branch base directory: {}", err),
                )
            })?),
            (None, None) => None,
        };
        if let Some(root) = &allowed_root {
            if !canonical.starts_with(root) {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    "branch rule path must stay under the base directory",
                ));
            }
        }
        if self.stack.iter().any(|path| path == &canonical) {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "branch rule cycle detected",
            ));
        }
        if self.allowed_root.is_none() {
            self.allowed_root = allowed_root;
        }
        self.stack.push(canonical);
        Ok(BranchPathGuard)
    }

    fn allowed_root(&self) -> Option<&Path> {
        self.allowed_root.as_deref()
    }

    fn exit(&mut self, _guard: BranchPathGuard) {
        self.stack.pop();
    }
}

struct BranchPathGuard;

fn merge_object_maps(out_map: &mut Map<String, JsonValue>, other_map: &Map<String, JsonValue>) {
    for (key, other_value) in other_map {
        match (out_map.get_mut(key), other_value) {
            (Some(JsonValue::Object(out_obj)), JsonValue::Object(other_obj)) => {
                merge_object_maps(out_obj, other_obj);
            }
            _ => {
                out_map.insert(key.clone(), other_value.clone());
            }
        }
    }
}

fn load_rule_from_path(
    base_dir: Option<&Path>,
    path: &str,
    allowed_root: Option<&Path>,
) -> Result<(RuleFile, PathBuf), TransformError> {
    let resolved = resolve_rule_path(base_dir, path);
    if let Some(allowed_root) = allowed_root {
        let canonical_resolved = resolved.canonicalize().map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to resolve branch rule: {}", err),
            )
            .with_path(path)
        })?;
        if !canonical_resolved.starts_with(allowed_root) {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "branch rule path must stay under the base directory",
            )
            .with_path(path));
        }
    }
    let yaml = std::fs::read_to_string(&resolved).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to read rule: {}", err),
        )
        .with_path(path)
    })?;
    let format = crate::RuleFormat::from_path(&resolved);
    let rule = crate::parse_rule_file_with_format(&yaml, format).map_err(|err| {
        TransformError::new(TransformErrorKind::InvalidInput, err.to_string()).with_path(path)
    })?;
    let resolved_base = resolved
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    Ok((rule, resolved_base))
}

fn resolve_rule_path(base_dir: Option<&Path>, path: &str) -> PathBuf {
    let rule_path = PathBuf::from(path);
    if rule_path.is_absolute() {
        rule_path
    } else if let Some(base_dir) = base_dir {
        base_dir.join(rule_path)
    } else {
        rule_path
    }
}

fn apply_finalize(
    finalize: &FinalizeSpec,
    output: JsonValue,
    context: Option<&JsonValue>,
) -> Result<JsonValue, TransformError> {
    let mut records = match output {
        JsonValue::Array(records) => records,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "finalize expects array output",
            )
            .with_path("finalize"));
        }
    };

    if let Some(filter) = &finalize.filter {
        let raw = expr_to_json_for_v2_condition(filter).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "finalize.filter must be a v2 condition",
            )
            .with_path("finalize.filter")
        })?;
        let cond = parse_v2_condition(&raw).map_err(|err| {
            TransformError::new(
                TransformErrorKind::ExprError,
                format!("invalid v2 condition: {}", err),
            )
            .with_path("finalize.filter")
        })?;
        let base_out = JsonValue::Array(records.clone());
        let mut filtered = Vec::new();
        for (index, item) in records.iter().enumerate() {
            let ctx = V2EvalContext::new().with_item(V2EvalItem { value: item, index });
            let keep = eval_v2_condition(&cond, item, context, &base_out, "finalize.filter", &ctx)?;
            if keep {
                filtered.push(item.clone());
            }
        }
        records = filtered;
    }

    if let Some(sort) = &finalize.sort {
        let tokens = parse_path(&sort.by).map_err(|_| {
            TransformError::new(
                TransformErrorKind::InvalidRecordsPath,
                "finalize.sort.by is invalid",
            )
            .with_path("finalize.sort.by")
        })?;

        struct SortItem {
            key: SortKey,
            index: usize,
            value: JsonValue,
        }

        let mut items = Vec::with_capacity(records.len());
        for (index, item) in records.iter().enumerate() {
            let key_value = get_path(item, &tokens).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::InvalidRef,
                    "finalize.sort.by path not found",
                )
                .with_path("finalize.sort.by")
            })?;
            let key = sort_key_from_value(key_value, "finalize.sort.by")?;
            items.push(SortItem {
                key,
                index,
                value: item.clone(),
            });
        }

        items.sort_by(|left, right| {
            let mut ordering = compare_sort_keys(&left.key, &right.key);
            if sort.order == "desc" {
                ordering = ordering.reverse();
            }
            if ordering == Ordering::Equal {
                left.index.cmp(&right.index)
            } else {
                ordering
            }
        });

        records = items.into_iter().map(|item| item.value).collect();
    }

    if let Some(offset) = finalize.offset {
        if offset > 0 && offset < records.len() {
            records = records.split_off(offset);
        } else if offset >= records.len() {
            records = Vec::new();
        }
    }

    if let Some(limit) = finalize.limit {
        if limit < records.len() {
            records.truncate(limit);
        }
    }

    let output = JsonValue::Array(records);
    if let Some(wrap) = &finalize.wrap {
        let wrapped = eval_wrap_value(wrap, &output, context, "finalize.wrap")?;
        return Ok(wrapped);
    }

    Ok(output)
}

fn apply_finalize_traced(
    finalize: &FinalizeSpec,
    output: JsonValue,
    context: Option<&JsonValue>,
    collector: &mut TraceCollector,
) -> Result<JsonValue, TransformError> {
    let mut records = match output {
        JsonValue::Array(records) => records,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "finalize expects array output",
            )
            .with_path("finalize"));
        }
    };

    if let Some(filter) = &finalize.filter {
        let raw = expr_to_json_for_v2_condition(filter).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "finalize.filter must be a v2 condition",
            )
            .with_path("finalize.filter")
        })?;
        let cond = parse_v2_condition(&raw).map_err(|err| {
            TransformError::new(
                TransformErrorKind::ExprError,
                format!("invalid v2 condition: {}", err),
            )
            .with_path("finalize.filter")
        })?;
        let base_out = JsonValue::Array(records.clone());
        let before_count = records.len();
        let mut filtered = Vec::new();
        for (index, item) in records.iter().enumerate() {
            let ctx = V2EvalContext::new().with_item(V2EvalItem { value: item, index });
            let item_path = format!("finalize.filter[{}]", index);
            let keep = eval_v2_condition_traced(
                &cond, item, context, &base_out, &item_path, &ctx, collector,
            )?;
            collector
                .emit(TraceEventKind::FinalizeFilter, TracePhase::Instant)
                .rule_path(&item_path)
                .input_path(canonical_item_path(""))
                .attr_index("item_index", index)
                .attr_bool("kept", keep)
                .input_value(item, collector.options(), Some("@item"))
                .finish_with_output(collector, &JsonValue::Bool(keep), None);
            if keep {
                filtered.push(item.clone());
            }
        }
        records = filtered;
        collector
            .emit(TraceEventKind::FinalizeFilter, TracePhase::Instant)
            .rule_path("finalize.filter")
            .attr_count("input_count", before_count)
            .attr_count("output_count", records.len())
            .finish_with_output(collector, &JsonValue::Array(records.clone()), None);
    }

    if let Some(sort) = &finalize.sort {
        let tokens = parse_path(&sort.by).map_err(|_| {
            TransformError::new(
                TransformErrorKind::InvalidRecordsPath,
                "finalize.sort.by is invalid",
            )
            .with_path("finalize.sort.by")
        })?;

        struct SortItem {
            key: SortKey,
            index: usize,
            value: JsonValue,
        }

        let mut items = Vec::with_capacity(records.len());
        for (index, item) in records.iter().enumerate() {
            let key_value = get_path(item, &tokens).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::InvalidRef,
                    "finalize.sort.by path not found",
                )
                .with_path("finalize.sort.by")
            })?;
            let key = sort_key_from_value(key_value, "finalize.sort.by")?;
            items.push(SortItem {
                key,
                index,
                value: item.clone(),
            });
        }

        items.sort_by(|left, right| {
            let mut ordering = compare_sort_keys(&left.key, &right.key);
            if sort.order == "desc" {
                ordering = ordering.reverse();
            }
            if ordering == Ordering::Equal {
                left.index.cmp(&right.index)
            } else {
                ordering
            }
        });

        for (to_index, item) in items.iter().enumerate() {
            collector
                .emit(TraceEventKind::FinalizeSort, TracePhase::Instant)
                .rule_path(format!("finalize.sort[{}]", item.index))
                .attr_index("from_index", item.index)
                .attr_index("to_index", to_index)
                .attr_enum("order", if sort.order == "desc" { "desc" } else { "asc" })
                .input_value(&item.value, collector.options(), Some("@item"))
                .finish_with_output(collector, &sort_key_to_json(&item.key), None);
        }

        records = items.into_iter().map(|item| item.value).collect();
        collector
            .emit(TraceEventKind::FinalizeSort, TracePhase::Instant)
            .rule_path("finalize.sort")
            .attr_enum("order", if sort.order == "desc" { "desc" } else { "asc" })
            .finish_with_output(collector, &JsonValue::Array(records.clone()), None);
    }

    if let Some(offset) = finalize.offset {
        if offset > 0 && offset < records.len() {
            records = records.split_off(offset);
        } else if offset >= records.len() {
            records = Vec::new();
        }
        collector
            .emit(TraceEventKind::FinalizeOffset, TracePhase::Instant)
            .rule_path("finalize.offset")
            .attr_index("offset", offset)
            .finish_with_output(collector, &JsonValue::Array(records.clone()), None);
    }

    if let Some(limit) = finalize.limit {
        if limit < records.len() {
            records.truncate(limit);
        }
        collector
            .emit(TraceEventKind::FinalizeLimit, TracePhase::Instant)
            .rule_path("finalize.limit")
            .attr_count("limit", limit)
            .finish_with_output(collector, &JsonValue::Array(records.clone()), None);
    }

    let output = JsonValue::Array(records);
    if let Some(wrap) = &finalize.wrap {
        let wrapped = eval_wrap_value(wrap, &output, context, "finalize.wrap")?;
        collector
            .emit(TraceEventKind::FinalizeWrap, TracePhase::Instant)
            .rule_path("finalize.wrap")
            .finish_with_output(collector, &wrapped, None);
        return Ok(wrapped);
    }

    Ok(output)
}

fn eval_wrap_value(
    value: &JsonValue,
    out: &JsonValue,
    context: Option<&JsonValue>,
    path: &str,
) -> Result<JsonValue, TransformError> {
    match value {
        JsonValue::Object(map) => {
            let mut out_map = serde_json::Map::new();
            for (key, value) in map {
                let child_path = format!("{}.{}", path, key);
                out_map.insert(
                    key.clone(),
                    eval_wrap_value(value, out, context, &child_path)?,
                );
            }
            Ok(JsonValue::Object(out_map))
        }
        _ => {
            let expr = parse_v2_expr(value).map_err(|err| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("invalid v2 expr: {}", err),
                )
                .with_path(path)
            })?;
            let ctx = V2EvalContext::new();
            match eval_v2_expr(&expr, out, context, out, path, &ctx)? {
                V2EvalValue::Missing => Ok(JsonValue::Null),
                V2EvalValue::Value(value) => Ok(value),
            }
        }
    }
}

fn sort_key_from_value(value: &JsonValue, path: &str) -> Result<SortKey, TransformError> {
    match value {
        JsonValue::Number(number) => number.as_f64().map(SortKey::Number).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "sort key must be a finite number",
            )
            .with_path(path)
        }),
        JsonValue::String(value) => Ok(SortKey::String(value.clone())),
        JsonValue::Bool(value) => Ok(SortKey::Bool(*value)),
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "sort key must be string/number/bool",
        )
        .with_path(path)),
    }
}

fn input_records_iter_with_options<'a>(
    rule: &RuleFile,
    input: InputData<'a>,
    options: &NormalizationOptions,
) -> Result<InputRecordsIter<'a>, TransformError> {
    Ok(InputRecordsIter::Normalized(
        normalize_records_with_options(rule, input, options)?,
    ))
}

enum InputRecordsIter<'a> {
    Normalized(NormalizedRecords<'a>),
}

impl Iterator for InputRecordsIter<'_> {
    type Item = Result<JsonValue, TransformError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            InputRecordsIter::Normalized(iter) => iter.next(),
        }
    }
}

fn eval_mapping(
    mapping: &crate::model::Mapping,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
    version: u8,
) -> Result<Option<JsonValue>, TransformError> {
    let value = if let Some(source) = &mapping.source {
        resolve_source(source, record, context, out, mapping_path)?
    } else if let Some(literal) = &mapping.value {
        EvalValue::Value(literal.clone())
    } else if let Some(expr) = &mapping.expr {
        // Check if this is a v2 expression (version 2)
        if version >= 2 {
            let expr_path = format!("{}.expr", mapping_path);
            // Try to interpret as v2 pipe
            let v2_json = expr_to_json_for_v2_pipe(expr);
            if let Some(json_val) = v2_json {
                let v2_pipe = parse_v2_pipe_from_value(&json_val).map_err(|e| {
                    TransformError::new(TransformErrorKind::ExprError, e.to_string())
                        .with_path(&expr_path)
                })?;
                let v2_ctx = V2EvalContext::new();
                let v2_result = eval_v2_pipe(&v2_pipe, record, context, out, &expr_path, &v2_ctx)?;
                // Convert v2 EvalValue to v1 EvalValue
                match v2_result {
                    V2EvalValue::Missing => EvalValue::Missing,
                    V2EvalValue::Value(v) => EvalValue::Value(v),
                }
            } else {
                // v2 but not a v2 pipe - use v1 eval
                eval_expr(expr, record, context, out, &expr_path, None)?
            }
        } else {
            // v1 rule - use v1 eval
            eval_expr(
                expr,
                record,
                context,
                out,
                &format!("{}.expr", mapping_path),
                None,
            )?
        }
    } else {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "mapping must define source, value, or expr",
        )
        .with_path(mapping_path));
    };

    let mut value = match value {
        EvalValue::Missing => {
            if let Some(default) = &mapping.default {
                default.clone()
            } else if mapping.required {
                return Err(TransformError::new(
                    TransformErrorKind::MissingRequired,
                    "required value is missing",
                )
                .with_path(mapping_path));
            } else {
                return Ok(None);
            }
        }
        EvalValue::Value(value) => value,
    };

    if value.is_null() {
        if mapping.required {
            return Err(TransformError::new(
                TransformErrorKind::MissingRequired,
                "required value is null",
            )
            .with_path(mapping_path));
        }
        return Ok(Some(value));
    }

    if let Some(type_name) = &mapping.value_type {
        value = cast_value(&value, type_name, &format!("{}.type", mapping_path))?;
    }

    Ok(Some(value))
}

fn eval_mapping_traced(
    mapping: &crate::model::Mapping,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
    version: u8,
    collector: &mut TraceCollector,
) -> Result<Option<JsonValue>, TransformError> {
    let value = if let Some(source) = &mapping.source {
        let value = resolve_source(source, record, context, out, mapping_path)?;
        collector
            .emit(TraceEventKind::SourceRead, TracePhase::Instant)
            .rule_path(format!("{}.source", mapping_path))
            .input_path(canonical_source_path(source))
            .finish_with_eval_output(collector, &value, Some(source));
        value
    } else if let Some(literal) = &mapping.value {
        collector
            .emit(TraceEventKind::LiteralEval, TracePhase::Instant)
            .rule_path(format!("{}.value", mapping_path))
            .finish_with_output(collector, literal, None);
        EvalValue::Value(literal.clone())
    } else if let Some(expr) = &mapping.expr {
        if version >= 2 {
            let expr_path = format!("{}.expr", mapping_path);
            let v2_json = expr_to_json_for_v2_pipe(expr);
            if let Some(json_val) = v2_json {
                let v2_pipe = parse_v2_pipe_from_value(&json_val).map_err(|e| {
                    TransformError::new(TransformErrorKind::ExprError, e.to_string())
                        .with_path(&expr_path)
                })?;
                let v2_ctx = V2EvalContext::new();
                let v2_result = eval_v2_pipe_traced(
                    &v2_pipe, record, context, out, &expr_path, &v2_ctx, collector,
                )?;
                match v2_result {
                    V2EvalValue::Missing => EvalValue::Missing,
                    V2EvalValue::Value(v) => EvalValue::Value(v),
                }
            } else {
                eval_expr_traced(expr, record, context, out, &expr_path, None, collector)?
            }
        } else {
            eval_expr_traced(
                expr,
                record,
                context,
                out,
                &format!("{}.expr", mapping_path),
                None,
                collector,
            )?
        }
    } else {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "mapping must define source, value, or expr",
        )
        .with_path(mapping_path));
    };

    let mut value = match value {
        EvalValue::Missing => {
            if let Some(default) = &mapping.default {
                collector
                    .emit(TraceEventKind::DefaultApplied, TracePhase::Instant)
                    .rule_path(format!("{}.default", mapping_path))
                    .finish_with_output(collector, default, None);
                default.clone()
            } else if mapping.required {
                return Err(TransformError::new(
                    TransformErrorKind::MissingRequired,
                    "required value is missing",
                )
                .with_path(mapping_path));
            } else {
                return Ok(None);
            }
        }
        EvalValue::Value(value) => value,
    };

    if value.is_null() {
        if mapping.required {
            return Err(TransformError::new(
                TransformErrorKind::MissingRequired,
                "required value is null",
            )
            .with_path(mapping_path));
        }
        return Ok(Some(value));
    }

    if let Some(type_name) = &mapping.value_type {
        value = cast_value(&value, type_name, &format!("{}.type", mapping_path))?;
        collector
            .emit(TraceEventKind::TypeCast, TracePhase::Instant)
            .rule_path(format!("{}.type", mapping_path))
            .finish_with_output(collector, &value, None);
    }

    Ok(Some(value))
}

fn canonical_source_path(source: &str) -> String {
    match parse_source(source) {
        Ok((Namespace::Input, path)) => canonical_input_path(path),
        Ok((Namespace::Context, path)) => canonical_context_path(path),
        Ok((Namespace::Out, path)) => canonical_out_path(path),
        _ => canonical_input_path(source),
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_pipe_traced<'a>(
    pipe: &V2Pipe,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    base_path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    collector
        .start_span(TraceEventKind::ExprStart, TracePhase::Start)
        .rule_path(base_path)
        .finish(collector);

    let mut current = match eval_v2_start(&pipe.start, record, context, out, base_path, ctx) {
        Ok(value) => {
            emit_v2_start_trace(&pipe.start, &value, base_path, collector);
            value
        }
        Err(error) => {
            collector
                .error_span(TraceEventKind::Error, "EXPR_ERROR", "expression failed")
                .rule_path(base_path)
                .finish(collector);
            return Err(error);
        }
    };
    let mut current_ctx = ctx.clone();

    for (step_index, step) in pipe.steps.iter().enumerate() {
        let step_path = format!("{}[{}]", base_path, step_index + 1);
        let step_ctx = current_ctx.clone().with_pipe_value(current.clone());
        let (next, next_ctx) = match eval_v2_step_traced(
            step, current, record, context, out, &step_path, &step_ctx, collector,
        ) {
            Ok(result) => result,
            Err(error) => {
                collector
                    .error_span(TraceEventKind::Error, "EXPR_ERROR", "expression failed")
                    .rule_path(base_path)
                    .finish(collector);
                return Err(error);
            }
        };
        current = next;
        current_ctx = next_ctx;
    }

    collector
        .end_span(TraceEventKind::ExprEnd, TracePhase::End)
        .rule_path(base_path)
        .finish_with_v2_eval_output(collector, &current, None);
    Ok(current)
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_step_traced<'a>(
    step: &V2Step,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    step_path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<(V2EvalValue, V2EvalContext<'a>), TransformError> {
    match step {
        V2Step::Op(op) => {
            collector
                .start_span(TraceEventKind::OpStart, TracePhase::Start)
                .rule_path(step_path)
                .operator(&op.op)
                .input_v2_eval_value(&pipe_value, collector.options(), None)
                .attr_count("arg_count", op.args.len())
                .finish(collector);
            let result = if v2_operator_has_item_level_trace(&op.op) {
                eval_v2_collection_op_traced(
                    op,
                    pipe_value.clone(),
                    record,
                    context,
                    out,
                    step_path,
                    ctx,
                    collector,
                )
            } else if v2_operator_has_eager_args(&op.op) {
                eval_v2_eager_op_traced(
                    op,
                    pipe_value.clone(),
                    record,
                    context,
                    out,
                    step_path,
                    ctx,
                    collector,
                )
            } else if v2_operator_has_lazy_arg_trace(&op.op) {
                eval_v2_lazy_op_traced(
                    op,
                    pipe_value.clone(),
                    record,
                    context,
                    out,
                    step_path,
                    ctx,
                    collector,
                )
            } else {
                eval_v2_op_step(op, pipe_value.clone(), record, context, out, step_path, ctx)
            };
            let output = match result {
                Ok(output) => output,
                Err(error) => {
                    collector
                        .error_span(TraceEventKind::OpError, "OP_ERROR", "operator failed")
                        .rule_path(step_path)
                        .operator(&op.op)
                        .input_v2_eval_value(&pipe_value, collector.options(), None)
                        .finish(collector);
                    return Err(error);
                }
            };
            collector
                .end_span(TraceEventKind::OpEnd, TracePhase::End)
                .rule_path(step_path)
                .operator(&op.op)
                .input_v2_eval_value(&pipe_value, collector.options(), None)
                .finish_with_v2_eval_output(collector, &output, None);
            Ok((output, ctx.clone()))
        }
        V2Step::Map(map) => {
            collector
                .start_span(TraceEventKind::OpStart, TracePhase::Start)
                .rule_path(step_path)
                .operator("map")
                .input_v2_eval_value(&pipe_value, collector.options(), None)
                .finish(collector);
            let arr = match &pipe_value {
                V2EvalValue::Missing => {
                    collector
                        .end_span(TraceEventKind::OpEnd, TracePhase::End)
                        .rule_path(step_path)
                        .operator("map")
                        .finish_with_v2_eval_output(collector, &V2EvalValue::Missing, None);
                    return Ok((V2EvalValue::Missing, ctx.clone()));
                }
                V2EvalValue::Value(JsonValue::Array(arr)) => arr,
                V2EvalValue::Value(_) => {
                    collector
                        .error_span(TraceEventKind::OpError, "OP_ERROR", "operator failed")
                        .rule_path(step_path)
                        .operator("map")
                        .finish(collector);
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "map step requires array",
                    )
                    .with_path(step_path));
                }
            };
            let mut results = Vec::with_capacity(arr.len());
            for (index, item_value) in arr.iter().enumerate() {
                let item_path = format!("{}[{}]", step_path, index);
                let item_eval_value = V2EvalValue::Value(item_value.clone());
                let item_ctx = ctx
                    .clone()
                    .with_pipe_value(item_eval_value.clone())
                    .with_item(V2EvalItem {
                        value: item_value,
                        index,
                    });
                let mut current = item_eval_value;
                let mut step_ctx = item_ctx.clone();

                collector
                    .start_span(TraceEventKind::CollectionItemStart, TracePhase::Start)
                    .rule_path(&item_path)
                    .input_path(canonical_item_path(""))
                    .attr_index("item_index", index)
                    .attr_enum("scope", "item")
                    .input_value(item_value, collector.options(), Some("@item"))
                    .finish(collector);

                for (nested_index, nested_step) in map.steps.iter().enumerate() {
                    let nested_ctx = step_ctx.clone().with_pipe_value(current.clone());
                    let (next, next_ctx) = match eval_v2_step_traced(
                        nested_step,
                        current,
                        record,
                        context,
                        out,
                        &format!("{}.step[{}]", item_path, nested_index),
                        &nested_ctx,
                        collector,
                    ) {
                        Ok(result) => result,
                        Err(error) => {
                            collector
                                .error_span(
                                    TraceEventKind::Error,
                                    "COLLECTION_ERROR",
                                    "item failed",
                                )
                                .rule_path(&item_path)
                                .finish(collector);
                            collector
                                .error_span(TraceEventKind::OpError, "OP_ERROR", "operator failed")
                                .rule_path(step_path)
                                .operator("map")
                                .input_v2_eval_value(&pipe_value, collector.options(), None)
                                .finish(collector);
                            return Err(error);
                        }
                    };
                    current = next;
                    step_ctx = next_ctx;
                }
                collector
                    .end_span(TraceEventKind::CollectionItemEnd, TracePhase::End)
                    .rule_path(&item_path)
                    .finish_with_v2_eval_output(collector, &current, Some("@item"));
                if let V2EvalValue::Value(value) = current {
                    results.push(value);
                }
            }
            collector
                .end_span(TraceEventKind::OpEnd, TracePhase::End)
                .rule_path(step_path)
                .operator("map")
                .finish_with_v2_eval_output(
                    collector,
                    &V2EvalValue::Value(JsonValue::Array(results.clone())),
                    None,
                );
            Ok((V2EvalValue::Value(JsonValue::Array(results)), ctx.clone()))
        }
        V2Step::Let(let_step) => {
            let new_ctx = eval_v2_let_step(
                let_step,
                pipe_value.clone(),
                record,
                context,
                out,
                step_path,
                ctx,
            )?;
            collector
                .emit(TraceEventKind::ChainStep, TracePhase::Instant)
                .rule_path(step_path)
                .input_v2_eval_value(&pipe_value, collector.options(), None)
                .finish(collector);
            let output = new_ctx.get_pipe_value().cloned().unwrap_or(pipe_value);
            Ok((output, new_ctx))
        }
        V2Step::If(if_step) => {
            let cond_ctx = ctx.clone().with_pipe_value(pipe_value.clone());
            let cond_path = format!("{}.cond", step_path);
            let cond = eval_v2_condition_traced(
                &if_step.cond,
                record,
                context,
                out,
                &cond_path,
                &cond_ctx,
                collector,
            )?;
            collector
                .emit(TraceEventKind::BranchEval, TracePhase::Instant)
                .rule_path(&cond_path)
                .finish_with_output(collector, &JsonValue::Bool(cond), None);
            if cond {
                collector
                    .start_span(TraceEventKind::BranchTaken, TracePhase::Start)
                    .rule_path(step_path)
                    .attr_enum("selected_branch", "then")
                    .finish(collector);
                let result = eval_v2_pipe_traced(
                    &if_step.then_branch,
                    record,
                    context,
                    out,
                    &format!("{}.then", step_path),
                    &cond_ctx,
                    collector,
                )?;
                collector
                    .end_span(TraceEventKind::BranchTaken, TracePhase::End)
                    .rule_path(step_path)
                    .finish_with_v2_eval_output(collector, &result, None);
                Ok((result, ctx.clone()))
            } else if let Some(else_branch) = &if_step.else_branch {
                collector
                    .start_span(TraceEventKind::BranchTaken, TracePhase::Start)
                    .rule_path(step_path)
                    .attr_enum("selected_branch", "else")
                    .finish(collector);
                let result = eval_v2_pipe_traced(
                    else_branch,
                    record,
                    context,
                    out,
                    &format!("{}.else", step_path),
                    &cond_ctx,
                    collector,
                )?;
                collector
                    .end_span(TraceEventKind::BranchTaken, TracePhase::End)
                    .rule_path(step_path)
                    .finish_with_v2_eval_output(collector, &result, None);
                Ok((result, ctx.clone()))
            } else {
                Ok((pipe_value, ctx.clone()))
            }
        }
        V2Step::Ref(v2_ref) => {
            let result = eval_v2_ref(v2_ref, record, context, out, step_path, ctx)?;
            let mut event = collector
                .emit(TraceEventKind::RefRead, TracePhase::Instant)
                .rule_path(step_path);
            if let Some(path) = canonical_v2_ref_path(v2_ref) {
                event = event.input_path(path);
            }
            event.finish_with_v2_eval_output(collector, &result, None);
            Ok((result, ctx.clone()))
        }
    }
}

fn v2_operator_has_eager_args(op: &str) -> bool {
    !matches!(
        op,
        "and"
            | "or"
            | "coalesce"
            | "lookup"
            | "lookup_first"
            | "map"
            | "filter"
            | "flat_map"
            | "group_by"
            | "key_by"
            | "partition"
            | "distinct_by"
            | "sort_by"
            | "find"
            | "find_index"
            | "zip_with"
            | "reduce"
            | "fold"
    )
}

fn v2_operator_has_item_level_trace(op: &str) -> bool {
    matches!(
        op,
        "map"
            | "filter"
            | "flat_map"
            | "group_by"
            | "key_by"
            | "partition"
            | "distinct_by"
            | "sort_by"
            | "find"
            | "find_index"
            | "reduce"
            | "fold"
    )
}

fn v2_operator_has_lazy_arg_trace(op: &str) -> bool {
    matches!(op, "and" | "or" | "coalesce")
}

fn v2_operator_skips_args_when_pipe_is_missing(op: &str) -> bool {
    matches!(
        op,
        "concat"
            | "replace"
            | "split"
            | "pad_start"
            | "pad_end"
            | "+"
            | "-"
            | "*"
            | "/"
            | "add"
            | "subtract"
            | "multiply"
            | "divide"
            | "round"
            | "to_base"
            | "date_format"
            | "to_unixtime"
            | "merge"
            | "deep_merge"
            | "get"
            | "keys"
            | "values"
            | "entries"
            | "len"
            | "from_entries"
            | "object_flatten"
            | "object_unflatten"
            | "flatten"
            | "take"
            | "drop"
            | "slice"
            | "chunk"
            | "zip"
            | "unzip"
            | "unique"
            | "index_of"
            | "contains"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "first"
            | "last"
            | "string"
            | "int"
            | "float"
            | "bool"
            | "trim"
            | "uppercase"
            | "lowercase"
            | "to_string"
            | "not"
    )
}

fn v2_operator_stops_after_missing_arg(op: &str) -> bool {
    matches!(
        op,
        "concat"
            | "replace"
            | "split"
            | "pad_start"
            | "pad_end"
            | "+"
            | "-"
            | "*"
            | "/"
            | "add"
            | "subtract"
            | "multiply"
            | "divide"
            | "round"
            | "to_base"
            | "date_format"
            | "to_unixtime"
            | "merge"
            | "deep_merge"
            | "get"
            | "pick"
            | "omit"
            | "flatten"
            | "take"
            | "drop"
            | "slice"
            | "chunk"
            | "zip"
            | "index_of"
            | "contains"
    )
}

fn emit_v2_arg_eval(
    collector: &mut TraceCollector,
    rule_path: &str,
    arg_index: usize,
    operator: &str,
    value: &V2EvalValue,
) {
    collector
        .emit(TraceEventKind::ArgEval, TracePhase::Instant)
        .rule_path(rule_path)
        .operator(operator)
        .attr_index("arg_index", arg_index)
        .finish_with_v2_eval_output(collector, value, None);
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_lazy_op_traced<'a>(
    op: &crate::v2_model::V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    step_path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    let step_ctx = ctx.clone().with_pipe_value(pipe_value.clone());
    match op.op.as_str() {
        "coalesce" => {
            if let V2EvalValue::Value(value) = &pipe_value
                && !value.is_null()
            {
                return Ok(pipe_value);
            }
            for (arg_index, arg) in op.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", step_path, arg_index);
                let value = eval_v2_expr_traced(
                    arg, record, context, out, &arg_path, &step_ctx, collector,
                )?;
                emit_v2_arg_eval(collector, &arg_path, arg_index, &op.op, &value);
                if let V2EvalValue::Value(json) = &value
                    && !json.is_null()
                {
                    return Ok(value);
                }
            }
            Ok(V2EvalValue::Missing)
        }
        "and" | "or" => {
            let is_and = op.op == "and";
            let total_len = op.args.len() + 1;
            if total_len < 2 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "expr.args must contain at least two items",
                )
                .with_path(format!("{}.args", step_path)));
            }

            let mut saw_missing = false;
            match &pipe_value {
                V2EvalValue::Missing => saw_missing = true,
                V2EvalValue::Value(value) => {
                    let flag = value_as_bool(value, step_path)?;
                    if is_and {
                        if !flag {
                            return Ok(V2EvalValue::Value(JsonValue::Bool(false)));
                        }
                    } else if flag {
                        return Ok(V2EvalValue::Value(JsonValue::Bool(true)));
                    }
                }
            }

            for (arg_index, arg) in op.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", step_path, arg_index);
                let value = eval_v2_expr_traced(
                    arg, record, context, out, &arg_path, &step_ctx, collector,
                )?;
                emit_v2_arg_eval(collector, &arg_path, arg_index, &op.op, &value);
                match value {
                    V2EvalValue::Missing => {
                        saw_missing = true;
                    }
                    V2EvalValue::Value(value) => {
                        let flag = value_as_bool(&value, &arg_path)?;
                        if is_and {
                            if !flag {
                                return Ok(V2EvalValue::Value(JsonValue::Bool(false)));
                            }
                        } else if flag {
                            return Ok(V2EvalValue::Value(JsonValue::Bool(true)));
                        }
                    }
                }
            }

            if saw_missing {
                Ok(V2EvalValue::Missing)
            } else {
                Ok(V2EvalValue::Value(JsonValue::Bool(is_and)))
            }
        }
        _ => eval_v2_op_step(op, pipe_value, record, context, out, step_path, ctx),
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_eager_op_traced<'a>(
    op: &crate::v2_model::V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    step_path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    if matches!(pipe_value, V2EvalValue::Missing)
        && v2_operator_skips_args_when_pipe_is_missing(&op.op)
    {
        return eval_v2_op_step(op, pipe_value, record, context, out, step_path, ctx);
    }

    let step_ctx = ctx.clone().with_pipe_value(pipe_value.clone());
    let mut arg_values = Vec::with_capacity(op.args.len());
    for (arg_index, arg) in op.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", step_path, arg_index);
        let value =
            eval_v2_expr_traced(arg, record, context, out, &arg_path, &step_ctx, collector)?;
        emit_v2_arg_eval(collector, &arg_path, arg_index, &op.op, &value);
        let is_missing = matches!(value, V2EvalValue::Missing);
        arg_values.push(value);
        if is_missing && v2_operator_stops_after_missing_arg(&op.op) {
            break;
        }
    }
    let cached_ctx = ctx
        .clone()
        .with_pipe_value(pipe_value.clone())
        .with_precomputed_op_args(step_path, arg_values);
    eval_v2_op_step(op, pipe_value, record, context, out, step_path, &cached_ctx)
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_expr_traced<'a>(
    expr: &crate::v2_model::V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    match expr {
        crate::v2_model::V2Expr::Pipe(pipe) => {
            eval_v2_pipe_traced(pipe, record, context, out, path, ctx, collector)
        }
        crate::v2_model::V2Expr::V1Fallback(_) => {
            eval_v2_expr(expr, record, context, out, path, ctx)
        }
    }
}

fn v2_eval_array_from_value(
    value: V2EvalValue,
    path: &str,
) -> Result<Vec<JsonValue>, TransformError> {
    match value {
        V2EvalValue::Missing => Ok(Vec::new()),
        V2EvalValue::Value(value) => {
            if value.is_null() {
                Ok(Vec::new())
            } else if let JsonValue::Array(items) = value {
                Ok(items)
            } else {
                Err(
                    TransformError::new(TransformErrorKind::ExprError, "expr arg must be an array")
                        .with_path(path),
                )
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_expr_or_null_traced<'a>(
    expr: &crate::v2_model::V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<JsonValue, TransformError> {
    match eval_v2_expr_traced(expr, record, context, out, path, ctx, collector)? {
        V2EvalValue::Missing => Ok(JsonValue::Null),
        V2EvalValue::Value(value) => Ok(value),
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_predicate_expr_traced<'a>(
    expr: &crate::v2_model::V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<bool, TransformError> {
    match eval_v2_expr_traced(expr, record, context, out, path, ctx, collector)? {
        V2EvalValue::Missing => Ok(false),
        V2EvalValue::Value(value) => {
            if value.is_null() {
                Ok(false)
            } else {
                value_as_bool(&value, path)
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_key_expr_string_traced<'a>(
    expr: &crate::v2_model::V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<String, TransformError> {
    let value = match eval_v2_expr_traced(expr, record, context, out, path, ctx, collector)? {
        V2EvalValue::Missing => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be missing",
            )
            .with_path(path));
        }
        V2EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(path));
    }
    value_to_string(&value, path)
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_sort_key_traced<'a>(
    expr: &crate::v2_model::V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<SortKey, TransformError> {
    let value = match eval_v2_expr_traced(expr, record, context, out, path, ctx, collector)? {
        V2EvalValue::Missing => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be missing",
            )
            .with_path(path));
        }
        V2EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(path));
    }
    sort_key_from_value(&value, path)
}

fn emit_v2_collection_item_start(
    collector: &mut TraceCollector,
    item_path: &str,
    operator: &str,
    index: usize,
    item: &JsonValue,
) {
    collector
        .start_span(TraceEventKind::CollectionItemStart, TracePhase::Start)
        .rule_path(item_path)
        .operator(operator)
        .input_path(canonical_item_path(""))
        .attr_index("item_index", index)
        .attr_enum("scope", "item")
        .input_value(item, collector.options(), Some("@item"))
        .finish(collector);
}

fn finish_v2_collection_item(
    collector: &mut TraceCollector,
    item_path: &str,
    operator: &str,
    index: usize,
    output: &V2EvalValue,
    bool_attr: Option<(&'static str, bool)>,
) {
    let mut event = collector
        .end_span(TraceEventKind::CollectionItemEnd, TracePhase::End)
        .rule_path(item_path)
        .operator(operator)
        .attr_index("item_index", index);
    if let Some((key, value)) = bool_attr {
        event = event.attr_bool(key, value);
    }
    event.finish_with_v2_eval_output(collector, output, Some("@item"));
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_collection_op_traced<'a>(
    op_step: &crate::v2_model::V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    let step_ctx = ctx.clone().with_pipe_value(pipe_value.clone());
    let operator = op_step.op.as_str();

    match operator {
        "map" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "map requires exactly one argument",
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut results = Vec::new();
            for (index, item) in array.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index });
                let value = eval_v2_expr_traced(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                    collector,
                )?;
                emit_v2_arg_eval(collector, &arg_path, 0, operator, &value);
                finish_v2_collection_item(collector, &item_path, operator, index, &value, None);
                if let V2EvalValue::Value(value) = value {
                    results.push(value);
                }
            }
            Ok(V2EvalValue::Value(JsonValue::Array(results)))
        }
        "filter" | "partition" | "find" | "find_index" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("{operator} requires exactly one argument"),
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut kept = Vec::new();
            let mut rejected = Vec::new();
            for (index, item) in array.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index });
                let matches = eval_v2_predicate_expr_traced(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                    collector,
                )?;
                let match_value = V2EvalValue::Value(JsonValue::Bool(matches));
                emit_v2_arg_eval(collector, &arg_path, 0, operator, &match_value);
                finish_v2_collection_item(
                    collector,
                    &item_path,
                    operator,
                    index,
                    &match_value,
                    Some(("matched", matches)),
                );
                match operator {
                    "filter" => {
                        if matches {
                            kept.push(item.clone());
                        }
                    }
                    "partition" => {
                        if matches {
                            kept.push(item.clone());
                        } else {
                            rejected.push(item.clone());
                        }
                    }
                    "find" if matches => return Ok(V2EvalValue::Value(item.clone())),
                    "find_index" if matches => {
                        return Ok(V2EvalValue::Value(JsonValue::Number((index as i64).into())));
                    }
                    _ => {}
                }
            }
            match operator {
                "filter" => Ok(V2EvalValue::Value(JsonValue::Array(kept))),
                "partition" => Ok(V2EvalValue::Value(JsonValue::Array(vec![
                    JsonValue::Array(kept),
                    JsonValue::Array(rejected),
                ]))),
                "find" => Ok(V2EvalValue::Value(JsonValue::Null)),
                "find_index" => Ok(V2EvalValue::Value(JsonValue::Number((-1).into()))),
                _ => unreachable!(),
            }
        }
        "flat_map" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "flat_map requires exactly one argument",
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut results = Vec::new();
            for (index, item) in array.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index });
                let value = eval_v2_expr_or_null_traced(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                    collector,
                )?;
                let output = V2EvalValue::Value(value.clone());
                emit_v2_arg_eval(collector, &arg_path, 0, operator, &output);
                finish_v2_collection_item(collector, &item_path, operator, index, &output, None);
                match value {
                    JsonValue::Array(items) => results.extend(items),
                    value => results.push(value),
                }
            }
            Ok(V2EvalValue::Value(JsonValue::Array(results)))
        }
        "group_by" | "key_by" | "distinct_by" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("{operator} requires exactly one argument"),
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut grouped = serde_json::Map::new();
            let mut keyed = serde_json::Map::new();
            let mut distinct = Vec::new();
            let mut seen = HashSet::new();
            for (index, item) in array.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index });
                let key = eval_v2_key_expr_string_traced(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                    collector,
                )?;
                let key_output = V2EvalValue::Value(JsonValue::String(key.clone()));
                emit_v2_arg_eval(collector, &arg_path, 0, operator, &key_output);
                let selected = match operator {
                    "group_by" => {
                        let entry = grouped
                            .entry(key)
                            .or_insert_with(|| JsonValue::Array(Vec::new()));
                        if let JsonValue::Array(items) = entry {
                            items.push(item.clone());
                        }
                        true
                    }
                    "key_by" => {
                        keyed.insert(key, item.clone());
                        true
                    }
                    "distinct_by" => {
                        if seen.insert(key) {
                            distinct.push(item.clone());
                            true
                        } else {
                            false
                        }
                    }
                    _ => unreachable!(),
                };
                finish_v2_collection_item(
                    collector,
                    &item_path,
                    operator,
                    index,
                    &key_output,
                    Some(("selected", selected)),
                );
            }
            match operator {
                "group_by" => Ok(V2EvalValue::Value(JsonValue::Object(grouped))),
                "key_by" => Ok(V2EvalValue::Value(JsonValue::Object(keyed))),
                "distinct_by" => Ok(V2EvalValue::Value(JsonValue::Array(distinct))),
                _ => unreachable!(),
            }
        }
        "sort_by" => {
            if !(1..=2).contains(&op_step.args.len()) {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "sort_by requires one or two arguments",
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            if array.is_empty() {
                return Ok(V2EvalValue::Value(JsonValue::Array(Vec::new())));
            }
            let expr_path = format!("{}.args[0]", path);
            let order = if op_step.args.len() == 2 {
                let order_path = format!("{}.args[1]", path);
                let order_value = eval_v2_expr_traced(
                    &op_step.args[1],
                    record,
                    context,
                    out,
                    &order_path,
                    &step_ctx,
                    collector,
                )?;
                emit_v2_arg_eval(collector, &order_path, 1, operator, &order_value);
                let order = match order_value {
                    V2EvalValue::Missing => return Ok(V2EvalValue::Missing),
                    V2EvalValue::Value(value) => value_to_string(&value, &order_path)?,
                };
                if order != "asc" && order != "desc" {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "order must be asc or desc",
                    )
                    .with_path(order_path));
                }
                order
            } else {
                "asc".to_string()
            };

            struct TracedSortItem {
                key: SortKey,
                index: usize,
                value: JsonValue,
            }

            let mut items = Vec::with_capacity(array.len());
            let mut key_kind = None;
            for (index, item) in array.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index });
                let key = eval_v2_sort_key_traced(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &expr_path,
                    &item_ctx,
                    collector,
                )?;
                let kind = key.kind();
                if let Some(existing) = key_kind {
                    if existing != kind {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "sort_by keys must be all the same type",
                        )
                        .with_path(&expr_path));
                    }
                } else {
                    key_kind = Some(kind);
                }
                let key_value = sort_key_to_json(&key);
                let key_output = V2EvalValue::Value(key_value);
                emit_v2_arg_eval(collector, &expr_path, 0, operator, &key_output);
                finish_v2_collection_item(
                    collector,
                    &item_path,
                    operator,
                    index,
                    &key_output,
                    None,
                );
                items.push(TracedSortItem {
                    key,
                    index,
                    value: item.clone(),
                });
            }

            items.sort_by(|left, right| {
                let mut ordering = compare_sort_keys(&left.key, &right.key);
                if order == "desc" {
                    ordering = ordering.reverse();
                }
                if ordering == Ordering::Equal {
                    left.index.cmp(&right.index)
                } else {
                    ordering
                }
            });
            Ok(V2EvalValue::Value(JsonValue::Array(
                items.into_iter().map(|item| item.value).collect(),
            )))
        }
        "reduce" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "reduce requires exactly one argument",
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            if array.is_empty() {
                return Ok(V2EvalValue::Value(JsonValue::Null));
            }
            let expr_path = format!("{}.args[0]", path);
            let mut acc = array[0].clone();
            for (index, item) in array.iter().enumerate().skip(1) {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index })
                    .with_acc(&acc);
                let value = eval_v2_expr_or_null_traced(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &expr_path,
                    &item_ctx,
                    collector,
                )?;
                let output = V2EvalValue::Value(value.clone());
                emit_v2_arg_eval(collector, &expr_path, 0, operator, &output);
                acc = value;
                finish_v2_collection_item(collector, &item_path, operator, index, &output, None);
            }
            Ok(V2EvalValue::Value(acc))
        }
        "fold" => {
            if op_step.args.len() != 2 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "fold requires exactly two arguments",
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            let init_path = format!("{}.args[0]", path);
            let initial = eval_v2_expr_traced(
                &op_step.args[0],
                record,
                context,
                out,
                &init_path,
                &step_ctx,
                collector,
            )?;
            emit_v2_arg_eval(collector, &init_path, 0, operator, &initial);
            let mut acc = match initial {
                V2EvalValue::Missing => return Ok(V2EvalValue::Missing),
                V2EvalValue::Value(value) => value,
            };
            let expr_path = format!("{}.args[1]", path);
            for (index, item) in array.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index })
                    .with_acc(&acc);
                let value = eval_v2_expr_or_null_traced(
                    &op_step.args[1],
                    record,
                    context,
                    out,
                    &expr_path,
                    &item_ctx,
                    collector,
                )?;
                let output = V2EvalValue::Value(value.clone());
                emit_v2_arg_eval(collector, &expr_path, 1, operator, &output);
                acc = value;
                finish_v2_collection_item(collector, &item_path, operator, index, &output, None);
            }
            Ok(V2EvalValue::Value(acc))
        }
        _ => eval_v2_op_step(op_step, pipe_value, record, context, out, path, ctx),
    }
}

fn sort_key_to_json(key: &SortKey) -> JsonValue {
    match key {
        SortKey::Number(value) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        SortKey::String(value) => JsonValue::String(value.clone()),
        SortKey::Bool(value) => JsonValue::Bool(*value),
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_condition_traced<'a>(
    condition: &V2Condition,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<bool, TransformError> {
    match condition {
        V2Condition::All(conditions) => {
            for (index, cond) in conditions.iter().enumerate() {
                let cond_path = format!("{}[{}]", path, index);
                if !eval_v2_condition_traced(
                    cond, record, context, out, &cond_path, ctx, collector,
                )? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        V2Condition::Any(conditions) => {
            for (index, cond) in conditions.iter().enumerate() {
                let cond_path = format!("{}[{}]", path, index);
                if eval_v2_condition_traced(cond, record, context, out, &cond_path, ctx, collector)?
                {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        V2Condition::Comparison(comparison) => {
            eval_v2_comparison_traced(comparison, record, context, out, path, ctx, collector)
        }
        V2Condition::Expr(expr) => {
            let expr_path = format!("{}.expr", path);
            let value =
                eval_v2_expr_traced(expr, record, context, out, &expr_path, ctx, collector)?;
            match value {
                V2EvalValue::Value(JsonValue::Bool(flag)) => Ok(flag),
                V2EvalValue::Missing => Ok(false),
                V2EvalValue::Value(_) => Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "when/record_when must evaluate to boolean",
                )
                .with_path(&expr_path)),
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_comparison_traced<'a>(
    comparison: &crate::v2_model::V2Comparison,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<bool, TransformError> {
    if comparison.args.len() != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!(
                "comparison requires exactly 2 arguments, got {}",
                comparison.args.len()
            ),
        )
        .with_path(path));
    }

    let operator = v2_comparison_operator_name(comparison.op);
    collector
        .start_span(TraceEventKind::OpStart, TracePhase::Start)
        .rule_path(path)
        .operator(operator)
        .attr_count("arg_count", 2)
        .finish(collector);

    let result =
        (|| {
            let left_path = format!("{}.args[0]", path);
            let right_path = format!("{}.args[1]", path);
            let left = eval_v2_expr_traced(
                &comparison.args[0],
                record,
                context,
                out,
                &left_path,
                ctx,
                collector,
            )?;
            emit_v2_arg_eval(collector, &left_path, 0, operator, &left);
            let right = eval_v2_expr_traced(
                &comparison.args[1],
                record,
                context,
                out,
                &right_path,
                ctx,
                collector,
            )?;
            emit_v2_arg_eval(collector, &right_path, 1, operator, &right);

            match comparison.op {
                V2ComparisonOp::Eq => Ok(compare_v2_eval_eq(&left, &right)),
                V2ComparisonOp::Ne => Ok(!compare_v2_eval_eq(&left, &right)),
                V2ComparisonOp::Gt => compare_v2_eval_ord(&left, &right, path)
                    .map(|ordering| ordering == Ordering::Greater),
                V2ComparisonOp::Gte => compare_v2_eval_ord(&left, &right, path)
                    .map(|ordering| ordering != Ordering::Less),
                V2ComparisonOp::Lt => compare_v2_eval_ord(&left, &right, path)
                    .map(|ordering| ordering == Ordering::Less),
                V2ComparisonOp::Lte => compare_v2_eval_ord(&left, &right, path)
                    .map(|ordering| ordering != Ordering::Greater),
                V2ComparisonOp::Match => compare_v2_eval_match(&left, &right, path),
            }
        })();

    match result {
        Ok(flag) => {
            collector
                .end_span(TraceEventKind::OpEnd, TracePhase::End)
                .rule_path(path)
                .operator(operator)
                .finish_with_output(collector, &JsonValue::Bool(flag), None);
            Ok(flag)
        }
        Err(error) => {
            collector
                .error_span(TraceEventKind::OpError, "OP_ERROR", "operator failed")
                .rule_path(path)
                .operator(operator)
                .finish(collector);
            Err(error)
        }
    }
}

fn v2_comparison_operator_name(op: V2ComparisonOp) -> &'static str {
    match op {
        V2ComparisonOp::Eq => "eq",
        V2ComparisonOp::Ne => "ne",
        V2ComparisonOp::Gt => "gt",
        V2ComparisonOp::Gte => "gte",
        V2ComparisonOp::Lt => "lt",
        V2ComparisonOp::Lte => "lte",
        V2ComparisonOp::Match => "match",
    }
}

fn compare_v2_eval_eq(left: &V2EvalValue, right: &V2EvalValue) -> bool {
    match (left, right) {
        (V2EvalValue::Value(left), V2EvalValue::Value(right)) => left == right,
        (V2EvalValue::Missing, V2EvalValue::Missing) => true,
        (V2EvalValue::Missing, V2EvalValue::Value(right)) => right.is_null(),
        (V2EvalValue::Value(left), V2EvalValue::Missing) => left.is_null(),
    }
}

fn compare_v2_eval_ord(
    left: &V2EvalValue,
    right: &V2EvalValue,
    path: &str,
) -> Result<Ordering, TransformError> {
    match (left, right) {
        (V2EvalValue::Value(left), V2EvalValue::Value(right)) => {
            if let (Some(left), Some(right)) = (json_value_as_f64(left), json_value_as_f64(right)) {
                return Ok(left.partial_cmp(&right).unwrap_or(Ordering::Equal));
            }
            if let (Some(left), Some(right)) = (left.as_str(), right.as_str()) {
                return Ok(left.cmp(right));
            }
            Err(TransformError::new(
                TransformErrorKind::ExprError,
                "cannot compare values of different types",
            )
            .with_path(path))
        }
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "cannot compare missing values",
        )
        .with_path(path)),
    }
}

fn compare_v2_eval_match(
    left: &V2EvalValue,
    right: &V2EvalValue,
    path: &str,
) -> Result<bool, TransformError> {
    let text = match left {
        V2EvalValue::Value(JsonValue::String(value)) => value,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "match operator requires string on left side",
            )
            .with_path(path));
        }
    };
    let pattern = match right {
        V2EvalValue::Value(JsonValue::String(value)) => value,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "match operator requires regex pattern string on right side",
            )
            .with_path(path));
        }
    };
    Regex::new(pattern)
        .map_err(|err| {
            TransformError::new(
                TransformErrorKind::ExprError,
                format!("invalid regex pattern: {}", err),
            )
            .with_path(path)
        })
        .map(|regex| regex.is_match(text))
}

fn json_value_as_f64(value: &JsonValue) -> Option<f64> {
    match value {
        JsonValue::Number(number) => number.as_f64(),
        JsonValue::String(value) => value.parse::<f64>().ok(),
        _ => None,
    }
}

fn emit_v2_start_trace(
    start: &V2Start,
    value: &V2EvalValue,
    path: &str,
    collector: &mut TraceCollector,
) {
    match start {
        V2Start::Ref(v2_ref) => {
            let mut event = collector
                .emit(TraceEventKind::RefRead, TracePhase::Instant)
                .rule_path(path);
            if let Some(input_path) = canonical_v2_ref_path(v2_ref) {
                event = event.input_path(input_path);
            }
            event.finish_with_v2_eval_output(collector, value, None);
        }
        V2Start::Literal(_) => {
            collector
                .emit(TraceEventKind::LiteralEval, TracePhase::Instant)
                .rule_path(path)
                .finish_with_v2_eval_output(collector, value, None);
        }
        V2Start::PipeValue | V2Start::V1Expr(_) => {
            collector
                .emit(TraceEventKind::ChainStep, TracePhase::Instant)
                .rule_path(path)
                .finish_with_v2_eval_output(collector, value, None);
        }
    }
}

fn canonical_v2_ref_path(v2_ref: &V2Ref) -> Option<String> {
    match v2_ref {
        V2Ref::Input(path) => Some(canonical_input_path(path)),
        V2Ref::Context(path) => Some(canonical_context_path(path)),
        V2Ref::Out(path) => Some(canonical_out_path(path)),
        V2Ref::Item(path) => Some(canonical_item_path(path)),
        V2Ref::Acc(path) => Some(canonical_acc_path(path)),
        V2Ref::Local(_) => None,
    }
}

fn eval_when(
    mapping: &crate::model::Mapping,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
    warnings: &mut Vec<TransformWarning>,
    rule_version: u8,
) -> bool {
    let expr = match &mapping.when {
        Some(expr) => expr,
        None => return true,
    };

    let when_path = format!("{}.when", mapping_path);
    match eval_when_expr(expr, record, context, out, &when_path, rule_version) {
        Ok(flag) => flag,
        Err(err) => {
            warnings.push(err.into());
            false
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_when_traced(
    mapping: &crate::model::Mapping,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
    warnings: &mut Vec<TransformWarning>,
    rule_version: u8,
    collector: &mut TraceCollector,
) -> bool {
    let expr = match &mapping.when {
        Some(expr) => expr,
        None => return true,
    };

    let when_path = format!("{}.when", mapping_path);
    match eval_when_expr_traced(
        expr,
        record,
        context,
        out,
        &when_path,
        rule_version,
        collector,
    ) {
        Ok(flag) => flag,
        Err(err) => {
            warnings.push(err.into());
            false
        }
    }
}

fn eval_record_when(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
) -> bool {
    let expr = match &rule.record_when {
        Some(expr) => expr,
        None => return true,
    };

    let empty_out = JsonValue::Object(Map::new());
    match eval_when_expr(
        expr,
        record,
        context,
        &empty_out,
        "record_when",
        rule.version,
    ) {
        Ok(flag) => flag,
        Err(err) => {
            warnings.push(err.into());
            false
        }
    }
}

fn eval_record_when_traced(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    collector: &mut TraceCollector,
) -> bool {
    let expr = match &rule.record_when {
        Some(expr) => expr,
        None => return true,
    };

    let empty_out = JsonValue::Object(Map::new());
    match eval_when_expr_traced(
        expr,
        record,
        context,
        &empty_out,
        "record_when",
        rule.version,
        collector,
    ) {
        Ok(flag) => flag,
        Err(err) => {
            warnings.push(err.into());
            false
        }
    }
}

fn eval_bool_expr(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    path: &str,
) -> Result<bool, TransformError> {
    let value = eval_expr(expr, record, context, out, path, None)?;
    let value = match value {
        EvalValue::Missing => JsonValue::Null,
        EvalValue::Value(value) => value,
    };
    match value {
        JsonValue::Bool(flag) => Ok(flag),
        _ => Err(when_type_error(path)),
    }
}

fn eval_bool_expr_traced(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    path: &str,
    collector: &mut TraceCollector,
) -> Result<bool, TransformError> {
    let value = eval_expr_traced(expr, record, context, out, path, None, collector)?;
    let value = match value {
        EvalValue::Missing => JsonValue::Null,
        EvalValue::Value(value) => value,
    };
    match value {
        JsonValue::Bool(flag) => Ok(flag),
        _ => Err(when_type_error(path)),
    }
}

fn eval_when_expr(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    path: &str,
    rule_version: u8,
) -> Result<bool, TransformError> {
    if rule_version >= 2 {
        if let Some(raw_value) = expr_to_json_for_v2_condition(expr) {
            let condition = parse_v2_condition(&raw_value).map_err(|err| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("invalid v2 condition: {}", err),
                )
                .with_path(path)
            })?;
            let ctx = V2EvalContext::new();
            return eval_v2_condition(&condition, record, context, out, path, &ctx);
        }
    }

    eval_bool_expr(expr, record, context, out, path)
}

fn eval_when_expr_traced(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    path: &str,
    rule_version: u8,
    collector: &mut TraceCollector,
) -> Result<bool, TransformError> {
    if rule_version >= 2 {
        if let Some(raw_value) = expr_to_json_for_v2_condition(expr) {
            let condition = parse_v2_condition(&raw_value).map_err(|err| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("invalid v2 condition: {}", err),
                )
                .with_path(path)
            })?;
            let ctx = V2EvalContext::new();
            return eval_v2_condition_traced(
                &condition, record, context, out, path, &ctx, collector,
            );
        }
    }

    eval_bool_expr_traced(expr, record, context, out, path, collector)
}

fn when_type_error(path: &str) -> TransformError {
    TransformError::new(
        TransformErrorKind::ExprError,
        "when/record_when must evaluate to boolean",
    )
    .with_path(path)
}

fn resolve_source(
    source: &str,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
) -> Result<EvalValue, TransformError> {
    let (namespace, path) =
        parse_source(source).map_err(|err| err.with_path(format!("{}.source", mapping_path)))?;
    let tokens = parse_path_tokens(
        path,
        TransformErrorKind::InvalidRef,
        format!("{}.source", mapping_path),
    )?;
    let target = match namespace {
        Namespace::Input => Some(record),
        Namespace::Context => context,
        Namespace::Out => Some(out),
        Namespace::Item | Namespace::Acc | Namespace::Pipe | Namespace::Local => {
            return Err(TransformError::new(
                TransformErrorKind::InvalidRef,
                "ref namespace must be input|context|out",
            )
            .with_path(format!("{}.source", mapping_path)));
        }
    };

    match target.and_then(|value| get_path(value, &tokens)) {
        Some(value) => Ok(EvalValue::Value(value.clone())),
        None => Ok(EvalValue::Missing),
    }
}

fn eval_expr(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    match expr {
        Expr::Literal(value) => Ok(EvalValue::Value(value.clone())),
        Expr::Ref(expr_ref) => eval_ref(expr_ref, record, context, out, base_path, locals),
        Expr::Op(expr_op) => eval_op(expr_op, record, context, out, base_path, None, locals),
        Expr::Chain(expr_chain) => eval_chain(expr_chain, record, context, out, base_path, locals),
    }
}

fn eval_expr_traced(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    collector
        .start_span(TraceEventKind::ExprStart, TracePhase::Start)
        .rule_path(base_path)
        .finish(collector);

    let result = match expr {
        Expr::Literal(value) => {
            let value = EvalValue::Value(value.clone());
            collector
                .emit(TraceEventKind::LiteralEval, TracePhase::Instant)
                .rule_path(base_path)
                .finish_with_eval_output(collector, &value, None);
            Ok(value)
        }
        Expr::Ref(expr_ref) => {
            let value = eval_ref(expr_ref, record, context, out, base_path, locals);
            if let Ok(value) = &value {
                collector
                    .emit(TraceEventKind::RefRead, TracePhase::Instant)
                    .rule_path(base_path)
                    .input_path(canonical_ref_path(&expr_ref.ref_path))
                    .finish_with_eval_output(collector, value, Some(&expr_ref.ref_path));
            }
            value
        }
        Expr::Op(expr_op) => {
            collector
                .start_span(TraceEventKind::OpStart, TracePhase::Start)
                .rule_path(base_path)
                .operator(&expr_op.op)
                .finish(collector);
            let op_result = match expr_op.op.as_str() {
                "coalesce" => eval_coalesce_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    locals,
                    collector,
                ),
                "and" => eval_bool_and_or_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    true,
                    locals,
                    collector,
                ),
                "or" => eval_bool_and_or_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    false,
                    locals,
                    collector,
                ),
                "map" => eval_array_map_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    locals,
                    collector,
                ),
                "reduce" => eval_array_reduce_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    locals,
                    collector,
                ),
                "fold" => eval_array_fold_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    locals,
                    collector,
                ),
                op if v1_operator_has_scoped_expr_args(op) => {
                    eval_op(expr_op, record, context, out, base_path, None, locals)
                }
                _ => eval_eager_op_traced(
                    expr_op, record, context, out, base_path, locals, collector,
                ),
            };

            match op_result {
                Ok(value) => {
                    collector
                        .end_span(TraceEventKind::OpEnd, TracePhase::End)
                        .rule_path(base_path)
                        .operator(&expr_op.op)
                        .finish_with_eval_output(collector, &value, None);
                    Ok(value)
                }
                Err(error) => {
                    collector
                        .error_span(TraceEventKind::OpError, "OP_ERROR", "operator failed")
                        .rule_path(base_path)
                        .operator(&expr_op.op)
                        .finish(collector);
                    Err(error)
                }
            }
        }
        Expr::Chain(expr_chain) => eval_chain(expr_chain, record, context, out, base_path, locals),
    };

    match &result {
        Ok(value) => {
            collector
                .end_span(TraceEventKind::ExprEnd, TracePhase::End)
                .rule_path(base_path)
                .finish_with_eval_output(collector, value, None);
        }
        Err(_) => {
            collector
                .error_span(TraceEventKind::Error, "EXPR_ERROR", "expression failed")
                .rule_path(base_path)
                .finish(collector);
        }
    }

    result
}

#[allow(clippy::too_many_arguments)]
fn eval_eager_op_traced(
    expr_op: &ExprOp,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    let mut arg_values = Vec::with_capacity(expr_op.args.len());
    for (arg_index, arg) in expr_op.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", base_path, arg_index);
        let arg_value = eval_expr_traced(arg, record, context, out, &arg_path, locals, collector)?;
        emit_arg_eval(collector, &arg_path, arg_index, &arg_value);
        let is_missing = matches!(arg_value, EvalValue::Missing);
        arg_values.push(arg_value);
        if is_missing && v1_operator_stops_after_missing_arg(&expr_op.op) {
            break;
        }
    }
    let cached_locals = locals_with_precomputed_args(locals, base_path, &arg_values);
    eval_op(
        expr_op,
        record,
        context,
        out,
        base_path,
        None,
        Some(&cached_locals),
    )
}

fn v1_operator_has_scoped_expr_args(op: &str) -> bool {
    matches!(
        op,
        "filter"
            | "flat_map"
            | "zip_with"
            | "group_by"
            | "key_by"
            | "partition"
            | "distinct_by"
            | "sort_by"
            | "find"
            | "find_index"
    )
}

fn v1_operator_stops_after_missing_arg(op: &str) -> bool {
    matches!(
        op,
        "concat"
            | "+"
            | "-"
            | "*"
            | "/"
            | "replace"
            | "split"
            | "pad_start"
            | "pad_end"
            | "round"
            | "to_base"
            | "date_format"
            | "to_unixtime"
            | "merge"
            | "deep_merge"
            | "get"
            | "pick"
            | "omit"
            | "flatten"
            | "take"
            | "drop"
            | "slice"
            | "chunk"
            | "zip"
            | "index_of"
            | "contains"
    )
}

fn emit_arg_eval(
    collector: &mut TraceCollector,
    arg_path: &str,
    arg_index: usize,
    arg_value: &EvalValue,
) {
    collector
        .emit(TraceEventKind::ArgEval, TracePhase::Instant)
        .rule_path(arg_path)
        .attr_index("arg_index", arg_index)
        .input_eval_value(arg_value, collector.options(), None)
        .finish(collector);
}

#[allow(clippy::too_many_arguments)]
fn eval_expr_at_index_traced(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    if let Some(injected) = injected {
        if index == 0 {
            return Ok(injected.clone());
        }
        let arg = args.get(index - 1).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "expr.args index is out of bounds",
            )
            .with_path(format!("{}.args[{}]", base_path, index))
        })?;
        let arg_path = format!("{}.args[{}]", base_path, index);
        return eval_expr_traced(arg, record, context, out, &arg_path, locals, collector);
    }

    let arg = args.get(index).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[{}]", base_path, index))
    })?;
    let arg_path = format!("{}.args[{}]", base_path, index);
    eval_expr_traced(arg, record, context, out, &arg_path, locals, collector)
}

#[allow(clippy::too_many_arguments)]
fn eval_array_arg_traced(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<Vec<JsonValue>, TransformError> {
    let arg_path = format!("{}.args[{}]", base_path, index);
    let value = eval_expr_at_index_traced(
        index, args, injected, record, context, out, base_path, locals, collector,
    )?;
    emit_arg_eval(collector, &arg_path, index, &value);
    match value {
        EvalValue::Missing => Ok(Vec::new()),
        EvalValue::Value(value) => {
            if value.is_null() {
                Ok(Vec::new())
            } else if let JsonValue::Array(items) = value {
                Ok(items)
            } else {
                Err(
                    TransformError::new(TransformErrorKind::ExprError, "expr arg must be an array")
                        .with_path(arg_path),
                )
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_coalesce_traced(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len == 0 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must be a non-empty array",
        )
        .with_path(format!("{}.args", base_path)));
    }

    for index in 0..total_len {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let value = eval_expr_at_index_traced(
            index, args, injected, record, context, out, base_path, locals, collector,
        )?;
        emit_arg_eval(collector, &arg_path, index, &value);
        match value {
            EvalValue::Missing => continue,
            EvalValue::Value(value) => {
                if value.is_null() {
                    continue;
                }
                return Ok(EvalValue::Value(value));
            }
        }
    }
    Ok(EvalValue::Missing)
}

#[allow(clippy::too_many_arguments)]
fn eval_array_map_traced(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg_traced(
        0, args, injected, record, context, out, base_path, locals, collector,
    )?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Vec::with_capacity(array.len());
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let value = eval_expr_traced(
            expr,
            record,
            context,
            out,
            &expr_path,
            Some(&item_locals),
            collector,
        )?;
        emit_arg_eval(collector, &expr_path, expr_index, &value);
        results.push(match value {
            EvalValue::Missing => JsonValue::Null,
            EvalValue::Value(value) => value,
        });
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

#[allow(clippy::too_many_arguments)]
fn eval_array_reduce_traced(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg_traced(
        0, args, injected, record, context, out, base_path, locals, collector,
    )?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut acc = array[0].clone();
    for (index, item) in array.iter().enumerate().skip(1) {
        let item_locals = EvalLocals {
            item: Some(EvalItem { value: item, index }),
            acc: Some(&acc),
            pipe: locals.and_then(|locals| locals.pipe),
            locals: locals.and_then(|locals| locals.locals),
            precomputed_op_args: None,
        };
        let value = eval_expr_traced(
            expr,
            record,
            context,
            out,
            &expr_path,
            Some(&item_locals),
            collector,
        )?;
        emit_arg_eval(collector, &expr_path, expr_index, &value);
        acc = match value {
            EvalValue::Missing => JsonValue::Null,
            EvalValue::Value(value) => value,
        };
    }

    Ok(EvalValue::Value(acc))
}

#[allow(clippy::too_many_arguments)]
fn eval_array_fold_traced(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 3 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg_traced(
        0, args, injected, record, context, out, base_path, locals, collector,
    )?;
    let initial_path = format!("{}.args[1]", base_path);
    let initial = eval_expr_at_index_traced(
        1, args, injected, record, context, out, base_path, locals, collector,
    )?;
    emit_arg_eval(collector, &initial_path, 1, &initial);
    let mut acc = match initial {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };

    let expr = arg_expr_at(2, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[2]", base_path))
    })?;
    let expr_index = if injected.is_some() { 1 } else { 2 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    for (index, item) in array.iter().enumerate() {
        let item_locals = EvalLocals {
            item: Some(EvalItem { value: item, index }),
            acc: Some(&acc),
            pipe: locals.and_then(|locals| locals.pipe),
            locals: locals.and_then(|locals| locals.locals),
            precomputed_op_args: None,
        };
        let value = eval_expr_traced(
            expr,
            record,
            context,
            out,
            &expr_path,
            Some(&item_locals),
            collector,
        )?;
        emit_arg_eval(collector, &expr_path, expr_index, &value);
        acc = match value {
            EvalValue::Missing => JsonValue::Null,
            EvalValue::Value(value) => value,
        };
    }

    Ok(EvalValue::Value(acc))
}

#[allow(clippy::too_many_arguments)]
fn eval_bool_and_or_traced(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    is_and: bool,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let mut saw_missing = false;
    for index in 0..total_len {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let value = eval_expr_at_index_traced(
            index, args, injected, record, context, out, base_path, locals, collector,
        )?;
        emit_arg_eval(collector, &arg_path, index, &value);
        match value {
            EvalValue::Missing => {
                saw_missing = true;
                continue;
            }
            EvalValue::Value(value) => {
                let flag = value_as_bool(&value, &arg_path)?;
                if is_and {
                    if !flag {
                        return Ok(EvalValue::Value(JsonValue::Bool(false)));
                    }
                } else if flag {
                    return Ok(EvalValue::Value(JsonValue::Bool(true)));
                }
            }
        }
    }

    if saw_missing {
        Ok(EvalValue::Missing)
    } else {
        Ok(EvalValue::Value(JsonValue::Bool(is_and)))
    }
}

fn canonical_ref_path(ref_path: &str) -> String {
    match parse_ref(ref_path) {
        Ok((Namespace::Input, path)) => canonical_input_path(path),
        Ok((Namespace::Context, path)) => canonical_context_path(path),
        Ok((Namespace::Out, path)) => canonical_out_path(path),
        Ok((Namespace::Item, path)) => canonical_item_path(path),
        Ok((Namespace::Acc, path)) => canonical_acc_path(path),
        _ => canonical_input_path(ref_path),
    }
}

fn eval_chain(
    expr_chain: &ExprChain,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    if expr_chain.chain.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.chain must be a non-empty array",
        )
        .with_path(format!("{}.chain", base_path)));
    }

    let first_path = format!("{}.chain[0]", base_path);
    let mut current = eval_expr(
        &expr_chain.chain[0],
        record,
        context,
        out,
        &first_path,
        locals,
    )?;

    for (index, step) in expr_chain.chain.iter().enumerate().skip(1) {
        let step_path = format!("{}.chain[{}]", base_path, index);
        let expr_op = match step {
            Expr::Op(expr_op) => expr_op,
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "expr.chain items after first must be op",
                )
                .with_path(step_path));
            }
        };

        let injected = current.clone();
        current = eval_op(
            expr_op,
            record,
            context,
            out,
            &step_path,
            Some(&injected),
            locals,
        )?;
    }

    Ok(current)
}

fn eval_ref(
    expr_ref: &ExprRef,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let (namespace, path) =
        parse_ref(&expr_ref.ref_path).map_err(|err| err.with_path(base_path))?;
    let tokens = parse_path_tokens(path, TransformErrorKind::InvalidRef, base_path.to_string())?;
    let target = match namespace {
        Namespace::Input => Some(record),
        Namespace::Context => context,
        Namespace::Out => Some(out),
        Namespace::Item => {
            let item = locals.and_then(|locals| locals.item).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    "item is only available within array ops",
                )
                .with_path(base_path)
            })?;
            let (root, rest) = match tokens.split_first() {
                Some((PathToken::Key(key), rest)) if key == "value" => (item.value, rest),
                Some((PathToken::Key(key), rest)) if key == "index" => {
                    if !rest.is_empty() {
                        return Ok(EvalValue::Missing);
                    }
                    let value = JsonValue::Number(serde_json::Number::from(item.index as u64));
                    return Ok(EvalValue::Value(value));
                }
                _ => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "item ref must start with value or index",
                    )
                    .with_path(base_path));
                }
            };
            return match get_path(root, rest) {
                Some(value) => Ok(EvalValue::Value(value.clone())),
                None => Ok(EvalValue::Missing),
            };
        }
        Namespace::Acc => {
            let acc = locals.and_then(|locals| locals.acc).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    "acc is only available within reduce/fold ops",
                )
                .with_path(base_path)
            })?;
            let (root, rest) = match tokens.split_first() {
                Some((PathToken::Key(key), rest)) if key == "value" => (acc, rest),
                _ => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "acc ref must start with value",
                    )
                    .with_path(base_path));
                }
            };
            return match get_path(root, rest) {
                Some(value) => Ok(EvalValue::Value(value.clone())),
                None => Ok(EvalValue::Missing),
            };
        }
        Namespace::Pipe => {
            let pipe_value = locals.and_then(|locals| locals.pipe).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    "pipe is only available within v2 pipes",
                )
                .with_path(base_path)
            })?;
            let (root, rest) = match tokens.split_first() {
                Some((PathToken::Key(key), rest)) if key == "value" => (pipe_value, rest),
                _ => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "pipe ref must start with value",
                    )
                    .with_path(base_path));
                }
            };
            let value = match root {
                EvalValue::Missing => return Ok(EvalValue::Missing),
                EvalValue::Value(value) => value,
            };
            return match get_path(value, rest) {
                Some(value) => Ok(EvalValue::Value(value.clone())),
                None => Ok(EvalValue::Missing),
            };
        }
        Namespace::Local => {
            let locals_map = locals.and_then(|locals| locals.locals).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    "local is only available within v2 pipes",
                )
                .with_path(base_path)
            })?;
            let (first, rest) = match tokens.split_first() {
                Some((PathToken::Key(key), rest)) => (key, rest),
                _ => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "local ref must start with a key",
                    )
                    .with_path(base_path));
                }
            };
            let local_value = locals_map.get(first).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("undefined local: {}", first),
                )
                .with_path(base_path)
            })?;
            let value = match local_value {
                EvalValue::Missing => return Ok(EvalValue::Missing),
                EvalValue::Value(value) => value,
            };
            return match get_path(value, rest) {
                Some(value) => Ok(EvalValue::Value(value.clone())),
                None => Ok(EvalValue::Missing),
            };
        }
    };

    match target.and_then(|value| get_path(value, &tokens)) {
        Some(value) => Ok(EvalValue::Value(value.clone())),
        None => Ok(EvalValue::Missing),
    }
}

pub(crate) fn eval_op(
    expr_op: &ExprOp,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    injected: Option<&EvalValue>,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(&expr_op.args, injected);
    if total_len == 0 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must be a non-empty array",
        )
        .with_path(format!("{}.args", base_path)));
    }

    match expr_op.op.as_str() {
        "concat" => {
            let mut parts = Vec::new();
            for index in 0..total_len {
                let arg_path = format!("{}.args[{}]", base_path, index);
                let value = eval_expr_at_index(
                    index,
                    &expr_op.args,
                    injected,
                    record,
                    context,
                    out,
                    base_path,
                    locals,
                )?;
                match value {
                    EvalValue::Missing => return Ok(EvalValue::Missing),
                    EvalValue::Value(value) => {
                        if value.is_null() {
                            return Err(TransformError::new(
                                TransformErrorKind::ExprError,
                                "concat does not accept null",
                            )
                            .with_path(arg_path));
                        }
                        let part = value_to_string(&value, &arg_path)?;
                        parts.push(part);
                    }
                }
            }
            Ok(EvalValue::Value(JsonValue::String(parts.join(""))))
        }
        "coalesce" => {
            for index in 0..total_len {
                let value = eval_expr_at_index(
                    index,
                    &expr_op.args,
                    injected,
                    record,
                    context,
                    out,
                    base_path,
                    locals,
                )?;
                match value {
                    EvalValue::Missing => continue,
                    EvalValue::Value(value) => {
                        if value.is_null() {
                            continue;
                        }
                        return Ok(EvalValue::Value(value));
                    }
                }
            }
            Ok(EvalValue::Missing)
        }
        "to_string" => eval_unary_string_op(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
            |value, path| value_to_string(value, path).map(JsonValue::String),
        ),
        "trim" => eval_unary_string_op(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
            |value, path| {
                let s = value_as_string(value, path)?;
                Ok(JsonValue::String(s.trim().to_string()))
            },
        ),
        "lowercase" => eval_unary_string_op(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
            |value, path| {
                let s = value_as_string(value, path)?;
                Ok(JsonValue::String(s.to_lowercase()))
            },
        ),
        "uppercase" => eval_unary_string_op(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
            |value, path| {
                let s = value_as_string(value, path)?;
                Ok(JsonValue::String(s.to_uppercase()))
            },
        ),
        "replace" => eval_replace(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "split" => eval_split(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "pad_start" => eval_pad(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            true,
            locals,
        ),
        "pad_end" => eval_pad(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            false,
            locals,
        ),
        "lookup" => eval_lookup(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            false,
            locals,
        ),
        "lookup_first" => eval_lookup(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            true,
            locals,
        ),
        "merge" => eval_json_merge(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            false,
            locals,
        ),
        "deep_merge" => eval_json_merge(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            true,
            locals,
        ),
        "get" => eval_json_get(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "pick" => eval_json_pick(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "omit" => eval_json_omit(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "keys" => eval_json_keys(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "values" => eval_json_values(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "entries" => eval_json_entries(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "len" => eval_len(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "from_entries" => eval_json_from_entries(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "object_flatten" => eval_json_object_flatten(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "object_unflatten" => eval_json_object_unflatten(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "map" => eval_array_map(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "filter" => eval_array_filter(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "flat_map" => eval_array_flat_map(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "flatten" => eval_array_flatten(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "take" => eval_array_take(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "drop" => eval_array_drop(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "slice" => eval_array_slice(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "chunk" => eval_array_chunk(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "zip" => eval_array_zip(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "zip_with" => eval_array_zip_with(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "unzip" => eval_array_unzip(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "group_by" => eval_array_group_by(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "key_by" => eval_array_key_by(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "partition" => eval_array_partition(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "unique" => eval_array_unique(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "distinct_by" => eval_array_distinct_by(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "sort_by" => eval_array_sort_by(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "find" => eval_array_find(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "find_index" => eval_array_find_index(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "index_of" => eval_array_index_of(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "contains" => eval_array_contains(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "sum" => eval_array_sum(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "avg" => eval_array_avg(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "min" => eval_array_min(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "max" => eval_array_max(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "reduce" => eval_array_reduce(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "fold" => eval_array_fold(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "+" | "-" | "*" | "/" => {
            eval_numeric_op(expr_op, injected, record, context, out, base_path, locals)
        }
        "round" => eval_round(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "to_base" => eval_to_base(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "date_format" => eval_date_format(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "to_unixtime" => eval_to_unixtime(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "and" => eval_bool_and_or(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            true,
            locals,
        ),
        "or" => eval_bool_and_or(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            false,
            locals,
        ),
        "not" => eval_bool_not(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "==" | "!=" | "<" | "<=" | ">" | ">=" | "~=" => {
            eval_compare(expr_op, injected, record, context, out, base_path, locals)
        }
        _ => Err(
            TransformError::new(TransformErrorKind::ExprError, "expr.op is not supported")
                .with_path(format!("{}.op", base_path)),
        ),
    }
}

fn eval_unary_string_op<F>(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    op: F,
) -> Result<EvalValue, TransformError>
where
    F: FnOnce(&JsonValue, &str) -> Result<JsonValue, TransformError>,
{
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let arg_path = format!("{}.args[0]", base_path);
    let value = eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    match value {
        EvalValue::Missing => Ok(EvalValue::Missing),
        EvalValue::Value(value) => {
            if value.is_null() {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "expr arg must not be null",
                )
                .with_path(arg_path));
            }
            op(&value, &arg_path).map(EvalValue::Value)
        }
    }
}

fn args_len(args: &[Expr], injected: Option<&EvalValue>) -> usize {
    args.len() + usize::from(injected.is_some())
}

fn arg_expr_at<'a>(
    index: usize,
    args: &'a [Expr],
    injected: Option<&EvalValue>,
) -> Option<&'a Expr> {
    if injected.is_some() {
        if index == 0 {
            None
        } else {
            args.get(index - 1)
        }
    } else {
        args.get(index)
    }
}

fn eval_expr_at_index(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    if injected.is_none() {
        if let Some((cached_base_path, cached_values)) =
            locals.and_then(|locals| locals.precomputed_op_args)
        {
            if cached_base_path == base_path {
                return cached_values.get(index).cloned().ok_or_else(|| {
                    TransformError::new(
                        TransformErrorKind::ExprError,
                        "expr.args index is out of bounds",
                    )
                    .with_path(format!("{}.args[{}]", base_path, index))
                });
            }
        }
    }

    if let Some(injected) = injected {
        if index == 0 {
            return Ok(injected.clone());
        }
        let arg = args.get(index - 1).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "expr.args index is out of bounds",
            )
            .with_path(format!("{}.args[{}]", base_path, index))
        })?;
        let arg_path = format!("{}.args[{}]", base_path, index);
        return eval_expr(arg, record, context, out, &arg_path, locals);
    }

    let arg = args.get(index).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[{}]", base_path, index))
    })?;
    let arg_path = format!("{}.args[{}]", base_path, index);
    eval_expr(arg, record, context, out, &arg_path, locals)
}

fn eval_arg_value_at(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<Option<JsonValue>, TransformError> {
    match eval_expr_at_index(
        index, args, injected, record, context, out, base_path, locals,
    )? {
        EvalValue::Missing => Ok(None),
        EvalValue::Value(value) => Ok(Some(value)),
    }
}

fn eval_arg_string_at(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<Option<String>, TransformError> {
    let value = match eval_arg_value_at(
        index, args, injected, record, context, out, base_path, locals,
    )? {
        None => return Ok(None),
        Some(value) => value,
    };
    let arg_path = format!("{}.args[{}]", base_path, index);
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(arg_path));
    }
    value_as_string(&value, &arg_path).map(Some)
}

fn eval_expr_value_or_null_at(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<JsonValue, TransformError> {
    match eval_expr_at_index(
        index, args, injected, record, context, out, base_path, locals,
    )? {
        EvalValue::Missing => Ok(JsonValue::Null),
        EvalValue::Value(value) => Ok(value),
    }
}

#[derive(Clone, Copy)]
enum ReplaceMode {
    LiteralFirst,
    LiteralAll,
    RegexFirst,
    RegexAll,
}

fn parse_replace_mode(value: &str, path: &str) -> Result<ReplaceMode, TransformError> {
    match value {
        "all" => Ok(ReplaceMode::LiteralAll),
        "regex" => Ok(ReplaceMode::RegexFirst),
        "regex_all" => Ok(ReplaceMode::RegexAll),
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "replace mode must be all|regex|regex_all",
        )
        .with_path(path)),
    }
}

fn eval_replace(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(3..=4).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain three or four items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value =
        match eval_arg_string_at(0, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let pattern =
        match eval_arg_string_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let replacement =
        match eval_arg_string_at(2, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let pattern_path = format!("{}.args[1]", base_path);

    let mode = if total_len == 4 {
        let mode_path = format!("{}.args[3]", base_path);
        let mode_value =
            match eval_arg_string_at(3, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        parse_replace_mode(&mode_value, &mode_path)?
    } else {
        ReplaceMode::LiteralFirst
    };

    let replaced = match mode {
        ReplaceMode::LiteralFirst => value.replacen(&pattern, &replacement, 1),
        ReplaceMode::LiteralAll => value.replace(&pattern, &replacement),
        ReplaceMode::RegexFirst => {
            let regex = cached_regex(&pattern, &pattern_path)?;
            regex.replace(&value, replacement.as_str()).to_string()
        }
        ReplaceMode::RegexAll => {
            let regex = cached_regex(&pattern, &pattern_path)?;
            regex.replace_all(&value, replacement.as_str()).to_string()
        }
    };

    Ok(EvalValue::Value(JsonValue::String(replaced)))
}

fn eval_split(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value =
        match eval_arg_string_at(0, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let delimiter =
        match eval_arg_string_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let delimiter_path = format!("{}.args[1]", base_path);

    if delimiter.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "split delimiter must not be empty",
        )
        .with_path(delimiter_path));
    }

    let parts = value
        .split(&delimiter)
        .map(|part| JsonValue::String(part.to_string()))
        .collect::<Vec<_>>();

    Ok(EvalValue::Value(JsonValue::Array(parts)))
}

fn eval_pad(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    pad_start: bool,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(2..=3).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain two or three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value =
        match eval_arg_string_at(0, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };

    let length_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let length_path = format!("{}.args[1]", base_path);
    if length_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(length_path));
    }
    let length = value_to_i64(
        &length_value,
        &length_path,
        "pad length must be a non-negative integer",
    )?;
    if length < 0 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "pad length must be a non-negative integer",
        )
        .with_path(length_path));
    }

    let pad_string = if total_len == 3 {
        match eval_arg_string_at(2, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        }
    } else {
        " ".to_string()
    };

    let target_len = usize::try_from(length).map_err(|_| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "pad length must be a non-negative integer",
        )
        .with_path(length_path)
    })?;

    let padded = pad_string_value(&value, target_len, &pad_string, pad_start);
    Ok(EvalValue::Value(JsonValue::String(padded)))
}

fn pad_string_value(value: &str, target_len: usize, pad: &str, pad_start: bool) -> String {
    let value_len = value.chars().count();
    if value_len >= target_len || pad.is_empty() {
        return value.to_string();
    }

    let needed = target_len - value_len;
    let pad_len = pad.chars().count();
    let repeats = (needed + pad_len - 1) / pad_len;
    let pad_buf = pad.repeat(repeats);
    let pad_slice = pad_buf.chars().take(needed).collect::<String>();

    if pad_start {
        format!("{}{}", pad_slice, value)
    } else {
        format!("{}{}", value, pad_slice)
    }
}

fn eval_numeric_op(
    expr_op: &ExprOp,
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let op = expr_op.op.as_str();
    let args = &expr_op.args;
    let total_len = args_len(args, injected);

    let requires_exact_two = matches!(op, "-" | "/");
    if requires_exact_two && total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }
    if !requires_exact_two && total_len < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let mut result: f64 = 0.0;
    for index in 0..total_len {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let value = match eval_arg_value_at(
            index, args, injected, record, context, out, base_path, locals,
        )? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
        if value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(arg_path));
        }
        let number = value_to_number(&value, &arg_path, "operand must be a number")?;
        if index == 0 {
            result = number;
        } else {
            result = match op {
                "+" => result + number,
                "-" => result - number,
                "*" => result * number,
                "/" => result / number,
                _ => result,
            };
        }
    }

    Ok(EvalValue::Value(json_number_from_f64(result, base_path)?))
}

fn eval_round(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(1..=2).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain one or two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value = match eval_arg_value_at(0, args, injected, record, context, out, base_path, locals)?
    {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let value_path = format!("{}.args[0]", base_path);
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(value_path));
    }
    let number = value_to_number(&value, &value_path, "operand must be a number")?;

    let scale = if total_len == 2 {
        let scale_path = format!("{}.args[1]", base_path);
        let scale_value =
            match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if scale_value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(scale_path));
        }
        let scale = value_to_i64(
            &scale_value,
            &scale_path,
            "scale must be a non-negative integer",
        )?;
        if scale < 0 {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "scale must be a non-negative integer",
            )
            .with_path(scale_path));
        }
        if scale > 308 {
            return Err(
                TransformError::new(TransformErrorKind::ExprError, "scale is too large")
                    .with_path(scale_path),
            );
        }
        scale as i32
    } else {
        0
    };

    let rounded = if scale == 0 {
        number.round()
    } else {
        let factor = 10f64.powi(scale);
        (number * factor).round() / factor
    };

    Ok(EvalValue::Value(json_number_from_f64(rounded, base_path)?))
}

fn eval_to_base(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value = match eval_arg_value_at(0, args, injected, record, context, out, base_path, locals)?
    {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let base_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let value_path = format!("{}.args[0]", base_path);
    let base_path_arg = format!("{}.args[1]", base_path);
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(value_path));
    }
    if base_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(base_path_arg));
    }

    let number = value_to_i64(&value, &value_path, "value must be an integer")?;
    let base = value_to_i64(&base_value, &base_path_arg, "base must be an integer")?;
    if !(2..=36).contains(&base) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "base must be between 2 and 36",
        )
        .with_path(base_path_arg));
    }

    let formatted = to_radix_string(number, base as u32, &value_path)?;
    Ok(EvalValue::Value(JsonValue::String(formatted)))
}

fn eval_date_format(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(2..=4).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain two to four items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value =
        match eval_arg_string_at(0, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let output_format =
        match eval_arg_string_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let value_path = format!("{}.args[0]", base_path);
    let mut input_formats: Option<Vec<String>> = None;
    let mut timezone: Option<FixedOffset> = None;

    if total_len >= 3 {
        let input_path = format!("{}.args[2]", base_path);
        let input_value =
            match eval_arg_value_at(2, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if input_value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(input_path));
        }

        if let Some(value) = input_value.as_str() {
            if looks_like_timezone(value) {
                timezone = Some(parse_timezone(value, &input_path)?);
            } else {
                input_formats = Some(parse_format_list(&input_value, &input_path)?);
            }
        } else {
            input_formats = Some(parse_format_list(&input_value, &input_path)?);
        }
    }

    if total_len == 4 {
        let tz_path = format!("{}.args[3]", base_path);
        let tz_value =
            match eval_arg_string_at(3, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        timezone = Some(parse_timezone(&tz_value, &tz_path)?);
    }

    let dt = parse_datetime(&value, input_formats.as_deref(), timezone, &value_path)?;
    let dt = match timezone {
        Some(offset) => dt.with_timezone(&offset),
        None => dt,
    };
    let formatted = dt.format(&output_format).to_string();
    Ok(EvalValue::Value(JsonValue::String(formatted)))
}

fn eval_to_unixtime(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(1..=3).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain one to three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value =
        match eval_arg_string_at(0, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    let value_path = format!("{}.args[0]", base_path);

    let mut unit = "s".to_string();
    let mut timezone: Option<FixedOffset> = None;

    if total_len >= 2 {
        let arg_path = format!("{}.args[1]", base_path);
        let arg_value =
            match eval_arg_string_at(1, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if total_len == 3 {
            if arg_value != "s" && arg_value != "ms" {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "unit must be s or ms",
                )
                .with_path(arg_path));
            }
            unit = arg_value;
        } else if arg_value == "s" || arg_value == "ms" {
            unit = arg_value;
        } else if looks_like_timezone(&arg_value) {
            timezone = Some(parse_timezone(&arg_value, &arg_path)?);
        } else {
            return Err(
                TransformError::new(TransformErrorKind::ExprError, "unit must be s or ms")
                    .with_path(arg_path),
            );
        }
    }

    if total_len == 3 {
        let tz_path = format!("{}.args[2]", base_path);
        let tz_value =
            match eval_arg_string_at(2, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        timezone = Some(parse_timezone(&tz_value, &tz_path)?);
    }

    let dt = parse_datetime(&value, None, timezone, &value_path)?;
    let dt = match timezone {
        Some(offset) => dt.with_timezone(&offset),
        None => dt,
    };
    let timestamp = if unit == "ms" {
        dt.timestamp_millis()
    } else {
        dt.timestamp()
    };

    Ok(EvalValue::Value(JsonValue::Number(timestamp.into())))
}

fn eval_lookup(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    first_only: bool,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(3..=4).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "lookup args must be [collection, key_path, match_value, output_path?]",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let collection_path = format!("{}.args[0]", base_path);
    let collection =
        match eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)? {
            EvalValue::Missing => return Ok(EvalValue::Missing),
            EvalValue::Value(value) => value,
        };
    let collection_array = match collection {
        JsonValue::Array(items) => items,
        JsonValue::Null => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "lookup collection must be an array",
            )
            .with_path(collection_path));
        }
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "lookup collection must be an array",
            )
            .with_path(collection_path));
        }
    };

    let key_expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "lookup key_path must be a non-empty string literal",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let key_path = literal_string(key_expr).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "lookup key_path must be a non-empty string literal",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    if key_path.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "lookup key_path must be a non-empty string literal",
        )
        .with_path(format!("{}.args[1]", base_path)));
    }
    let key_tokens = parse_path(key_path).map_err(|_| {
        TransformError::new(TransformErrorKind::ExprError, "lookup key_path is invalid")
            .with_path(format!("{}.args[1]", base_path))
    })?;

    let output_tokens = if total_len == 4 {
        let output_expr = arg_expr_at(3, args, injected).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "lookup output_path must be a non-empty string literal",
            )
            .with_path(format!("{}.args[3]", base_path))
        })?;
        let value = literal_string(output_expr).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "lookup output_path must be a non-empty string literal",
            )
            .with_path(format!("{}.args[3]", base_path))
        })?;
        if value.is_empty() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "lookup output_path must be a non-empty string literal",
            )
            .with_path(format!("{}.args[3]", base_path)));
        }
        let tokens = parse_path(value).map_err(|_| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "lookup output_path is invalid",
            )
            .with_path(format!("{}.args[3]", base_path))
        })?;
        Some(tokens)
    } else {
        None
    };

    let match_path = format!("{}.args[2]", base_path);
    let match_value =
        match eval_expr_at_index(2, args, injected, record, context, out, base_path, locals)? {
            EvalValue::Missing => return Ok(EvalValue::Missing),
            EvalValue::Value(value) => value,
        };
    if match_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "lookup match_value must not be null",
        )
        .with_path(match_path));
    }
    let match_key = value_to_string(&match_value, &match_path)?;

    let mut results = Vec::new();
    for item in &collection_array {
        let key_value = match get_path(item, &key_tokens) {
            Some(value) => value,
            None => continue,
        };
        let item_key = match value_to_string_optional(key_value) {
            Some(value) => value,
            None => continue,
        };
        if item_key != match_key {
            continue;
        }

        let selected = match output_tokens.as_ref() {
            Some(tokens) => get_path(item, tokens),
            None => Some(item),
        };

        if let Some(value) = selected {
            if first_only {
                return Ok(EvalValue::Value(value.clone()));
            }
            results.push(value.clone());
        }
    }

    if results.is_empty() {
        Ok(EvalValue::Missing)
    } else {
        Ok(EvalValue::Value(JsonValue::Array(results)))
    }
}

fn locals_with_item<'a>(locals: Option<&EvalLocals<'a>>, item: EvalItem<'a>) -> EvalLocals<'a> {
    EvalLocals {
        item: Some(item),
        acc: locals.and_then(|locals| locals.acc),
        pipe: locals.and_then(|locals| locals.pipe),
        locals: locals.and_then(|locals| locals.locals),
        precomputed_op_args: locals.and_then(|locals| locals.precomputed_op_args),
    }
}

fn locals_with_precomputed_args<'a>(
    locals: Option<&EvalLocals<'a>>,
    base_path: &'a str,
    arg_values: &'a [EvalValue],
) -> EvalLocals<'a> {
    EvalLocals {
        item: locals.and_then(|locals| locals.item),
        acc: locals.and_then(|locals| locals.acc),
        pipe: locals.and_then(|locals| locals.pipe),
        locals: locals.and_then(|locals| locals.locals),
        precomputed_op_args: Some((base_path, arg_values)),
    }
}

fn eval_array_arg(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<Vec<JsonValue>, TransformError> {
    let arg_path = format!("{}.args[{}]", base_path, index);
    match eval_expr_at_index(
        index, args, injected, record, context, out, base_path, locals,
    )? {
        EvalValue::Missing => Ok(Vec::new()),
        EvalValue::Value(value) => {
            if value.is_null() {
                Ok(Vec::new())
            } else if let JsonValue::Array(items) = value {
                Ok(items)
            } else {
                Err(
                    TransformError::new(TransformErrorKind::ExprError, "expr arg must be an array")
                        .with_path(arg_path),
                )
            }
        }
    }
}

fn eval_expr_or_null(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<JsonValue, TransformError> {
    match eval_expr(expr, record, context, out, base_path, locals)? {
        EvalValue::Missing => Ok(JsonValue::Null),
        EvalValue::Value(value) => Ok(value),
    }
}

fn eval_predicate_expr(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<bool, TransformError> {
    match eval_expr(expr, record, context, out, base_path, locals)? {
        EvalValue::Missing => Ok(false),
        EvalValue::Value(value) => {
            if value.is_null() {
                return Ok(false);
            }
            let flag = value_as_bool(&value, base_path)?;
            Ok(flag)
        }
    }
}

fn eval_key_expr_string(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<String, TransformError> {
    let value = match eval_expr(expr, record, context, out, base_path, locals)? {
        EvalValue::Missing => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be missing",
            )
            .with_path(base_path));
        }
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(base_path));
    }
    value_to_string(&value, base_path)
}

fn ensure_eq_compatible(value: &JsonValue, path: &str) -> Result<(), TransformError> {
    if value.is_null() {
        return Ok(());
    }
    if value_to_string_optional(value).is_some() {
        return Ok(());
    }
    Err(expr_type_error(
        "value must be string/number/bool or null",
        path,
    ))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SortKeyKind {
    Number,
    String,
    Bool,
}

#[derive(Clone)]
enum SortKey {
    Number(f64),
    String(String),
    Bool(bool),
}

impl SortKey {
    fn kind(&self) -> SortKeyKind {
        match self {
            SortKey::Number(_) => SortKeyKind::Number,
            SortKey::String(_) => SortKeyKind::String,
            SortKey::Bool(_) => SortKeyKind::Bool,
        }
    }
}

fn compare_sort_keys(left: &SortKey, right: &SortKey) -> Ordering {
    match (left, right) {
        (SortKey::Number(l), SortKey::Number(r)) => l.partial_cmp(r).unwrap_or(Ordering::Equal),
        (SortKey::String(l), SortKey::String(r)) => l.cmp(r),
        (SortKey::Bool(l), SortKey::Bool(r)) => l.cmp(r),
        _ => Ordering::Equal,
    }
}

fn eval_sort_key(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<SortKey, TransformError> {
    let value = match eval_expr(expr, record, context, out, base_path, locals)? {
        EvalValue::Missing => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be missing",
            )
            .with_path(base_path));
        }
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(base_path));
    }

    match value {
        JsonValue::Number(number) => {
            let value = number
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or_else(|| expr_type_error("sort_by key must be a finite number", base_path))?;
            Ok(SortKey::Number(value))
        }
        JsonValue::String(value) => Ok(SortKey::String(value)),
        JsonValue::Bool(value) => Ok(SortKey::Bool(value)),
        _ => Err(expr_type_error(
            "sort_by key must be string/number/bool",
            base_path,
        )),
    }
}

fn eval_array_map(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Vec::with_capacity(array.len());
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        results.push(value);
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_filter(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Vec::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        if eval_predicate_expr(expr, record, context, out, &expr_path, Some(&item_locals))? {
            results.push(item.clone());
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_flat_map(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Vec::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        match value {
            JsonValue::Array(items) => results.extend(items),
            value => results.push(value),
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn flatten_value(value: &JsonValue, depth: usize, out: &mut Vec<JsonValue>) {
    if depth == 0 {
        out.push(value.clone());
        return;
    }

    if let JsonValue::Array(items) = value {
        for item in items {
            flatten_value(item, depth - 1, out);
        }
    } else {
        out.push(value.clone());
    }
}

fn eval_array_flatten(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(1..=2).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain one or two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let depth = if total_len == 2 {
        let depth_path = format!("{}.args[1]", base_path);
        let depth_value =
            match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if depth_value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(depth_path));
        }
        let depth = value_to_i64(
            &depth_value,
            &depth_path,
            "depth must be a non-negative integer",
        )?;
        if depth < 0 {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "depth must be a non-negative integer",
            )
            .with_path(depth_path));
        }
        usize::try_from(depth).map_err(|_| {
            TransformError::new(TransformErrorKind::ExprError, "depth is too large")
                .with_path(depth_path)
        })?
    } else {
        1
    };

    let mut results = Vec::new();
    for item in &array {
        flatten_value(item, depth, &mut results);
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_take(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let count_path = format!("{}.args[1]", base_path);
    let count_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    if count_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(count_path));
    }
    let count = value_to_i64(&count_value, &count_path, "count must be an integer")?;

    let len = array.len() as i64;
    let results = if count >= 0 {
        let take_count = count.min(len).max(0) as usize;
        array[..take_count].to_vec()
    } else {
        let abs_count = if count == i64::MIN {
            (i64::MAX as u64) + 1
        } else {
            (-count) as u64
        };
        let take_count = abs_count.min(array.len() as u64) as usize;
        let start = array.len().saturating_sub(take_count);
        array[start..].to_vec()
    };

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_drop(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let count_path = format!("{}.args[1]", base_path);
    let count_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    if count_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(count_path));
    }
    let count = value_to_i64(&count_value, &count_path, "count must be an integer")?;

    let len = array.len() as i64;
    let results = if count >= 0 {
        let drop_count = count.min(len).max(0) as usize;
        array[drop_count..].to_vec()
    } else {
        let abs_count = if count == i64::MIN {
            (i64::MAX as u64) + 1
        } else {
            (-count) as u64
        };
        let drop_count = abs_count.min(array.len() as u64) as usize;
        let end = array.len().saturating_sub(drop_count);
        array[..end].to_vec()
    };

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_slice(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(2..=3).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain two or three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let len = array.len() as i64;

    let start_path = format!("{}.args[1]", base_path);
    let start_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    if start_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(start_path));
    }
    let start = value_to_i64(&start_value, &start_path, "start must be an integer")?;

    let end = if total_len == 3 {
        let end_path = format!("{}.args[2]", base_path);
        let end_value =
            match eval_arg_value_at(2, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if end_value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(end_path));
        }
        value_to_i64(&end_value, &end_path, "end must be an integer")?
    } else {
        len
    };

    let mut start_index = if start < 0 { len + start } else { start };
    let mut end_index = if end < 0 { len + end } else { end };
    start_index = start_index.clamp(0, len);
    end_index = end_index.clamp(0, len);

    let results = if end_index <= start_index {
        Vec::new()
    } else {
        array[start_index as usize..end_index as usize].to_vec()
    };

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_chunk(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let size_path = format!("{}.args[1]", base_path);
    let size_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    if size_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(size_path));
    }
    let size = value_to_i64(&size_value, &size_path, "size must be a positive integer")?;
    if size <= 0 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "size must be a positive integer",
        )
        .with_path(size_path));
    }
    let size = usize::try_from(size).map_err(|_| {
        TransformError::new(TransformErrorKind::ExprError, "size is too large").with_path(size_path)
    })?;

    let mut chunks = Vec::new();
    let mut index = 0;
    while index < array.len() {
        let end = (index + size).min(array.len());
        chunks.push(JsonValue::Array(array[index..end].to_vec()));
        index = end;
    }

    Ok(EvalValue::Value(JsonValue::Array(chunks)))
}

fn eval_array_zip(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let mut arrays = Vec::new();
    for index in 0..total_len {
        arrays.push(eval_array_arg(
            index, args, injected, record, context, out, base_path, locals,
        )?);
    }

    let min_len = arrays.iter().map(|items| items.len()).min().unwrap_or(0);
    let mut results = Vec::with_capacity(min_len);
    for idx in 0..min_len {
        let mut row = Vec::with_capacity(arrays.len());
        for array in &arrays {
            row.push(array[idx].clone());
        }
        results.push(JsonValue::Array(row));
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_zip_with(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len < 3 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let expr_index = total_len - 1;
    let expr = arg_expr_at(expr_index, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[{}]", base_path, expr_index))
    })?;
    let expr_arg_index = if injected.is_some() {
        expr_index - 1
    } else {
        expr_index
    };
    let expr_path = format!("{}.args[{}]", base_path, expr_arg_index);

    let mut arrays = Vec::new();
    for index in 0..expr_index {
        arrays.push(eval_array_arg(
            index, args, injected, record, context, out, base_path, locals,
        )?);
    }

    let min_len = arrays.iter().map(|items| items.len()).min().unwrap_or(0);
    let mut results = Vec::with_capacity(min_len);
    for idx in 0..min_len {
        let mut row = Vec::with_capacity(arrays.len());
        for array in &arrays {
            row.push(array[idx].clone());
        }
        let row_value = JsonValue::Array(row);
        let item_locals = locals_with_item(
            locals,
            EvalItem {
                value: &row_value,
                index: idx,
            },
        );
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        results.push(value);
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_unzip(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Array(Vec::new())));
    }

    let mut columns: Vec<Vec<JsonValue>> = Vec::new();
    let mut expected_len: Option<usize> = None;
    for item in &array {
        let items = match item {
            JsonValue::Array(items) => items,
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "unzip items must be arrays",
                )
                .with_path(format!("{}.args[0]", base_path)));
            }
        };
        if let Some(expected) = expected_len {
            if items.len() != expected {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "unzip items must have the same length",
                )
                .with_path(format!("{}.args[0]", base_path)));
            }
        } else {
            expected_len = Some(items.len());
            columns = vec![Vec::with_capacity(array.len()); items.len()];
        }
        for (index, value) in items.iter().enumerate() {
            if let Some(column) = columns.get_mut(index) {
                column.push(value.clone());
            }
        }
    }

    let output = columns
        .into_iter()
        .map(JsonValue::Array)
        .collect::<Vec<_>>();
    Ok(EvalValue::Value(JsonValue::Array(output)))
}

fn eval_array_group_by(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Map::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let key = eval_key_expr_string(expr, record, context, out, &expr_path, Some(&item_locals))?;
        let entry = results
            .entry(key)
            .or_insert_with(|| JsonValue::Array(Vec::new()));
        if let JsonValue::Array(items) = entry {
            items.push(item.clone());
        }
    }

    Ok(EvalValue::Value(JsonValue::Object(results)))
}

fn eval_array_key_by(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Map::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let key = eval_key_expr_string(expr, record, context, out, &expr_path, Some(&item_locals))?;
        results.insert(key, item.clone());
    }

    Ok(EvalValue::Value(JsonValue::Object(results)))
}

fn eval_array_partition(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut matched = Vec::new();
    let mut unmatched = Vec::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        if eval_predicate_expr(expr, record, context, out, &expr_path, Some(&item_locals))? {
            matched.push(item.clone());
        } else {
            unmatched.push(item.clone());
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(vec![
        JsonValue::Array(matched),
        JsonValue::Array(unmatched),
    ])))
}

fn eval_array_unique(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let item_path = format!("{}.args[0]", base_path);

    let mut results: Vec<JsonValue> = Vec::new();
    for item in array {
        ensure_eq_compatible(&item, &item_path)?;
        let mut exists = false;
        for existing in &results {
            if compare_eq(&item, existing, &item_path, &item_path)? {
                exists = true;
                break;
            }
        }
        if !exists {
            results.push(item);
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_distinct_by(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Vec::new();
    let mut seen = HashSet::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let key = eval_key_expr_string(expr, record, context, out, &expr_path, Some(&item_locals))?;
        if seen.insert(key) {
            results.push(item.clone());
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_sort_by(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(2..=3).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain two or three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Array(Vec::new())));
    }

    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let order = if total_len == 3 {
        let order_path = format!("{}.args[2]", base_path);
        let value =
            match eval_arg_string_at(2, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if value != "asc" && value != "desc" {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "order must be asc or desc",
            )
            .with_path(order_path));
        }
        value
    } else {
        "asc".to_string()
    };

    struct SortItem {
        key: SortKey,
        index: usize,
        value: JsonValue,
    }

    let mut items = Vec::with_capacity(array.len());
    let mut key_kind: Option<SortKeyKind> = None;
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let key = eval_sort_key(expr, record, context, out, &expr_path, Some(&item_locals))?;
        let kind = key.kind();
        if let Some(existing) = key_kind {
            if existing != kind {
                return Err(expr_type_error(
                    "sort_by keys must be all the same type",
                    &expr_path,
                ));
            }
        } else {
            key_kind = Some(kind);
        }
        items.push(SortItem {
            key,
            index,
            value: item.clone(),
        });
    }

    items.sort_by(|left, right| {
        let mut ordering = compare_sort_keys(&left.key, &right.key);
        if order == "desc" {
            ordering = ordering.reverse();
        }
        if ordering == Ordering::Equal {
            left.index.cmp(&right.index)
        } else {
            ordering
        }
    });

    let results = items.into_iter().map(|item| item.value).collect::<Vec<_>>();
    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_find(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        if eval_predicate_expr(expr, record, context, out, &expr_path, Some(&item_locals))? {
            return Ok(EvalValue::Value(item.clone()));
        }
    }

    Ok(EvalValue::Value(JsonValue::Null))
}

fn eval_array_find_index(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        if eval_predicate_expr(expr, record, context, out, &expr_path, Some(&item_locals))? {
            return Ok(EvalValue::Value(JsonValue::Number((index as i64).into())));
        }
    }

    Ok(EvalValue::Value(JsonValue::Number((-1).into())))
}

fn eval_array_index_of(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let value_path = format!("{}.args[1]", base_path);
    let value =
        eval_expr_value_or_null_at(1, args, injected, record, context, out, base_path, locals)?;

    ensure_eq_compatible(&value, &value_path)?;
    let item_path = format!("{}.args[0]", base_path);
    for (index, item) in array.iter().enumerate() {
        ensure_eq_compatible(item, &item_path)?;
        if compare_eq(item, &value, &item_path, &value_path)? {
            return Ok(EvalValue::Value(JsonValue::Number((index as i64).into())));
        }
    }

    Ok(EvalValue::Value(JsonValue::Number((-1).into())))
}

fn eval_array_contains(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let value_path = format!("{}.args[1]", base_path);
    let value =
        eval_expr_value_or_null_at(1, args, injected, record, context, out, base_path, locals)?;

    ensure_eq_compatible(&value, &value_path)?;
    let item_path = format!("{}.args[0]", base_path);
    for item in &array {
        ensure_eq_compatible(item, &item_path)?;
        if compare_eq(item, &value, &item_path, &value_path)? {
            return Ok(EvalValue::Value(JsonValue::Bool(true)));
        }
    }

    Ok(EvalValue::Value(JsonValue::Bool(false)))
}

fn eval_array_sum(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let item_path = format!("{}.args[0]", base_path);
    let mut sum = 0.0;
    for item in &array {
        let value = value_to_number(item, &item_path, "array item must be a number")?;
        sum += value;
    }

    Ok(EvalValue::Value(json_number_from_f64(sum, base_path)?))
}

fn eval_array_avg(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let item_path = format!("{}.args[0]", base_path);
    let mut sum = 0.0;
    for item in &array {
        let value = value_to_number(item, &item_path, "array item must be a number")?;
        sum += value;
    }
    let avg = sum / array.len() as f64;

    Ok(EvalValue::Value(json_number_from_f64(avg, base_path)?))
}

fn eval_array_min(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let item_path = format!("{}.args[0]", base_path);
    let mut min_value: Option<f64> = None;
    for item in &array {
        let value = value_to_number(item, &item_path, "array item must be a number")?;
        min_value = Some(match min_value {
            Some(current) => current.min(value),
            None => value,
        });
    }

    Ok(EvalValue::Value(json_number_from_f64(
        min_value.unwrap_or(0.0),
        base_path,
    )?))
}

fn eval_array_max(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let item_path = format!("{}.args[0]", base_path);
    let mut max_value: Option<f64> = None;
    for item in &array {
        let value = value_to_number(item, &item_path, "array item must be a number")?;
        max_value = Some(match max_value {
            Some(current) => current.max(value),
            None => value,
        });
    }

    Ok(EvalValue::Value(json_number_from_f64(
        max_value.unwrap_or(0.0),
        base_path,
    )?))
}

fn eval_array_reduce(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut acc = array[0].clone();
    for (index, item) in array.iter().enumerate().skip(1) {
        let item_locals = EvalLocals {
            item: Some(EvalItem { value: item, index }),
            acc: Some(&acc),
            pipe: locals.and_then(|locals| locals.pipe),
            locals: locals.and_then(|locals| locals.locals),
            precomputed_op_args: locals.and_then(|locals| locals.precomputed_op_args),
        };
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        acc = value;
    }

    Ok(EvalValue::Value(acc))
}

fn eval_array_fold(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 3 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let initial =
        match eval_expr_at_index(1, args, injected, record, context, out, base_path, locals)? {
            EvalValue::Missing => return Ok(EvalValue::Missing),
            EvalValue::Value(value) => value,
        };

    let expr = arg_expr_at(2, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[2]", base_path))
    })?;
    let expr_index = if injected.is_some() { 1 } else { 2 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut acc = initial;
    for (index, item) in array.iter().enumerate() {
        let item_locals = EvalLocals {
            item: Some(EvalItem { value: item, index }),
            acc: Some(&acc),
            pipe: locals.and_then(|locals| locals.pipe),
            locals: locals.and_then(|locals| locals.locals),
            precomputed_op_args: locals.and_then(|locals| locals.precomputed_op_args),
        };
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        acc = value;
    }

    Ok(EvalValue::Value(acc))
}

fn eval_json_merge(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    deep: bool,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let mut result: Option<Map<String, JsonValue>> = None;
    for index in 0..total_len {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let value = eval_expr_at_index(
            index, args, injected, record, context, out, base_path, locals,
        )?;
        let value = match value {
            EvalValue::Missing => continue,
            EvalValue::Value(value) => value,
        };
        if value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(arg_path));
        }
        let obj = match value {
            JsonValue::Object(map) => map,
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "expr arg must be object",
                )
                .with_path(arg_path));
            }
        };

        match result {
            Some(ref mut existing) => merge_object(existing, &obj, deep),
            None => result = Some(obj),
        }
    }

    match result {
        Some(map) => Ok(EvalValue::Value(JsonValue::Object(map))),
        None => Ok(EvalValue::Missing),
    }
}

fn eval_json_get(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let base_value =
        eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    let base_value = match base_value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if base_value.is_null() {
        return Ok(EvalValue::Missing);
    }

    let path_path = format!("{}.args[1]", base_path);
    let path_value =
        eval_expr_at_index(1, args, injected, record, context, out, base_path, locals)?;
    let path_value = match path_value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if path_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(path_path));
    }
    let path = value_as_string(&path_value, &path_path)?;
    if path.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "path must be a non-empty string",
        )
        .with_path(path_path));
    }
    let tokens = parse_path_tokens(&path, TransformErrorKind::ExprError, &path_path)?;
    match get_path(&base_value, &tokens) {
        Some(value) => Ok(EvalValue::Value(value.clone())),
        None => Ok(EvalValue::Missing),
    }
}

fn eval_json_pick(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let base_path_arg = format!("{}.args[0]", base_path);
    let base_value =
        eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    let base_value = match base_value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if base_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(base_path_arg));
    }
    let base_obj = match base_value {
        JsonValue::Object(map) => map,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must be object",
            )
            .with_path(base_path_arg));
        }
    };
    let base_value = JsonValue::Object(base_obj);

    let paths = eval_json_paths_arg(
        args, injected, record, context, out, base_path, locals, 1, true,
    )?;
    let Some(paths) = paths else {
        return Ok(EvalValue::Missing);
    };

    let mut output = JsonValue::Object(Map::new());
    for tokens in paths {
        if let Some(value) = get_path(&base_value, &tokens) {
            set_path_with_indexes(&mut output, &tokens, value.clone(), base_path)?;
        }
    }

    Ok(EvalValue::Value(output))
}

fn eval_json_omit(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let base_path_arg = format!("{}.args[0]", base_path);
    let base_value =
        eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    let mut base_value = match base_value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if base_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(base_path_arg));
    }
    let base_obj = match base_value {
        JsonValue::Object(map) => map,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must be object",
            )
            .with_path(base_path_arg));
        }
    };
    base_value = JsonValue::Object(base_obj);

    let paths = eval_json_paths_arg(
        args, injected, record, context, out, base_path, locals, 1, false,
    )?;
    let Some(paths) = paths else {
        return Ok(EvalValue::Missing);
    };

    for tokens in paths {
        remove_path(&mut base_value, &tokens);
    }

    Ok(EvalValue::Value(base_value))
}

fn eval_json_keys(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_json_object_unary(
        args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
        |map| {
            Ok(JsonValue::Array(
                map.keys().cloned().map(JsonValue::String).collect(),
            ))
        },
    )
}

fn eval_json_values(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_json_object_unary(
        args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
        |map| Ok(JsonValue::Array(map.values().cloned().collect())),
    )
}

fn eval_json_entries(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_json_object_unary(
        args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
        |map| {
            let mut entries = Vec::with_capacity(map.len());
            for (key, value) in map {
                let mut entry = Map::new();
                entry.insert("key".to_string(), JsonValue::String(key.clone()));
                entry.insert("value".to_string(), value.clone());
                entries.push(JsonValue::Object(entry));
            }
            Ok(JsonValue::Array(entries))
        },
    )
}

fn eval_len(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let arg_path = format!("{}.args[0]", base_path);
    let value = eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    let value = match value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(arg_path));
    }

    let len = match value {
        JsonValue::String(value) => value.chars().count(),
        JsonValue::Array(items) => items.len(),
        JsonValue::Object(map) => map.len(),
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must be string, array, or object",
            )
            .with_path(arg_path));
        }
    };

    Ok(EvalValue::Value(JsonValue::Number(
        serde_json::Number::from(len as u64),
    )))
}

fn eval_json_from_entries(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(1..=2).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain one or two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let arg_path = format!("{}.args[0]", base_path);
    let first_value =
        eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    let first_value = match first_value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if first_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(&arg_path));
    }

    if total_len == 1 {
        return match first_value {
            JsonValue::Object(map) => Ok(EvalValue::Value(JsonValue::Object(map))),
            JsonValue::Array(items) => {
                let mut output = Map::new();
                for (index, item) in items.iter().enumerate() {
                    let entry_path = format!("{}[{}]", arg_path, index);
                    match item {
                        JsonValue::Array(pair) => {
                            if pair.len() != 2 {
                                return Err(TransformError::new(
                                    TransformErrorKind::ExprError,
                                    "entries must have exactly two items",
                                )
                                .with_path(&entry_path));
                            }
                            let key_path = format!("{}[0]", entry_path);
                            let key = value_to_string(&pair[0], &key_path)?;
                            let value = pair[1].clone();
                            output.insert(key, value);
                        }
                        JsonValue::Object(map) => {
                            let key_path = format!("{}.key", entry_path);
                            let value_path = format!("{}.value", entry_path);
                            let key_value = map.get("key").ok_or_else(|| {
                                TransformError::new(
                                    TransformErrorKind::ExprError,
                                    "entry must contain key",
                                )
                                .with_path(&key_path)
                            })?;
                            if key_value.is_null() {
                                return Err(TransformError::new(
                                    TransformErrorKind::ExprError,
                                    "entry key must not be null",
                                )
                                .with_path(&key_path));
                            }
                            let value_value = map.get("value").ok_or_else(|| {
                                TransformError::new(
                                    TransformErrorKind::ExprError,
                                    "entry must contain value",
                                )
                                .with_path(&value_path)
                            })?;
                            let key = value_to_string(key_value, &key_path)?;
                            output.insert(key, value_value.clone());
                        }
                        _ => {
                            return Err(TransformError::new(
                                TransformErrorKind::ExprError,
                                "entries must be arrays or objects",
                            )
                            .with_path(&entry_path));
                        }
                    }
                }
                Ok(EvalValue::Value(JsonValue::Object(output)))
            }
            _ => Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must be object or array",
            )
            .with_path(arg_path)),
        };
    }

    let key = value_to_string(&first_value, &arg_path)?;
    let value =
        match eval_expr_at_index(1, args, injected, record, context, out, base_path, locals)? {
            EvalValue::Missing => return Ok(EvalValue::Missing),
            EvalValue::Value(value) => value,
        };
    let mut output = Map::new();
    output.insert(key, value);
    Ok(EvalValue::Value(JsonValue::Object(output)))
}

fn eval_json_object_flatten(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_json_object_unary(
        args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
        |map| {
            let mut output = Map::new();
            let mut tokens = Vec::new();
            flatten_object(map, &mut tokens, &mut output, base_path)?;
            Ok(JsonValue::Object(output))
        },
    )
}

fn eval_json_object_unflatten(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_json_object_unary(
        args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
        |map| {
            let mut paths = Vec::with_capacity(map.len());
            let mut values = Vec::with_capacity(map.len());
            for (key, value) in map {
                let tokens = parse_path_tokens(
                    key,
                    TransformErrorKind::ExprError,
                    format!("{}.args[0]", base_path),
                )?;
                if tokens
                    .iter()
                    .any(|token| matches!(token, PathToken::Index(_)))
                {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "array indexes are not allowed in path",
                    )
                    .with_path(format!("{}.args[0]", base_path)));
                }
                if has_path_conflict(&paths, &tokens) {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "path conflicts with another path",
                    )
                    .with_path(format!("{}.args[0]", base_path)));
                }
                paths.push(tokens);
                values.push(value.clone());
            }

            let mut root = JsonValue::Object(Map::new());
            for (tokens, value) in paths.into_iter().zip(values) {
                set_path_object_only(&mut root, &tokens, value, base_path)?;
            }

            Ok(root)
        },
    )
}

fn eval_json_object_unary<F>(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    op: F,
) -> Result<EvalValue, TransformError>
where
    F: FnOnce(&Map<String, JsonValue>) -> Result<JsonValue, TransformError>,
{
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let arg_path = format!("{}.args[0]", base_path);
    let value = eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    let value = match value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(arg_path));
    }
    let map = match value {
        JsonValue::Object(map) => map,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must be object",
            )
            .with_path(arg_path));
        }
    };

    op(&map).map(EvalValue::Value)
}

fn eval_json_paths_arg(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    index: usize,
    allow_terminal_index: bool,
) -> Result<Option<Vec<Vec<PathToken>>>, TransformError> {
    let arg_path = format!("{}.args[{}]", base_path, index);
    let value = eval_expr_at_index(
        index, args, injected, record, context, out, base_path, locals,
    )?;
    let value = match value {
        EvalValue::Missing => return Ok(None),
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(arg_path));
    }
    let items: Vec<(String, String)> = match value {
        JsonValue::String(path) => vec![(arg_path.clone(), path)],
        JsonValue::Array(items) => items
            .iter()
            .enumerate()
            .map(|(path_index, item)| {
                let item_path = format!("{}.args[{}][{}]", base_path, index, path_index);
                let path = item.as_str().ok_or_else(|| {
                    TransformError::new(
                        TransformErrorKind::ExprError,
                        "paths must be a string or array of strings",
                    )
                    .with_path(&item_path)
                })?;
                Ok::<(String, String), TransformError>((item_path, path.to_string()))
            })
            .collect::<Result<Vec<_>, TransformError>>()?,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "paths must be a string or array of strings",
            )
            .with_path(arg_path));
        }
    };

    let mut paths = Vec::new();
    for (item_path, path) in items {
        let tokens = parse_path_tokens(&path, TransformErrorKind::ExprError, &item_path)?;
        if !allow_terminal_index && matches!(tokens.last(), Some(PathToken::Index(_))) {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "path must not end with array index",
            )
            .with_path(item_path));
        }
        if has_duplicate_path(&paths, &tokens) {
            continue;
        }
        if has_path_conflict(&paths, &tokens) {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "path conflicts with another path",
            )
            .with_path(item_path));
        }
        paths.push(tokens);
    }

    Ok(Some(paths))
}

fn has_duplicate_path(paths: &[Vec<PathToken>], tokens: &[PathToken]) -> bool {
    paths.iter().any(|existing| existing == tokens)
}

fn has_path_conflict(paths: &[Vec<PathToken>], tokens: &[PathToken]) -> bool {
    paths
        .iter()
        .any(|existing| is_path_prefix(existing, tokens) || is_path_prefix(tokens, existing))
}

fn is_path_prefix(prefix: &[PathToken], tokens: &[PathToken]) -> bool {
    if prefix.len() > tokens.len() {
        return false;
    }
    prefix.iter().zip(tokens).all(|(left, right)| left == right)
}

fn merge_object(
    target: &mut Map<String, JsonValue>,
    incoming: &Map<String, JsonValue>,
    deep: bool,
) {
    for (key, value) in incoming {
        if deep {
            if let (Some(JsonValue::Object(target_obj)), JsonValue::Object(incoming_obj)) =
                (target.get_mut(key), value)
            {
                merge_object(target_obj, incoming_obj, true);
                continue;
            }
        }
        target.insert(key.clone(), value.clone());
    }
}

fn flatten_object(
    map: &Map<String, JsonValue>,
    tokens: &mut Vec<PathToken>,
    output: &mut Map<String, JsonValue>,
    base_path: &str,
) -> Result<(), TransformError> {
    for (key, value) in map {
        if key.is_empty() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "object_flatten does not support empty keys",
            )
            .with_path(format!("{}.args[0]", base_path)));
        }
        if key.contains('[') || key.contains(']') {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "object_flatten does not support keys with '[' or ']'",
            )
            .with_path(format!("{}.args[0]", base_path)));
        }
        tokens.push(PathToken::Key(key.clone()));
        match value {
            JsonValue::Object(child) => {
                if child.is_empty() {
                    let path = format_path_tokens(tokens);
                    output.insert(path, JsonValue::Object(Map::new()));
                } else {
                    flatten_object(child, tokens, output, base_path)?;
                }
            }
            _ => {
                let path = format_path_tokens(tokens);
                output.insert(path, value.clone());
            }
        }
        tokens.pop();
    }
    Ok(())
}

fn format_path_tokens(tokens: &[PathToken]) -> String {
    let mut path = String::new();
    for token in tokens {
        match token {
            PathToken::Key(key) => {
                if needs_bracket_quote(key) {
                    let escaped = key.replace('\\', "\\\\").replace('"', "\\\"");
                    path.push('[');
                    path.push('"');
                    path.push_str(&escaped);
                    path.push('"');
                    path.push(']');
                } else {
                    if !path.is_empty() {
                        path.push('.');
                    }
                    path.push_str(key);
                }
            }
            PathToken::Index(index) => {
                path.push('[');
                path.push_str(&index.to_string());
                path.push(']');
            }
        }
    }
    path
}

fn needs_bracket_quote(key: &str) -> bool {
    key.contains('.')
}

fn set_path_object_only(
    root: &mut JsonValue,
    tokens: &[PathToken],
    value: JsonValue,
    base_path: &str,
) -> Result<(), TransformError> {
    if tokens.is_empty() {
        return Err(
            TransformError::new(TransformErrorKind::ExprError, "path is empty")
                .with_path(format!("{}.args[0]", base_path)),
        );
    }

    let mut current = root;
    for (index, token) in tokens.iter().enumerate() {
        let key = match token {
            PathToken::Key(key) => key,
            PathToken::Index(_) => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "array indexes are not allowed in path",
                )
                .with_path(format!("{}.args[0]", base_path)));
            }
        };
        let is_last = index == tokens.len() - 1;

        match current {
            JsonValue::Object(map) => {
                if is_last {
                    if map.contains_key(key) {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "path conflicts with existing value",
                        )
                        .with_path(format!("{}.args[0]", base_path)));
                    }
                    map.insert(key.clone(), value);
                    return Ok(());
                }

                let entry = map
                    .entry(key.clone())
                    .or_insert_with(|| JsonValue::Object(Map::new()));
                if !entry.is_object() {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "path conflicts with non-object value",
                    )
                    .with_path(format!("{}.args[0]", base_path)));
                }
                current = entry;
            }
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "path conflicts with non-object value",
                )
                .with_path(format!("{}.args[0]", base_path)));
            }
        }
    }

    Ok(())
}

fn set_path_with_indexes(
    root: &mut JsonValue,
    tokens: &[PathToken],
    value: JsonValue,
    base_path: &str,
) -> Result<(), TransformError> {
    if tokens.is_empty() {
        return Err(
            TransformError::new(TransformErrorKind::ExprError, "path is empty")
                .with_path(format!("{}.args[1]", base_path)),
        );
    }

    let mut current = root;
    for (index, token) in tokens.iter().enumerate() {
        let is_last = index == tokens.len() - 1;
        match token {
            PathToken::Key(key) => {
                let next_token = tokens.get(index + 1);
                match current {
                    JsonValue::Object(map) => {
                        if is_last {
                            map.insert(key.clone(), value);
                            return Ok(());
                        }
                        let entry = map.entry(key.clone()).or_insert_with(|| match next_token {
                            Some(PathToken::Index(_)) => JsonValue::Array(Vec::new()),
                            _ => JsonValue::Object(Map::new()),
                        });
                        let expect_index = matches!(next_token, Some(PathToken::Index(_)));
                        let entry_is_array = matches!(entry, JsonValue::Array(_));
                        let entry_is_object = matches!(entry, JsonValue::Object(_));
                        if !(expect_index && entry_is_array || !expect_index && entry_is_object) {
                            return Err(TransformError::new(
                                TransformErrorKind::ExprError,
                                "path conflicts with non-object value",
                            )
                            .with_path(format!("{}.args[1]", base_path)));
                        }
                        current = entry;
                    }
                    _ => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "path conflicts with non-object value",
                        )
                        .with_path(format!("{}.args[1]", base_path)));
                    }
                }
            }
            PathToken::Index(path_index) => {
                let next_token = tokens.get(index + 1);
                match current {
                    JsonValue::Array(items) => {
                        if items.len() <= *path_index {
                            items.resize_with(path_index + 1, || JsonValue::Null);
                        }
                        if is_last {
                            items[*path_index] = value;
                            return Ok(());
                        }
                        let entry = &mut items[*path_index];
                        if entry.is_null() {
                            *entry = match next_token {
                                Some(PathToken::Index(_)) => JsonValue::Array(Vec::new()),
                                _ => JsonValue::Object(Map::new()),
                            };
                        }
                        let expect_index = matches!(next_token, Some(PathToken::Index(_)));
                        let entry_is_array = matches!(entry, JsonValue::Array(_));
                        let entry_is_object = matches!(entry, JsonValue::Object(_));
                        if !(expect_index && entry_is_array || !expect_index && entry_is_object) {
                            return Err(TransformError::new(
                                TransformErrorKind::ExprError,
                                "path conflicts with non-object value",
                            )
                            .with_path(format!("{}.args[1]", base_path)));
                        }
                        current = entry;
                    }
                    _ => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "path conflicts with non-object value",
                        )
                        .with_path(format!("{}.args[1]", base_path)));
                    }
                }
            }
        }
    }

    Ok(())
}

fn remove_path(root: &mut JsonValue, tokens: &[PathToken]) {
    if tokens.is_empty() {
        return;
    }

    let (first, rest) = tokens.split_first().unwrap();
    match first {
        PathToken::Key(key) => {
            if let JsonValue::Object(map) = root {
                if rest.is_empty() {
                    map.remove(key);
                    return;
                }
                if let Some(next) = map.get_mut(key) {
                    remove_path(next, rest);
                }
            }
        }
        PathToken::Index(index) => {
            if let JsonValue::Array(items) = root {
                if let Some(next) = items.get_mut(*index) {
                    remove_path(next, rest);
                }
            }
        }
    }
}

fn eval_bool_and_or(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    is_and: bool,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let mut saw_missing = false;
    for index in 0..total_len {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let value = eval_expr_at_index(
            index, args, injected, record, context, out, base_path, locals,
        )?;
        match value {
            EvalValue::Missing => {
                saw_missing = true;
                continue;
            }
            EvalValue::Value(value) => {
                let flag = value_as_bool(&value, &arg_path)?;
                if is_and {
                    if !flag {
                        return Ok(EvalValue::Value(JsonValue::Bool(false)));
                    }
                } else if flag {
                    return Ok(EvalValue::Value(JsonValue::Bool(true)));
                }
            }
        }
    }

    if saw_missing {
        Ok(EvalValue::Missing)
    } else {
        Ok(EvalValue::Value(JsonValue::Bool(is_and)))
    }
}

fn eval_bool_not(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let arg_path = format!("{}.args[0]", base_path);
    let value = eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    match value {
        EvalValue::Missing => Ok(EvalValue::Missing),
        EvalValue::Value(value) => {
            let flag = value_as_bool(&value, &arg_path)?;
            Ok(EvalValue::Value(JsonValue::Bool(!flag)))
        }
    }
}

fn eval_compare(
    expr_op: &ExprOp,
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(&expr_op.args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let left_path = format!("{}.args[0]", base_path);
    let right_path = format!("{}.args[1]", base_path);
    let left = eval_expr_value_or_null_at(
        0,
        &expr_op.args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
    )?;
    let right = eval_expr_value_or_null_at(
        1,
        &expr_op.args,
        injected,
        record,
        context,
        out,
        base_path,
        locals,
    )?;

    let result = match expr_op.op.as_str() {
        "==" => compare_eq(&left, &right, &left_path, &right_path)?,
        "!=" => !compare_eq(&left, &right, &left_path, &right_path)?,
        "<" => compare_numbers(&left, &right, &left_path, &right_path, |l, r| l < r)?,
        "<=" => compare_numbers(&left, &right, &left_path, &right_path, |l, r| l <= r)?,
        ">" => compare_numbers(&left, &right, &left_path, &right_path, |l, r| l > r)?,
        ">=" => compare_numbers(&left, &right, &left_path, &right_path, |l, r| l >= r)?,
        "~=" => match_regex(&left, &right, &left_path, &right_path)?,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr.op is not supported",
            )
            .with_path(format!("{}.op", base_path)));
        }
    };

    Ok(EvalValue::Value(JsonValue::Bool(result)))
}

fn compare_eq(
    left: &JsonValue,
    right: &JsonValue,
    left_path: &str,
    right_path: &str,
) -> Result<bool, TransformError> {
    if left.is_null() || right.is_null() {
        return Ok(left.is_null() && right.is_null());
    }

    let left_value = value_to_string(left, left_path)?;
    let right_value = value_to_string(right, right_path)?;
    Ok(left_value == right_value)
}

fn compare_numbers<F>(
    left: &JsonValue,
    right: &JsonValue,
    left_path: &str,
    right_path: &str,
    compare: F,
) -> Result<bool, TransformError>
where
    F: FnOnce(f64, f64) -> bool,
{
    let left_value = value_to_number(left, left_path, "comparison operand must be a number")?;
    let right_value = value_to_number(right, right_path, "comparison operand must be a number")?;
    Ok(compare(left_value, right_value))
}

fn match_regex(
    left: &JsonValue,
    right: &JsonValue,
    left_path: &str,
    right_path: &str,
) -> Result<bool, TransformError> {
    let value = value_as_string(left, left_path)?;
    let pattern = value_as_string(right, right_path)?;
    let regex = cached_regex(&pattern, right_path)?;
    Ok(regex.is_match(&value))
}

const DEFAULT_DATE_FORMATS_WITH_TZ: [&str; 8] = [
    "%Y-%m-%dT%H:%M:%S%:z",
    "%Y-%m-%d %H:%M:%S%:z",
    "%Y-%m-%dT%H:%M:%S%.f%:z",
    "%Y-%m-%d %H:%M:%S%.f%:z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S%z",
    "%Y/%m/%d %H:%M:%S%:z",
    "%Y/%m/%d %H:%M:%S%z",
];

const DEFAULT_DATE_FORMATS: [&str; 12] = [
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y%m%d",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y/%m/%d %H:%M:%S%.f",
];

fn parse_format_list(value: &JsonValue, path: &str) -> Result<Vec<String>, TransformError> {
    match value {
        JsonValue::String(s) => {
            if s.is_empty() {
                Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "input_format must not be empty",
                )
                .with_path(path))
            } else {
                Ok(vec![s.clone()])
            }
        }
        JsonValue::Array(items) => {
            if items.is_empty() {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "input_format must not be empty",
                )
                .with_path(path));
            }
            let mut formats = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                let value = match item.as_str() {
                    Some(value) => value,
                    None => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "input_format must be a string or array of strings",
                        )
                        .with_path(item_path));
                    }
                };
                if value.is_empty() {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "input_format must not be empty",
                    )
                    .with_path(item_path));
                }
                formats.push(value.to_string());
            }
            Ok(formats)
        }
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "input_format must be a string or array of strings",
        )
        .with_path(path)),
    }
}

fn parse_datetime(
    value: &str,
    formats: Option<&[String]>,
    timezone: Option<FixedOffset>,
    path: &str,
) -> Result<DateTime<FixedOffset>, TransformError> {
    if let Some(formats) = formats {
        return parse_datetime_with_formats(value, formats, timezone, path);
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt);
    }
    if let Ok(dt) = DateTime::parse_from_rfc2822(value) {
        return Ok(dt);
    }

    for format in DEFAULT_DATE_FORMATS_WITH_TZ {
        if let Ok(dt) = DateTime::parse_from_str(value, format) {
            return Ok(dt);
        }
    }

    parse_datetime_with_formats(
        value,
        &DEFAULT_DATE_FORMATS
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>(),
        timezone,
        path,
    )
}

fn parse_datetime_with_formats(
    value: &str,
    formats: &[String],
    timezone: Option<FixedOffset>,
    path: &str,
) -> Result<DateTime<FixedOffset>, TransformError> {
    for format in formats {
        if let Ok(dt) = DateTime::parse_from_str(value, format) {
            return Ok(dt);
        }
        if let Ok(naive) = NaiveDateTime::parse_from_str(value, format) {
            return apply_timezone(naive, timezone, path);
        }
        if let Ok(date) = NaiveDate::parse_from_str(value, format) {
            let naive = date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| expr_type_error("date is invalid", path))?;
            return apply_timezone(naive, timezone, path);
        }
    }

    Err(
        TransformError::new(TransformErrorKind::ExprError, "date format is invalid")
            .with_path(path),
    )
}

fn apply_timezone(
    naive: NaiveDateTime,
    timezone: Option<FixedOffset>,
    path: &str,
) -> Result<DateTime<FixedOffset>, TransformError> {
    let offset = timezone.unwrap_or_else(|| FixedOffset::east_opt(0).unwrap());
    offset
        .from_local_datetime(&naive)
        .single()
        .ok_or_else(|| expr_type_error("date is invalid", path))
}

fn looks_like_timezone(value: &str) -> bool {
    if value.eq_ignore_ascii_case("utc") || value == "Z" {
        return true;
    }
    matches!(value.chars().next(), Some('+') | Some('-'))
}

fn parse_timezone(value: &str, path: &str) -> Result<FixedOffset, TransformError> {
    if value.eq_ignore_ascii_case("utc") || value == "Z" {
        return FixedOffset::east_opt(0).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "timezone must be UTC or an offset like +09:00",
            )
            .with_path(path)
        });
    }

    let (sign, rest) = match value.chars().next() {
        Some('+') => (1i32, &value[1..]),
        Some('-') => (-1i32, &value[1..]),
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "timezone must be UTC or an offset like +09:00",
            )
            .with_path(path));
        }
    };

    let (hours, minutes) = if let Some((h, m)) = rest.split_once(':') {
        let hours = h.parse::<i32>().ok();
        let minutes = m.parse::<i32>().ok();
        match (hours, minutes) {
            (Some(hours), Some(minutes)) => (hours, minutes),
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "timezone must be UTC or an offset like +09:00",
                )
                .with_path(path));
            }
        }
    } else {
        match rest.len() {
            2 => {
                let hours = rest.parse::<i32>().ok();
                match hours {
                    Some(hours) => (hours, 0),
                    None => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "timezone must be UTC or an offset like +09:00",
                        )
                        .with_path(path));
                    }
                }
            }
            4 => {
                let hours = rest[..2].parse::<i32>().ok();
                let minutes = rest[2..].parse::<i32>().ok();
                match (hours, minutes) {
                    (Some(hours), Some(minutes)) => (hours, minutes),
                    _ => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "timezone must be UTC or an offset like +09:00",
                        )
                        .with_path(path));
                    }
                }
            }
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "timezone must be UTC or an offset like +09:00",
                )
                .with_path(path));
            }
        }
    };

    if !(0..=23).contains(&hours) || !(0..=59).contains(&minutes) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "timezone must be UTC or an offset like +09:00",
        )
        .with_path(path));
    }

    let offset_seconds = sign * (hours * 3600 + minutes * 60);
    FixedOffset::east_opt(offset_seconds).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "timezone must be UTC or an offset like +09:00",
        )
        .with_path(path)
    })
}

fn value_to_string(value: &JsonValue, path: &str) -> Result<String, TransformError> {
    match value {
        JsonValue::String(s) => Ok(s.clone()),
        JsonValue::Number(n) => Ok(number_to_string(n)),
        JsonValue::Bool(b) => Ok(b.to_string()),
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "value must be string/number/bool",
        )
        .with_path(path)),
    }
}

fn value_as_string(value: &JsonValue, path: &str) -> Result<String, TransformError> {
    match value {
        JsonValue::String(s) => Ok(s.clone()),
        _ => Err(
            TransformError::new(TransformErrorKind::ExprError, "value must be a string")
                .with_path(path),
        ),
    }
}

fn value_as_bool(value: &JsonValue, path: &str) -> Result<bool, TransformError> {
    match value {
        JsonValue::Bool(flag) => Ok(*flag),
        _ => Err(expr_type_error("value must be a boolean", path)),
    }
}

fn value_to_number(value: &JsonValue, path: &str, message: &str) -> Result<f64, TransformError> {
    match value {
        JsonValue::Number(n) => n
            .as_f64()
            .filter(|f| f.is_finite())
            .ok_or_else(|| expr_type_error(message, path)),
        JsonValue::String(s) => s
            .parse::<f64>()
            .ok()
            .filter(|f| f.is_finite())
            .ok_or_else(|| expr_type_error(message, path)),
        _ => Err(expr_type_error(message, path)),
    }
}

fn value_to_i64(value: &JsonValue, path: &str, message: &str) -> Result<i64, TransformError> {
    match value {
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i)
            } else if let Some(u) = n.as_u64() {
                i64::try_from(u).map_err(|_| expr_type_error(message, path))
            } else if let Some(f) = n.as_f64() {
                if f.is_finite() && (f.fract()).abs() < f64::EPSILON {
                    let value = f as i64;
                    if (value as f64 - f).abs() < f64::EPSILON {
                        Ok(value)
                    } else {
                        Err(expr_type_error(message, path))
                    }
                } else {
                    Err(expr_type_error(message, path))
                }
            } else {
                Err(expr_type_error(message, path))
            }
        }
        JsonValue::String(s) => s.parse::<i64>().map_err(|_| expr_type_error(message, path)),
        _ => Err(expr_type_error(message, path)),
    }
}

fn json_number_from_f64(value: f64, path: &str) -> Result<JsonValue, TransformError> {
    if !value.is_finite() {
        return Err(expr_type_error("number result is not finite", path));
    }
    if (value.fract()).abs() < f64::EPSILON {
        let as_i64 = value as i64;
        if (as_i64 as f64 - value).abs() < f64::EPSILON {
            return Ok(JsonValue::Number(as_i64.into()));
        }
    }
    serde_json::Number::from_f64(value)
        .map(JsonValue::Number)
        .ok_or_else(|| expr_type_error("number result is not finite", path))
}

fn to_radix_string(value: i64, base: u32, path: &str) -> Result<String, TransformError> {
    let digits = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if base < 2 || base > 36 {
        return Err(expr_type_error("base must be between 2 and 36", path));
    }

    if value == 0 {
        return Ok("0".to_string());
    }

    let is_negative = value < 0;
    let mut n = value
        .checked_abs()
        .ok_or_else(|| expr_type_error("value is out of range for base conversion", path))?
        as u64;

    let mut buf = Vec::new();
    while n > 0 {
        let idx = (n % base as u64) as usize;
        buf.push(digits[idx] as char);
        n /= base as u64;
    }
    if is_negative {
        buf.push('-');
    }
    buf.reverse();
    Ok(buf.iter().collect())
}

fn value_to_string_optional(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(s) => Some(s.clone()),
        JsonValue::Number(n) => Some(number_to_string(n)),
        JsonValue::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn expr_type_error(message: &str, path: &str) -> TransformError {
    TransformError::new(TransformErrorKind::ExprError, message).with_path(path)
}

fn number_to_string(number: &serde_json::Number) -> String {
    if let Some(i) = number.as_i64() {
        return i.to_string();
    }
    if let Some(u) = number.as_u64() {
        return u.to_string();
    }
    if let Some(f) = number.as_f64() {
        let mut s = format!("{}", f);
        if s.contains('.') {
            while s.ends_with('0') {
                s.pop();
            }
            if s.ends_with('.') {
                s.pop();
            }
        }
        return s;
    }
    number.to_string()
}

fn cast_value(value: &JsonValue, type_name: &str, path: &str) -> Result<JsonValue, TransformError> {
    match type_name {
        "string" => Ok(JsonValue::String(value_to_string(value, path)?)),
        "int" => cast_to_int(value, path),
        "float" => cast_to_float(value, path),
        "bool" => cast_to_bool(value, path),
        _ => Err(TransformError::new(
            TransformErrorKind::TypeCastFailed,
            "type must be string|int|float|bool",
        )
        .with_path(path)),
    }
}

fn cast_to_int(value: &JsonValue, path: &str) -> Result<JsonValue, TransformError> {
    match value {
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(JsonValue::Number(i.into()))
            } else if let Some(f) = n.as_f64() {
                if (f.fract()).abs() < f64::EPSILON {
                    Ok(JsonValue::Number((f as i64).into()))
                } else {
                    Err(type_cast_error("int", path))
                }
            } else {
                Err(type_cast_error("int", path))
            }
        }
        JsonValue::String(s) => s
            .parse::<i64>()
            .map(|i| JsonValue::Number(i.into()))
            .map_err(|_| type_cast_error("int", path)),
        _ => Err(type_cast_error("int", path)),
    }
}

fn cast_to_float(value: &JsonValue, path: &str) -> Result<JsonValue, TransformError> {
    match value {
        JsonValue::Number(n) => n
            .as_f64()
            .ok_or_else(|| type_cast_error("float", path))
            .and_then(|f| {
                serde_json::Number::from_f64(f)
                    .map(JsonValue::Number)
                    .ok_or_else(|| type_cast_error("float", path))
            }),
        JsonValue::String(s) => s
            .parse::<f64>()
            .map_err(|_| type_cast_error("float", path))
            .and_then(|f| {
                serde_json::Number::from_f64(f)
                    .map(JsonValue::Number)
                    .ok_or_else(|| type_cast_error("float", path))
            }),
        _ => Err(type_cast_error("float", path)),
    }
}

fn cast_to_bool(value: &JsonValue, path: &str) -> Result<JsonValue, TransformError> {
    match value {
        JsonValue::Bool(b) => Ok(JsonValue::Bool(*b)),
        JsonValue::String(s) => match s.to_lowercase().as_str() {
            "true" => Ok(JsonValue::Bool(true)),
            "false" => Ok(JsonValue::Bool(false)),
            _ => Err(type_cast_error("bool", path)),
        },
        _ => Err(type_cast_error("bool", path)),
    }
}

fn type_cast_error(type_name: &str, path: &str) -> TransformError {
    TransformError::new(
        TransformErrorKind::TypeCastFailed,
        format!("failed to cast to {}", type_name),
    )
    .with_path(path)
}

fn parse_source(source: &str) -> Result<(Namespace, &str), TransformError> {
    if let Some((prefix, path)) = source.split_once('.') {
        if path.is_empty() {
            return Err(TransformError::new(
                TransformErrorKind::InvalidRef,
                "reference path is empty",
            ));
        }
        let namespace = match prefix {
            "input" => Namespace::Input,
            "context" => Namespace::Context,
            "out" => Namespace::Out,
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidRef,
                    "ref namespace must be input|context|out",
                ));
            }
        };
        Ok((namespace, path))
    } else {
        if source.is_empty() {
            return Err(TransformError::new(
                TransformErrorKind::InvalidRef,
                "reference path is empty",
            ));
        }
        Ok((Namespace::Input, source))
    }
}

fn parse_ref(value: &str) -> Result<(Namespace, &str), TransformError> {
    let (prefix, path) = value.split_once('.').ok_or_else(|| {
        TransformError::new(TransformErrorKind::InvalidRef, "ref must include namespace")
    })?;

    if path.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidRef,
            "ref path is empty",
        ));
    }

    let namespace = match prefix {
        "input" => Namespace::Input,
        "context" => Namespace::Context,
        "out" => Namespace::Out,
        "item" => Namespace::Item,
        "acc" => Namespace::Acc,
        "pipe" => Namespace::Pipe,
        "local" => Namespace::Local,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::InvalidRef,
                "ref namespace must be input|context|out|item|acc|pipe|local",
            ));
        }
    };

    Ok((namespace, path))
}

fn parse_path_tokens(
    path: &str,
    kind: TransformErrorKind,
    error_path: impl Into<String>,
) -> Result<Vec<PathToken>, TransformError> {
    parse_path(path)
        .map_err(|err| TransformError::new(kind, err.message()).with_path(error_path.into()))
}

fn set_path(
    root: &mut JsonValue,
    path: &str,
    value: JsonValue,
    mapping_path: &str,
) -> Result<(), TransformError> {
    let tokens = parse_path_tokens(
        path,
        TransformErrorKind::InvalidTarget,
        format!("{}.target", mapping_path),
    )?;
    if tokens.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidTarget,
            "target path is invalid",
        )
        .with_path(format!("{}.target", mapping_path)));
    }

    let mut current = root;
    for (index, token) in tokens.iter().enumerate() {
        let is_last = index == tokens.len() - 1;
        let key = match token {
            PathToken::Key(key) => key,
            PathToken::Index(_) => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidTarget,
                    "target path must not include indexes",
                )
                .with_path(format!("{}.target", mapping_path)));
            }
        };

        match current {
            JsonValue::Object(map) => {
                if is_last {
                    map.insert(key.to_string(), value);
                    return Ok(());
                }

                let entry = map
                    .entry(key.to_string())
                    .or_insert_with(|| JsonValue::Object(Map::new()));
                if !entry.is_object() {
                    return Err(TransformError::new(
                        TransformErrorKind::InvalidTarget,
                        "target path conflicts with non-object value",
                    )
                    .with_path(format!("{}.target", mapping_path)));
                }
                current = entry;
            }
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidTarget,
                    "target root must be an object",
                )
                .with_path(format!("{}.target", mapping_path)));
            }
        }
    }

    Ok(())
}

fn literal_string(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(value) => value.as_str(),
        _ => None,
    }
}

/// Convert an Expr to JSON value for v2 pipe parsing.
/// Returns Some if the expr looks like a v2 pipe expression:
/// - Literal(Array) -> direct array
/// - Ref where ref_path starts with @ -> single element array
/// - Chain where first element starts with @ -> convert to array
/// Returns None if it looks like v1 expression and should be handled by v1 eval.
fn expr_to_json_for_v2_pipe(expr: &Expr) -> Option<JsonValue> {
    match expr {
        Expr::Literal(JsonValue::Array(arr)) => {
            // Direct array - v2 pipe
            Some(JsonValue::Array(arr.clone()))
        }
        Expr::Literal(JsonValue::String(s)) => {
            if is_v2_ref(s) || is_pipe_value(s) || is_literal_escape(s) {
                Some(JsonValue::String(s.clone()))
            } else {
                None
            }
        }
        Expr::Ref(expr_ref)
            if expr_ref.ref_path.starts_with('@') || is_literal_escape(&expr_ref.ref_path) =>
        {
            // Single v2 reference or literal escape (serde collapsed 1-element array)
            // Wrap it as single-element array
            Some(JsonValue::Array(vec![JsonValue::String(
                expr_ref.ref_path.clone(),
            )]))
        }
        Expr::Chain(chain) => {
            // Check if first element is a v2 ref
            if let Some(first) = chain.chain.first() {
                if let Expr::Ref(r) = first {
                    if r.ref_path.starts_with('@') {
                        // Convert chain to array
                        let arr: Vec<JsonValue> =
                            chain.chain.iter().map(|e| expr_to_json_value(e)).collect();
                        return Some(JsonValue::Array(arr));
                    }
                }
            }
            None
        }
        _ => None,
    }
}

/// Convert an Expr to JSON value for v2 condition parsing.
/// Accepts literal values and v2-looking refs/chains while avoiding v1-only forms.
fn expr_to_json_for_v2_condition(expr: &Expr) -> Option<JsonValue> {
    match expr {
        Expr::Literal(value) => Some(value.clone()),
        Expr::Ref(ref_expr)
            if ref_expr.ref_path.starts_with('@') || is_literal_escape(&ref_expr.ref_path) =>
        {
            Some(JsonValue::String(ref_expr.ref_path.clone()))
        }
        Expr::Chain(chain) => {
            if let Some(first) = chain.chain.first() {
                if let Expr::Ref(r) = first {
                    if r.ref_path.starts_with('@') {
                        let arr: Vec<JsonValue> =
                            chain.chain.iter().map(expr_to_json_value).collect();
                        return Some(JsonValue::Array(arr));
                    }
                }
            }
            None
        }
        _ => None,
    }
}

/// Helper to convert Expr to JsonValue (for Chain conversion)
fn expr_to_json_value(expr: &Expr) -> JsonValue {
    match expr {
        Expr::Ref(r) => JsonValue::String(r.ref_path.clone()),
        Expr::Literal(v) => v.clone(),
        Expr::Op(op) => {
            let mut obj = serde_json::Map::new();
            let args: Vec<JsonValue> = op.args.iter().map(expr_to_json_value).collect();
            obj.insert(op.op.clone(), JsonValue::Array(args));
            JsonValue::Object(obj)
        }
        Expr::Chain(chain) => {
            let arr: Vec<JsonValue> = chain.chain.iter().map(expr_to_json_value).collect();
            JsonValue::Array(arr)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Namespace {
    Input,
    Context,
    Out,
    Item,
    Acc,
    Pipe,
    Local,
}

#[derive(Clone, Copy)]
pub(crate) struct EvalItem<'a> {
    pub(crate) value: &'a JsonValue,
    pub(crate) index: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct EvalLocals<'a> {
    pub(crate) item: Option<EvalItem<'a>>,
    pub(crate) acc: Option<&'a JsonValue>,
    pub(crate) pipe: Option<&'a EvalValue>,
    pub(crate) locals: Option<&'a HashMap<String, EvalValue>>,
    pub(crate) precomputed_op_args: Option<(&'a str, &'a [EvalValue])>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EvalValue {
    Missing,
    Value(JsonValue),
}

// =============================================================================
// T21: v2 transform integration tests
// =============================================================================

#[cfg(test)]
mod v2_transform_tests {
    use super::*;
    use crate::parse_rule_file;

    #[test]
    fn test_v2_simple_ref_transform() {
        let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: user_name
    expr:
      - "@input.name"
"#;
        let rule = parse_rule_file(yaml).unwrap();
        let input = r#"[{"name": "Alice"}]"#;
        let result = transform(&rule, input, None).unwrap();
        assert_eq!(result, serde_json::json!([{"user_name": "Alice"}]));
    }

    #[test]
    fn test_v2_scalar_ref_transform() {
        let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: user_name
    expr: "@input.name"
"#;
        let rule = parse_rule_file(yaml).unwrap();
        let input = r#"[{"name": "Alice"}]"#;
        let result = transform(&rule, input, None).unwrap();
        assert_eq!(result, serde_json::json!([{"user_name": "Alice"}]));
    }

    #[test]
    fn test_v2_literal_object_with_lookup_key_is_literal() {
        let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: payload
    expr:
      lookup: 1
"#;
        let rule = parse_rule_file(yaml).unwrap();
        let input = r#"[{"id": 1}]"#;
        let result = transform(&rule, input, None).unwrap();
        assert_eq!(result, serde_json::json!([{"payload": {"lookup": 1}}]));
    }

    #[test]
    fn test_v2_pipe_with_ops_transform() {
        let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: name
    expr:
      - "@input.name"
      - trim
      - uppercase
"#;
        let rule = parse_rule_file(yaml).unwrap();
        let input = r#"[{"name": "  alice  "}]"#;
        let result = transform(&rule, input, None).unwrap();
        assert_eq!(result, serde_json::json!([{"name": "ALICE"}]));
    }

    #[test]
    fn test_v2_context_ref_transform() {
        let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: rate
    expr:
      - "@context.rate"
"#;
        let rule = parse_rule_file(yaml).unwrap();
        let input = r#"[{"id": 1}]"#;
        let context = serde_json::json!({"rate": 1.5});
        let result = transform(&rule, input, Some(&context)).unwrap();
        assert_eq!(result, serde_json::json!([{"rate": 1.5}]));
    }

    #[test]
    fn test_v2_out_ref_transform() {
        let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: first_name
    expr:
      - "@input.name"
  - target: greeting
    expr:
      - "Hello, "
      - concat: ["@out.first_name"]
"#;
        let rule = parse_rule_file(yaml).unwrap();
        let input = r#"[{"name": "Bob"}]"#;
        let result = transform(&rule, input, None).unwrap();
        assert_eq!(
            result,
            serde_json::json!([{"first_name": "Bob", "greeting": "Hello, Bob"}])
        );
    }

    #[test]
    fn test_v2_with_let_step_transform() {
        let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: total
    expr:
      - "@input.price"
      - let: { base: "$" }
      - multiply: [1.1]
"#;
        let rule = parse_rule_file(yaml).unwrap();
        let input = r#"[{"price": 100}]"#;
        let result = transform(&rule, input, None).unwrap();
        let total = result[0]["total"].as_f64().unwrap();
        assert!((total - 110.0).abs() < 0.001);
    }

    #[test]
    fn test_v2_with_if_step_transform() {
        let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: discount
    expr:
      - "@input.total"
      - if:
          cond:
            gt: ["$", 1000]
          then:
            - "$"
            - multiply: [0.9]
          else:
            - "$"
"#;
        let rule = parse_rule_file(yaml).unwrap();
        let input = r#"[{"total": 2000}, {"total": 500}]"#;
        let result = transform(&rule, input, None).unwrap();
        let first = result[0]["discount"].as_f64().unwrap();
        let second = result[1]["discount"].as_f64().unwrap();
        assert!((first - 1800.0).abs() < 0.001);
        assert!((second - 500.0).abs() < 0.001);
    }

    #[test]
    fn test_v2_with_map_step_transform() {
        let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: items
    expr:
      - "@input.values"
      - map:
        - multiply: [2]
"#;
        let rule = parse_rule_file(yaml).unwrap();
        let input = r#"[{"values": [1, 2, 3]}]"#;
        let result = transform(&rule, input, None).unwrap();
        // multiply returns f64, so [2.0, 4.0, 6.0]
        assert_eq!(result, serde_json::json!([{"items": [2.0, 4.0, 6.0]}]));
    }

    #[test]
    fn test_v2_v1_mixed_mappings() {
        // v1 style mapping (source) should still work in version 2
        let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: name
    source: name
  - target: upper_name
    expr:
      - "@input.name"
      - uppercase
"#;
        let rule = parse_rule_file(yaml).unwrap();
        let input = r#"[{"name": "alice"}]"#;
        let result = transform(&rule, input, None).unwrap();
        assert_eq!(
            result,
            serde_json::json!([{"name": "alice", "upper_name": "ALICE"}])
        );
    }

    #[test]
    fn test_v2_lookup_first_transform() {
        let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: dept_name
    expr:
      - lookup_first:
        - "@context.departments"
        - id
        - "@input.dept_id"
        - name
"#;
        let rule = parse_rule_file(yaml).unwrap();
        let input = r#"[{"dept_id": 2}]"#;
        let context = serde_json::json!({
            "departments": [
                {"id": 1, "name": "Engineering"},
                {"id": 2, "name": "Marketing"},
                {"id": 3, "name": "Sales"}
            ]
        });
        let result = transform(&rule, input, Some(&context)).unwrap();
        assert_eq!(result, serde_json::json!([{"dept_name": "Marketing"}]));
    }

    #[test]
    fn test_v2_lookup_first_with_pipe_value_transform() {
        let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: dept_name
    expr:
      - "@context.departments"
      - lookup_first:
        - id
        - "@input.dept_id"
        - name
"#;
        let rule = parse_rule_file(yaml).unwrap();
        let input = r#"[{"dept_id": 2}]"#;
        let context = serde_json::json!({
            "departments": [
                {"id": 1, "name": "Engineering"},
                {"id": 2, "name": "Marketing"},
                {"id": 3, "name": "Sales"}
            ]
        });
        let result = transform(&rule, input, Some(&context)).unwrap();
        assert_eq!(result, serde_json::json!([{"dept_name": "Marketing"}]));
    }

    #[test]
    fn test_v1_rules_still_work() {
        // Ensure v1 rules are not affected
        let yaml = r#"
version: 1
input:
  format: json
mappings:
  - target: name
    source: name
  - target: upper
    expr:
      op: uppercase
      args:
        - { ref: input.name }
"#;
        let rule = parse_rule_file(yaml).unwrap();
        let input = r#"[{"name": "test"}]"#;
        let result = transform(&rule, input, None).unwrap();
        assert_eq!(
            result,
            serde_json::json!([{"name": "test", "upper": "TEST"}])
        );
    }
}
