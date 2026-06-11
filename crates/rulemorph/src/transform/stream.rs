use serde_json::Value as JsonValue;
use std::path::Path;

use crate::error::{TransformError, TransformErrorKind, TransformWarning};
use crate::model::RuleFile;
use crate::normalization::{InputData, NormalizationOptions};

use super::records::{InputRecordsIter, input_records_iter_with_options};
use super::{BranchContext, EvalLimits, RuleRecordInput, apply_rule_to_record};

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
    limits: EvalLimits,
    compiled_rule: Option<super::CompiledRule>,
    legacy_warning_pending: bool,
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
            limits: EvalLimits::from(options),
            compiled_rule: None,
            legacy_warning_pending: crate::is_legacy_v1_rule(rule),
            done: false,
        })
    }

    pub(in crate::transform) fn suppress_legacy_v1_warning(mut self) -> Self {
        self.legacy_warning_pending = false;
        self
    }
}

impl<'a> Iterator for TransformStream<'a> {
    type Item = Result<TransformStreamItem, TransformError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            let mut warnings = Vec::new();
            if self.legacy_warning_pending {
                self.legacy_warning_pending = false;
                if let Some(warning) = crate::legacy_v1_rule_warning(self.rule) {
                    warnings.push(warning);
                }
            }

            let record = match self.records.next() {
                None => {
                    self.done = true;
                    if warnings.is_empty() {
                        return None;
                    }
                    return Some(Ok(TransformStreamItem {
                        output: None,
                        warnings,
                    }));
                }
                Some(Ok(record)) => record,
                Some(Err(err)) => {
                    self.done = true;
                    return Some(Err(err));
                }
            };

            let compiled_rule = self
                .compiled_rule
                .get_or_insert_with(|| super::CompiledRule::new(self.rule));
            let mut branch_context = BranchContext::default();
            match apply_rule_to_record(RuleRecordInput {
                rule: self.rule,
                record: &record,
                context: self.context,
                warnings: &mut warnings,
                base_dir: self.base_dir,
                branch_context: &mut branch_context,
                limits: self.limits,
                compiled_rule: Some(compiled_rule),
            }) {
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

fn ensure_stream_supported(rule: &RuleFile) -> Result<(), TransformError> {
    if rule.finalize.is_some() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "finalize is not supported in stream mode",
        ));
    }
    Ok(())
}

pub fn transform_stream<'a>(
    rule: &'a RuleFile,
    input: &'a str,
    context: Option<&'a JsonValue>,
) -> Result<TransformStream<'a>, TransformError> {
    ensure_stream_supported(rule)?;
    TransformStream::new(rule, input, context, None)
}

pub fn transform_stream_input<'a>(
    rule: &'a RuleFile,
    input: InputData<'a>,
    context: Option<&'a JsonValue>,
) -> Result<TransformStream<'a>, TransformError> {
    ensure_stream_supported(rule)?;
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
    ensure_stream_supported(rule)?;
    TransformStream::new(rule, input, context, Some(base_dir))
}

pub fn transform_stream_input_with_base_dir<'a>(
    rule: &'a RuleFile,
    input: InputData<'a>,
    context: Option<&'a JsonValue>,
    base_dir: &'a Path,
) -> Result<TransformStream<'a>, TransformError> {
    ensure_stream_supported(rule)?;
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
    ensure_stream_supported(rule)?;
    TransformStream::new_with_options(rule, input, context, None, options)
}

pub fn transform_stream_input_with_options<'a>(
    rule: &'a RuleFile,
    input: InputData<'a>,
    context: Option<&'a JsonValue>,
    options: &NormalizationOptions,
) -> Result<TransformStream<'a>, TransformError> {
    ensure_stream_supported(rule)?;
    TransformStream::new_with_input_and_options(rule, input, context, None, options)
}

pub fn transform_stream_with_base_dir_and_options<'a>(
    rule: &'a RuleFile,
    input: &'a str,
    context: Option<&'a JsonValue>,
    base_dir: &'a Path,
    options: &NormalizationOptions,
) -> Result<TransformStream<'a>, TransformError> {
    ensure_stream_supported(rule)?;
    TransformStream::new_with_options(rule, input, context, Some(base_dir), options)
}

pub fn transform_stream_input_with_base_dir_and_options<'a>(
    rule: &'a RuleFile,
    input: InputData<'a>,
    context: Option<&'a JsonValue>,
    base_dir: &'a Path,
    options: &NormalizationOptions,
) -> Result<TransformStream<'a>, TransformError> {
    ensure_stream_supported(rule)?;
    TransformStream::new_with_input_and_options(rule, input, context, Some(base_dir), options)
}
