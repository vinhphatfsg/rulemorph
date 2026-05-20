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

mod input;
mod preflight;
mod record;
mod trace;

use input::transform_with_warnings_inner;

pub use preflight::{
    preflight_validate, preflight_validate_input, preflight_validate_input_with_base_dir,
    preflight_validate_input_with_warnings, preflight_validate_input_with_warnings_with_base_dir,
    preflight_validate_input_with_warnings_with_base_dir_and_options,
    preflight_validate_with_base_dir, preflight_validate_with_warnings,
    preflight_validate_with_warnings_with_base_dir,
};
pub(super) use record::transform_record_with_warnings_inner;
pub use record::{
    transform_record, transform_record_with_base_dir, transform_record_with_warnings,
    transform_record_with_warnings_with_base_dir,
};
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
