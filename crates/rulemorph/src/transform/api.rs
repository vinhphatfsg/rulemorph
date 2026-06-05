use serde_json::Value as JsonValue;
use std::path::Path;

use crate::error::{TransformError, TransformWarning};
use crate::model::RuleFile;
use crate::normalization::{InputData, NormalizationOptions};

use super::records::input_records_iter_with_options;
use super::stream::{
    transform_stream_input_with_base_dir_and_options, transform_stream_input_with_options,
};
use super::{BranchContext, CompiledRule, EvalLimits, apply_finalize, apply_rule_to_record};

mod input;
mod preflight;
mod record;
mod trace;

pub use input::{
    transform, transform_input, transform_input_with_base_dir,
    transform_input_with_base_dir_and_options, transform_input_with_options,
    transform_input_with_warnings, transform_input_with_warnings_with_base_dir,
    transform_input_with_warnings_with_base_dir_and_options,
    transform_input_with_warnings_with_options, transform_with_base_dir, transform_with_options,
    transform_with_warnings, transform_with_warnings_with_base_dir,
    transform_with_warnings_with_base_dir_and_options, transform_with_warnings_with_options,
};

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
