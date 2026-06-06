mod cache;
mod custom_ops;
mod dto;
mod error;
mod excel_ref;
mod expr_json;
mod locator;
mod model;
pub mod normalization;
mod path;
mod rule_source;
pub mod serde_guard;
pub mod trace;
mod transform;
pub mod v2_eval;
pub mod v2_model;
mod v2_operator;
pub mod v2_parser;
pub mod v2_validator;
mod validator;
mod xml_name;

/// Library version from Cargo.toml
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub use dto::{DtoError, DtoLanguage, generate_dto};
pub use error::{
    ErrorCode, RuleError, TransformError, TransformErrorKind, TransformWarning, ValidationResult,
    YamlLocation,
};
pub use model::{
    CustomOpDef, Expr, ExprChain, ExprOp, ExprRef, InputFormat, InputSpec, Mapping, RuleFile,
    RuleType, RuleTypeField, RuleTypeKind,
};
pub use normalization::{
    InputData, NormalizationOptions, NormalizedRecords, normalize_records,
    normalize_records_with_options,
};
pub use path::{PathError, PathToken, get_path, parse_path};
pub use rule_source::{RuleFormat, RuleParseError, parse_rule_file, parse_rule_file_with_format};
pub use trace::{
    RecordTrace, TraceAttributeValue, TraceDiagnostic, TraceEvent, TraceEventKind, TraceJsonType,
    TracePhase, TraceRedactionOptions, TraceTruncation, TraceValueMode, TraceValueModeName,
    TraceValueSnapshot, TraceValueState, TransformRecordTraceResult, TransformTrace,
    TransformTraceError, TransformTraceOptions, TransformTraceResult,
};
pub use transform::{
    TransformStream, TransformStreamItem, preflight_validate, preflight_validate_input,
    preflight_validate_input_with_base_dir, preflight_validate_input_with_warnings,
    preflight_validate_input_with_warnings_with_base_dir,
    preflight_validate_input_with_warnings_with_base_dir_and_options,
    preflight_validate_with_base_dir, preflight_validate_with_warnings,
    preflight_validate_with_warnings_with_base_dir, transform, transform_input,
    transform_input_with_base_dir, transform_input_with_base_dir_and_options,
    transform_input_with_options, transform_input_with_trace,
    transform_input_with_trace_with_base_dir_and_options, transform_input_with_warnings,
    transform_input_with_warnings_with_base_dir,
    transform_input_with_warnings_with_base_dir_and_options,
    transform_input_with_warnings_with_options, transform_record, transform_record_with_base_dir,
    transform_record_with_trace, transform_record_with_warnings,
    transform_record_with_warnings_with_base_dir, transform_stream, transform_stream_input,
    transform_stream_input_with_base_dir, transform_stream_input_with_base_dir_and_options,
    transform_stream_input_with_options, transform_stream_with_base_dir,
    transform_stream_with_base_dir_and_options, transform_stream_with_options,
    transform_with_base_dir, transform_with_options, transform_with_warnings,
    transform_with_warnings_with_base_dir, transform_with_warnings_with_base_dir_and_options,
    transform_with_warnings_with_options,
};
pub use validator::{
    validate_rule_file, validate_rule_file_with_base_dir, validate_rule_file_with_source,
    validate_rule_file_with_source_and_base_dir,
};
