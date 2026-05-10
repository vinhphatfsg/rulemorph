mod cache;
mod dto;
mod error;
mod locator;
mod model;
pub mod normalization;
mod path;
pub mod serde_guard;
mod transform;
pub mod v2_eval;
pub mod v2_model;
pub mod v2_parser;
pub mod v2_validator;
mod validator;

/// Library version from Cargo.toml
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub use dto::{DtoError, DtoLanguage, generate_dto};
pub use error::{
    ErrorCode, RuleError, TransformError, TransformErrorKind, TransformWarning, ValidationResult,
    YamlLocation,
};
pub use model::{Expr, ExprChain, ExprOp, ExprRef, InputFormat, InputSpec, Mapping, RuleFile};
pub use normalization::{
    InputData, NormalizationOptions, NormalizedRecords, normalize_records,
    normalize_records_with_options,
};
pub use path::{PathError, PathToken, get_path, parse_path};
pub use transform::{
    TransformStream, TransformStreamItem, preflight_validate, preflight_validate_input,
    preflight_validate_input_with_base_dir, preflight_validate_input_with_warnings,
    preflight_validate_input_with_warnings_with_base_dir, preflight_validate_with_base_dir,
    preflight_validate_with_warnings, preflight_validate_with_warnings_with_base_dir, transform,
    transform_input, transform_input_with_base_dir, transform_input_with_base_dir_and_options,
    transform_input_with_options, transform_input_with_warnings,
    transform_input_with_warnings_with_base_dir,
    transform_input_with_warnings_with_base_dir_and_options,
    transform_input_with_warnings_with_options, transform_record, transform_record_with_base_dir,
    transform_record_with_warnings, transform_record_with_warnings_with_base_dir, transform_stream,
    transform_stream_input, transform_stream_input_with_base_dir,
    transform_stream_input_with_base_dir_and_options, transform_stream_input_with_options,
    transform_stream_with_base_dir, transform_stream_with_base_dir_and_options,
    transform_stream_with_options, transform_with_base_dir, transform_with_options,
    transform_with_warnings, transform_with_warnings_with_base_dir,
    transform_with_warnings_with_base_dir_and_options, transform_with_warnings_with_options,
};
pub use validator::{validate_rule_file, validate_rule_file_with_source};

use std::fmt;
use std::sync::{Mutex, OnceLock};

use cache::LruCache;
use serde::de::Error as _;

const RULE_CACHE_CAPACITY: usize = 128;

fn rule_cache() -> &'static Mutex<LruCache<String, RuleFile>> {
    static RULE_CACHE: OnceLock<Mutex<LruCache<String, RuleFile>>> = OnceLock::new();
    RULE_CACHE.get_or_init(|| Mutex::new(LruCache::new(RULE_CACHE_CAPACITY)))
}

pub fn parse_rule_file(yaml: &str) -> Result<RuleFile, serde_yaml::Error> {
    parse_rule_file_yaml(yaml)
}

fn parse_rule_file_yaml(yaml: &str) -> Result<RuleFile, serde_yaml::Error> {
    let key = rule_cache_key(RuleFormat::Yaml, yaml);
    if let Some(rule) = {
        let mut cache = rule_cache().lock().unwrap_or_else(|err| err.into_inner());
        cache.get_cloned(&key)
    } {
        return Ok(rule);
    }

    let value = serde_guard::parse_yaml_value_strict(yaml).map_err(serde_yaml::Error::custom)?;
    let rule: RuleFile = serde_yaml::from_value(value)?;
    {
        let mut cache = rule_cache().lock().unwrap_or_else(|err| err.into_inner());
        cache.insert(key, rule.clone());
    }
    Ok(rule)
}

fn parse_rule_file_json(json: &str) -> Result<RuleFile, RuleParseError> {
    let key = rule_cache_key(RuleFormat::Json, json);
    if let Some(rule) = {
        let mut cache = rule_cache().lock().unwrap_or_else(|err| err.into_inner());
        cache.get_cloned(&key)
    } {
        return Ok(rule);
    }

    let value = serde_guard::parse_json_value_strict(json)
        .map_err(|err| RuleParseError::new(RuleFormat::Json, err))?;
    let rule: RuleFile = serde_json::from_value(value)
        .map_err(|err| RuleParseError::new(RuleFormat::Json, err.to_string()))?;
    {
        let mut cache = rule_cache().lock().unwrap_or_else(|err| err.into_inner());
        cache.insert(key, rule.clone());
    }
    Ok(rule)
}

fn rule_cache_key(format: RuleFormat, source: &str) -> String {
    format!("{}:\0{}", format.as_str(), source)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleFormat {
    Yaml,
    Json,
}

impl RuleFormat {
    pub fn from_path(path: &std::path::Path) -> Self {
        match path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("json") => RuleFormat::Json,
            _ => RuleFormat::Yaml,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            RuleFormat::Yaml => "yaml",
            RuleFormat::Json => "json",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleParseError {
    pub format: RuleFormat,
    pub message: String,
}

impl RuleParseError {
    pub fn new(format: RuleFormat, message: impl Into<String>) -> Self {
        Self {
            format,
            message: message.into(),
        }
    }
}

impl fmt::Display for RuleParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "failed to parse {} rules: {}",
            self.format.as_str(),
            self.message
        )
    }
}

impl std::error::Error for RuleParseError {}

pub fn parse_rule_file_with_format(
    source: &str,
    format: RuleFormat,
) -> Result<RuleFile, RuleParseError> {
    match format {
        RuleFormat::Yaml => {
            parse_rule_file_yaml(source).map_err(|err| RuleParseError::new(format, err.to_string()))
        }
        RuleFormat::Json => parse_rule_file_json(source),
    }
}
