mod cache;
mod error;
mod locator;
mod model;
mod path;
mod dto;
mod transform;
mod validator;
pub mod v2_model;
pub mod v2_parser;
pub mod v2_eval;
pub mod v2_validator;

/// Library version from Cargo.toml
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub use error::{
    ErrorCode, RuleError, TransformError, TransformErrorKind, TransformWarning, ValidationResult,
    YamlLocation,
};
pub use dto::{generate_dto, DtoError, DtoLanguage};
pub use model::{Expr, ExprChain, ExprOp, ExprRef, InputFormat, InputSpec, Mapping, RuleFile};
pub use path::{get_path, parse_path, PathError, PathToken};
pub use transform::{
    preflight_validate, preflight_validate_with_base_dir, preflight_validate_with_warnings,
    preflight_validate_with_warnings_with_base_dir, transform, transform_record,
    transform_record_with_base_dir, transform_record_with_warnings,
    transform_record_with_warnings_with_base_dir, transform_stream, transform_stream_with_base_dir,
    transform_with_base_dir, transform_with_warnings, transform_with_warnings_with_base_dir,
    TransformStream, TransformStreamItem,
};
pub use validator::{validate_rule_file, validate_rule_file_with_source};

use std::sync::{Mutex, OnceLock};

use cache::LruCache;

const RULE_CACHE_CAPACITY: usize = 128;

fn rule_cache() -> &'static Mutex<LruCache<String, RuleFile>> {
    static RULE_CACHE: OnceLock<Mutex<LruCache<String, RuleFile>>> = OnceLock::new();
    RULE_CACHE.get_or_init(|| Mutex::new(LruCache::new(RULE_CACHE_CAPACITY)))
}

pub fn parse_rule_file(yaml: &str) -> Result<RuleFile, serde_yaml::Error> {
    let key = yaml.to_string();
    if let Some(rule) = {
        let mut cache = rule_cache().lock().unwrap_or_else(|err| err.into_inner());
        cache.get_cloned(&key)
    } {
        return Ok(rule);
    }

    let rule: RuleFile = serde_yaml::from_str(yaml)?;
    {
        let mut cache = rule_cache().lock().unwrap_or_else(|err| err.into_inner());
        cache.insert(key, rule.clone());
    }
    Ok(rule)
}
