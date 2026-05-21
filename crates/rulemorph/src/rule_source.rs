use std::fmt;
use std::sync::{Mutex, OnceLock};

use serde::de::Error as _;

use crate::cache::LruCache;
use crate::error::YamlLocation;
use crate::model::RuleFile;

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

    let value = crate::serde_guard::parse_yaml_value_strict(yaml).map_err(|err| {
        if err.location().is_some()
            && let Err(original) = serde_yaml::from_str::<serde_yaml::Value>(yaml)
        {
            return original;
        }
        serde_yaml::Error::custom(err.to_string())
    })?;
    let rule: RuleFile = serde_yaml::from_value(value)?;
    {
        let mut cache = rule_cache().lock().unwrap_or_else(|err| err.into_inner());
        cache.insert(key, rule.clone());
    }
    Ok(rule)
}

fn parse_rule_file_yaml_with_error(yaml: &str) -> Result<RuleFile, RuleParseError> {
    let key = rule_cache_key(RuleFormat::Yaml, yaml);
    if let Some(rule) = {
        let mut cache = rule_cache().lock().unwrap_or_else(|err| err.into_inner());
        cache.get_cloned(&key)
    } {
        return Ok(rule);
    }

    let value = crate::serde_guard::parse_yaml_value_strict(yaml)
        .map_err(|err| RuleParseError::from_yaml_error(err.to_string(), err.location()))?;
    let rule: RuleFile = serde_yaml::from_value(value)
        .map_err(|err| RuleParseError::from_serde_yaml(RuleFormat::Yaml, err))?;
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

    let value = crate::serde_guard::parse_json_value_strict(json)
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
    pub location: Option<YamlLocation>,
}

impl RuleParseError {
    pub fn new(format: RuleFormat, message: impl Into<String>) -> Self {
        Self {
            format,
            message: message.into(),
            location: None,
        }
    }

    fn from_yaml_error(message: impl Into<String>, location: Option<(usize, usize)>) -> Self {
        let mut err = Self::new(RuleFormat::Yaml, message);
        if let Some((line, column)) = location {
            err.location = Some(YamlLocation { line, column });
        }
        err
    }

    fn from_serde_yaml(format: RuleFormat, err: serde_yaml::Error) -> Self {
        let location = err.location().map(|loc| YamlLocation {
            line: loc.line(),
            column: loc.column(),
        });
        Self {
            format,
            message: err.to_string(),
            location,
        }
    }

    pub fn line_column(&self) -> Option<(usize, usize)> {
        self.location
            .as_ref()
            .map(|location| (location.line, location.column))
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
        RuleFormat::Yaml => parse_rule_file_yaml_with_error(source),
        RuleFormat::Json => parse_rule_file_json(source),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{RuleFormat, RuleParseError};
    use crate::error::YamlLocation;

    #[test]
    fn rule_format_from_path_keeps_json_extension_detection() {
        assert_eq!(
            RuleFormat::from_path(Path::new("rules.json")),
            RuleFormat::Json
        );
        assert_eq!(
            RuleFormat::from_path(Path::new("rules.JSON")),
            RuleFormat::Json
        );
        assert_eq!(
            RuleFormat::from_path(Path::new("rules.yaml")),
            RuleFormat::Yaml
        );
        assert_eq!(
            RuleFormat::from_path(Path::new("rules.yml")),
            RuleFormat::Yaml
        );
        assert_eq!(RuleFormat::from_path(Path::new("rules")), RuleFormat::Yaml);
    }

    #[test]
    fn rule_parse_error_display_and_location_are_stable() {
        let err = RuleParseError {
            format: RuleFormat::Json,
            message: "duplicate key".to_string(),
            location: Some(YamlLocation { line: 2, column: 4 }),
        };

        assert_eq!(err.line_column(), Some((2, 4)));
        assert_eq!(err.to_string(), "failed to parse json rules: duplicate key");
    }
}
