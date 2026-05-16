use std::path::Path;

use rulemorph::serde_guard::parse_yaml_value_strict;
use serde::de::DeserializeOwned;

use super::diagnostics::{RulesDirError, push_error, push_parse_error};

pub(super) fn read_rule_source(path: &Path, errors: &mut Vec<RulesDirError>) -> Option<String> {
    match std::fs::read_to_string(path) {
        Ok(source) => Some(source),
        Err(err) => {
            push_error(errors, "ReadFailed", path, err.to_string(), None, None);
            None
        }
    }
}

pub(super) fn parse_yaml<T: DeserializeOwned>(
    path: &Path,
    source: &str,
    errors: &mut Vec<RulesDirError>,
) -> Option<T> {
    let value = match parse_yaml_value_strict(source) {
        Ok(value) => value,
        Err(err) => {
            push_parse_error(errors, path, &err.to_string(), err.location());
            return None;
        }
    };
    match serde_yaml::from_value(value) {
        Ok(value) => Some(value),
        Err(err) => {
            let location = err.location().map(|loc| (loc.line(), loc.column()));
            push_parse_error(errors, path, &err.to_string(), location);
            None
        }
    }
}

pub(super) fn parse_rule_type(
    path: &Path,
    source: &str,
    errors: &mut Vec<RulesDirError>,
) -> Option<String> {
    let meta: serde_yaml::Value = match parse_yaml_value_strict(source) {
        Ok(value) => value,
        Err(err) => {
            push_parse_error(errors, path, &err.to_string(), err.location());
            return None;
        }
    };
    Some(
        meta.get("type")
            .and_then(|value| value.as_str())
            .unwrap_or("normal")
            .to_string(),
    )
}
