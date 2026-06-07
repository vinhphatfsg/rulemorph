use std::path::{Path, PathBuf};

use rulemorph::{InputData, RuleFile, RuleFormat, parse_rule_file_with_format};

use crate::diagnostics::parse_error_json;
use crate::errors::CallError;
use crate::sandbox::read_allowed_file;

pub(crate) fn load_rule_from_source(
    rules_path: Option<&str>,
    rules_text: Option<&str>,
    rules_format: Option<&str>,
) -> Result<(RuleFile, String, Option<PathBuf>), CallError> {
    let format_override = parse_rules_format(rules_format)?;
    match (rules_path, rules_text) {
        (Some(path), None) => {
            let (resolved_path, yaml) = read_allowed_file(path, "rules")?;
            let format = format_override.unwrap_or_else(|| RuleFormat::from_path(&resolved_path));
            let rule = parse_rule_file_with_format(&yaml, format).map_err(|err| {
                let message = format!("failed to parse rules: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, Some(path))]),
                }
            })?;
            let base_dir = resolved_path.parent().map(Path::to_path_buf);
            Ok((rule, yaml, base_dir))
        }
        (None, Some(text)) => {
            let format = format_override.unwrap_or(RuleFormat::Yaml);
            let rule = parse_rule_file_with_format(text, format).map_err(|err| {
                let message = format!("failed to parse rules: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, None)]),
                }
            })?;
            Ok((rule, text.to_string(), None))
        }
        _ => Err(CallError::InvalidParams(
            "rules_path or rules_text is required".to_string(),
        )),
    }
}

fn parse_rules_format(value: Option<&str>) -> Result<Option<RuleFormat>, CallError> {
    match value {
        None => Ok(None),
        Some(value) if value.eq_ignore_ascii_case("yaml") => Ok(Some(RuleFormat::Yaml)),
        Some(value) if value.eq_ignore_ascii_case("json") => Ok(Some(RuleFormat::Json)),
        Some(_) => Err(CallError::InvalidParams(
            "rules_format must be yaml or json".to_string(),
        )),
    }
}

pub(crate) fn validate_transform_format(value: Option<&str>) -> Result<(), CallError> {
    let Some(value) = value else {
        return Ok(());
    };
    if matches!(
        value.to_ascii_lowercase().as_str(),
        "csv" | "json" | "yaml" | "toml" | "xml" | "html" | "excel" | "markdown"
    ) {
        return Ok(());
    }
    Err(CallError::InvalidParams(
        "format must be csv, json, yaml, toml, xml, html, excel, or markdown".to_string(),
    ))
}

pub(crate) enum OwnedInput {
    Text(String),
    Bytes(Vec<u8>),
}

impl OwnedInput {
    pub(crate) fn as_input_data(&self) -> InputData<'_> {
        match self {
            OwnedInput::Text(value) => InputData::Text(value),
            OwnedInput::Bytes(value) => InputData::Bytes(value),
        }
    }
}

pub(crate) fn rule_has_file_branch(rule: &RuleFile) -> bool {
    rule.steps
        .as_ref()
        .is_some_and(|steps| steps.iter().any(|step| step.branch.is_some()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_transform_format_accepts_markdown() {
        assert!(validate_transform_format(Some("markdown")).is_ok());
        assert!(validate_transform_format(Some("MARKDOWN")).is_ok());
    }
}
