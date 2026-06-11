use std::fs;
use std::path::{Path, PathBuf};

use rulemorph::{InputFormat, RuleFile, RuleFormat, parse_rule_file_with_format};

use super::super::{FormatOverride, RulesFormatArg};

pub(crate) fn load_rule(
    path: &PathBuf,
    override_format: Option<RulesFormatArg>,
) -> Result<(RuleFile, String), i32> {
    let yaml = match fs::read_to_string(path) {
        Ok(data) => data,
        Err(err) => {
            eprintln!("failed to read rules: {}", err);
            return Err(1);
        }
    };

    let format = detect_rule_format(path, override_format);
    let rule = match parse_rule_file_with_format(&yaml, format) {
        Ok(rule) => rule,
        Err(err) => {
            eprintln!("failed to parse rules: {}", err);
            return Err(1);
        }
    };

    Ok((rule, yaml))
}

fn detect_rule_format(path: &Path, override_format: Option<RulesFormatArg>) -> RuleFormat {
    match override_format {
        Some(RulesFormatArg::Yaml) => RuleFormat::Yaml,
        Some(RulesFormatArg::Json) => RuleFormat::Json,
        None => RuleFormat::from_path(path),
    }
}

pub(crate) fn rule_base_dir(path: &Path) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    if parent.as_os_str().is_empty() {
        Path::new(".").to_path_buf()
    } else {
        parent.to_path_buf()
    }
}

pub(crate) fn apply_format_override(rule: &mut RuleFile, format: Option<FormatOverride>) {
    if let Some(format) = format {
        rule.input.format = match format {
            FormatOverride::Csv => InputFormat::Csv,
            FormatOverride::Json => InputFormat::Json,
        };
    }
}
