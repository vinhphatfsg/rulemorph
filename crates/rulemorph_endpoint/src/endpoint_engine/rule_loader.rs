use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use rulemorph::{
    LEGACY_V1_RULE_DEPRECATION_MESSAGE, RuleFile, RuleFormat, is_legacy_v1_rule,
    parse_rule_file_with_format, validate_rule_file_with_source,
};
use serde_json::Value as JsonValue;

use super::network_rule::{CompiledNetworkRule, NetworkRuleFile, compile_network_rule};

#[derive(Debug)]
pub(super) struct LoadedRule {
    pub(super) rule: RuleFile,
    pub(super) base_dir: PathBuf,
}

pub(super) enum RuleKind {
    Normal(Box<LoadedRule>),
    Network(Box<CompiledNetworkRule>),
}

pub(super) fn load_rule_kind(path: &Path) -> Result<RuleKind> {
    let source = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let meta = rulemorph::serde_guard::parse_yaml_value_strict(&source)
        .map_err(|err| anyhow!(err))
        .with_context(|| format!("failed to parse {}", path.display()))?;
    let rule_type = meta
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("normal");
    match rule_type {
        "network" => {
            let raw: NetworkRuleFile = serde_yaml::from_value(meta)
                .with_context(|| format!("failed to parse {}", path.display()))?;
            let compiled = compile_network_rule(raw, path)?;
            Ok(RuleKind::Network(Box::new(compiled)))
        }
        "endpoint" => Err(anyhow!("endpoint rule not allowed as step")),
        _ => {
            let rule = parse_rule_file_with_format(&source, RuleFormat::from_path(path))
                .with_context(|| format!("failed to parse {}", path.display()))?;
            warn_legacy_v1_rule(path, &rule);
            validate_rule_file_with_source(&rule, &source)
                .map_err(|err| anyhow!("failed to validate {}: {:?}", path.display(), err))?;
            let base_dir = path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf();
            Ok(RuleKind::Normal(Box::new(LoadedRule { rule, base_dir })))
        }
    }
}

pub(super) fn warn_legacy_v1_rule(path: &Path, rule: &RuleFile) {
    if is_legacy_v1_rule(rule) {
        tracing::warn!(
            rule_path = %path.display(),
            message = LEGACY_V1_RULE_DEPRECATION_MESSAGE,
            "deprecated Rulemorph rule version"
        );
    }
}

pub(super) fn yaml_source_to_json(source: &str) -> Option<JsonValue> {
    let raw = rulemorph::serde_guard::parse_yaml_value_strict(source).ok()?;
    serde_json::to_value(raw).ok()
}
