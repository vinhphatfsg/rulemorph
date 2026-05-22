use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use super::{CompiledEndpointRule, EndpointRuleFile, resolve_rule_path};

mod diagnostics;
mod network;
mod rules;
mod source;
mod state;

use self::diagnostics::push_error;
pub use self::diagnostics::{RulesDirError, RulesDirErrors};
use self::rules::validate_rule_path;
use self::source::{parse_yaml, read_rule_source};
use self::state::{RuleRefUsage, ValidationState};

pub fn validate_rules_dir(rules_dir: &Path) -> std::result::Result<(), RulesDirErrors> {
    let mut errors = Vec::new();
    let endpoint_path = rules_dir.join("endpoint.yaml");
    let source = match read_rule_source(&endpoint_path, &mut errors) {
        Some(source) => source,
        None => return Err(RulesDirErrors { errors }),
    };

    let raw: EndpointRuleFile = match parse_yaml(&endpoint_path, &source, &mut errors) {
        Some(raw) => raw,
        None => return Err(RulesDirErrors { errors }),
    };

    if raw.version != 2 {
        push_error(
            &mut errors,
            "InvalidVersion",
            &endpoint_path,
            "endpoint rule version must be 2",
            Some("version".to_string()),
            None,
        );
    }
    if raw.rule_type != "endpoint" {
        push_error(
            &mut errors,
            "InvalidRuleType",
            &endpoint_path,
            "endpoint rule type must be endpoint",
            Some("type".to_string()),
            None,
        );
    }
    if let Err(err) = CompiledEndpointRule::compile(raw.clone(), &endpoint_path) {
        push_error(
            &mut errors,
            "EndpointCompileFailed",
            &endpoint_path,
            err.to_string(),
            None,
            None,
        );
    }

    let base_dir = endpoint_path.parent().unwrap_or_else(|| Path::new("."));
    let mut refs: BTreeSet<PathBuf> = BTreeSet::new();
    let mut ref_usage: HashMap<PathBuf, RuleRefUsage> = HashMap::new();
    for endpoint in &raw.endpoints {
        for step in &endpoint.steps {
            let resolved = resolve_rule_path(base_dir, &step.rule);
            refs.insert(resolved.clone());
            ref_usage
                .entry(resolved)
                .and_modify(|usage| usage.merge(RuleRefUsage::step()))
                .or_insert_with(RuleRefUsage::step);
            if let Some(catch) = &step.catch {
                for target in catch.values() {
                    let resolved = resolve_rule_path(base_dir, target);
                    refs.insert(resolved.clone());
                    ref_usage
                        .entry(resolved)
                        .and_modify(|usage| usage.merge(RuleRefUsage::catch_rule()))
                        .or_insert_with(RuleRefUsage::catch_rule);
                }
            }
        }
        if let Some(catch) = &endpoint.catch {
            for target in catch.values() {
                let resolved = resolve_rule_path(base_dir, target);
                refs.insert(resolved.clone());
                ref_usage
                    .entry(resolved)
                    .and_modify(|usage| usage.merge(RuleRefUsage::catch_rule()))
                    .or_insert_with(RuleRefUsage::catch_rule);
            }
        }
    }

    let mut state = ValidationState::default();
    for path in refs {
        let usage = ref_usage.get(&path).copied().unwrap_or_default();
        validate_rule_path(&path, usage, &mut state, &mut errors);
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(RulesDirErrors { errors })
    }
}
