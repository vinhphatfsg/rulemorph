use crate::{OutputError, OutputErrorKind, limits};
use rulemorph::{PathToken, RuleFile};
use serde_json::json;

pub(crate) fn parse(text: &str) -> Result<RuleFile, OutputError> {
    if text.len() > 16 * 1024 {
        return Err(limits::limit("rules exceed 16 KiB"));
    }
    rulemorph::serde_guard::parse_yaml_value_strict_with_limits(text, 32, 2048, 256, 16 * 1024)
        .map_err(|_| {
            OutputError::new(
                OutputErrorKind::InvalidRules,
                "rules contain invalid/duplicate YAML or exceed parser limits",
            )
        })?;
    let rule = rulemorph::parse_rule_file(text).map_err(|_| {
        OutputError::new(
            OutputErrorKind::InvalidRules,
            "rules must be valid YAML/JSON",
        )
    })?;
    if rule.input.format != rulemorph::InputFormat::Json
        || rule.steps.is_some()
        || !rule.defs.is_empty()
        || !rule.codecs.is_empty()
        || rule.record_when.is_some()
    {
        return Err(unsupported(
            "normalization supports JSON input and direct mappings only; steps/defs/codecs/record_when are not supported",
        ));
    }
    if rule.mappings.len() > 64 {
        return Err(limits::limit("mappings exceed 64"));
    }
    for mapping in &rule.mappings {
        if mapping.expr.is_some() || mapping.when.is_some() {
            return Err(unsupported(
                "normalization supports explicit source/value/type/default/required mappings; expressions and filters are not supported",
            ));
        }
        let tokens = rulemorph::parse_path(&mapping.target).map_err(|_| {
            OutputError::new(OutputErrorKind::InvalidRules, "invalid mapping target")
        })?;
        if tokens.len() > 16
            || tokens.iter().any(|token| match token {
                PathToken::Index(index) => *index > 255,
                PathToken::Key(key) => key.len() > 256,
            })
        {
            return Err(limits::limit(
                "mapping target exceeds depth/index/key limits",
            ));
        }
    }
    if let Some(finalize) = &rule.finalize {
        if finalize.filter.is_some()
            || finalize.sort.is_some()
            || finalize.limit.is_some()
            || finalize.offset.is_some()
        {
            return Err(unsupported(
                "normalization never filters, sorts or truncates records",
            ));
        }
        if let Some(wrap) = &finalize.wrap
            && wrap != "@out"
        {
            let field = wrap.as_array().filter(|items| items.len() == 2 && items[0] == "@out")
                    .and_then(|items| items[1].as_object()).filter(|object| object.len() == 1)
                    .and_then(|object| object.get("map")).and_then(|value| value.as_array())
                    .filter(|steps| steps.len() == 1).and_then(|steps| steps[0].as_str())
                    .and_then(|reference| reference.strip_prefix("@item."))
                    .ok_or_else(|| unsupported("finalize.wrap supports only @out or [@out, {map: [@item.<explicit required target>]}]"))?;
            if !rule
                .mappings
                .iter()
                .any(|mapping| mapping.target == field && mapping.required)
            {
                return Err(unsupported(
                    "projection must select an explicit required mapping target; missing values are not synthesized",
                ));
            }
            if *wrap != json!(["@out", {"map":[format!("@item.{field}")]}]) {
                return Err(unsupported("invalid projection"));
            }
        }
    }
    rulemorph::validate_rule_file(&rule)
        .map_err(|_| OutputError::new(OutputErrorKind::InvalidRules, "rule validation failed"))?;
    Ok(rule)
}
fn unsupported(message: &str) -> OutputError {
    OutputError::new(OutputErrorKind::UnsupportedRules, message)
}
