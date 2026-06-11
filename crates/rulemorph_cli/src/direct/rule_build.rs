use rulemorph::{RuleFormat, parse_rule_file_with_format};

use super::DIRECT_VALUE_TARGET;
use super::input_config::DirectInputFormat;
use super::output_spec::DirectOutputSpec;

pub(super) fn build_direct_rule(
    output_spec: &DirectOutputSpec,
    input_format: DirectInputFormat,
    input_config: serde_json::Value,
) -> Result<rulemorph::RuleFile, String> {
    let input_format_name = match input_format {
        DirectInputFormat::Csv => "csv",
        DirectInputFormat::Json => "json",
        DirectInputFormat::Excel => "excel",
    };
    let mut input = serde_json::Map::new();
    input.insert(
        "format".to_string(),
        serde_json::Value::String(input_format_name.to_string()),
    );
    input.insert(input_format_name.to_string(), input_config);
    let mappings = match output_spec {
        DirectOutputSpec::Rule(expr) => vec![serde_json::json!({
            "target": DIRECT_VALUE_TARGET,
            "expr": expr.clone()
        })],
        DirectOutputSpec::Fields(fields) | DirectOutputSpec::OutputMap(fields) => fields
            .iter()
            .map(|field| {
                serde_json::json!({
                    "target": field.target,
                    "expr": field.expr
                })
            })
            .collect(),
    };
    let rule_json = serde_json::json!({
        "version": 2,
        "input": serde_json::Value::Object(input),
        "mappings": mappings
    });
    let source = serde_json::to_string(&rule_json)
        .map_err(|err| format!("failed to encode inline rule: {}", err))?;
    parse_rule_file_with_format(&source, RuleFormat::Json)
        .map_err(|err| format!("failed to parse inline rule: {}", err))
}
