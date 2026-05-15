use std::collections::HashSet;

use rulemorph::{Expr, InputFormat, RuleFile};
use serde_json::{Value, json};
use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};

use crate::diagnostics::parse_error_json;
use crate::errors::CallError;

pub(crate) fn build_input_yaml(format: &str, records_path: Option<&str>) -> YamlValue {
    let mut input_map = YamlMapping::new();
    input_map.insert(yaml_key("format"), YamlValue::String(format.to_string()));
    if format.eq_ignore_ascii_case("json") {
        let mut json_map = YamlMapping::new();
        if let Some(records_path) = records_path {
            json_map.insert(
                yaml_key("records_path"),
                YamlValue::String(records_path.to_string()),
            );
        }
        input_map.insert(yaml_key("json"), YamlValue::Mapping(json_map));
    } else {
        input_map.insert(yaml_key("csv"), YamlValue::Mapping(YamlMapping::new()));
    }
    YamlValue::Mapping(input_map)
}

pub(crate) fn update_yaml_input_spec(
    root: &mut YamlValue,
    format: Option<&str>,
    records_path: Option<&str>,
) {
    if format.is_none() && records_path.is_none() {
        return;
    }
    let Some(root_map) = root.as_mapping_mut() else {
        return;
    };
    let input_value = root_map
        .entry(yaml_key("input"))
        .or_insert_with(|| YamlValue::Mapping(YamlMapping::new()));
    let Some(input_map) = input_value.as_mapping_mut() else {
        return;
    };

    if let Some(format) = format {
        input_map.insert(yaml_key("format"), YamlValue::String(format.to_string()));
    }
    if let Some(records_path) = records_path {
        let json_value = input_map
            .entry(yaml_key("json"))
            .or_insert_with(|| YamlValue::Mapping(YamlMapping::new()));
        if let Some(json_map) = json_value.as_mapping_mut() {
            json_map.insert(
                yaml_key("records_path"),
                YamlValue::String(records_path.to_string()),
            );
        }
    }
}

pub(crate) fn yaml_mappings_sequence_mut(
    root: &mut YamlValue,
) -> Result<&mut Vec<YamlValue>, CallError> {
    let Some(root_map) = root.as_mapping_mut() else {
        let message = "rules yaml must be a mapping".to_string();
        return Err(CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        });
    };
    let Some(mappings_value) = root_map.get_mut(&yaml_key("mappings")) else {
        let message = "rules yaml is missing mappings".to_string();
        return Err(CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        });
    };
    mappings_value.as_sequence_mut().ok_or_else(|| {
        let message = "rules yaml mappings must be a sequence".to_string();
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        }
    })
}

pub(crate) fn update_yaml_mapping(
    mappings: &mut Vec<YamlValue>,
    index: usize,
    source: Option<&str>,
) -> Result<(), CallError> {
    let Some(mapping_value) = mappings.get_mut(index) else {
        let message = "mapping index out of range".to_string();
        return Err(CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        });
    };
    let Some(mapping_map) = mapping_value.as_mapping_mut() else {
        let message = "mapping entry must be a mapping".to_string();
        return Err(CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        });
    };

    if let Some(source) = source {
        mapping_map.insert(yaml_key("source"), YamlValue::String(source.to_string()));
        mapping_map.remove(&yaml_key("value"));
        mapping_map.remove(&yaml_key("expr"));
    } else {
        mapping_map.remove(&yaml_key("source"));
        mapping_map.remove(&yaml_key("expr"));
        mapping_map.insert(yaml_key("value"), YamlValue::Null);
        mapping_map.insert(yaml_key("required"), YamlValue::Bool(false));
    }
    Ok(())
}

pub(crate) fn yaml_key(key: &str) -> YamlValue {
    YamlValue::String(key.to_string())
}

pub(crate) fn collect_missing_refs(
    target: &str,
    expr: Option<&Expr>,
    when: Option<&Expr>,
    input_paths: &HashSet<String>,
    out: &mut Vec<Value>,
    seen: &mut HashSet<String>,
) {
    for expr in [expr, when] {
        let Some(expr) = expr else { continue };
        let mut refs = Vec::new();
        collect_expr_refs(expr, &mut refs);
        for reference in refs {
            let Some(path) = input_ref_path(&reference) else {
                continue;
            };
            if input_paths.contains(&path) {
                continue;
            }
            let key = format!("{}|{}", target, reference);
            if seen.insert(key) {
                out.push(json!({
                    "target": target,
                    "ref": reference,
                    "path": path
                }));
            }
        }
    }
}

fn collect_expr_refs(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Ref(reference) => out.push(reference.ref_path.clone()),
        Expr::Op(op) => {
            for arg in &op.args {
                collect_expr_refs(arg, out);
            }
        }
        Expr::Chain(chain) => {
            for item in &chain.chain {
                collect_expr_refs(item, out);
            }
        }
        Expr::Literal(_) => {}
    }
}

fn input_ref_path(reference: &str) -> Option<String> {
    let trimmed = reference.trim();
    if let Some(rest) = trimmed.strip_prefix("input.") {
        if rest.is_empty() {
            None
        } else {
            Some(rest.to_string())
        }
    } else {
        None
    }
}

pub(crate) fn apply_format_override(
    rule: &mut RuleFile,
    format: Option<&str>,
) -> Result<(), String> {
    let Some(format) = format else {
        return Ok(());
    };
    let normalized = format.to_lowercase();
    rule.input.format = match normalized.as_str() {
        "csv" => InputFormat::Csv,
        "json" => InputFormat::Json,
        "yaml" => InputFormat::Yaml,
        "toml" => InputFormat::Toml,
        "xml" => InputFormat::Xml,
        "html" => InputFormat::Html,
        "excel" => InputFormat::Excel,
        _ => return Err(format!("unknown format: {}", format)),
    };
    Ok(())
}
