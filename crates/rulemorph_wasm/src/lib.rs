mod profile;

use rulemorph::{DtoLanguage, InputData, NormalizationOptions, RuleFile, serde_guard::*};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use wasm_bindgen::prelude::*;

pub const MAX_REQUEST_BYTES: usize = 96 * 1024;
pub const MAX_OUTPUT_BYTES: usize = 256 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Call {
    name: String,
    arguments: Arguments,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Arguments {
    rules_text: Option<String>,
    input_text: Option<String>,
    input_json: Option<Value>,
    language: Option<String>,
    name: Option<String>,
    format: Option<String>,
    records_path: Option<String>,
}

/// Pure, pathless tool boundary shared by native tests and the Worker.
#[wasm_bindgen]
pub fn call_tool(request: &str) -> String {
    let result = execute(request).and_then(|value| {
        let text = serde_json::to_string(&value).map_err(|e| e.to_string())?;
        if text.len() > MAX_OUTPUT_BYTES {
            return Err("output exceeds 256 KiB".into());
        }
        Ok(text)
    });
    match result {
        Ok(text) => json!({"content":[{"type":"text","text":text}],"isError":false}).to_string(),
        Err(message) => {
            json!({"content":[{"type":"text","text":message}],"isError":true}).to_string()
        }
    }
}

fn options() -> NormalizationOptions {
    NormalizationOptions {
        max_input_bytes: 64 * 1024,
        max_records: 128,
        max_depth: 32,
        max_array_len: 256,
        max_text_bytes: 64 * 1024,
        max_yaml_aliases: 0,
        max_yaml_expanded_nodes: 4096,
        max_xml_nodes: 4096,
        max_range_items: Some(256),
        max_object_fields: 256,
        max_object_depth: 32,
        max_generated_json_nodes: 4096,
        max_generated_json_bytes: MAX_OUTPUT_BYTES,
        ..NormalizationOptions::default()
    }
}

fn parse_rules(text: &str) -> Result<(RuleFile, usize), String> {
    if text.len() > 16 * 1024 {
        return Err("rules_text exceeds 16 KiB".into());
    }
    // Bound YAML expansion and nesting before core parsing (also accepts JSON).
    parse_yaml_value_strict_with_limits(text, 32, 2048, 256, 16 * 1024)
        .map_err(|e| e.to_string())?;
    let rule = rulemorph::parse_rule_file(text).map_err(|e| e.to_string())?;
    if !matches!(
        rule.input.format,
        rulemorph::InputFormat::Json
            | rulemorph::InputFormat::Csv
            | rulemorph::InputFormat::Yaml
            | rulemorph::InputFormat::Toml
            | rulemorph::InputFormat::Xml
    ) {
        return Err("HTTP input format must be json, csv, yaml, toml or xml".into());
    }
    let cost = profile::check(&rule)?;
    Ok((rule, cost))
}

fn execute(request: &str) -> Result<Value, String> {
    if request.len() > MAX_REQUEST_BYTES {
        return Err("request exceeds 96 KiB".into());
    }
    parse_json_value_strict(request).map_err(|e| e.to_string())?;
    let call: Call = serde_json::from_str(request).map_err(|e| e.to_string())?;
    let a = &call.arguments;
    match call.name.as_str() {
        "transform" | "validate_rules" | "generate_dto" => {
            if a.format.is_some() || a.records_path.is_some() {
                return Err("format and records_path are analyze_input arguments".into());
            }
            let (rule, cost) =
                parse_rules(a.rules_text.as_deref().ok_or("rules_text is required")?)?;
            let validation = rulemorph::validate_rule_file(&rule);
            if call.name == "validate_rules" {
                reject_input(a)?;
                return Ok(match validation {
                    Ok(()) => json!({"valid":true,"errors":[]}),
                    Err(errors) => {
                        json!({"valid":false,"errors":errors.iter().map(|e| json!({"code": e.code.as_str(), "message":e.message,"path":e.path})).collect::<Vec<_>>()})
                    }
                });
            }
            if validation.is_err() {
                return Err("rule validation failed; use validate_rules for diagnostics".into());
            }
            if call.name == "generate_dto" {
                reject_input(a)?;
                let language = match a.language.as_deref().ok_or("language is required")? {
                    "rust" => DtoLanguage::Rust,
                    "typescript" => DtoLanguage::TypeScript,
                    "python" => DtoLanguage::Python,
                    "go" => DtoLanguage::Go,
                    "java" => DtoLanguage::Java,
                    "kotlin" => DtoLanguage::Kotlin,
                    "swift" => DtoLanguage::Swift,
                    _ => return Err("unsupported DTO language".into()),
                };
                return rulemorph::generate_dto(&rule, language, a.name.as_deref())
                    .map(|dto| json!({"dto":dto}))
                    .map_err(|e| e.to_string());
            }
            if a.language.is_some() || a.name.is_some() {
                return Err("language and name are DTO arguments".into());
            }
            let text = input(a, &rule)?;
            let records = rulemorph::normalize_records_with_options(
                &rule,
                InputData::Text(&text),
                &options(),
            )
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
            if records.len() * cost > 16_384 {
                return Err("processing budget exceeds 16384 record-expression units".into());
            }
            rulemorph::transform_with_options(&rule, &text, None, &options())
                .map(|output| json!({"output":output}))
                .map_err(|e| e.to_string())
        }
        "analyze_input" => analyze(a),
        _ => Err("unknown tool".into()),
    }
}

fn reject_input(a: &Arguments) -> Result<(), String> {
    if a.input_text.is_some() || a.input_json.is_some() {
        Err("this tool does not accept input data".into())
    } else {
        Ok(())
    }
}

fn input(a: &Arguments, rule: &RuleFile) -> Result<String, String> {
    match (&a.input_text, &a.input_json) {
        (Some(text), None) => Ok(text.clone()),
        (None, Some(value)) if rule.input.format == rulemorph::InputFormat::Json => {
            Ok(value.to_string())
        }
        _ => Err(
            "provide exactly one input_text or input_json; input_json requires json format".into(),
        ),
    }
}

fn analyze(a: &Arguments) -> Result<Value, String> {
    if a.rules_text.is_some() || a.language.is_some() || a.name.is_some() {
        return Err("unexpected analyze_input argument".into());
    }
    let format = a.format.as_deref().unwrap_or("json");
    if !matches!(format, "json" | "csv") {
        return Err("analysis supports json or csv".into());
    }
    if format == "csv" && a.records_path.is_some() {
        return Err("records_path requires json format".into());
    }
    let spec = if format == "json" {
        json!({"format":format,"json":{"records_path":a.records_path}})
    } else {
        json!({"format":format,"csv":{"has_header":true}})
    };
    let rule =
        rulemorph::parse_rule_file(&json!({"version":2,"input":spec,"mappings":[]}).to_string())
            .map_err(|e| e.to_string())?;
    let text = input(a, &rule)?;
    let records =
        rulemorph::normalize_records_with_options(&rule, InputData::Text(&text), &options())
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
    let mut paths = BTreeMap::<String, BTreeSet<&str>>::new();
    for record in &records {
        visit(record, "", &mut paths)?;
    }
    Ok(json!({"summary":{"records":records.len(),"paths":paths.len()},"paths":paths}))
}

fn visit(
    value: &Value,
    path: &str,
    paths: &mut BTreeMap<String, BTreeSet<&'static str>>,
) -> Result<(), String> {
    let kind = match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    };
    paths.entry(path.into()).or_default().insert(kind);
    if paths.len() > 256 {
        return Err("analysis path limit is 256".into());
    }
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                visit(value, &format!("{path}[{}]", json!(key)), paths)?;
            }
        }
        Value::Array(items) => {
            for value in items {
                visit(value, &format!("{path}[]"), paths)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Validate raw HTTP JSON before JavaScript parsing can discard duplicate keys.
#[wasm_bindgen]
pub fn valid_request_json(request: &str) -> bool {
    request.len() <= MAX_REQUEST_BYTES && parse_json_value_strict(request).is_ok()
}
