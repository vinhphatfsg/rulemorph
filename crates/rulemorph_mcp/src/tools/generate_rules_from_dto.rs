mod prep;

use serde_json::{Map, Value, json};
use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};

use crate::diagnostics::parse_error_json;
use crate::dto_language::dto_error_json;
use crate::dto_parse::parse_dto_schema;
use crate::dto_schema::generate_mappings_from_schema;
use crate::errors::CallError;
use crate::input_analysis::{analyze_records, build_input_paths, select_candidates};
use crate::path_expr::leaf_from_path;
use crate::rules_yaml::{build_input_yaml, yaml_key};
use prep::prepare_dto_generation;

pub(crate) fn run_generate_rules_from_dto_tool(
    args: &Map<String, Value>,
) -> Result<Value, CallError> {
    let prepared = prepare_dto_generation(args)?;

    let schema =
        parse_dto_schema(&prepared.dto_text, prepared.dto_language).map_err(|message| {
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![dto_error_json(&message)]),
            }
        })?;
    let generated = generate_mappings_from_schema(&schema).map_err(|message| CallError::Tool {
        message: message.clone(),
        errors: Some(vec![dto_error_json(&message)]),
    })?;

    let stats = analyze_records(&prepared.records, None);
    let input_paths = build_input_paths(&stats);
    let max_candidates = prepared.max_candidates;

    let mut candidates_meta = Vec::new();
    let mut unmapped = Vec::new();
    let mut mapped = 0usize;

    let mut mappings_yaml = Vec::new();
    for mapping in &generated {
        let target_leaf = leaf_from_path(&mapping.target).unwrap_or_default();
        let candidates = select_candidates(
            &target_leaf,
            None,
            mapping.value_type.as_deref(),
            &input_paths,
            max_candidates,
        );
        let selected = candidates.first().cloned();

        let mut mapping_map = YamlMapping::new();
        mapping_map.insert(
            yaml_key("target"),
            YamlValue::String(mapping.target.clone()),
        );
        if let Some(value_type) = mapping.value_type.as_deref() {
            mapping_map.insert(yaml_key("type"), YamlValue::String(value_type.to_string()));
        }
        if let Some(selected) = selected.as_ref() {
            mapped += 1;
            mapping_map.insert(
                yaml_key("source"),
                YamlValue::String(selected.source.clone()),
            );
            if mapping.required {
                mapping_map.insert(yaml_key("required"), YamlValue::Bool(true));
            }
        } else {
            unmapped.push(mapping.target.clone());
            mapping_map.insert(yaml_key("value"), YamlValue::Null);
            mapping_map.insert(yaml_key("required"), YamlValue::Bool(false));
        }
        mappings_yaml.push(YamlValue::Mapping(mapping_map));

        let candidates_json: Vec<Value> = candidates
            .iter()
            .map(|candidate| {
                json!({
                    "source": candidate.source,
                    "score": candidate.score,
                    "reason": candidate.reason,
                    "confidence": candidate.confidence
                })
            })
            .collect();
        let mut entry = json!({
            "target": mapping.target,
            "candidates": candidates_json
        });
        if let Some(selected) = selected {
            entry["selected"] = json!(selected.source);
            entry["confidence"] = json!(selected.confidence);
        }
        candidates_meta.push(entry);
    }

    let input_yaml = build_input_yaml(&prepared.format_str, prepared.records_path.as_deref());
    let mut root = YamlMapping::new();
    root.insert(yaml_key("version"), YamlValue::Number(1.into()));
    root.insert(yaml_key("input"), input_yaml);
    root.insert(yaml_key("mappings"), YamlValue::Sequence(mappings_yaml));
    let yaml_value = YamlValue::Mapping(root);
    let output_text = serde_yaml::to_string(&yaml_value).map_err(|err| {
        let message = format!("failed to serialize rules yaml: {}", err);
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        }
    })?;

    let mut meta = serde_json::Map::new();
    meta.insert(
        "summary".to_string(),
        json!({
            "total": generated.len(),
            "mapped": mapped,
            "unmapped": unmapped.len()
        }),
    );
    meta.insert("candidates".to_string(), Value::Array(candidates_meta));
    if !unmapped.is_empty() {
        meta.insert("unmapped".to_string(), json!(unmapped));
    }

    Ok(json!({
        "content": [
            {
                "type": "text",
                "text": output_text
            }
        ],
        "meta": meta
    }))
}
