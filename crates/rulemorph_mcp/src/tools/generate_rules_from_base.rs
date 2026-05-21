mod prep;

use std::collections::HashSet;

use serde_json::{Map, Value, json};

use crate::diagnostics::parse_error_json;
use crate::errors::CallError;
use crate::input_analysis::{analyze_records, build_input_paths, select_candidates};
use crate::path_expr::leaf_from_path;
use crate::rules_yaml::{collect_missing_refs, update_yaml_mapping, yaml_mappings_sequence_mut};
use prep::prepare_base_generation;

pub(crate) fn run_generate_rules_from_base_tool(
    args: &Map<String, Value>,
) -> Result<Value, CallError> {
    let prepared = prepare_base_generation(args)?;
    let mut yaml_value = prepared.yaml_value;

    let stats = analyze_records(&prepared.records, None);
    let input_paths = build_input_paths(&stats);
    let input_path_set: HashSet<String> =
        input_paths.iter().map(|info| info.path.clone()).collect();

    let max_candidates = prepared.max_candidates;
    let mut candidates_meta = Vec::new();
    let mut unmapped = Vec::new();
    let mut missing_refs = Vec::new();
    let mut missing_ref_set = HashSet::new();
    let mut mapped = 0usize;
    let mut with_expr = 0usize;
    let mut with_value = 0usize;

    let mappings = yaml_mappings_sequence_mut(&mut yaml_value)?;

    for (index, mapping) in prepared.rule.mappings.iter().enumerate() {
        collect_missing_refs(
            &mapping.target,
            mapping.expr.as_ref(),
            mapping.when.as_ref(),
            &input_path_set,
            &mut missing_refs,
            &mut missing_ref_set,
        );
        if mapping.expr.is_some() {
            with_expr += 1;
            continue;
        }
        if mapping.value.is_some() {
            with_value += 1;
            continue;
        }

        let target_leaf = leaf_from_path(&mapping.target).unwrap_or_default();
        let candidates = select_candidates(
            &target_leaf,
            mapping.source.as_deref(),
            mapping.value_type.as_deref(),
            &input_paths,
            max_candidates,
        );
        let selected = candidates.first().cloned();

        if let Some(selected) = selected.as_ref() {
            mapped += 1;
            update_yaml_mapping(mappings, index, Some(&selected.source))?;
        } else {
            unmapped.push(mapping.target.clone());
            update_yaml_mapping(mappings, index, None)?;
        }

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
            "total": prepared.rule.mappings.len(),
            "mapped": mapped,
            "unmapped": unmapped.len(),
            "with_expr": with_expr,
            "with_value": with_value
        }),
    );
    meta.insert("candidates".to_string(), Value::Array(candidates_meta));
    if !unmapped.is_empty() {
        meta.insert("unmapped".to_string(), json!(unmapped));
    }
    if !missing_refs.is_empty() {
        meta.insert("missing_refs".to_string(), Value::Array(missing_refs));
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
