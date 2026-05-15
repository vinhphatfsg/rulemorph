use std::collections::HashSet;

use rulemorph::InputFormat;
use serde_json::{Map, Value, json};
use serde_yaml::Value as YamlValue;

use crate::args::{get_optional_json_value, get_optional_string, get_optional_usize};
use crate::diagnostics::parse_error_json;
use crate::errors::CallError;
use crate::input_analysis::{analyze_records, build_input_paths, select_candidates};
use crate::input_records::{
    InputDataFormat, json_records_from_value, parse_csv_records, parse_json_records_strict,
};
use crate::path_expr::leaf_from_path;
use crate::rule_source::load_rule_from_source;
use crate::rules_yaml::{
    collect_missing_refs, update_yaml_input_spec, update_yaml_mapping, yaml_mappings_sequence_mut,
};
use crate::sandbox::read_allowed_to_string;

pub(crate) fn run_generate_rules_from_base_tool(
    args: &Map<String, Value>,
) -> Result<Value, CallError> {
    let rules_path = get_optional_string(args, "rules_path").map_err(CallError::InvalidParams)?;
    let rules_text = get_optional_string(args, "rules_text").map_err(CallError::InvalidParams)?;
    let rules_format =
        get_optional_string(args, "rules_format").map_err(CallError::InvalidParams)?;
    let input_path = get_optional_string(args, "input_path").map_err(CallError::InvalidParams)?;
    let input_text = get_optional_string(args, "input_text").map_err(CallError::InvalidParams)?;
    let input_json =
        get_optional_json_value(args, "input_json").map_err(CallError::InvalidParams)?;
    let format = get_optional_string(args, "format").map_err(CallError::InvalidParams)?;
    let records_path =
        get_optional_string(args, "records_path").map_err(CallError::InvalidParams)?;
    let max_candidates =
        get_optional_usize(args, "max_candidates").map_err(CallError::InvalidParams)?;

    let rule_source_count = rules_path.is_some() as u8 + rules_text.is_some() as u8;
    if rule_source_count == 0 {
        return Err(CallError::InvalidParams(
            "rules_path or rules_text is required".to_string(),
        ));
    }
    if rule_source_count > 1 {
        return Err(CallError::InvalidParams(
            "rules_path and rules_text are mutually exclusive".to_string(),
        ));
    }

    let input_source_count =
        input_path.is_some() as u8 + input_text.is_some() as u8 + input_json.is_some() as u8;
    if input_source_count == 0 {
        return Err(CallError::InvalidParams(
            "input_path, input_text, or input_json is required".to_string(),
        ));
    }
    if input_source_count > 1 {
        return Err(CallError::InvalidParams(
            "input_path, input_text, and input_json are mutually exclusive".to_string(),
        ));
    }

    if input_json.is_some()
        && format
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case("csv"))
    {
        return Err(CallError::InvalidParams(
            "format must be json when input_json is provided".to_string(),
        ));
    }
    if format.as_deref().is_some_and(|value| {
        !value.eq_ignore_ascii_case("csv") && !value.eq_ignore_ascii_case("json")
    }) {
        return Err(CallError::InvalidParams(
            "format must be csv or json".to_string(),
        ));
    }

    let (rule, yaml, _) = load_rule_from_source(
        rules_path.as_deref(),
        rules_text.as_deref(),
        rules_format.as_deref(),
    )?;
    let mut yaml_value: YamlValue = serde_yaml::from_str(&yaml).map_err(|err| {
        let message = format!("failed to parse rules yaml: {}", err);
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        }
    })?;

    let input_text = match (input_path.as_deref(), input_text.as_deref()) {
        (Some(path), None) => read_allowed_to_string(path, "input")?,
        (None, Some(text)) => text.to_string(),
        (None, None) => String::new(),
        _ => {
            return Err(CallError::InvalidParams(
                "input_path, input_text, or input_json is required".to_string(),
            ));
        }
    };

    let records_path = records_path.or_else(|| {
        rule.input
            .json
            .as_ref()
            .and_then(|json| json.records_path.clone())
    });

    let parse_format = if input_json.is_some() {
        InputDataFormat::Json
    } else if let Some(format) = format.as_deref() {
        if format.eq_ignore_ascii_case("csv") {
            InputDataFormat::Csv
        } else {
            InputDataFormat::Json
        }
    } else {
        match rule.input.format {
            InputFormat::Csv => InputDataFormat::Csv,
            InputFormat::Json => InputDataFormat::Json,
            InputFormat::Yaml
            | InputFormat::Toml
            | InputFormat::Xml
            | InputFormat::Html
            | InputFormat::Excel => InputDataFormat::Json,
        }
    };

    let has_input_json = input_json.is_some();
    let records = match (parse_format, input_json) {
        (InputDataFormat::Json, Some(value)) => {
            json_records_from_value(&value, records_path.as_deref())?
        }
        (InputDataFormat::Json, None) => {
            parse_json_records_strict(&input_text, records_path.as_deref(), input_path.as_deref())?
        }
        (InputDataFormat::Csv, _) => parse_csv_records(&input_text).map_err(|err| {
            let message = format!("failed to parse input CSV: {}", err);
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![parse_error_json(&message, input_path.as_deref())]),
            }
        })?,
    };

    let format_override = if has_input_json {
        Some("json".to_string())
    } else {
        format
    };
    let format_for_yaml = if format_override.is_some() {
        format_override.as_deref()
    } else if records_path.is_some() {
        Some("json")
    } else {
        None
    };
    update_yaml_input_spec(&mut yaml_value, format_for_yaml, records_path.as_deref());

    let stats = analyze_records(&records, None);
    let input_paths = build_input_paths(&stats);
    let input_path_set: HashSet<String> =
        input_paths.iter().map(|info| info.path.clone()).collect();

    let max_candidates = max_candidates.unwrap_or(3);
    let mut candidates_meta = Vec::new();
    let mut unmapped = Vec::new();
    let mut missing_refs = Vec::new();
    let mut missing_ref_set = HashSet::new();
    let mut mapped = 0usize;
    let mut with_expr = 0usize;
    let mut with_value = 0usize;

    let mappings = yaml_mappings_sequence_mut(&mut yaml_value)?;

    for (index, mapping) in rule.mappings.iter().enumerate() {
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
            "total": rule.mappings.len(),
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
