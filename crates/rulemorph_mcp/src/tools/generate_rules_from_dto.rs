use serde_json::{Map, Value, json};
use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};

use crate::args::{get_optional_json_value, get_optional_string, get_optional_usize};
use crate::diagnostics::parse_error_json;
use crate::dto_language::{dto_error_json, parse_dto_source_language};
use crate::dto_parse::parse_dto_schema;
use crate::dto_schema::generate_mappings_from_schema;
use crate::errors::CallError;
use crate::input_analysis::{analyze_records, build_input_paths, select_candidates};
use crate::input_records::{
    InputDataFormat, json_records_from_value, normalize_format, parse_csv_records,
    parse_json_records_strict,
};
use crate::path_expr::leaf_from_path;
use crate::rules_yaml::{build_input_yaml, yaml_key};
use crate::sandbox::read_allowed_to_string;

pub(crate) fn run_generate_rules_from_dto_tool(
    args: &Map<String, Value>,
) -> Result<Value, CallError> {
    let dto_text = get_optional_string(args, "dto_text").map_err(CallError::InvalidParams)?;
    let dto_language =
        get_optional_string(args, "dto_language").map_err(CallError::InvalidParams)?;
    let input_path = get_optional_string(args, "input_path").map_err(CallError::InvalidParams)?;
    let input_text = get_optional_string(args, "input_text").map_err(CallError::InvalidParams)?;
    let input_json =
        get_optional_json_value(args, "input_json").map_err(CallError::InvalidParams)?;
    let format = get_optional_string(args, "format").map_err(CallError::InvalidParams)?;
    let records_path =
        get_optional_string(args, "records_path").map_err(CallError::InvalidParams)?;
    let max_candidates =
        get_optional_usize(args, "max_candidates").map_err(CallError::InvalidParams)?;

    let dto_text =
        dto_text.ok_or_else(|| CallError::InvalidParams("dto_text is required".to_string()))?;
    let dto_language = dto_language
        .ok_or_else(|| CallError::InvalidParams("dto_language is required".to_string()))?;
    let dto_language =
        parse_dto_source_language(&dto_language).map_err(CallError::InvalidParams)?;

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

    let has_input_json = input_json.is_some();
    let parse_format = if has_input_json {
        InputDataFormat::Json
    } else if let Some(format) = format.as_deref() {
        if format.eq_ignore_ascii_case("csv") {
            InputDataFormat::Csv
        } else {
            InputDataFormat::Json
        }
    } else {
        normalize_format(None, &input_text)
    };

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

    let schema = parse_dto_schema(&dto_text, dto_language).map_err(|message| CallError::Tool {
        message: message.clone(),
        errors: Some(vec![dto_error_json(&message)]),
    })?;
    let generated = generate_mappings_from_schema(&schema).map_err(|message| CallError::Tool {
        message: message.clone(),
        errors: Some(vec![dto_error_json(&message)]),
    })?;

    let stats = analyze_records(&records, None);
    let input_paths = build_input_paths(&stats);
    let max_candidates = max_candidates.unwrap_or(3);

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

    let format_str = if has_input_json {
        "json".to_string()
    } else if let Some(format) = format.as_deref() {
        if format.eq_ignore_ascii_case("csv") {
            "csv".to_string()
        } else {
            "json".to_string()
        }
    } else {
        match parse_format {
            InputDataFormat::Csv => "csv".to_string(),
            InputDataFormat::Json => "json".to_string(),
        }
    };

    let input_yaml = build_input_yaml(&format_str, records_path.as_deref());
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
