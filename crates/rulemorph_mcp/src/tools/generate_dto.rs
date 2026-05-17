use rulemorph::generate_dto;
use serde_json::{Map, Value, json};

use crate::args::get_optional_string;
use crate::dto_language::{dto_error_json, dto_language_to_str, parse_dto_language};
use crate::errors::CallError;
use crate::rule_source::load_rule_from_source;

pub(crate) fn run_generate_dto_tool(args: &Map<String, Value>) -> Result<Value, CallError> {
    let rules_path = get_optional_string(args, "rules_path").map_err(CallError::InvalidParams)?;
    let rules_text = get_optional_string(args, "rules_text").map_err(CallError::InvalidParams)?;
    let rules_format =
        get_optional_string(args, "rules_format").map_err(CallError::InvalidParams)?;
    let language = get_optional_string(args, "language").map_err(CallError::InvalidParams)?;
    let name = get_optional_string(args, "name").map_err(CallError::InvalidParams)?;

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

    let language =
        language.ok_or_else(|| CallError::InvalidParams("language is required".to_string()))?;
    let language = parse_dto_language(&language).map_err(CallError::InvalidParams)?;

    let (rule, _, _) = load_rule_from_source(
        rules_path.as_deref(),
        rules_text.as_deref(),
        rules_format.as_deref(),
    )?;
    let dto = generate_dto(&rule, language, name.as_deref()).map_err(|err| {
        let message = format!("failed to generate dto: {}", err);
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![dto_error_json(&message)]),
        }
    })?;

    let mut meta = serde_json::Map::new();
    meta.insert("language".to_string(), json!(dto_language_to_str(language)));
    if let Some(name) = name {
        meta.insert("name".to_string(), json!(name));
    }

    Ok(json!({
        "content": [
            {
                "type": "text",
                "text": dto
            }
        ],
        "meta": meta
    }))
}
