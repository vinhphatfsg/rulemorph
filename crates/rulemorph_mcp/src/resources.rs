use serde_json::{Value, json};

const RESOURCE_URI_RULES_SPEC_EN: &str = "rulemorph://docs/rules_spec_en";
const RESOURCE_URI_RULES_SPEC_JA: &str = "rulemorph://docs/rules_spec_ja";
const RESOURCE_URI_README: &str = "rulemorph://docs/readme";
const RESOURCE_RULES_SPEC_EN: &str = include_str!("../../../docs/rules_spec_en.md");
const RESOURCE_RULES_SPEC_JA: &str = include_str!("../../../docs/rules_spec_ja.md");
const RESOURCE_README: &str = include_str!("../../../README.md");

pub(crate) fn resources_list_result() -> Value {
    json!({
        "resources": [
            {
                "uri": RESOURCE_URI_RULES_SPEC_EN,
                "name": "rules_spec_en",
                "description": "Rule specification (English).",
                "mimeType": "text/markdown"
            },
            {
                "uri": RESOURCE_URI_RULES_SPEC_JA,
                "name": "rules_spec_ja",
                "description": "ルール仕様 (日本語).",
                "mimeType": "text/markdown"
            },
            {
                "uri": RESOURCE_URI_README,
                "name": "readme",
                "description": "Project README.",
                "mimeType": "text/markdown"
            }
        ]
    })
}

pub(crate) fn resources_read_result(params: &Value) -> Result<Value, String> {
    let obj = params
        .as_object()
        .ok_or_else(|| "params must be an object".to_string())?;
    let uri = obj
        .get("uri")
        .and_then(|value| value.as_str())
        .ok_or_else(|| "params.uri is required".to_string())?;
    let text = match uri {
        RESOURCE_URI_RULES_SPEC_EN => RESOURCE_RULES_SPEC_EN,
        RESOURCE_URI_RULES_SPEC_JA => RESOURCE_RULES_SPEC_JA,
        RESOURCE_URI_README => RESOURCE_README,
        _ => return Err("unknown resource uri".to_string()),
    };

    Ok(json!({
        "contents": [
            {
                "uri": uri,
                "mimeType": "text/markdown",
                "text": text
            }
        ]
    }))
}
