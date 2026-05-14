use serde_json::{Map, Value, json};

pub(crate) fn prompts_list_result() -> Value {
    json!({
        "prompts": [
            {
                "name": "rule_from_input_base",
                "description": "Generate rules from base rules and input samples.",
                "arguments": [
                    { "name": "rules_text", "description": "Base rules YAML.", "required": true },
                    { "name": "input_sample", "description": "Input sample (JSON/CSV).", "required": true },
                    { "name": "format", "description": "Sample format for rule generation (json or csv).", "required": false },
                    { "name": "records_path", "description": "Records path for JSON input.", "required": false }
                ]
            },
            {
                "name": "rule_from_dto",
                "description": "Generate rules from DTO schema and input samples.",
                "arguments": [
                    { "name": "dto_text", "description": "DTO source text.", "required": true },
                    { "name": "dto_language", "description": "DTO language (rust/typescript).", "required": true },
                    { "name": "input_sample", "description": "Input sample (JSON/CSV).", "required": true },
                    { "name": "format", "description": "Sample format for rule generation (json or csv).", "required": false },
                    { "name": "records_path", "description": "Records path for JSON input.", "required": false }
                ]
            },
            {
                "name": "explain_errors",
                "description": "Explain validation/transform errors and suggest fixes.",
                "arguments": [
                    { "name": "errors_json", "description": "Errors array from tool output.", "required": true },
                    { "name": "rules_text", "description": "Optional rules YAML for context.", "required": false }
                ]
            }
        ]
    })
}

pub(crate) fn prompts_get_result(params: &Value) -> Result<Value, String> {
    let obj = params
        .as_object()
        .ok_or_else(|| "params must be an object".to_string())?;
    let name = obj
        .get("name")
        .and_then(|value| value.as_str())
        .ok_or_else(|| "params.name is required".to_string())?;
    let args = obj.get("arguments").and_then(|value| value.as_object());

    let (description, template) = match name {
        "rule_from_input_base" => (
            "Generate rules from base rules and input samples.",
            r#"You are generating a rulemorph YAML file.
The base rules define the output shape. Keep existing expr/value/default/required unless mapping is unresolved.
Use the input sample to map sources. Unmapped targets must use value: null and required: false.
Return YAML only.

Base rules:
{{rules_text}}

Input sample:
{{input_sample}}

Optional format: {{format}}
Optional records_path: {{records_path}}
"#,
        ),
        "rule_from_dto" => (
            "Generate rules from DTO schema and input samples.",
            r#"You are generating a rulemorph YAML file whose output matches the DTO schema.
Use the input sample to map sources. Unmapped targets must use value: null and required: false.
Return YAML only.

DTO:
{{dto_text}}

DTO language: {{dto_language}}

Input sample:
{{input_sample}}

Optional format: {{format}}
Optional records_path: {{records_path}}
"#,
        ),
        "explain_errors" => (
            "Explain validation/transform errors and suggest fixes.",
            r#"Explain the following validation/transform errors and suggest fixes.

Errors:
{{errors_json}}

Rules (optional):
{{rules_text}}
"#,
        ),
        _ => return Err("unknown prompt name".to_string()),
    };

    let content = apply_prompt_args(template, args);
    Ok(json!({
        "description": description,
        "messages": [
            {
                "role": "user",
                "content": content
            }
        ]
    }))
}

fn apply_prompt_args(template: &str, args: Option<&Map<String, Value>>) -> String {
    let mut content = template.to_string();
    if let Some(args) = args {
        for (key, value) in args {
            let replacement = match value {
                Value::String(value) => value.clone(),
                _ => value.to_string(),
            };
            content = content.replace(&format!("{{{{{}}}}}", key), &replacement);
        }
    }
    content
}
