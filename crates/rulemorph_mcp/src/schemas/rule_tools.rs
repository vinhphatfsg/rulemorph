use serde_json::{Value, json};

pub(crate) fn transform_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "rules_path": {
                "type": "string",
                "description": "Path to the YAML or JSON rules file. Mutually exclusive with rules_text.",
                "examples": ["rules.yaml"]
            },
            "rules_text": {
                "type": "string",
                "description": "Inline YAML or JSON rules content. Mutually exclusive with rules_path.",
                "examples": ["version: 1\ninput:\n  format: json\n  json: {}\nmappings:\n  - target: \"id\"\n    source: \"id\""]
            },
            "rules_format": rules_format_schema(),
            "input_path": {
                "type": "string",
                "description": "Path to the input file. Mutually exclusive with input_text and input_json.",
                "examples": ["input.json"]
            },
            "input_text": {
                "type": "string",
                "description": "Inline text input. Mutually exclusive with input_path and input_json.",
                "examples": ["{\"items\":[{\"id\":1}]}"]
            },
            "input_json": {
                "type": ["object", "array"],
                "description": "Inline typed JSON value. Mutually exclusive with input_path and input_text. Duplicate-key validation applies to raw JSON input_text/input_path, not to input_json after JSON-RPC decoding.",
                "examples": [[{"id": 1}]]
            },
            "context_path": {
                "type": "string",
                "description": "Optional path to a JSON context file. Mutually exclusive with context_json.",
                "examples": ["context.json"]
            },
            "context_json": {
                "type": "object",
                "description": "Optional inline JSON context value. Mutually exclusive with context_path.",
                "examples": [{"tenant_id": "t-001"}]
            },
            "format": {
                "type": "string",
                "enum": ["csv", "json", "yaml", "toml", "xml", "html", "excel", "markdown"],
                "description": "Override input format from the rule file.",
                "examples": ["json", "markdown"]
            },
            "ndjson": {
                "type": "boolean",
                "description": "Emit NDJSON output (one JSON object per line).",
                "examples": [false]
            },
            "validate": {
                "type": "boolean",
                "description": "Validate the rule file before transforming.",
                "examples": [true]
            },
            "output_path": {
                "type": "string",
                "description": "Optional path to write the output.",
                "examples": ["out.json"]
            },
            "max_output_bytes": {
                "type": "integer",
                "minimum": 1,
                "description": "Maximum output size in bytes before truncation.",
                "examples": [1000000]
            },
            "preview_rows": {
                "type": "integer",
                "minimum": 1,
                "description": "Maximum rows to return when ndjson=true.",
                "examples": [100]
            },
            "return_output_json": {
                "type": "boolean",
                "description": "Include parsed output JSON in meta.output when ndjson=false and within size limits.",
                "examples": [false]
            }
        }
    })
}

pub(super) fn rules_format_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["yaml", "json"],
        "description": "Rule parser format. Defaults to file extension for rules_path and yaml for rules_text.",
        "examples": ["json"]
    })
}

pub(crate) fn validate_rules_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "rules_path": {
                "type": "string",
                "description": "Path to the YAML or JSON rules file. Mutually exclusive with rules_text.",
                "examples": ["rules.yaml"]
            },
            "rules_text": {
                "type": "string",
                "description": "Inline YAML or JSON rules content. Mutually exclusive with rules_path.",
                "examples": ["version: 1\ninput:\n  format: json\n  json: {}\nmappings:\n  - target: \"id\"\n    source: \"id\""]
            },
            "rules_format": rules_format_schema()
        }
    })
}

pub(crate) fn generate_dto_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "rules_path": {
                "type": "string",
                "description": "Path to the YAML or JSON rules file. Mutually exclusive with rules_text.",
                "examples": ["rules.yaml"]
            },
            "rules_text": {
                "type": "string",
                "description": "Inline YAML or JSON rules content. Mutually exclusive with rules_path.",
                "examples": ["version: 1\ninput:\n  format: json\n  json: {}\nmappings:\n  - target: \"id\"\n    source: \"id\""]
            },
            "rules_format": rules_format_schema(),
            "language": {
                "type": "string",
                "enum": ["rust", "typescript", "python", "go", "java", "kotlin", "swift"],
                "description": "DTO output language.",
                "examples": ["typescript"]
            },
            "name": {
                "type": "string",
                "description": "Optional DTO root type name.",
                "examples": ["Record"]
            }
        },
        "required": ["language"]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transform_schema_accepts_markdown_format_override() {
        let schema = transform_input_schema();
        let formats = schema
            .pointer("/properties/format/enum")
            .and_then(Value::as_array)
            .expect("format enum");

        assert!(formats.iter().any(|value| value == "markdown"));
    }
}
