use serde_json::{Value, json};

mod rule_tools;

use self::rule_tools::rules_format_schema;
pub(crate) use self::rule_tools::{
    generate_dto_input_schema, transform_input_schema, validate_rules_input_schema,
};

pub(crate) fn list_ops_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {}
    })
}

pub(crate) fn analyze_input_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "input_path": {
                "type": "string",
                "description": "Path to the input CSV/JSON file. Mutually exclusive with input_text and input_json.",
                "examples": ["input.json"]
            },
            "input_text": {
                "type": "string",
                "description": "Inline input text (CSV or JSON). Mutually exclusive with input_path and input_json.",
                "examples": ["{\"items\":[{\"id\":1}]}"]
            },
            "input_json": {
                "type": ["object", "array"],
                "description": "Inline typed JSON value. Mutually exclusive with input_path and input_text. Duplicate-key validation applies to raw JSON input_text/input_path, not to input_json after JSON-RPC decoding.",
                "examples": [[{"id": 1}]]
            },
            "format": {
                "type": "string",
                "enum": ["csv", "json"],
                "description": "Input format when input_text/input_path is used.",
                "examples": ["json"]
            },
            "records_path": {
                "type": "string",
                "description": "Optional records path for JSON inputs.",
                "examples": ["items"]
            },
            "max_paths": {
                "type": "integer",
                "minimum": 1,
                "description": "Maximum number of unique paths to include in the response.",
                "examples": [200]
            }
        }
    })
}

pub(crate) fn generate_rules_from_base_input_schema() -> Value {
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
                "description": "Path to the input CSV/JSON file. Mutually exclusive with input_text and input_json.",
                "examples": ["input.json"]
            },
            "input_text": {
                "type": "string",
                "description": "Inline input text (CSV or JSON). Mutually exclusive with input_path and input_json.",
                "examples": ["{\"items\":[{\"id\":1}]}"]
            },
            "input_json": {
                "type": ["object", "array"],
                "description": "Inline typed JSON value. Mutually exclusive with input_path and input_text. Duplicate-key validation applies to raw JSON input_text/input_path, not to input_json after JSON-RPC decoding.",
                "examples": [[{"id": 1}]]
            },
            "format": {
                "type": "string",
                "enum": ["csv", "json"],
                "description": "Override input format.",
                "examples": ["json"]
            },
            "records_path": {
                "type": "string",
                "description": "Optional records path for JSON inputs.",
                "examples": ["items"]
            },
            "max_candidates": {
                "type": "integer",
                "minimum": 1,
                "description": "Maximum number of candidates to return per target.",
                "examples": [3]
            }
        }
    })
}

pub(crate) fn generate_rules_from_dto_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "dto_text": {
                "type": "string",
                "description": "DTO source text.",
                "examples": ["export interface Record { id: string; }"]
            },
            "dto_language": {
                "type": "string",
                "enum": ["rust", "typescript", "python", "go", "java", "kotlin", "swift"],
                "description": "DTO language.",
                "examples": ["typescript"]
            },
            "input_path": {
                "type": "string",
                "description": "Path to the input CSV/JSON file. Mutually exclusive with input_text and input_json.",
                "examples": ["input.json"]
            },
            "input_text": {
                "type": "string",
                "description": "Inline input text (CSV or JSON). Mutually exclusive with input_path and input_json.",
                "examples": ["{\"items\":[{\"id\":1}]}"]
            },
            "input_json": {
                "type": ["object", "array"],
                "description": "Inline typed JSON value. Mutually exclusive with input_path and input_text. Duplicate-key validation applies to raw JSON input_text/input_path, not to input_json after JSON-RPC decoding.",
                "examples": [[{"id": 1}]]
            },
            "format": {
                "type": "string",
                "enum": ["csv", "json"],
                "description": "Override input format.",
                "examples": ["json"]
            },
            "records_path": {
                "type": "string",
                "description": "Optional records path for JSON inputs.",
                "examples": ["items"]
            },
            "max_candidates": {
                "type": "integer",
                "minimum": 1,
                "description": "Maximum number of candidates to return per target.",
                "examples": [3]
            }
        },
        "required": ["dto_text", "dto_language"]
    })
}
