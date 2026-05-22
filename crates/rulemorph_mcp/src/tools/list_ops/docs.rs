use serde_json::{Value, json};

pub(super) fn category_docs_json() -> Value {
    json!({
        "string_ops": {
            "summary": "String transformations and formatting.",
            "examples": [
                {
                    "op": "replace",
                    "expr": { "op": "replace", "args": ["a-b", "-", "_", "all"] }
                },
                {
                    "op": "concat",
                    "expr": {
                        "op": "concat",
                        "args": [ { "ref": "input.first" }, " ", { "ref": "input.last" } ]
                    }
                }
            ]
        },
        "json_ops": {
            "summary": "Object merge and structural helpers.",
            "examples": [
                {
                    "op": "merge",
                    "expr": {
                        "op": "merge",
                        "args": [ { "ref": "input.base" }, { "ref": "context.override" } ]
                    }
                },
                {
                    "op": "get",
                    "expr": { "op": "get", "args": [ { "ref": "input.obj" }, "id" ] }
                },
                {
                    "op": "pick",
                    "expr": { "op": "pick", "args": [ { "ref": "input.obj" }, ["id"] ] }
                }
            ]
        },
        "array_ops": {
            "summary": "Array transforms and aggregations.",
            "examples": [
                {
                    "op": "map",
                    "expr": {
                        "op": "map",
                        "args": [ { "ref": "input.values" }, { "ref": "item.value" } ]
                    }
                },
                {
                    "op": "filter",
                    "expr": {
                        "op": "filter",
                        "args": [
                            { "ref": "input.values" },
                            { "op": ">", "args": [ { "ref": "item.value" }, 0 ] }
                        ]
                    }
                }
            ]
        },
        "numeric_ops": {
            "summary": "Numeric arithmetic and formatting.",
            "examples": [
                { "op": "+", "expr": { "op": "+", "args": [1, 2, 3] } },
                { "op": "round", "expr": { "op": "round", "args": [12.345, 2] } }
            ]
        },
        "date_ops": {
            "summary": "Date/time parsing and formatting.",
            "examples": [
                {
                    "op": "date_format",
                    "expr": { "op": "date_format", "args": ["2024-01-02", "%Y/%m/%d"] }
                }
            ]
        }
    })
}
