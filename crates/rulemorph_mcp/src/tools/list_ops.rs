use serde_json::{Value, json};

pub(crate) fn run_list_ops_tool() -> Value {
    let ops = json!({
        "expr_ops": [
            "concat",
            "coalesce",
            "to_string",
            "trim",
            "lowercase",
            "uppercase",
            "replace",
            "split",
            "pad_start",
            "pad_end",
            "lookup",
            "lookup_first",
            "merge",
            "deep_merge",
            "get",
            "pick",
            "omit",
            "keys",
            "values",
            "entries",
            "len",
            "from_entries",
            "object_flatten",
            "object_unflatten",
            "map",
            "filter",
            "flat_map",
            "flatten",
            "take",
            "drop",
            "slice",
            "chunk",
            "zip",
            "zip_with",
            "unzip",
            "group_by",
            "key_by",
            "partition",
            "unique",
            "distinct_by",
            "sort_by",
            "find",
            "find_index",
            "index_of",
            "contains",
            "sum",
            "avg",
            "min",
            "max",
            "reduce",
            "fold",
            "+",
            "-",
            "*",
            "/",
            "round",
            "to_base",
            "date_format",
            "to_unixtime"
        ],
        "categories": {
            "string_ops": [
                "concat",
                "to_string",
                "trim",
                "lowercase",
                "uppercase",
                "replace",
                "split",
                "pad_start",
                "pad_end"
            ],
            "json_ops": [
                "merge",
                "deep_merge",
                "get",
                "pick",
                "omit",
                "keys",
                "values",
                "entries",
                "len",
                "from_entries",
                "object_flatten",
                "object_unflatten"
            ],
            "array_ops": [
                "map",
                "filter",
                "flat_map",
                "flatten",
                "take",
                "drop",
                "slice",
                "chunk",
                "zip",
                "zip_with",
                "unzip",
                "group_by",
                "key_by",
                "partition",
                "unique",
                "distinct_by",
                "sort_by",
                "find",
                "find_index",
                "index_of",
                "contains",
                "sum",
                "avg",
                "min",
                "max",
                "reduce",
                "fold"
            ],
            "numeric_ops": ["+", "-", "*", "/", "round", "to_base", "sum", "avg", "min", "max"],
            "date_ops": ["date_format", "to_unixtime"]
        },
        "category_docs": {
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
        },
        "logical_ops": ["and", "or", "not"],
        "comparison_ops": ["==", "!=", "<", "<=", ">", ">=", "~="],
        "type_casts": ["string", "int", "float", "bool"]
    });

    let text = serde_json::to_string_pretty(&ops)
        .unwrap_or_else(|_| "{\"error\":\"failed to serialize ops\"}".to_string());

    json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "meta": {
            "ops": ops
        }
    })
}
