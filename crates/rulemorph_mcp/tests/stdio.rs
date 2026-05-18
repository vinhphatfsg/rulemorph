use std::fs;

use serde_json::{Value, json};
use tempfile::tempdir;

mod common;

use common::stdio::{
    McpServer, assert_tool_schema_enum, assert_tool_schema_required, call_tool, call_tool_rule,
    content_json, content_text, core_fixtures_dir, initialize, list_tools, mapping_by_target,
    tool_by_name, tool_call_request, tool_schema_property, tools_array,
};

#[test]
fn initialize_and_list_tools() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let response = list_tools(&mut server, 2);

    let tools = tools_array(&response);
    let expected = [
        "transform",
        "validate_rules",
        "generate_dto",
        "list_ops",
        "analyze_input",
        "generate_rules_from_base",
        "generate_rules_from_dto",
    ];
    let names = tools
        .iter()
        .map(|tool| tool["name"].as_str().expect("tool name"))
        .collect::<Vec<_>>();
    assert_eq!(names, expected);
    for name in expected {
        assert!(tools.iter().any(|tool| tool["name"] == name));
    }
    for name in [
        "transform",
        "validate_rules",
        "generate_dto",
        "generate_rules_from_base",
    ] {
        let tool = tool_by_name(tools, name);
        assert_eq!(
            tool_schema_property(tool, "rules_format")["enum"],
            json!(["yaml", "json"]),
            "rules_format schema missing for {name}"
        );
    }
    let transform_tool = tool_by_name(tools, "transform");
    assert_tool_schema_enum(
        transform_tool,
        "format",
        json!(["csv", "json", "yaml", "toml", "xml", "html", "excel"]),
    );
    let input_json_description = tool_schema_property(transform_tool, "input_json")["description"]
        .as_str()
        .expect("input_json description");
    assert!(input_json_description.contains("Inline typed JSON value"));
    assert!(input_json_description.contains("Duplicate-key validation"));

    let generate_dto_tool = tool_by_name(tools, "generate_dto");
    assert_tool_schema_required(generate_dto_tool, json!(["language"]));
    assert_tool_schema_enum(
        generate_dto_tool,
        "language",
        json!([
            "rust",
            "typescript",
            "python",
            "go",
            "java",
            "kotlin",
            "swift"
        ]),
    );

    let list_ops_tool = tool_by_name(tools, "list_ops");
    assert_eq!(list_ops_tool["inputSchema"]["properties"], json!({}));

    let analyze_input_tool = tool_by_name(tools, "analyze_input");
    assert_tool_schema_enum(analyze_input_tool, "format", json!(["csv", "json"]));
    assert_eq!(
        tool_schema_property(analyze_input_tool, "max_paths")["minimum"],
        json!(1)
    );

    let generate_rules_from_base_tool = tool_by_name(tools, "generate_rules_from_base");
    assert_eq!(
        tool_schema_property(generate_rules_from_base_tool, "max_candidates")["minimum"],
        json!(1)
    );
    assert!(
        generate_rules_from_base_tool["inputSchema"]["properties"]
            .as_object()
            .expect("properties")
            .contains_key("records_path")
    );

    let generate_rules_from_dto_tool = tool_by_name(tools, "generate_rules_from_dto");
    assert_tool_schema_required(
        generate_rules_from_dto_tool,
        json!(["dto_text", "dto_language"]),
    );
    assert_tool_schema_enum(
        generate_rules_from_dto_tool,
        "dto_language",
        json!([
            "rust",
            "typescript",
            "python",
            "go",
            "java",
            "kotlin",
            "swift"
        ]),
    );

    server.shutdown();
}

include!("stdio/transform.rs");

#[test]
fn tools_call_unknown_tool_returns_tool_error_payload() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 6,
        "method": "tools/call",
        "params": {
            "name": "unknown_tool",
            "arguments": {}
        }
    });

    let response = server.send(&request);
    assert_eq!(response["result"]["isError"], true);
    assert_eq!(
        response["result"]["content"][0],
        json!({
            "type": "text",
            "text": "unknown tool: unknown_tool"
        })
    );
    assert!(response["result"]["meta"].is_null());

    server.shutdown();
}

#[test]
fn validate_rules_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "id"
"#;

    let request = tool_call_request(
        8,
        "validate_rules",
        json!({
            "rules_text": rules_text
        }),
    );

    let response = server.send(&request);
    assert_eq!(content_text(&response), "ok");

    server.shutdown();
}

#[test]
fn validate_rules_failure() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: csv
mappings: []
"#;

    let request = tool_call_request(
        9,
        "validate_rules",
        json!({
            "rules_text": rules_text
        }),
    );

    let response = server.send(&request);
    assert_eq!(response["result"]["isError"], true);
    assert!(response["result"]["meta"]["errors"].is_array());

    server.shutdown();
}

include!("stdio/dto.rs");

#[test]
fn list_ops_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = tool_call_request(11, "list_ops", json!({}));

    let response = server.send(&request);
    assert!(response["result"]["meta"]["ops"]["type_casts"].is_array());
    assert!(response["result"]["meta"]["ops"]["categories"]["json_ops"].is_array());
    assert!(response["result"]["meta"]["ops"]["categories"]["array_ops"].is_array());
    assert!(response["result"]["meta"]["ops"]["category_docs"]["json_ops"]["examples"].is_array());
    assert!(
        response["result"]["meta"]["ops"]["category_docs"]["string_ops"]["examples"].is_array()
    );
    assert_eq!(
        response["result"]["meta"]["ops"]["logical_ops"],
        json!(["and", "or", "not"])
    );
    assert_eq!(
        response["result"]["meta"]["ops"]["comparison_ops"],
        json!(["==", "!=", "<", "<=", ">", ">=", "~="])
    );
    assert_eq!(
        response["result"]["meta"]["ops"]["type_casts"],
        json!(["string", "int", "float", "bool"])
    );
    assert_eq!(
        response["result"]["meta"]["ops"]["categories"]["numeric_ops"],
        json!([
            "+", "-", "*", "/", "round", "to_base", "sum", "avg", "min", "max"
        ])
    );
    assert_eq!(
        response["result"]["meta"]["ops"]["categories"]["date_ops"],
        json!(["date_format", "to_unixtime"])
    );
    assert_eq!(
        response["result"]["meta"]["ops"]["expr_ops"],
        json!([
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
        ])
    );
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("ops text");
    let text_ops: Value = serde_json::from_str(text).expect("ops text json");
    assert_eq!(text_ops, response["result"]["meta"]["ops"]);

    server.shutdown();
}

#[test]
fn analyze_input_json_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 12,
        "method": "tools/call",
        "params": {
            "name": "analyze_input",
            "arguments": {
                "input_json": {
                    "id": 1,
                    "name": "Ada"
                }
            }
        }
    });

    let response = server.send(&request);
    let paths = response["result"]["meta"]["paths"]
        .as_array()
        .expect("paths array");
    assert!(paths.iter().any(|item| item["path"] == "id"));
    assert!(paths.iter().any(|item| item["path"] == "name"));

    server.shutdown();
}

#[test]
fn analyze_input_max_paths_limits_reported_paths() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 121,
        "method": "tools/call",
        "params": {
            "name": "analyze_input",
            "arguments": {
                "input_json": {
                    "id": 1,
                    "name": "Ada",
                    "active": true
                },
                "max_paths": 2
            }
        }
    });

    let response = server.send(&request);
    let paths = response["result"]["meta"]["paths"]
        .as_array()
        .expect("paths array");
    assert_eq!(paths.len(), 2);
    assert_eq!(response["result"]["meta"]["summary"]["paths"], json!(2));

    server.shutdown();
}

#[test]
fn analyze_input_records_path_supports_quoted_segments() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 121,
        "method": "tools/call",
        "params": {
            "name": "analyze_input",
            "arguments": {
                "input_json": {
                    "payload": {
                        "items.with.dot": [
                            {
                                "user.name": "Ada",
                                "age": 37
                            }
                        ]
                    }
                },
                "records_path": "payload[\"items.with.dot\"]"
            }
        }
    });

    let response = server.send(&request);
    let paths = response["result"]["meta"]["paths"]
        .as_array()
        .expect("paths array");
    assert!(paths.iter().any(|item| item["path"] == "[\"user.name\"]"));
    assert!(paths.iter().any(|item| item["path"] == "age"));
    assert_eq!(response["result"]["meta"]["summary"]["records"], json!(1));

    server.shutdown();
}

#[test]
fn analyze_input_invalid_records_path_returns_json_rpc_error() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 122,
        "method": "tools/call",
        "params": {
            "name": "analyze_input",
            "arguments": {
                "input_json": {
                    "payload": [
                        { "id": 1 }
                    ]
                },
                "records_path": "payload."
            }
        }
    });

    let response = server.send(&request);
    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(
        response["error"]["message"],
        "records_path is invalid: path syntax is invalid"
    );

    server.shutdown();
}

#[test]
fn raw_json_input_text_rejects_duplicate_keys_for_analysis_and_generation() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let duplicate_input = r#"{"items":[{"id":1,"id":2}]}"#;
    let base_rules_text = r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "old_id"
"#;
    let dto_text = r#"export interface Record {
  id: string;
}"#;

    let cases = [
        json!({
            "jsonrpc": "2.0",
            "id": 1201,
            "method": "tools/call",
            "params": {
                "name": "analyze_input",
                "arguments": {
                    "input_text": duplicate_input,
                    "format": "json"
                }
            }
        }),
        json!({
            "jsonrpc": "2.0",
            "id": 1202,
            "method": "tools/call",
            "params": {
                "name": "generate_rules_from_base",
                "arguments": {
                    "rules_text": base_rules_text,
                    "input_text": duplicate_input,
                    "format": "json"
                }
            }
        }),
        json!({
            "jsonrpc": "2.0",
            "id": 1203,
            "method": "tools/call",
            "params": {
                "name": "generate_rules_from_dto",
                "arguments": {
                    "dto_text": dto_text,
                    "dto_language": "typescript",
                    "input_text": duplicate_input,
                    "format": "json"
                }
            }
        }),
    ];

    for request in cases {
        let response = server.send(&request);
        assert_eq!(response["result"]["isError"], true);
        let message = response["result"]["content"][0]["text"]
            .as_str()
            .expect("error text");
        assert!(message.contains("duplicate key"), "{message}");
    }

    server.shutdown();
}

#[test]
fn analyze_input_csv_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 13,
        "method": "tools/call",
        "params": {
            "name": "analyze_input",
            "arguments": {
                "input_text": "id,active,name,score,empty\n1,true,Ada,3.5,\n2,false,Bob,4,\n",
                "format": "csv"
            }
        }
    });

    let response = server.send(&request);
    let paths = response["result"]["meta"]["paths"]
        .as_array()
        .expect("paths array");
    assert!(paths.iter().any(|item| item["path"] == "id"));
    let id_path = paths
        .iter()
        .find(|item| item["path"] == "id")
        .expect("id path");
    assert_eq!(id_path["types"]["number"], json!(2));
    let active_path = paths
        .iter()
        .find(|item| item["path"] == "active")
        .expect("active path");
    assert_eq!(active_path["types"]["bool"], json!(2));
    let empty_path = paths
        .iter()
        .find(|item| item["path"] == "empty")
        .expect("empty path");
    assert_eq!(empty_path["types"]["null"], json!(2));

    server.shutdown();
}

#[test]
fn analyze_input_csv_rejects_mismatched_field_count() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 131,
        "method": "tools/call",
        "params": {
            "name": "analyze_input",
            "arguments": {
                "input_text": "id,name\n1,Ada,extra\n",
                "format": "csv"
            }
        }
    });

    let response = server.send(&request);
    assert_eq!(response["result"]["isError"], true);
    let message = response["result"]["content"][0]["text"]
        .as_str()
        .expect("error text");
    assert!(message.contains("failed to parse input CSV"), "{message}");

    server.shutdown();
}

#[test]
fn analyze_input_csv_allows_more_than_default_normalization_record_limit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_path = dir.path().join("large.csv");
    let mut input_text = String::from("id\n");
    for index in 0..100_001 {
        input_text.push_str(&index.to_string());
        input_text.push('\n');
    }
    std::fs::write(&input_path, input_text).expect("write csv");

    let mut server = McpServer::start_with_allowed_root(dir.path());
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 132,
        "method": "tools/call",
        "params": {
            "name": "analyze_input",
            "arguments": {
                "input_path": input_path.to_string_lossy(),
                "format": "csv",
                "max_paths": 1
            }
        }
    });

    let response = server.send(&request);
    assert_ne!(response["result"]["isError"], json!(true));
    assert_eq!(
        response["result"]["meta"]["summary"]["records"],
        json!(100_001)
    );

    server.shutdown();
}

#[test]
fn generate_rules_from_base_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "old_id"
  - target: "name"
    source: "old_name"
"#;

    let rule = call_tool_rule(
        &mut server,
        14,
        "generate_rules_from_base",
        json!({
            "rules_text": rules_text,
            "input_json": {
                "id": 1,
                "name": "Ada"
            }
        }),
    );
    assert_eq!(rule.mappings[0].source.as_deref(), Some("id"));
    assert_eq!(rule.mappings[1].source.as_deref(), Some("name"));

    server.shutdown();
}

#[test]
fn generate_rules_from_base_csv_uses_scalar_type_boost() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: csv
  csv: {}
mappings:
  - target: "price"
    source: "old_price"
    type: float
"#;

    let rule = call_tool_rule(
        &mut server,
        141,
        "generate_rules_from_base",
        json!({
            "rules_text": rules_text,
            "input_text": "price_text,price_value\nabc,12.5\n",
            "format": "csv"
        }),
    );
    assert_eq!(rule.mappings[0].source.as_deref(), Some("price_value"));

    server.shutdown();
}

#[test]
fn resources_list_and_read() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let list_request = json!({
        "jsonrpc": "2.0",
        "id": 17,
        "method": "resources/list"
    });
    let list_response = server.send(&list_request);
    let resources = list_response["result"]["resources"]
        .as_array()
        .expect("resources array");
    assert!(
        resources
            .iter()
            .any(|item| item["uri"] == "rulemorph://docs/rules_spec_en")
    );

    let read_request = json!({
        "jsonrpc": "2.0",
        "id": 18,
        "method": "resources/read",
        "params": {
            "uri": "rulemorph://docs/rules_spec_en"
        }
    });
    let read_response = server.send(&read_request);
    let text = read_response["result"]["contents"][0]["text"]
        .as_str()
        .expect("resource text");
    assert!(text.contains("Expr"));

    server.shutdown();
}

#[test]
fn prompts_list_and_get() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let list_request = json!({
        "jsonrpc": "2.0",
        "id": 18,
        "method": "prompts/list"
    });
    let list_response = server.send(&list_request);
    let prompts = list_response["result"]["prompts"]
        .as_array()
        .expect("prompts array");
    assert!(
        prompts
            .iter()
            .any(|item| item["name"] == "rule_from_input_base")
    );

    let get_request = json!({
        "jsonrpc": "2.0",
        "id": 19,
        "method": "prompts/get",
        "params": {
            "name": "explain_errors",
            "arguments": {
                "errors_json": "[{\"message\":\"oops\"}]"
            }
        }
    });
    let get_response = server.send(&get_request);
    let content = get_response["result"]["messages"][0]["content"]
        .as_str()
        .expect("prompt content");
    assert!(content.contains("Errors:"));

    server.shutdown();
}
