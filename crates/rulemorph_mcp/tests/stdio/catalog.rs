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
