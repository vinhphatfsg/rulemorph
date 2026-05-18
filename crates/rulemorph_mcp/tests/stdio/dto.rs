include!("dto/multiline_languages.rs");

#[test]
fn generate_dto_typescript() {
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
        10,
        "generate_dto",
        json!({
            "rules_text": rules_text,
            "language": "typescript"
        }),
    );

    let response = server.send(&request);
    assert!(content_text(&response).contains("export interface"));

    server.shutdown();
}

#[test]
fn generate_dto_invalid_language_returns_json_rpc_error() {
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
        101,
        "generate_dto",
        json!({
            "rules_text": rules_text,
            "language": "ruby"
        }),
    );

    let response = server.send(&request);
    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(
        response["error"]["message"],
        "language must be one of rust, typescript, python, go, java, kotlin, swift"
    );

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = r#"export interface Record {
  id: string;
  name?: string;
}"#;

    let rule = call_tool_rule(
        &mut server,
        15,
        "generate_rules_from_dto",
        json!({
            "dto_text": dto_text,
            "dto_language": "typescript",
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
fn generate_rules_from_dto_nested_optional_object_keeps_required_semantics() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = r#"export interface Record {
  id: string;
  profile?: Profile;
}

export interface Profile {
  name: string;
  score?: number;
}"#;

    let rule = call_tool_rule(
        &mut server,
        1501,
        "generate_rules_from_dto",
        json!({
            "dto_text": dto_text,
            "dto_language": "typescript",
            "input_json": {
                "id": "001",
                "profile": {
                    "name": "Ada",
                    "score": 98.5
                }
            }
        }),
    );

    let id_mapping = mapping_by_target(&rule, "id");
    assert_eq!(id_mapping.source.as_deref(), Some("id"));
    assert!(id_mapping.required);

    let profile_name_mapping = mapping_by_target(&rule, "profile.name");
    assert_eq!(profile_name_mapping.source.as_deref(), Some("profile.name"));
    assert!(!profile_name_mapping.required);

    let profile_score_mapping = mapping_by_target(&rule, "profile.score");
    assert_eq!(
        profile_score_mapping.source.as_deref(),
        Some("profile.score")
    );
    assert_eq!(profile_score_mapping.value_type.as_deref(), Some("float"));
    assert!(!profile_score_mapping.required);

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_typescript_json_comment_rename() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = r#"export interface Record {
  /* json: "user_id" */ id: string;
  profile?: Profile;
}

export interface Profile {
  /* json: "city_name" */ city: string;
}"#;

    let rule = call_tool_rule(
        &mut server,
        1502,
        "generate_rules_from_dto",
        json!({
            "dto_text": dto_text,
            "dto_language": "typescript",
            "input_json": {
                "user_id": "001",
                "profile": {
                    "city_name": "Tokyo"
                }
            }
        }),
    );

    let id_mapping = mapping_by_target(&rule, "user_id");
    assert_eq!(id_mapping.source.as_deref(), Some("user_id"));
    assert_eq!(id_mapping.value_type.as_deref(), Some("string"));
    assert!(id_mapping.required);

    let city_mapping = mapping_by_target(&rule, "profile.city_name");
    assert_eq!(city_mapping.source.as_deref(), Some("profile.city_name"));
    assert!(!city_mapping.required);

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_invalid_language_returns_json_rpc_error() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let response = call_tool(
        &mut server,
        1510,
        "generate_rules_from_dto",
        json!({
            "dto_text": "type Record = { id: string }",
            "dto_language": "ruby",
            "input_json": {
                "id": 1
            }
        }),
    );
    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(
        response["error"]["message"],
        "dto_language must be rust, typescript, python, go, java, kotlin, or swift"
    );

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_csv_uses_scalar_type_boost() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = r#"
interface Product {
  price: number;
}
"#;
    let rule = call_tool_rule(
        &mut server,
        151,
        "generate_rules_from_dto",
        json!({
            "dto_text": dto_text,
            "dto_language": "typescript",
            "input_text": "price_text,price_value\nabc,12.5\n",
            "format": "csv"
        }),
    );
    let price_mapping = mapping_by_target(&rule, "price");
    assert_eq!(price_mapping.source.as_deref(), Some("price_value"));

    server.shutdown();
}

include!("dto/single_line_languages.rs");
