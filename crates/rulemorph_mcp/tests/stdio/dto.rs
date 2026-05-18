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

#[test]
fn generate_rules_from_dto_single_line_interface() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "export interface Record { id: string; name?: string; }";

    let rule = call_tool_rule(
        &mut server,
        16,
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
fn generate_rules_from_dto_single_line_rust_struct() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "pub struct Record { pub id: String, pub name: Option<String>, pub price: f64 }";

    let rule = call_tool_rule(
        &mut server,
        19,
        "generate_rules_from_dto",
        json!({
            "dto_text": dto_text,
            "dto_language": "rust",
            "input_json": {
                "id": "001",
                "name": "Ada",
                "price": 100.0
            }
        }),
    );
    assert_eq!(rule.mappings[0].source.as_deref(), Some("id"));
    assert_eq!(rule.mappings[1].source.as_deref(), Some("name"));
    assert_eq!(rule.mappings[2].source.as_deref(), Some("price"));

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_python_single_line_alias() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "class Record(BaseModel): id: str; name: Optional[str] = None; price: float = Field(alias=\"price_cents\")";

    let rule = call_tool_rule(
        &mut server,
        20,
        "generate_rules_from_dto",
        json!({
            "dto_text": dto_text,
            "dto_language": "python",
            "input_json": {
                "id": "001",
                "name": "Ada",
                "price_cents": 100.0
            }
        }),
    );

    let id_mapping = mapping_by_target(&rule, "id");
    assert_eq!(id_mapping.source.as_deref(), Some("id"));
    assert!(id_mapping.required);

    let name_mapping = mapping_by_target(&rule, "name");
    assert_eq!(name_mapping.source.as_deref(), Some("name"));
    assert!(!name_mapping.required);

    let price_mapping = mapping_by_target(&rule, "price_cents");
    assert_eq!(price_mapping.source.as_deref(), Some("price_cents"));
    assert!(price_mapping.required);

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_go_single_line_tags() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "type Record struct { ID string `json:\"id\"` Name *string `json:\"name,omitempty\"` Price float64 `json:\"price\"` }";

    let rule = call_tool_rule(
        &mut server,
        21,
        "generate_rules_from_dto",
        json!({
            "dto_text": dto_text,
            "dto_language": "go",
            "input_json": {
                "id": "001",
                "name": "Ada",
                "price": 100.0
            }
        }),
    );

    let id_mapping = mapping_by_target(&rule, "id");
    assert_eq!(id_mapping.source.as_deref(), Some("id"));
    assert!(id_mapping.required);

    let name_mapping = mapping_by_target(&rule, "name");
    assert_eq!(name_mapping.source.as_deref(), Some("name"));
    assert!(!name_mapping.required);

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_java_single_line_annotations() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "public class Record { @JsonProperty(\"user_id\") private String id; @SerializedName(\"full_name\") private Optional<String> name; }";

    let rule = call_tool_rule(
        &mut server,
        22,
        "generate_rules_from_dto",
        json!({
            "dto_text": dto_text,
            "dto_language": "java",
            "input_json": {
                "user_id": "001",
                "full_name": "Ada"
            }
        }),
    );

    let id_mapping = mapping_by_target(&rule, "user_id");
    assert_eq!(id_mapping.source.as_deref(), Some("user_id"));
    assert!(id_mapping.required);

    let name_mapping = mapping_by_target(&rule, "full_name");
    assert_eq!(name_mapping.source.as_deref(), Some("full_name"));
    assert!(!name_mapping.required);

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_kotlin_single_line_annotations() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "data class Record(@SerialName(\"user_id\") val id: String, @Json(name = \"full_name\") val name: String?, val price: Double)";

    let rule = call_tool_rule(
        &mut server,
        23,
        "generate_rules_from_dto",
        json!({
            "dto_text": dto_text,
            "dto_language": "kotlin",
            "input_json": {
                "user_id": "001",
                "full_name": "Ada",
                "price": 100.0
            }
        }),
    );

    let id_mapping = mapping_by_target(&rule, "user_id");
    assert_eq!(id_mapping.source.as_deref(), Some("user_id"));
    assert!(id_mapping.required);

    let name_mapping = mapping_by_target(&rule, "full_name");
    assert_eq!(name_mapping.source.as_deref(), Some("full_name"));
    assert!(!name_mapping.required);

    let price_mapping = mapping_by_target(&rule, "price");
    assert_eq!(price_mapping.source.as_deref(), Some("price"));
    assert!(price_mapping.required);

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_swift_single_line_coding_keys() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "struct Record: Codable { let id: String; let name: String?; let price: Double; enum CodingKeys: String, CodingKey { case id = \"user_id\", name, price = \"price_cents\" } }";

    let rule = call_tool_rule(
        &mut server,
        24,
        "generate_rules_from_dto",
        json!({
            "dto_text": dto_text,
            "dto_language": "swift",
            "input_json": {
                "user_id": "001",
                "name": "Ada",
                "price_cents": 100.0
            }
        }),
    );

    let id_mapping = mapping_by_target(&rule, "user_id");
    assert_eq!(id_mapping.source.as_deref(), Some("user_id"));
    assert!(id_mapping.required);

    let name_mapping = mapping_by_target(&rule, "name");
    assert_eq!(name_mapping.source.as_deref(), Some("name"));
    assert!(!name_mapping.required);

    let price_mapping = mapping_by_target(&rule, "price_cents");
    assert_eq!(price_mapping.source.as_deref(), Some("price_cents"));
    assert!(price_mapping.required);

    server.shutdown();
}
