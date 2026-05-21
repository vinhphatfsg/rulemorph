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
