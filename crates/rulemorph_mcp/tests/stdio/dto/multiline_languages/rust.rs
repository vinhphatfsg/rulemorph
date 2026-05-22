#[test]
fn generate_rules_from_dto_rust_multiline_struct_shape() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = r#"
pub struct Record {
    #[serde(rename = "user_id")]
    pub id: String,
    pub name: Option<String>,
    pub active: bool,
    pub count: i64,
    pub ratio: f32,
    pub profile: Option<Profile>,
    pub metadata: serde_json::Value,
}

pub struct Profile {
    #[serde(rename = "city_name")]
    pub city: String,
}
"#;

    let rule = call_tool_rule(
        &mut server,
        1901,
        "generate_rules_from_dto",
        json!({
            "dto_text": dto_text,
            "dto_language": "rust",
            "input_json": {
                "user_id": "001",
                "name": "Ada",
                "active": true,
                "count": 7,
                "ratio": 1.5,
                "profile": {
                    "city_name": "Tokyo"
                },
                "metadata": {
                    "team": "core"
                }
            }
        }),
    );

    let id_mapping = mapping_by_target(&rule, "user_id");
    assert_eq!(id_mapping.source.as_deref(), Some("user_id"));
    assert_eq!(id_mapping.value_type.as_deref(), Some("string"));
    assert!(id_mapping.required);

    let name_mapping = mapping_by_target(&rule, "name");
    assert_eq!(name_mapping.source.as_deref(), Some("name"));
    assert!(!name_mapping.required);

    let active_mapping = mapping_by_target(&rule, "active");
    assert_eq!(active_mapping.value_type.as_deref(), Some("bool"));

    let count_mapping = mapping_by_target(&rule, "count");
    assert_eq!(count_mapping.value_type.as_deref(), Some("int"));

    let ratio_mapping = mapping_by_target(&rule, "ratio");
    assert_eq!(ratio_mapping.value_type.as_deref(), Some("float"));

    let profile_city_mapping = mapping_by_target(&rule, "profile.city_name");
    assert_eq!(
        profile_city_mapping.source.as_deref(),
        Some("profile.city_name")
    );
    assert_eq!(profile_city_mapping.value_type.as_deref(), Some("string"));
    assert!(!profile_city_mapping.required);

    let metadata_mapping = mapping_by_target(&rule, "metadata");
    assert_eq!(metadata_mapping.source, None);
    assert_eq!(metadata_mapping.value_type, None);

    server.shutdown();
}
