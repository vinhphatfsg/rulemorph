#[test]
fn generate_rules_from_dto_go_multiline_struct_shape() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = r#"
type Record struct {
    ID string
    Name *string `json:"name,omitempty"`
    Internal string `json:"-"`
    Profile Profile `json:"profile"`
    Tags []string `json:"tags"`
    Metadata map[string]string `json:"metadata"`
    Active bool `json:"active"`
    Count int64 `json:"count"`
    Ratio float32 `json:"ratio"`
}

type Profile struct {
    City string `json:"city"`
}
"#;

    let rule = call_tool_rule(
        &mut server,
        2101,
        "generate_rules_from_dto",
        json!({
            "dto_text": dto_text,
            "dto_language": "go",
            "input_json": {
                "ID": "001",
                "name": "Ada",
                "profile": {
                    "city": "Tokyo"
                },
                "tags": ["admin"],
                "metadata": {
                    "team": "core"
                },
                "active": true,
                "count": 7,
                "ratio": 1.5
            }
        }),
    );

    assert!(
        !rule
            .mappings
            .iter()
            .any(|mapping| mapping.target == "Internal")
    );

    let id_mapping = mapping_by_target(&rule, "ID");
    assert_eq!(id_mapping.source.as_deref(), Some("ID"));
    assert_eq!(id_mapping.value_type.as_deref(), Some("string"));
    assert!(id_mapping.required);

    let name_mapping = mapping_by_target(&rule, "name");
    assert_eq!(name_mapping.source.as_deref(), Some("name"));
    assert!(!name_mapping.required);

    let profile_city_mapping = mapping_by_target(&rule, "profile.city");
    assert_eq!(profile_city_mapping.source.as_deref(), Some("profile.city"));
    assert_eq!(profile_city_mapping.value_type.as_deref(), Some("string"));
    assert!(profile_city_mapping.required);

    let tags_mapping = mapping_by_target(&rule, "tags");
    assert_eq!(tags_mapping.source.as_deref(), Some("tags"));
    assert_eq!(tags_mapping.value_type, None);

    let metadata_mapping = mapping_by_target(&rule, "metadata");
    assert_eq!(metadata_mapping.source, None);
    assert_eq!(metadata_mapping.value_type, None);

    let active_mapping = mapping_by_target(&rule, "active");
    assert_eq!(active_mapping.value_type.as_deref(), Some("bool"));

    let count_mapping = mapping_by_target(&rule, "count");
    assert_eq!(count_mapping.value_type.as_deref(), Some("int"));

    let ratio_mapping = mapping_by_target(&rule, "ratio");
    assert_eq!(ratio_mapping.value_type.as_deref(), Some("float"));

    server.shutdown();
}
