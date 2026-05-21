#[test]
fn generate_rules_from_dto_swift_multiline_class_and_optional_object() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = r#"
class Record: Codable {
    let id: String
    let name: Optional<String>
    let profile: Profile
    let active: Bool

    enum CodingKeys: String, CodingKey {
        case id = "user_id"
        case name
        case profile
        case active
    }
}

struct Profile: Codable {
    let city: String

    enum CodingKeys: String, CodingKey {
        case city = "city_name"
    }
}
"#;

    let rule = call_tool_rule(
        &mut server,
        2102,
        "generate_rules_from_dto",
        json!({
            "dto_text": dto_text,
            "dto_language": "swift",
            "input_json": {
                "user_id": "001",
                "name": "Ada",
                "profile": {
                    "city_name": "Tokyo"
                },
                "active": true
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

    let profile_city_mapping = mapping_by_target(&rule, "profile.city_name");
    assert_eq!(
        profile_city_mapping.source.as_deref(),
        Some("profile.city_name")
    );
    assert_eq!(profile_city_mapping.value_type.as_deref(), Some("string"));
    assert!(profile_city_mapping.required);

    let active_mapping = mapping_by_target(&rule, "active");
    assert_eq!(active_mapping.source.as_deref(), Some("active"));
    assert_eq!(active_mapping.value_type.as_deref(), Some("bool"));
    assert!(active_mapping.required);

    server.shutdown();
}
