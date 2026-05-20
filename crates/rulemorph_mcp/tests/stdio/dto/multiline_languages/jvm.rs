#[test]
fn generate_rules_from_dto_jvm_multiline_model_shape() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let java_dto = r#"
public record Record(
    @JsonProperty("user_id")
    String id,
    Optional<String> name,
    @Nullable
    Profile profile,
    boolean active,
    long count,
    double ratio
) {}

public class Profile {
    @JsonProperty("city_name")
    public String city;
}
"#;
    let kotlin_dto = r#"
data class Record(
    @Json(name = "user_id")
    val id: String,
    val name: String?,
    val profile: Profile?,
    val active: Boolean,
    val count: Long,
    val ratio: Double
)

data class Profile(
    @SerialName("city_name")
    val city: String
)
"#;

    for (id, language, dto_text) in [(2301, "java", java_dto), (2302, "kotlin", kotlin_dto)] {
        let rule = call_tool_rule(
            &mut server,
            id,
            "generate_rules_from_dto",
            json!({
                "dto_text": dto_text,
                "dto_language": language,
                "input_json": {
                    "user_id": "001",
                    "name": "Ada",
                    "profile": {
                        "city_name": "Tokyo"
                    },
                    "active": true,
                    "count": 7,
                    "ratio": 1.5
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
        assert!(!profile_city_mapping.required);

        let active_mapping = mapping_by_target(&rule, "active");
        assert_eq!(active_mapping.value_type.as_deref(), Some("bool"));

        let count_mapping = mapping_by_target(&rule, "count");
        assert_eq!(count_mapping.value_type.as_deref(), Some("int"));

        let ratio_mapping = mapping_by_target(&rule, "ratio");
        assert_eq!(ratio_mapping.value_type.as_deref(), Some("float"));
    }

    server.shutdown();
}
