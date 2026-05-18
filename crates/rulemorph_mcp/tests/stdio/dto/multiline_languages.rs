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

#[test]
fn generate_rules_from_dto_python_multiline_model_shape() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = r#"
from typing import Any, Optional
from pydantic import BaseModel, Field

class Record(BaseModel):
    id: str = Field(alias="user_id")
    name: typing.Optional[str] = None
    active: bool
    count: int
    ratio: float
    profile: Profile
    metadata: dict[str, Any]
    tags: list[str]

class Profile(BaseModel):
    city: str = Field(alias="city_name")
    nickname: str | None = None
"#;

    let rule = call_tool_rule(
        &mut server,
        20,
        "generate_rules_from_dto",
        json!({
            "dto_text": dto_text,
            "dto_language": "python",
            "input_json": {
                "user_id": "001",
                "name": "Ada",
                "active": true,
                "count": 3,
                "ratio": 1.5,
                "profile": {
                    "city_name": "Paris",
                    "nickname": null
                },
                "metadata": { "source": "api" },
                "tags": ["vip"]
            }
        }),
    );

    let id_mapping = mapping_by_target(&rule, "user_id");
    assert_eq!(id_mapping.source.as_deref(), Some("user_id"));
    assert_eq!(id_mapping.value_type.as_deref(), Some("string"));
    assert!(id_mapping.required);

    let name_mapping = mapping_by_target(&rule, "name");
    assert_eq!(name_mapping.source.as_deref(), Some("name"));
    assert_eq!(name_mapping.value_type.as_deref(), Some("string"));
    assert!(!name_mapping.required);

    let active_mapping = mapping_by_target(&rule, "active");
    assert_eq!(active_mapping.value_type.as_deref(), Some("bool"));
    assert!(active_mapping.required);

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

    let profile_nickname_mapping = mapping_by_target(&rule, "profile.nickname");
    assert_eq!(
        profile_nickname_mapping.source.as_deref(),
        Some("profile.nickname")
    );
    assert!(!profile_nickname_mapping.required);

    let metadata_mapping = mapping_by_target(&rule, "metadata");
    assert_eq!(metadata_mapping.source, None);
    assert_eq!(metadata_mapping.value_type, None);

    let tags_mapping = mapping_by_target(&rule, "tags");
    assert_eq!(tags_mapping.source.as_deref(), Some("tags"));
    assert_eq!(tags_mapping.value_type, None);

    server.shutdown();
}

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
