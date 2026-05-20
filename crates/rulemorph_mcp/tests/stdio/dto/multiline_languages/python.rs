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
