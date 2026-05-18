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
