use rulemorph::{DtoLanguage, generate_dto, parse_rule_file, validate_rule_file};

fn render(yaml: &str, language: DtoLanguage) -> String {
    let rule = parse_rule_file(yaml).expect("rule parses");
    validate_rule_file(&rule).expect("rule validates before dto render");
    generate_dto(&rule, language, Some("Record")).expect("dto renders")
}

#[test]
fn dto_render_validates_rule_before_generation() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: name
    expr: ["@input.name", { trim: "ignored" }]
"#;

    let panic = std::panic::catch_unwind(|| render(yaml, DtoLanguage::Rust))
        .expect_err("invalid rules should not reach DTO generation");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("<non-string panic>");
    assert!(
        message.contains("rule validates before dto render"),
        "unexpected panic message: {message}"
    );
}

#[test]
fn dto_infers_scalars_from_terminal_ops() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: active
    expr: ["@input.active_text", "bool"]
    required: true
  - target: score
    expr: ["@input.score_text", "int"]
    required: true
  - target: ratio
    expr: ["@input.total", { divide: [3] }]
    required: true
  - target: normalized
    expr: ["@input.name", "trim", "lowercase"]
    required: true
  - target: size
    expr: ["@input.items", "len"]
    required: true
"#;

    let rust = render(yaml, DtoLanguage::Rust);
    assert!(rust.contains("pub active: bool,"));
    assert!(rust.contains("pub score: i64,"));
    assert!(rust.contains("pub ratio: f64,"));
    assert!(rust.contains("pub normalized: String,"));
    assert!(rust.contains("pub size: i64,"));

    let typescript = render(yaml, DtoLanguage::TypeScript);
    assert!(typescript.contains("active: boolean;"));
    assert!(typescript.contains("score: number;"));
    assert!(typescript.contains("ratio: number;"));
    assert!(typescript.contains("normalized: string;"));
    assert!(typescript.contains("size: number;"));
}

#[test]
fn dto_infers_nullable_array_aggregates_and_fold_result() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: total
    expr: ["@input.items", "sum"]
    required: true
  - target: folded
    expr: ["@input.items", { fold: [0, ["@acc", "to_string"]] }]
    required: true
"#;

    let rust = render(yaml, DtoLanguage::Rust);
    assert!(rust.contains("pub total: Option<f64>,"));
    assert!(rust.contains("pub folded: Value,"));

    let typescript = render(yaml, DtoLanguage::TypeScript);
    assert!(typescript.contains("total: number | null;"));
    assert!(typescript.contains("folded: unknown;"));
}

#[test]
fn dto_rust_optional_nullable_field_uses_single_option() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: total
    expr: ["@input.items", "sum"]
"#;

    let rust = render(yaml, DtoLanguage::Rust);
    assert!(rust.contains("pub total: Option<f64>,"));
    assert!(!rust.contains("Option<Option<f64>>"));
}

#[test]
fn dto_oversized_unsigned_json_integer_falls_back_to_json_value() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: id
    value: 9223372036854775808
    required: true
"#;

    let rust = render(yaml, DtoLanguage::Rust);
    assert!(rust.contains("pub id: Value,"));
    assert!(!rust.contains("pub id: i64,"));
}

#[test]
fn dto_infers_literal_object_arrays_and_maps() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: payload
    expr:
      - { id: "demo", count: 3, tags: ["a", "b"], meta: { enabled: true } }
    required: true
  - target: parts
    expr: ["@input.csv", { split: [","] }]
    required: true
  - target: grouped
    expr: ["@input.items", { group_by: ["@item.category"] }]
    required: true
"#;

    let rust = render(yaml, DtoLanguage::Rust);
    assert!(rust.contains("pub payload: RecordPayload,"));
    assert!(rust.contains("pub tags: Vec<String>,"));
    assert!(rust.contains("pub meta: RecordPayloadMeta,"));
    assert!(rust.contains("pub enabled: bool,"));
    assert!(rust.contains("pub parts: Vec<String>,"));
    assert!(rust.contains("HashMap<String, Vec<Value>>"));

    let typescript = render(yaml, DtoLanguage::TypeScript);
    assert!(typescript.contains("payload: RecordPayload;"));
    assert!(typescript.contains("tags: string[];"));
    assert!(typescript.contains("meta: RecordPayloadMeta;"));
    assert!(typescript.contains("enabled: boolean;"));
    assert!(typescript.contains("parts: string[];"));
    assert!(typescript.contains("grouped: { [key: string]: unknown[] };"));
    assert!(!typescript.contains("Record<string,"));
}

#[test]
fn dto_infers_object_builder_static_shape() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: payload
    expr:
      - object:
          name: ["@input.name", uppercase]
          age: ["@input.age", int]
          nested:
            - object:
                active: true
    required: true
"#;

    let rust = render(yaml, DtoLanguage::Rust);
    assert!(rust.contains("pub payload: RecordPayload,"));
    assert!(rust.contains("pub name: Option<String>,"));
    assert!(rust.contains("pub age: Option<i64>,"));
    assert!(rust.contains("pub nested: Option<RecordPayloadNested>,"));
    assert!(rust.contains("pub active: Option<bool>,"));

    let typescript = render(yaml, DtoLanguage::TypeScript);
    assert!(typescript.contains("payload: RecordPayload;"));
    assert!(typescript.contains("name?: string;"));
    assert!(typescript.contains("age?: number;"));
    assert!(typescript.contains("nested?: RecordPayloadNested;"));
    assert!(typescript.contains("active?: boolean;"));
}

#[test]
fn dto_reuses_synthetic_parent_out_shapes() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: base.id
    value: "u1"
    required: true
  - target: base.active
    value: true
    required: true
  - target: copy
    expr: ["@out.base"]
    required: true
"#;

    let typescript = render(yaml, DtoLanguage::TypeScript);
    assert!(typescript.contains("base: RecordBase;"));
    assert!(typescript.contains("copy: RecordCopy;"));
    assert!(typescript.contains("interface RecordCopy"));
    assert!(typescript.contains("id: string;"));
    assert!(typescript.contains("active: boolean;"));
}

#[test]
fn dto_python_container_annotations_are_future_safe() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: parts
    expr: ["@input.csv", { split: [","] }]
    required: true
  - target: grouped
    expr: ["@input.items", { group_by: ["@item.category"] }]
    required: true
"#;

    let python = render(yaml, DtoLanguage::Python);
    assert!(python.starts_with("from __future__ import annotations\n"));
    assert!(python.contains("parts: list[str]"));
    assert!(python.contains("grouped: dict[str, list[Any]]"));
}

#[test]
fn dto_required_empty_object_does_not_emit_optional_imports() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: empty
    value: {}
    required: true
"#;

    let java = render(yaml, DtoLanguage::Java);
    assert!(!java.contains("import java.util.Optional;"));

    let python = render(yaml, DtoLanguage::Python);
    assert!(!python.contains("Optional"));
}

#[test]
fn dto_explicit_type_overrides_inference() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: active
    expr: ["@input.active_text", "bool"]
    type: string
    required: true
"#;

    let rust = render(yaml, DtoLanguage::Rust);
    assert!(rust.contains("pub active: String,"));

    let typescript = render(yaml, DtoLanguage::TypeScript);
    assert!(typescript.contains("active: string;"));
}

#[test]
fn dto_default_does_not_narrow_dynamic_unknown() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: payload
    source: input.payload
    default: { role: "user" }
    required: true
  - target: selected
    expr: ["@input.dynamic", { get: ["@input.path"] }]
    default: "fallback"
    required: true
"#;

    let rust = render(yaml, DtoLanguage::Rust);
    assert!(rust.contains("pub payload: Value,"));
    assert!(rust.contains("pub selected: Value,"));

    let typescript = render(yaml, DtoLanguage::TypeScript);
    assert!(typescript.contains("payload: unknown;"));
    assert!(typescript.contains("selected: unknown;"));
}

#[test]
fn dto_default_and_coalesce_do_not_narrow_nullable_unknown() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: firstItem
    expr: ["@input.items", "first"]
    default: "fallback"
    required: true
  - target: coalesced
    expr: ["@input.items", "first", { coalesce: ["fallback"] }]
    required: true
"#;

    let rust = render(yaml, DtoLanguage::Rust);
    assert!(rust.contains("pub first_item: Value,"));
    assert!(rust.contains("pub coalesced: Value,"));

    let typescript = render(yaml, DtoLanguage::TypeScript);
    assert!(typescript.contains("firstItem: unknown;"));
    assert!(typescript.contains("coalesced: unknown;"));
}

#[test]
fn dto_projection_falls_back_for_dynamic_or_indexed_paths() {
    let long_path = "profile.".repeat(200) + "name";
    let yaml = format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
mappings:
  - target: base
    value: {{ id: "u1", profile: {{ name: "Ada", age: 37 }}, active: true }}
    required: true
  - target: mixedPick
    expr: ["@out.base", {{ pick: ["id", "@input.dynamicPath"] }}]
    required: true
  - target: mixedOmit
    expr: ["@out.base", {{ omit: ["active", "@input.dynamicPath"] }}]
    required: true
  - target: indexedOut
    expr: ["@out.base[0].id"]
    required: true
  - target: hugePath
    expr: ["@out.base", {{ get: ["{}"] }}]
    required: true
"#,
        long_path
    );

    let rust = render(&yaml, DtoLanguage::Rust);
    assert!(rust.contains("pub mixed_pick: Value,"));
    assert!(rust.contains("pub mixed_omit: Value,"));
    assert!(rust.contains("pub indexed_out: Value,"));
    assert!(rust.contains("pub huge_path: Value,"));

    let typescript = render(&yaml, DtoLanguage::TypeScript);
    assert!(typescript.contains("mixedPick: unknown;"));
    assert!(typescript.contains("mixedOmit: unknown;"));
    assert!(typescript.contains("indexedOut: unknown;"));
    assert!(typescript.contains("hugePath: unknown;"));
}

#[test]
fn dto_out_object_copies_are_charged_to_generated_type_budget() {
    let mut mappings =
        String::from("  - target: base\n    value: { id: \"u1\" }\n    required: true\n");
    for index in 0..530 {
        mappings.push_str(&format!(
            "  - target: copy{index}\n    expr: [\"@out.base\"]\n    required: true\n"
        ));
    }
    let yaml = format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
mappings:
{}
"#,
        mappings
    );

    let rust = render(&yaml, DtoLanguage::Rust);
    assert!(rust.contains("pub copy529: Value,"));
}

#[test]
fn dto_inference_budget_falls_back_for_large_literal_shapes() {
    let mut fields = String::new();
    for index in 0..300 {
        fields.push_str(&format!("        k{}: {}\n", index, index));
    }
    let yaml = format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
mappings:
  - target: payload
    value:
{}
    required: true
"#,
        fields
    );

    let rust = render(&yaml, DtoLanguage::Rust);
    assert!(rust.contains("pub payload: Value,"));

    let typescript = render(&yaml, DtoLanguage::TypeScript);
    assert!(typescript.contains("payload: unknown;"));
}

#[test]
fn dto_inference_budget_is_shared_across_mappings_and_pipe_steps() {
    let mut mappings = String::new();
    for index in 0..600 {
        mappings.push_str(&format!(
            "  - target: payload{index}\n    value: {{ nested: {{ value: {index} }} }}\n    required: true\n"
        ));
    }

    let many_mappings_yaml = format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
mappings:
{}
"#,
        mappings
    );

    let many_mappings_rust = render(&many_mappings_yaml, DtoLanguage::Rust);
    assert!(many_mappings_rust.contains("pub payload599: Value,"));

    let long_pipe = std::iter::repeat_n("\"trim\"", 5000)
        .collect::<Vec<_>>()
        .join(", ");
    let long_pipe_yaml = format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
mappings:
  - target: value
    expr: ["@input.value", {}]
    required: true
"#,
        long_pipe
    );

    let long_pipe_rust = render(&long_pipe_yaml, DtoLanguage::Rust);
    assert!(long_pipe_rust.contains("pub value: Value,"));
}

#[test]
fn dto_inference_budget_falls_back_when_object_merge_expands_shape() {
    let mut base_fields = String::new();
    let mut merge_fields = Vec::new();
    for index in 0..200 {
        base_fields.push_str(&format!("        l{}: {}\n", index, index));
        merge_fields.push(format!("r{}: {}", index, index));
    }
    let merge_arg = merge_fields.join(", ");
    let yaml = format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
mappings:
  - target: base
    value:
{}
    required: true
  - target: merged
    expr: ["@out.base", {{ merge: [{{ {} }}] }}]
    required: true
"#,
        base_fields, merge_arg
    );

    let rust = render(&yaml, DtoLanguage::Rust);
    assert!(rust.contains("pub merged: Value,"));
}

#[test]
fn dto_infers_object_projection_and_merge_shapes() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: base
    value: { id: "u1", profile: { name: "Ada", age: 37 }, active: true }
    required: true
  - target: profile
    expr: ["@out.base", { get: ["profile"] }]
    required: true
  - target: publicBase
    expr: ["@out.base", { pick: ["id", "active"] }]
    required: true
  - target: publicProfile
    expr: ["@out.base", { pick: ["profile.name"] }]
    required: true
  - target: withoutAge
    expr: ["@out.base", { omit: ["profile.age"] }]
    required: true
  - target: merged
    expr: ["@out.publicBase", { merge: [{ plan: "pro" }] }]
    required: true
"#;

    let typescript = render(yaml, DtoLanguage::TypeScript);
    assert!(typescript.contains("profile: RecordProfile;"));
    assert!(typescript.contains("name: string;"));
    assert!(typescript.contains("age: number;"));
    assert!(typescript.contains("publicBase: RecordPublicBase;"));
    assert!(typescript.contains("id: string;"));
    assert!(typescript.contains("active: boolean;"));
    assert!(typescript.contains("publicProfile: RecordPublicProfile;"));
    assert!(typescript.contains("profile: RecordPublicProfileProfile;"));
    assert!(typescript.contains("interface RecordPublicProfileProfile"));
    assert!(typescript.contains("merged: RecordMerged;"));
    assert!(typescript.contains("plan: string;"));
}

#[test]
fn dto_renders_json_wrappers_across_languages() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: items
    expr: ["@input.obj", "values"]
    required: true
  - target: flat
    expr: ["@input.obj", "object_flatten"]
    required: true
  - target: maybe
    expr: ["@input.items", "first"]
    required: true
"#;

    let go = render(yaml, DtoLanguage::Go);
    assert!(go.contains("import \"encoding/json\""));
    assert!(go.contains("Items []json.RawMessage"));
    assert!(go.contains("Flat map[string]json.RawMessage"));
    assert!(go.contains("Maybe *json.RawMessage"));

    let java = render(yaml, DtoLanguage::Java);
    assert!(java.contains("import com.fasterxml.jackson.databind.JsonNode;"));
    assert!(java.contains("import java.util.List;"));
    assert!(java.contains("import java.util.Map;"));
    assert!(java.contains("import java.util.Optional;"));
    assert!(java.contains("public List<JsonNode> items;"));
    assert!(java.contains("public Map<String, JsonNode> flat;"));
    assert!(java.contains("public Optional<JsonNode> maybe;"));

    let kotlin = render(yaml, DtoLanguage::Kotlin);
    assert!(kotlin.contains("import com.fasterxml.jackson.databind.JsonNode"));
    assert!(kotlin.contains("import kotlin.collections.List"));
    assert!(kotlin.contains("import kotlin.collections.Map"));
    assert!(kotlin.contains("val items: List<JsonNode>"));
    assert!(kotlin.contains("val flat: Map<String, JsonNode>"));
    assert!(kotlin.contains("val maybe: JsonNode?"));

    let python = render(yaml, DtoLanguage::Python);
    assert!(python.contains("from typing import Optional, Any"));
    assert!(python.contains("items: list[Any]"));
    assert!(python.contains("flat: dict[str, Any]"));
    assert!(python.contains("maybe: Optional[Any]"));

    let swift = render(yaml, DtoLanguage::Swift);
    assert!(swift.contains("let items: [JSONValue]"));
    assert!(swift.contains("let flat: [String: JSONValue]"));
    assert!(swift.contains("let maybe: JSONValue?"));
    assert!(swift.contains("enum JSONValue: Codable"));
}
