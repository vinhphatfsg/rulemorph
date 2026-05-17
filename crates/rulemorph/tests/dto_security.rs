use rulemorph::{DtoLanguage, generate_dto, parse_rule_file};

fn malicious_rule() -> rulemorph::RuleFile {
    parse_rule_file(
        r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: '["bad\"key"]'
    value: "x"
  - target: "['tick`key']"
    value: "x"
  - target: "['bad*/key']"
    value: "x"
"#,
    )
    .expect("parse rule")
}

#[test]
fn dto_generation_sanitizes_type_name_and_escapes_json_keys() {
    let rule = malicious_rule();
    for language in [
        DtoLanguage::Rust,
        DtoLanguage::TypeScript,
        DtoLanguage::Python,
        DtoLanguage::Go,
        DtoLanguage::Java,
        DtoLanguage::Kotlin,
        DtoLanguage::Swift,
    ] {
        let output = generate_dto(&rule, language, Some("Record { hacked"))
            .expect("generate dto should not fail");
        assert!(!output.contains("Record { hacked"));
        assert!(!output.contains("bad*/key */"));
    }

    let rust =
        generate_dto(&rule, DtoLanguage::Rust, Some("Record { hacked")).expect("generate rust dto");
    assert!(rust.contains(r#"rename = "bad\"key""#));

    let go =
        generate_dto(&rule, DtoLanguage::Go, Some("Record { hacked")).expect("generate go dto");
    assert!(go.contains(r#""json:\"bad\\\"key\"""#));
    assert!(go.contains(r#""json:\"tick`key\"""#));
}

#[test]
fn dto_generation_stabilizes_identifier_collisions_and_digit_prefixes() {
    let rule = parse_rule_file(
        r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "foo-bar"
    value: "x"
  - target: "foo_bar"
    value: "x"
  - target: "['1name']"
    value: "x"
  - target: "class"
    value: "x"
"#,
    )
    .expect("parse rule");

    let rust = generate_dto(&rule, DtoLanguage::Rust, None).expect("generate rust dto");
    assert!(rust.contains("pub foo_bar: Value,"), "{rust}");
    assert!(rust.contains("pub foo_bar_2: Value,"), "{rust}");
    assert!(rust.contains("pub _1name: Value,"), "{rust}");
    assert!(rust.contains("pub class: Value,"), "{rust}");

    let typescript =
        generate_dto(&rule, DtoLanguage::TypeScript, None).expect("generate typescript dto");
    assert!(typescript.contains("fooBar: unknown;"), "{typescript}");
    assert!(typescript.contains("fooBar_2: unknown;"), "{typescript}");
    assert!(typescript.contains("_1name: unknown;"), "{typescript}");
    assert!(typescript.contains("class_: unknown;"), "{typescript}");

    let go = generate_dto(&rule, DtoLanguage::Go, None).expect("generate go dto");
    assert!(
        go.contains("FooBar json.RawMessage `json:\"foo-bar\"`"),
        "{go}"
    );
    assert!(
        go.contains("FooBar_2 json.RawMessage `json:\"foo_bar\"`"),
        "{go}"
    );
    assert!(
        go.contains("Field1name json.RawMessage `json:\"1name\"`"),
        "{go}"
    );
    assert!(
        go.contains("Class json.RawMessage `json:\"class\"`"),
        "{go}"
    );
}
