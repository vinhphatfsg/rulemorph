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
fn dto_generation_sanitizes_inferred_structured_keys() {
    let rule = parse_rule_file(
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: payload
    value:
      bad*/key:
        quote"key: "x"
        "slash\\key": "b"
        "line\nkey": "y"
      tick`key:
        - class: true
          "tab\tkey": "z"
      items:
        - id: "a"
          "bad*/item": 1
          "quote\"item": true
          "slash\\item": false
    required: true
  - target: byKey
    expr: ["@out.payload.items", { key_by: ["@item.id"] }]
    required: true
"#,
    )
    .expect("parse rule");

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
        assert!(!output.contains("bad*/item */"));
        assert!(!output.contains("quote\"key"));
        assert!(!output.contains("slash\\key"));
        assert!(!output.contains("line\nkey"));
        assert!(!output.contains("tab\tkey"));
    }

    let rust =
        generate_dto(&rule, DtoLanguage::Rust, Some("Record { hacked")).expect("generate rust dto");
    assert!(rust.contains(r#"rename = "bad*/key""#));
    assert!(rust.contains(r#"rename = "quote\"key""#));
    assert!(rust.contains(r#"rename = "slash\\key""#));
    assert!(rust.contains(r#"rename = "line\nkey""#));

    let go =
        generate_dto(&rule, DtoLanguage::Go, Some("Record { hacked")).expect("generate go dto");
    assert!(go.contains(r#""json:\"tick`key\"""#));
    assert!(go.contains(r#""json:\"quote\\\"key\"""#));
    assert!(go.contains(r#""json:\"slash\\\\key\"""#));
    assert!(go.contains(r#""json:\"line\\nkey\"""#));

    let python =
        generate_dto(&rule, DtoLanguage::Python, Some("Record")).expect("generate python dto");
    assert!(python.contains(r#"# json: "bad* /key""#));
    assert!(python.contains(r#"# json: "quote\"key""#));
    assert!(python.contains(r#"# json: "slash\\key""#));
    assert!(python.contains(r#"metadata={"json_key": "quote\"key"}"#));
    assert!(python.contains(r#"metadata={"json_key": "slash\\key"}"#));
    assert!(python.contains(r#"metadata={"json_key": "line\nkey"}"#));

    let typescript = generate_dto(&rule, DtoLanguage::TypeScript, Some("Record"))
        .expect("generate typescript dto");
    assert!(typescript.contains(r#"json: "bad* /key""#));
    assert!(typescript.contains(r#"json: "quote\"key""#));
    assert!(typescript.contains(r#"json: "slash\\key""#));
    assert!(typescript.contains(r#"json: "line\\nkey""#));
    assert!(typescript.contains("{ [key: string]:"));
    assert!(!typescript.contains("Record<string,"));

    let java = generate_dto(&rule, DtoLanguage::Java, Some("Record")).expect("generate java dto");
    assert!(java.contains(r#"@JsonProperty("bad*/key")"#));
    assert!(java.contains(r#"@JsonProperty("quote\"key")"#));
    assert!(java.contains(r#"@JsonProperty("slash\\key")"#));
    assert!(java.contains(r#"@JsonProperty("line\nkey")"#));

    let kotlin =
        generate_dto(&rule, DtoLanguage::Kotlin, Some("Record")).expect("generate kotlin dto");
    assert!(kotlin.contains(r#"@JsonProperty("bad*/key")"#));
    assert!(kotlin.contains(r#"@JsonProperty("quote\"key")"#));
    assert!(kotlin.contains(r#"@JsonProperty("slash\\key")"#));
    assert!(kotlin.contains(r#"@JsonProperty("line\nkey")"#));

    let swift =
        generate_dto(&rule, DtoLanguage::Swift, Some("Record")).expect("generate swift dto");
    assert!(swift.contains(r#"case badKey = "bad*/key""#));
    assert!(swift.contains(r#"case quoteKey = "quote\"key""#));
    assert!(swift.contains(r#"case slashKey = "slash\\key""#));
    assert!(swift.contains(r#"line\nkey"#));
}
