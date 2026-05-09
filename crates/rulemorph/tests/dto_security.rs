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
