use rulemorph_wasm::call_tool;
use serde_json::{Value, json};

const RULE: &str = "version: 2\ninput:\n  format: json\n  json: {}\nmappings:\n  - target: name\n    source: name\n    type: string\n";
fn call(name: &str, arguments: Value) -> Value {
    serde_json::from_str(&call_tool(
        &json!({"name":name,"arguments":arguments}).to_string(),
    ))
    .unwrap()
}
fn data(result: &Value) -> Value {
    assert_eq!(result["isError"], false, "{result}");
    serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap()
}

#[test]
fn inline_transform_matches_core() {
    let rule = rulemorph::parse_rule_file(RULE).unwrap();
    let input = json!([{ "name":"太郎" }]);
    let expected = rulemorph::transform(&rule, &input.to_string(), None).unwrap();
    assert_eq!(
        data(&call(
            "transform",
            json!({"rules_text":RULE,"input_json":input})
        ))["output"],
        expected
    );
}

#[test]
fn validates_and_generates_dto() {
    assert_eq!(
        data(&call("validate_rules", json!({"rules_text":RULE})))["valid"],
        true
    );
    for language in [
        "rust",
        "typescript",
        "python",
        "go",
        "java",
        "kotlin",
        "swift",
    ] {
        assert!(
            !data(&call(
                "generate_dto",
                json!({"rules_text":RULE,"language":language})
            ))["dto"]
                .as_str()
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(
        data(&call(
            "validate_rules",
            json!({"rules_text":RULE.replace("version: 2", "version: 9")})
        ))["valid"],
        false
    );
}

#[test]
fn analysis_has_no_samples_and_preserves_types() {
    let result = data(&call(
        "analyze_input",
        json!({"input_json":[{"secret":"do-not-return","n":2},{"n":null}]}),
    ));
    assert_eq!(result["summary"]["records"], 2);
    assert!(!result.to_string().contains("do-not-return"));
    assert_eq!(result["paths"]["[\"n\"]"], json!(["null", "number"]));
    assert_eq!(
        data(&call(
            "analyze_input",
            json!({"format":"csv","input_text":"name\n太郎\n"})
        ))["summary"]["records"],
        1
    );
}

#[test]
fn rejects_paths_branches_and_expanding_operations() {
    let bad_rules = [
        RULE.replace("target: name", "target: name[999999999]"),
        RULE.replace("source: name", "expr: ['@input.name', {range: [0, 1000000]}]"),
        "version: 2\ninput: {format: json, json: {}}\nsteps:\n  - branch: {when: true, then: ../../secret.yaml}".into(),
        format!("{RULE}\nfinalize: {{limit: 1}}"),
    ];
    for rules in bad_rules {
        assert_eq!(
            call("validate_rules", json!({"rules_text":rules}))["isError"],
            true
        );
    }
    for key in [
        "input_path",
        "rules_path",
        "output_path",
        "base_dir",
        "context",
        "max_records",
    ] {
        let mut args = json!({"rules_text":RULE,"input_json":[]});
        args[key] = json!("../../secret");
        assert_eq!(call("transform", args)["isError"], true);
    }
}

#[test]
fn rejects_resource_limit_overrides_and_oversized_data() {
    for args in [
        json!({"input_json":vec![json!({"name":"x"});129]}),
        json!({"input_text":"x".repeat(65537)}),
        json!({"input_text":"[{\"a\":1,\"a\":2}]"}),
        json!({"input_json":[],"input_text":"[]"}),
    ] {
        assert_eq!(call("analyze_input", args)["isError"], true);
    }
    assert_eq!(
        call("validate_rules", json!({"rules_text":"x".repeat(16385)}))["isError"],
        true
    );
}

#[test]
fn limits_processing_and_never_returns_truncated_success() {
    let mappings = (0..64)
        .map(|i| json!({"target":format!("v{i}"),"expr":["@input.name","trim"]}))
        .collect::<Vec<_>>();
    let rule =
        json!({"version":2,"input":{"format":"json","json":{}},"mappings":mappings}).to_string();
    let result = call(
        "transform",
        json!({"rules_text":rule,"input_json":vec![json!({"name":"x"});128]}),
    );
    assert_eq!(result["isError"], true);
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("processing budget")
    );
    let mappings = (0..64)
        .map(|i| json!({"target":format!("v{i}"),"source":"name"}))
        .collect::<Vec<_>>();
    let rule =
        json!({"version":2,"input":{"format":"json","json":{}},"mappings":mappings}).to_string();
    let result = call(
        "transform",
        json!({"rules_text":rule,"input_json":[{"name":"x".repeat(8192)}]}),
    );
    assert_eq!(result["isError"], true);
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("output exceeds")
    );
}

#[test]
fn conditions_cannot_hide_expanding_or_regex_operations() {
    for when in [
        "when: {eq: [[0, {range: [0, 1000000]}], 1]}",
        "when: {all: [{match: ['@input.name', '.*']}]}",
    ] {
        assert_eq!(
            call(
                "validate_rules",
                json!({"rules_text":format!("{RULE}    {when}\n")})
            )["isError"],
            true
        );
    }
}
