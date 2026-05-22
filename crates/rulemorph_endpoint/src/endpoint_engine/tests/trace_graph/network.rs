#[test]
fn network_nodes_include_request_duration_us() {
    let body_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings: []
"#;
    let body_rule = parse_rule_file(body_yaml).expect("parse body rule");
    let rule = CompiledNetworkRule {
        request: CompiledNetworkRequest {
            method: Method::GET,
            url: parse_v2_expr(&json!("https://example.com")).expect("parse url"),
            headers: HashMap::new(),
        },
        timeout: Duration::from_secs(1),
        select: None,
        body: None,
        body_map: None,
        body_rule: Some(LoadedRule {
            rule: body_rule,
            base_dir: PathBuf::from("."),
        }),
        body_rule_ref: Some("rules/body.yaml".to_string()),
        rule_ref: None,
        catch: None,
        retry: None,
        internal_auth: false,
        base_dir: PathBuf::from("."),
    };
    let timing = NetworkExecution {
        output: json!({}),
        request_us: 12,
        total_us: 34,
        body_rule_trace: Some(json!({
            "rule": { "path": "rules/body.yaml" },
            "records": []
        })),
    };

    let nodes = build_network_nodes_with_timing(&rule, &timing);
    let duration = nodes[0].get("duration_us").and_then(|value| value.as_u64());
    assert_eq!(duration, Some(34));
    let meta = nodes[0]
        .get("meta")
        .and_then(|value| value.as_object())
        .expect("meta");
    assert_eq!(meta.get("rule_ref"), Some(&json!("rules/body.yaml")));
    assert_eq!(meta.get("rule_ref_label"), Some(&json!("body_rule")));
    let child_trace = nodes[0]
        .get("child_trace")
        .and_then(|value| value.get("rule"))
        .and_then(|value| value.get("path"));
    assert_eq!(child_trace, Some(&json!("rules/body.yaml")));

    let children = nodes[0]
        .get("children")
        .and_then(|value| value.as_array())
        .expect("children");
    assert_eq!(children.len(), 2);
    let request = children[0]
        .get("duration_us")
        .and_then(|value| value.as_u64());
    assert_eq!(request, Some(12));
}
