#[test]
fn endpoint_error_trace_uses_rule_ref_for_path() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    std::fs::write(
        rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: rules/ok.yaml
    reply:
      status: 200
"#,
    )
    .expect("write endpoint");
    std::fs::create_dir_all(rules_dir.join("rules")).expect("create rules dir");
    std::fs::write(
        rules_dir.join("rules/ok.yaml"),
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "output.ok"
    value: true
"#,
    )
    .expect("write rule");

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://127.0.0.1:8080".to_string(), rules_dir.join(".data"))
            .with_ssrf_allow_private(true),
    )
    .expect("load engine");

    let resolved = rules_dir.join("rules/ok.yaml");
    let err = EndpointError::invalid("boom").with_path(resolved.clone());
    let trace = engine.endpoint_error_to_trace(&err);
    let path = trace
        .get("path")
        .and_then(|value| value.as_str())
        .expect("path");

    let expected = rule_ref_from_path(&engine.endpoint_rule.base_dir, &resolved);
    assert_eq!(path, expected);
    assert!(!Path::new(path).is_absolute());
}

#[test]
fn build_trace_emits_top_level_status() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    std::fs::write(
        rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps: []
    reply:
      status: 200
"#,
    )
    .expect("write endpoint.yaml");

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://localhost".to_string(), rules_dir.join(".data"))
            .with_ssrf_allow_private(true),
    )
    .expect("load engine");

    let trace = engine.build_trace(
        &Method::GET,
        "/api/test",
        json!({"input": true}),
        json!({"output": false}),
        "error".to_string(),
        Some(json!({"message": "boom"})),
        Vec::new(),
        12,
    );
    let status = trace.get("status").and_then(|value| value.as_str());
    assert_eq!(status, Some("error"));
}

#[test]
fn mapping_ops_include_duration_us() {
    let mappings = vec![Mapping {
        target: "name".to_string(),
        source: None,
        value: Some(json!("hello")),
        expr: None,
        when: None,
        value_type: None,
        required: false,
        default: None,
    }];
    let record = json!({});
    let mut out = json!({});
    let ops = build_mapping_ops_with_values(&mappings, &record, None, &mut out, 2, 0);
    let duration = ops[0].get("duration_us").and_then(|value| value.as_u64());
    assert!(duration.is_some());
}

#[test]
fn rule_nodes_include_step_duration_us() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - mappings:
      - target: name
        value: "hello"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({});
    let trace = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));
    let duration = trace.nodes[0]
        .get("duration_us")
        .and_then(|value| value.as_u64());
    assert!(duration.is_some());
}

#[test]
fn endpoint_trace_branch_step_includes_rule_refs_and_child_trace() {
    let temp = tempfile::tempdir().expect("tempdir");
    let base_dir = temp.path();
    std::fs::write(
        base_dir.join("then.yaml"),
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "result"
    value: "then"
"#,
    )
    .expect("write then rule");
    std::fs::write(
        base_dir.join("else.yaml"),
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "result"
    value: "else"
"#,
    )
    .expect("write else rule");
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: ["@input.kind", "then"] }
      then: ./then.yaml
      else: ./else.yaml
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({ "kind": "then" });
    let trace = build_rule_nodes_from_rule(&rule, &record, None, base_dir);
    let node = trace.nodes.first().expect("branch node");
    assert_eq!(node.get("kind"), Some(&json!("branch")));

    let meta = node
        .get("meta")
        .and_then(|value| value.as_object())
        .expect("branch meta");
    assert_eq!(meta.get("branch_taken"), Some(&json!("then")));
    assert_eq!(
        meta.get("rule_refs"),
        Some(&json!(["rules/then.yaml", "rules/else.yaml"]))
    );
    assert_eq!(
        meta.get("rule_ref_labels"),
        Some(&json!(["branch: then", "branch: else"]))
    );
    assert_eq!(meta.get("rule_ref"), Some(&json!("rules/then.yaml")));
    assert_eq!(meta.get("rule_ref_label"), Some(&json!("branch: then")));

    let child_rule_path = node
        .get("child_trace")
        .and_then(|value| value.get("rule"))
        .and_then(|value| value.get("path"));
    assert_eq!(child_rule_path, Some(&json!("rules/then.yaml")));
}

#[test]
fn rule_trace_duration_includes_finalize_duration() {
    let nodes = vec![json!({ "duration_us": 10 }), json!({ "duration_us": 15 })];
    let finalize = json!({ "duration_us": 7 });

    assert_eq!(sum_rule_trace_duration_us(&nodes, Some(&finalize)), 32);
}

#[test]
fn finalize_trace_includes_operation_nodes_in_order() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "score"
    source: "input.score"
finalize:
  filter:
    gte: ["@item.score", 10]
  sort:
    by: "score"
    order: "asc"
  limit: 1
  offset: 0
  wrap:
    data: "@out"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({ "score": 12 });
    let trace = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));
    let finalize = trace.finalize.expect("finalize trace");

    assert_eq!(
        finalize.get("status").and_then(|value| value.as_str()),
        Some("ok")
    );
    assert!(
        finalize
            .get("duration_us")
            .and_then(|value| value.as_u64())
            .is_some()
    );
    assert_eq!(
        finalize
            .get("input")
            .and_then(|value| value.as_array())
            .map(|items| items.len()),
        Some(1)
    );
    let labels: Vec<&str> = finalize
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("finalize nodes")
        .iter()
        .map(|node| {
            node.get("label")
                .and_then(|value| value.as_str())
                .expect("node label")
        })
        .collect();
    assert_eq!(labels, vec!["filter", "sort", "limit", "offset", "wrap"]);
    assert_eq!(
        finalize
            .get("nodes")
            .and_then(|value| value.as_array())
            .and_then(|nodes| nodes.first())
            .and_then(|node| node.get("args"))
            .and_then(|args| args.get("expr")),
        Some(&json!({ "gte": ["@item.score", 10] }))
    );
}

#[test]
fn finalize_trace_preserves_error_payload() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "score"
    source: "input.score"
finalize:
  wrap:
    data:
      - "@out"
      - unknown_op
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({ "score": 12 });
    let trace = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));
    let finalize = trace.finalize.expect("finalize trace");
    let error = finalize.get("error").expect("finalize error");

    assert_eq!(
        finalize.get("status").and_then(|value| value.as_str()),
        Some("error")
    );
    assert_eq!(
        error.get("code").and_then(|value| value.as_str()),
        Some("ExprError")
    );
    assert_eq!(
        error.get("message").and_then(|value| value.as_str()),
        Some("expr.op is not supported")
    );
    assert_eq!(
        error.get("path").and_then(|value| value.as_str()),
        Some("finalize.wrap.data[1].op")
    );
}

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

#[test]
#[ignore]
fn trace_timing_perf_smoke() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - mappings:
      - target: name
        value: "hello"
  - mappings:
      - target: upper
        expr: ["@out.name", uppercase]
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({});
    let iterations = 100u64;
    let started = Instant::now();
    for _ in 0..iterations {
        let _ = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));
    }
    let total_us = started.elapsed().as_micros() as u64;
    println!("trace timing avg: {} μs", total_us / iterations);
}
