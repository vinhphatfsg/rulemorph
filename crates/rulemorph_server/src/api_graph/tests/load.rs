use super::super::build_api_graph;

#[test]
fn graph_loads_json_rule_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let rules_dir = dir.path().join("api_rules");
    std::fs::create_dir_all(&rules_dir).expect("create rules dir");
    std::fs::write(
        rules_dir.join("rule.json"),
        r#"{
  "version": 2,
  "input": { "format": "json", "json": {} },
  "mappings": [{ "target": "id", "source": "id" }]
}
"#,
    )
    .expect("write json rule");

    let graph = build_api_graph(dir.path()).expect("graph");
    assert!(graph.nodes.iter().any(|node| node.kind == "normal"));
}
