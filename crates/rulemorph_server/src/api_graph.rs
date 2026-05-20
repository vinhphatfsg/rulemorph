use std::path::Path;

use anyhow::Result;

mod dto;
mod load;
mod ops;
mod primitives;

pub use self::dto::{ApiGraphEdge, ApiGraphNode, ApiGraphOp, ApiGraphResponse};

pub fn build_api_graph(data_dir: &Path) -> Result<ApiGraphResponse> {
    load::build_api_graph(data_dir)
}

#[cfg(test)]
mod tests {
    use super::ops::{
        EndpointDef, EndpointRuleFile, EndpointStep, NetworkRequest, NetworkRuleFile, endpoint_ops,
        network_ops, normal_ops,
    };
    use super::*;
    use rulemorph::parse_rule_file;
    use serde_json::json;

    #[test]
    fn endpoint_ops_include_all_step_refs() {
        let rule = EndpointRuleFile {
            _rule_type: "endpoint".to_string(),
            endpoints: vec![EndpointDef {
                method: "GET".to_string(),
                path: "/users/{id}".to_string(),
                steps: vec![
                    EndpointStep {
                        rule: "./a.yaml".to_string(),
                    },
                    EndpointStep {
                        rule: "./b.yaml".to_string(),
                    },
                ],
            }],
        };
        let data_dir = Path::new("/tmp/rules");
        let endpoint_path = Path::new("/tmp/rules/api_rules/endpoint.yaml");
        let ops = endpoint_ops(&rule, data_dir, endpoint_path);
        assert_eq!(ops.len(), 1);
        let refs = &ops[0].refs;
        assert!(refs.contains(&"api_rules/a.yaml".to_string()));
        assert!(refs.contains(&"api_rules/b.yaml".to_string()));
    }

    #[test]
    fn network_ops_include_body_rule_ref() {
        let rule = NetworkRuleFile {
            _rule_type: "network".to_string(),
            request: NetworkRequest {
                method: "POST".to_string(),
                url: json!("https://example.com"),
            },
            body_rule: Some("./body.yaml".to_string()),
        };
        let data_dir = Path::new("/tmp/rules");
        let rule_path = Path::new("/tmp/rules/api_rules/network.yaml");
        let ops = network_ops(&rule, data_dir, rule_path);
        let body_op = ops
            .iter()
            .find(|op| op.label == "body_rule")
            .expect("body_rule op");
        assert_eq!(body_op.refs, vec!["api_rules/body.yaml".to_string()]);
    }

    #[test]
    fn normal_ops_include_branch_refs() {
        let yaml = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: ["@input.kind", "a"] }
      then: ./then.yaml
      else: ./else.yaml
"#;
        let rule = parse_rule_file(yaml).expect("parse rule");
        let data_dir = Path::new("/tmp/rules");
        let rule_path = Path::new("/tmp/rules/api_rules/rule.yaml");
        let ops = normal_ops(&rule, data_dir, rule_path);
        let branch_op = ops
            .iter()
            .find(|op| op.label.contains("branch"))
            .expect("branch op");
        assert!(branch_op.refs.contains(&"api_rules/then.yaml".to_string()));
        assert!(branch_op.refs.contains(&"api_rules/else.yaml".to_string()));
    }

    #[test]
    fn normal_ops_include_finalize() {
        let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings: []
finalize:
  filter: { eq: ["@input.kind", "a"] }
  limit: 10
"#;
        let rule = parse_rule_file(yaml).expect("parse rule");
        let data_dir = Path::new("/tmp/rules");
        let rule_path = Path::new("/tmp/rules/api_rules/rule.yaml");
        let ops = normal_ops(&rule, data_dir, rule_path);
        let finalize = ops
            .iter()
            .find(|op| op.label == "finalize")
            .expect("finalize op");
        let detail = finalize.detail.as_deref().unwrap_or("");
        assert!(detail.contains("filter"));
        assert!(detail.contains("limit"));
    }

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
}
