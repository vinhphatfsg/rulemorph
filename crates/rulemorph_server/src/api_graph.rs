use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::Result;
use rulemorph::serde_guard::parse_yaml_value_strict;
use rulemorph::{RuleFormat, parse_rule_file_with_format};
use serde::Serialize;
use serde_json::Value as JsonValue;
use serde_yaml::Value as YamlValue;
use walkdir::WalkDir;

mod ops;

use self::ops::{endpoint_ops, network_ops, normal_ops, resolve_rule_path};

#[derive(Debug, Serialize)]
pub struct ApiGraphResponse {
    pub nodes: Vec<ApiGraphNode>,
    pub edges: Vec<ApiGraphEdge>,
}

#[derive(Debug, Serialize)]
pub struct ApiGraphNode {
    pub id: String,
    pub label: String,
    pub kind: String,
    pub path: String,
    pub ops: Vec<ApiGraphOp>,
}

#[derive(Debug, Serialize)]
pub struct ApiGraphOp {
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub refs: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ApiGraphEdge {
    pub source: String,
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub kind: String,
}

#[derive(Debug, serde::Deserialize)]
struct EndpointRuleFile {
    #[serde(rename = "type")]
    _rule_type: String,
    #[serde(default)]
    endpoints: Vec<EndpointDef>,
}

#[derive(Debug, serde::Deserialize)]
struct EndpointDef {
    method: String,
    path: String,
    #[serde(default)]
    steps: Vec<EndpointStep>,
}

#[derive(Debug, serde::Deserialize)]
struct EndpointStep {
    rule: String,
}

#[derive(Debug, serde::Deserialize)]
struct NetworkRuleFile {
    #[serde(rename = "type")]
    _rule_type: String,
    request: NetworkRequest,
    #[serde(default)]
    body_rule: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct NetworkRequest {
    method: String,
    url: JsonValue,
}

pub fn build_api_graph(data_dir: &Path) -> Result<ApiGraphResponse> {
    let data_dir = normalize_path(data_dir);
    let mut nodes: HashMap<String, ApiGraphNode> = HashMap::new();
    let mut edges: Vec<ApiGraphEdge> = Vec::new();
    let mut edge_keys: HashSet<String> = HashSet::new();

    let yaml_files = collect_rule_files(&data_dir);
    for path in yaml_files {
        let path = normalize_path(&path);
        let raw = match std::fs::read_to_string(&path) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let yaml_value: YamlValue = match parse_yaml_value_strict(&raw) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let rule_type = yaml_value
            .get("type")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        if rule_type == "endpoint" {
            let endpoint: EndpointRuleFile = match serde_yaml::from_value(yaml_value) {
                Ok(value) => value,
                Err(_) => continue,
            };
            let node_id = rule_id(&data_dir, &path);
            let label = format!("endpoint · {}", rule_label(&path));
            nodes.insert(
                node_id.clone(),
                ApiGraphNode {
                    id: node_id.clone(),
                    label,
                    kind: "endpoint".to_string(),
                    path: rule_path_display(&data_dir, &path),
                    ops: endpoint_ops(&endpoint, &data_dir, &path),
                },
            );

            let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
            for endpoint_def in &endpoint.endpoints {
                let label = format!("{} {}", endpoint_def.method, endpoint_def.path);
                for step in &endpoint_def.steps {
                    let target_path = normalize_path(&resolve_rule_path(base_dir, &step.rule));
                    let target_id = rule_id(&data_dir, &target_path);
                    if !nodes.contains_key(&target_id) {
                        insert_placeholder(&mut nodes, &data_dir, &target_path);
                    }
                    push_edge(
                        &mut edges,
                        &mut edge_keys,
                        &node_id,
                        &target_id,
                        Some(label.clone()),
                        "endpoint",
                    );
                }
            }
            continue;
        }

        if rule_type == "network" {
            let network: NetworkRuleFile = match serde_yaml::from_value(yaml_value) {
                Ok(value) => value,
                Err(_) => continue,
            };
            let node_id = rule_id(&data_dir, &path);
            let label = format!("network · {}", rule_label(&path));
            nodes.insert(
                node_id.clone(),
                ApiGraphNode {
                    id: node_id.clone(),
                    label,
                    kind: "network".to_string(),
                    path: rule_path_display(&data_dir, &path),
                    ops: network_ops(&network, &data_dir, &path),
                },
            );
            if let Some(body_rule) = network.body_rule.as_ref() {
                let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
                let target_path = normalize_path(&resolve_rule_path(base_dir, body_rule));
                let target_id = rule_id(&data_dir, &target_path);
                if !nodes.contains_key(&target_id) {
                    insert_placeholder(&mut nodes, &data_dir, &target_path);
                }
                push_edge(
                    &mut edges,
                    &mut edge_keys,
                    &node_id,
                    &target_id,
                    Some("body_rule".to_string()),
                    "ref",
                );
            }
            continue;
        }

        // Try normal rule (v2)
        if let Ok(rule) = parse_rule_file_with_format(&raw, RuleFormat::from_path(&path)) {
            let node_id = rule_id(&data_dir, &path);
            let label = format!("normal · {}", rule_label(&path));
            nodes.insert(
                node_id.clone(),
                ApiGraphNode {
                    id: node_id.clone(),
                    label,
                    kind: "normal".to_string(),
                    path: rule_path_display(&data_dir, &path),
                    ops: normal_ops(&rule, &data_dir, &path),
                },
            );
            // branch references
            let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
            if let Some(steps) = rule.steps.as_ref() {
                for step in steps {
                    if let Some(branch) = step.branch.as_ref() {
                        let then_path = normalize_path(&resolve_rule_path(base_dir, &branch.then));
                        let then_id = rule_id(&data_dir, &then_path);
                        if !nodes.contains_key(&then_id) {
                            insert_placeholder(&mut nodes, &data_dir, &then_path);
                        }
                        push_edge(
                            &mut edges,
                            &mut edge_keys,
                            &node_id,
                            &then_id,
                            Some("branch: then".to_string()),
                            "branch",
                        );
                        if let Some(other) = branch.r#else.as_ref() {
                            let else_path = normalize_path(&resolve_rule_path(base_dir, &other));
                            let else_id = rule_id(&data_dir, &else_path);
                            if !nodes.contains_key(&else_id) {
                                insert_placeholder(&mut nodes, &data_dir, &else_path);
                            }
                            push_edge(
                                &mut edges,
                                &mut edge_keys,
                                &node_id,
                                &else_id,
                                Some("branch: else".to_string()),
                                "branch",
                            );
                        }
                    }
                }
            }
        }
    }

    Ok(ApiGraphResponse {
        nodes: nodes.into_values().collect(),
        edges,
    })
}

fn collect_rule_files(data_dir: &Path) -> Vec<PathBuf> {
    WalkDir::new(data_dir)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext == "yaml" || ext == "yml" || ext == "json")
                .unwrap_or(false)
        })
        .map(|entry| entry.path().to_path_buf())
        .collect()
}

fn rule_id(data_dir: &Path, path: &Path) -> String {
    let path = normalize_path(path);
    let data_dir = normalize_path(data_dir);
    if let Ok(rel) = path.strip_prefix(&data_dir) {
        rel.to_string_lossy().replace('\\', "/")
    } else {
        path.to_string_lossy().replace('\\', "/")
    }
}

fn rule_path_display(data_dir: &Path, path: &Path) -> String {
    rule_id(data_dir, path)
}

fn rule_label(path: &Path) -> String {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("rule")
        .to_string()
}

fn insert_placeholder(nodes: &mut HashMap<String, ApiGraphNode>, data_dir: &Path, path: &Path) {
    let id = rule_id(data_dir, path);
    nodes.entry(id.clone()).or_insert(ApiGraphNode {
        id,
        label: format!("missing · {}", rule_label(path)),
        kind: "missing".to_string(),
        path: rule_path_display(data_dir, path),
        ops: Vec::new(),
    });
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                result.pop();
            }
            other => result.push(other.as_os_str()),
        }
    }
    result
}

fn push_edge(
    edges: &mut Vec<ApiGraphEdge>,
    edge_keys: &mut HashSet<String>,
    source: &str,
    target: &str,
    label: Option<String>,
    kind: &str,
) {
    let key = format!("{}::{}::{}", source, target, label.as_deref().unwrap_or(""));
    if edge_keys.contains(&key) {
        return;
    }
    edge_keys.insert(key);
    edges.push(ApiGraphEdge {
        source: source.to_string(),
        target: target.to_string(),
        label,
        kind: kind.to_string(),
    });
}

#[cfg(test)]
mod tests {
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
