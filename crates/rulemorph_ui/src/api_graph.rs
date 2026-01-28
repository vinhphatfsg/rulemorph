use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::Result;
use rulemorph::{parse_rule_file, Expr, ExprChain, ExprOp, ExprRef, Mapping, RuleFile};
use serde::Serialize;
use serde_json::Value as JsonValue;
use serde_yaml::Value as YamlValue;
use walkdir::WalkDir;

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

    let yaml_files = collect_yaml_files(&data_dir);
    for path in yaml_files {
        let path = normalize_path(&path);
        let raw = match std::fs::read_to_string(&path) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let yaml_value: YamlValue = match serde_yaml::from_str(&raw) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let rule_type = yaml_value
            .get("type")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        if rule_type == "endpoint" {
            let endpoint: EndpointRuleFile = match serde_yaml::from_str(&raw) {
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
            let network: NetworkRuleFile = match serde_yaml::from_str(&raw) {
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
                    ops: network_ops(&network),
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
        if let Ok(rule) = parse_rule_file(&raw) {
            let node_id = rule_id(&data_dir, &path);
            let label = format!("normal · {}", rule_label(&path));
            nodes.insert(
                node_id.clone(),
                ApiGraphNode {
                    id: node_id.clone(),
                    label,
                    kind: "normal".to_string(),
                    path: rule_path_display(&data_dir, &path),
                    ops: normal_ops(&rule),
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

fn collect_yaml_files(data_dir: &Path) -> Vec<PathBuf> {
    WalkDir::new(data_dir)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext == "yaml" || ext == "yml")
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

fn endpoint_ops(rule: &EndpointRuleFile, data_dir: &Path, endpoint_path: &Path) -> Vec<ApiGraphOp> {
    let base_dir = endpoint_path.parent().unwrap_or_else(|| Path::new("."));
    rule.endpoints
        .iter()
        .map(|endpoint| {
            let refs = endpoint
                .steps
                .iter()
                .take(1)
                .map(|step| {
                    let target = normalize_path(&resolve_rule_path(base_dir, &step.rule));
                    rule_id(data_dir, &target)
                })
                .collect::<Vec<_>>();
            ApiGraphOp {
                label: format!("{} {}", endpoint.method, endpoint.path),
                detail: Some(format!("steps: {}", endpoint.steps.len())),
                refs,
            }
        })
        .collect()
}

fn network_ops(rule: &NetworkRuleFile) -> Vec<ApiGraphOp> {
    let mut ops = Vec::new();
    let url = serde_json::to_string(&rule.request.url).unwrap_or_else(|_| "\"?\"".to_string());
    ops.push(ApiGraphOp {
        label: "request".to_string(),
        detail: Some(format!("{} {}", rule.request.method, url)),
        refs: Vec::new(),
    });
    if let Some(body_rule) = rule.body_rule.as_ref() {
        ops.push(ApiGraphOp {
            label: "body_rule".to_string(),
            detail: Some(body_rule.to_string()),
            refs: Vec::new(),
        });
    }
    ops
}

fn normal_ops(rule: &RuleFile) -> Vec<ApiGraphOp> {
    let mut ops = Vec::new();
    if !rule.mappings.is_empty() {
        push_mapping_ops(&mut ops, None, &rule.mappings);
    }
    if let Some(steps) = rule.steps.as_ref() {
        for step in steps {
            let prefix = step.name.clone().unwrap_or_else(|| "step".to_string());
            if let Some(mappings) = step.mappings.as_ref() {
                push_mapping_ops(&mut ops, Some(&prefix), mappings);
            }
            if let Some(branch) = step.branch.as_ref() {
                ops.push(ApiGraphOp {
                    label: format!("{} · branch", prefix),
                    detail: Some(format!("then: {}", branch.then)),
                    refs: Vec::new(),
                });
            }
        }
    }
    ops
}

fn push_mapping_ops(ops: &mut Vec<ApiGraphOp>, prefix: Option<&str>, mappings: &[Mapping]) {
    for mapping in mappings {
        let name = mapping.target.clone();
        if let Some(expr) = mapping.expr.as_ref() {
            let steps = expr_steps(expr);
            for step in steps {
                ops.push(ApiGraphOp {
                    label: format!("{} · {}", mapping_label(prefix, &name), step),
                    detail: None,
                    refs: Vec::new(),
                });
            }
        } else if let Some(source) = mapping.source.as_ref() {
            ops.push(ApiGraphOp {
                label: format!("{} · source", mapping_label(prefix, &name)),
                detail: Some(source.clone()),
                refs: Vec::new(),
            });
        } else if let Some(value) = mapping.value.as_ref() {
            let detail = serde_json::to_string(value).unwrap_or_else(|_| "literal".to_string());
            ops.push(ApiGraphOp {
                label: format!("{} · value", mapping_label(prefix, &name)),
                detail: Some(detail),
                refs: Vec::new(),
            });
        }
    }
}

fn mapping_label(prefix: Option<&str>, target: &str) -> String {
    match prefix {
        Some(prefix) => format!("{}/{}", prefix, target),
        None => target.to_string(),
    }
}

fn expr_steps(expr: &Expr) -> Vec<String> {
    match expr {
        Expr::Chain(ExprChain { chain }) => chain.iter().map(expr_step_label).collect(),
        _ => vec![expr_step_label(expr)],
    }
}

fn expr_step_label(expr: &Expr) -> String {
    match expr {
        Expr::Ref(ExprRef { ref_path }) => format!("ref {}", ref_path),
        Expr::Op(ExprOp { op, args }) => {
            if args.is_empty() {
                op.clone()
            } else {
                format!("{}(...)", op)
            }
        }
        Expr::Chain(_) => "chain".to_string(),
        Expr::Literal(_) => "literal".to_string(),
    }
}

fn resolve_rule_path(base_dir: &Path, rule: &str) -> PathBuf {
    let path = PathBuf::from(rule);
    if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
    }
}
