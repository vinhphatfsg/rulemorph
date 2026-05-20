use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::Result;
use rulemorph::serde_guard::parse_yaml_value_strict;
use rulemorph::{RuleFormat, parse_rule_file_with_format};
use serde_yaml::Value as YamlValue;

use super::dto::{ApiGraphEdge, ApiGraphNode, ApiGraphResponse};
use super::ops::{
    EndpointRuleFile, NetworkRuleFile, endpoint_ops, network_ops, normal_ops, resolve_rule_path,
};
use super::primitives::{
    collect_rule_files, insert_placeholder, normalize_path, push_edge, rule_id, rule_label,
    rule_path_display,
};

pub(super) fn build_api_graph(data_dir: &Path) -> Result<ApiGraphResponse> {
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
                            let else_path = normalize_path(&resolve_rule_path(base_dir, other));
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
