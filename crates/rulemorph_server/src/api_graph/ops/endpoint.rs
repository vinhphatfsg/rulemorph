use std::path::Path;

use super::resolve_rule_path;
use crate::api_graph::ApiGraphOp;
use crate::api_graph::primitives::{normalize_path, rule_id};

#[derive(Debug, serde::Deserialize)]
pub(in crate::api_graph) struct EndpointRuleFile {
    #[serde(rename = "type")]
    pub(in crate::api_graph) _rule_type: String,
    #[serde(default)]
    pub(in crate::api_graph) endpoints: Vec<EndpointDef>,
}

#[derive(Debug, serde::Deserialize)]
pub(in crate::api_graph) struct EndpointDef {
    pub(in crate::api_graph) method: String,
    pub(in crate::api_graph) path: String,
    #[serde(default)]
    pub(in crate::api_graph) steps: Vec<EndpointStep>,
}

#[derive(Debug, serde::Deserialize)]
pub(in crate::api_graph) struct EndpointStep {
    pub(in crate::api_graph) rule: String,
}

pub(in crate::api_graph) fn endpoint_ops(
    rule: &EndpointRuleFile,
    data_dir: &Path,
    endpoint_path: &Path,
) -> Vec<ApiGraphOp> {
    let base_dir = endpoint_path.parent().unwrap_or_else(|| Path::new("."));
    rule.endpoints
        .iter()
        .map(|endpoint| {
            let refs = endpoint
                .steps
                .iter()
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
