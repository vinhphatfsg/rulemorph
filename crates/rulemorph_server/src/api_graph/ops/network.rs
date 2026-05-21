use std::path::Path;

use serde_json::Value as JsonValue;

use super::resolve_rule_path;
use crate::api_graph::ApiGraphOp;
use crate::api_graph::primitives::{normalize_path, rule_id};

#[derive(Debug, serde::Deserialize)]
pub(in crate::api_graph) struct NetworkRuleFile {
    #[serde(rename = "type")]
    pub(in crate::api_graph) _rule_type: String,
    pub(in crate::api_graph) request: NetworkRequest,
    #[serde(default)]
    pub(in crate::api_graph) body_rule: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub(in crate::api_graph) struct NetworkRequest {
    pub(in crate::api_graph) method: String,
    pub(in crate::api_graph) url: JsonValue,
}

pub(in crate::api_graph) fn network_ops(
    rule: &NetworkRuleFile,
    data_dir: &Path,
    rule_path: &Path,
) -> Vec<ApiGraphOp> {
    let mut ops = Vec::new();
    let url = serde_json::to_string(&rule.request.url).unwrap_or_else(|_| "\"?\"".to_string());
    ops.push(ApiGraphOp {
        label: "request".to_string(),
        detail: Some(format!("{} {}", rule.request.method, url)),
        refs: Vec::new(),
    });
    if let Some(body_rule) = rule.body_rule.as_ref() {
        let base_dir = rule_path.parent().unwrap_or_else(|| Path::new("."));
        let target = normalize_path(&resolve_rule_path(base_dir, body_rule));
        ops.push(ApiGraphOp {
            label: "body_rule".to_string(),
            detail: Some(body_rule.to_string()),
            refs: vec![rule_id(data_dir, &target)],
        });
    }
    ops
}
