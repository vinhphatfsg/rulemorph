use std::path::{Path, PathBuf};

use rulemorph::{Expr, ExprChain, ExprOp, ExprRef, Mapping, RuleFile};
use serde_json::Value as JsonValue;

use super::ApiGraphOp;
use super::primitives::{normalize_path, rule_id};

#[derive(Debug, serde::Deserialize)]
pub(super) struct EndpointRuleFile {
    #[serde(rename = "type")]
    pub(super) _rule_type: String,
    #[serde(default)]
    pub(super) endpoints: Vec<EndpointDef>,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct EndpointDef {
    pub(super) method: String,
    pub(super) path: String,
    #[serde(default)]
    pub(super) steps: Vec<EndpointStep>,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct EndpointStep {
    pub(super) rule: String,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct NetworkRuleFile {
    #[serde(rename = "type")]
    pub(super) _rule_type: String,
    pub(super) request: NetworkRequest,
    #[serde(default)]
    pub(super) body_rule: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct NetworkRequest {
    pub(super) method: String,
    pub(super) url: JsonValue,
}

pub(super) fn endpoint_ops(
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

pub(super) fn network_ops(
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

pub(super) fn normal_ops(rule: &RuleFile, data_dir: &Path, rule_path: &Path) -> Vec<ApiGraphOp> {
    let mut ops = Vec::new();
    let base_dir = rule_path.parent().unwrap_or_else(|| Path::new("."));
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
                let then_path = normalize_path(&resolve_rule_path(base_dir, &branch.then));
                let mut refs = vec![rule_id(data_dir, &then_path)];
                if let Some(other) = branch.r#else.as_ref() {
                    let else_path = normalize_path(&resolve_rule_path(base_dir, other));
                    refs.push(rule_id(data_dir, &else_path));
                }
                ops.push(ApiGraphOp {
                    label: format!("{} · branch", prefix),
                    detail: Some(format!("then: {}", branch.then)),
                    refs,
                });
            }
        }
    }
    if let Some(finalize) = rule.finalize.as_ref() {
        let mut parts = Vec::new();
        if finalize.filter.is_some() {
            parts.push("filter");
        }
        if finalize.sort.is_some() {
            parts.push("sort");
        }
        if finalize.limit.is_some() {
            parts.push("limit");
        }
        if finalize.offset.is_some() {
            parts.push("offset");
        }
        if finalize.wrap.is_some() {
            parts.push("wrap");
        }
        let detail = if parts.is_empty() {
            "enabled".to_string()
        } else {
            parts.join(", ")
        };
        ops.push(ApiGraphOp {
            label: "finalize".to_string(),
            detail: Some(detail),
            refs: Vec::new(),
        });
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

pub(super) fn resolve_rule_path(base_dir: &Path, rule: &str) -> PathBuf {
    let path = PathBuf::from(rule);
    if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
    }
}
