use std::collections::HashSet;

use rulemorph::Expr;
use serde_json::{Value, json};

pub(crate) fn collect_missing_refs(
    target: &str,
    expr: Option<&Expr>,
    when: Option<&Expr>,
    input_paths: &HashSet<String>,
    out: &mut Vec<Value>,
    seen: &mut HashSet<String>,
) {
    for expr in [expr, when] {
        let Some(expr) = expr else { continue };
        let mut refs = Vec::new();
        collect_expr_refs(expr, &mut refs);
        for reference in refs {
            let Some(path) = input_ref_path(&reference) else {
                continue;
            };
            if input_paths.contains(&path) {
                continue;
            }
            let key = format!("{}|{}", target, reference);
            if seen.insert(key) {
                out.push(json!({
                    "target": target,
                    "ref": reference,
                    "path": path
                }));
            }
        }
    }
}

fn collect_expr_refs(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Ref(reference) => out.push(reference.ref_path.clone()),
        Expr::Op(op) => {
            for arg in &op.args {
                collect_expr_refs(arg, out);
            }
        }
        Expr::Chain(chain) => {
            for item in &chain.chain {
                collect_expr_refs(item, out);
            }
        }
        Expr::Literal(_) => {}
    }
}

fn input_ref_path(reference: &str) -> Option<String> {
    let trimmed = reference.trim();
    if let Some(rest) = trimmed.strip_prefix("input.") {
        if rest.is_empty() {
            None
        } else {
            Some(rest.to_string())
        }
    } else {
        None
    }
}
