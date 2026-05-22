use std::collections::{HashMap, HashSet};

use crate::error::ErrorCode;
use crate::v2_model::{V2Condition, V2Expr, V2Pipe, V2Ref, V2Start, V2Step};

use super::V2ValidationCtx;

/// Collect all @out references from a v2 expression
pub fn collect_out_references(expr: &V2Expr) -> HashSet<String> {
    let mut refs = HashSet::new();
    collect_out_refs_recursive(expr, &mut refs);
    refs
}

fn collect_out_refs_recursive(expr: &V2Expr, refs: &mut HashSet<String>) {
    match expr {
        V2Expr::Pipe(pipe) => {
            collect_out_refs_from_start(&pipe.start, refs);
            for step in &pipe.steps {
                collect_out_refs_from_step(step, refs);
            }
        }
        V2Expr::V1Fallback(_) => {}
    }
}

fn collect_out_refs_from_start(start: &V2Start, refs: &mut HashSet<String>) {
    match start {
        V2Start::Ref(V2Ref::Out(path)) => {
            if !path.is_empty() {
                refs.insert(path.clone());
            }
        }
        _ => {}
    }
}

fn collect_out_refs_from_step(step: &V2Step, refs: &mut HashSet<String>) {
    match step {
        V2Step::Op(op_step) => {
            for arg in &op_step.args {
                collect_out_refs_recursive(arg, refs);
            }
        }
        V2Step::Let(let_step) => {
            for (_, expr) in &let_step.bindings {
                collect_out_refs_recursive(expr, refs);
            }
        }
        V2Step::If(if_step) => {
            collect_out_refs_from_condition(&if_step.cond, refs);
            collect_out_refs_from_pipe(&if_step.then_branch, refs);
            if let Some(ref else_branch) = if_step.else_branch {
                collect_out_refs_from_pipe(else_branch, refs);
            }
        }
        V2Step::Map(map_step) => {
            for step in &map_step.steps {
                collect_out_refs_from_step(step, refs);
            }
        }
        V2Step::Ref(V2Ref::Out(path)) => {
            if !path.is_empty() {
                refs.insert(path.clone());
            }
        }
        V2Step::Ref(_) => {}
    }
}

fn collect_out_refs_from_pipe(pipe: &V2Pipe, refs: &mut HashSet<String>) {
    collect_out_refs_from_start(&pipe.start, refs);
    for step in &pipe.steps {
        collect_out_refs_from_step(step, refs);
    }
}

fn collect_out_refs_from_condition(cond: &V2Condition, refs: &mut HashSet<String>) {
    match cond {
        V2Condition::All(conditions) | V2Condition::Any(conditions) => {
            for c in conditions {
                collect_out_refs_from_condition(c, refs);
            }
        }
        V2Condition::Comparison(comp) => {
            for arg in &comp.args {
                collect_out_refs_recursive(arg, refs);
            }
        }
        V2Condition::Expr(expr) => {
            collect_out_refs_recursive(expr, refs);
        }
    }
}

/// Check for cyclic dependencies among mappings
pub fn validate_no_cyclic_dependencies(
    targets_with_deps: &[(String, HashSet<String>)],
    base_path: &str,
    ctx: &mut V2ValidationCtx<'_>,
) {
    // Build adjacency list: target -> depends on targets
    let graph: HashMap<String, HashSet<String>> = targets_with_deps.iter().cloned().collect();

    // Detect cycles using DFS
    let mut visited: HashSet<String> = HashSet::new();
    let mut rec_stack: HashSet<String> = HashSet::new();

    for (target, _) in targets_with_deps {
        if has_cycle(target, &graph, &mut visited, &mut rec_stack) {
            ctx.push_error(
                ErrorCode::CyclicDependency,
                format!("cyclic dependency detected involving target: {}", target),
                &format!("{}.{}", base_path, target),
            );
        }
    }
}

fn has_cycle(
    node: &str,
    graph: &HashMap<String, HashSet<String>>,
    visited: &mut HashSet<String>,
    rec_stack: &mut HashSet<String>,
) -> bool {
    if rec_stack.contains(node) {
        return true;
    }
    if visited.contains(node) {
        return false;
    }

    visited.insert(node.to_string());
    rec_stack.insert(node.to_string());

    if let Some(deps) = graph.get(node) {
        for dep in deps {
            if has_cycle(dep, graph, visited, rec_stack) {
                return true;
            }
        }
    }

    rec_stack.remove(node);
    false
}

#[cfg(test)]
mod tests;
