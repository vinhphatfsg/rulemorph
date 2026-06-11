use super::*;

pub(super) fn validate_dependency_cycles(
    rule: &RuleFile,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
) {
    let mut graph: HashMap<String, HashSet<String>> = HashMap::new();
    for (name, def) in &rule.defs {
        let mut deps = HashSet::new();
        collect_def_dependencies(rule, def, &mut deps);
        graph.insert(name.clone(), deps);
    }
    let mut visited = HashSet::new();
    let mut stack = Vec::new();
    for name in rule.defs.keys() {
        if detects_cycle(name, &graph, &mut visited, &mut stack) {
            push_rule_error(
                errors,
                locator,
                ErrorCode::CyclicDependency,
                format!("cyclic custom op dependency involving `{}`", name),
                &format!("defs.{}", name),
            );
        }
    }
}

pub(super) fn collect_def_dependencies(
    rule: &RuleFile,
    def: &CustomOpDef,
    deps: &mut HashSet<String>,
) {
    if let Some(expr) = &def.expr
        && let Some(value) = crate::expr_json::expr_to_json_for_v2_pipe(expr)
        && let Ok(expr) = parse_v2_expr(&value)
    {
        collect_expr_dependencies(rule, &expr, deps);
    }
    if let Some(mappings) = &def.mappings {
        for mapping in mappings {
            if let Some(expr) = &mapping.expr
                && let Some(value) = crate::expr_json::expr_to_json_for_v2_pipe(expr)
                && let Ok(expr) = parse_v2_expr(&value)
            {
                collect_expr_dependencies(rule, &expr, deps);
            }
            if let Some(when) = &mapping.when
                && let Some(value) = crate::expr_json::expr_to_json_for_v2_condition(when)
                && let Ok(condition) = parse_v2_condition(&value)
            {
                collect_condition_dependencies(rule, &condition, deps);
            }
        }
    }
}

pub(super) fn collect_expr_dependencies(
    rule: &RuleFile,
    expr: &V2Expr,
    deps: &mut HashSet<String>,
) {
    let V2Expr::Pipe(pipe) = expr else {
        return;
    };
    collect_pipe_dependencies(rule, pipe, deps);
}

pub(super) fn collect_step_dependencies(
    rule: &RuleFile,
    step: &V2Step,
    deps: &mut HashSet<String>,
) {
    match step {
        V2Step::Op(op) => {
            if rule.defs.contains_key(&op.op) {
                deps.insert(op.op.clone());
            }
            for arg in &op.args {
                collect_expr_dependencies(rule, arg, deps);
            }
        }
        V2Step::Object(object) => {
            for field in &object.fields {
                if let V2ObjectFieldValue::Expr(expr) = &field.value {
                    collect_expr_dependencies(rule, expr, deps);
                }
            }
        }
        V2Step::CustomCall(call) => {
            collect_custom_call_dependencies(rule, call, deps);
        }
        V2Step::Let(let_step) => {
            for (_, expr) in &let_step.bindings {
                collect_expr_dependencies(rule, expr, deps);
            }
        }
        V2Step::If(if_step) => {
            collect_condition_dependencies(rule, &if_step.cond, deps);
            collect_pipe_dependencies(rule, &if_step.then_branch, deps);
            if let Some(else_branch) = &if_step.else_branch {
                collect_pipe_dependencies(rule, else_branch, deps);
            }
        }
        V2Step::Map(map_step) => {
            for step in &map_step.steps {
                collect_step_dependencies(rule, step, deps);
            }
        }
        V2Step::Ref(_) => {}
    }
}

pub(super) fn collect_pipe_dependencies(
    rule: &RuleFile,
    pipe: &V2Pipe,
    deps: &mut HashSet<String>,
) {
    if let Some(Ok(call)) = parse_known_custom_call_literal_start(rule, &pipe.start) {
        collect_custom_call_dependencies(rule, &call, deps);
    }
    for step in &pipe.steps {
        collect_step_dependencies(rule, step, deps);
    }
}

pub(super) fn collect_custom_call_dependencies(
    rule: &RuleFile,
    call: &V2CustomCallStep,
    deps: &mut HashSet<String>,
) {
    if rule.defs.contains_key(&call.op) {
        deps.insert(call.op.clone());
    }
    if let Some(with) = &call.with {
        for (_, arg) in with {
            if let V2CallArg::Expr(expr) = arg {
                collect_expr_dependencies(rule, expr, deps);
            }
        }
    }
}

pub(super) fn collect_condition_dependencies(
    rule: &RuleFile,
    condition: &V2Condition,
    deps: &mut HashSet<String>,
) {
    match condition {
        V2Condition::All(items) | V2Condition::Any(items) => {
            for item in items {
                collect_condition_dependencies(rule, item, deps);
            }
        }
        V2Condition::Comparison(comparison) => {
            for arg in &comparison.args {
                collect_expr_dependencies(rule, arg, deps);
            }
        }
        V2Condition::Expr(expr) => collect_expr_dependencies(rule, expr, deps),
    }
}

pub(super) fn detects_cycle(
    name: &str,
    graph: &HashMap<String, HashSet<String>>,
    visited: &mut HashSet<String>,
    stack: &mut Vec<String>,
) -> bool {
    if stack.iter().any(|entry| entry == name) {
        return true;
    }
    if !visited.insert(name.to_string()) {
        return false;
    }
    stack.push(name.to_string());
    if let Some(deps) = graph.get(name) {
        for dep in deps {
            if detects_cycle(dep, graph, visited, stack) {
                return true;
            }
        }
    }
    stack.pop();
    false
}
