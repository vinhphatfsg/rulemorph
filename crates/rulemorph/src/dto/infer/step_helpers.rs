use super::*;

pub(super) fn infer_let_step(
    let_step: &V2LetStep,
    rule: &RuleFile,
    state: &mut InferenceState,
    scope: &mut Scope,
    input_type: FieldType,
    depth: usize,
) -> FieldType {
    if !state.enter_node(depth) {
        return FieldType::JsonValue;
    }
    scope.pipe = input_type.clone();
    for (name, expr) in &let_step.bindings {
        let value_type = infer_arg_expr(expr, rule, state, scope.clone(), depth + 1);
        scope.locals.insert(name.clone(), value_type);
    }
    input_type
}

pub(super) fn infer_if_step(
    if_step: &V2IfStep,
    rule: &RuleFile,
    state: &mut InferenceState,
    scope: &Scope,
    input_type: FieldType,
    depth: usize,
) -> FieldType {
    if !state.enter_node(depth) {
        return FieldType::JsonValue;
    }
    let then_type = infer_pipe(
        &if_step.then_branch,
        rule,
        state,
        scope.clone().with_pipe(input_type.clone()),
        depth + 1,
    );
    let else_type = if_step
        .else_branch
        .as_ref()
        .map(|else_branch| {
            infer_pipe(
                else_branch,
                rule,
                state,
                scope.clone().with_pipe(input_type.clone()),
                depth + 1,
            )
        })
        .unwrap_or(input_type);
    merge_types(then_type, else_type)
}

pub(super) fn infer_map_step(
    map_step: &V2MapStep,
    rule: &RuleFile,
    state: &mut InferenceState,
    scope: &Scope,
    input_type: FieldType,
    depth: usize,
) -> FieldType {
    if !state.enter_node(depth) {
        return FieldType::JsonValue;
    }
    let element = array_element_type(&input_type);
    let mut item_scope = scope
        .clone()
        .with_pipe(element.clone())
        .with_item(Some(element));
    let mut current = item_scope.pipe.clone();
    for step in &map_step.steps {
        current = infer_step(step, rule, state, &mut item_scope, current, depth + 1);
        item_scope.pipe = current.clone();
    }
    FieldType::Array(Box::new(current))
}

pub(super) fn infer_arg_expr(
    expr: &V2Expr,
    rule: &RuleFile,
    state: &mut InferenceState,
    scope: Scope,
    depth: usize,
) -> FieldType {
    if !state.enter_node(depth) {
        return FieldType::JsonValue;
    }
    match expr {
        V2Expr::Pipe(pipe) => infer_pipe(pipe, rule, state, scope, depth + 1),
        V2Expr::V1Fallback(_) => FieldType::JsonValue,
    }
}

pub(super) fn infer_ref(
    value_ref: &V2Ref,
    state: &mut InferenceState,
    scope: &Scope,
    _depth: usize,
) -> FieldType {
    match value_ref {
        V2Ref::Input(path) => scoped_path_type(scope.input.as_ref(), path),
        V2Ref::Context(_) => FieldType::JsonValue,
        V2Ref::Pipe(path) => scoped_path_type(Some(&scope.pipe), path),
        V2Ref::Out(path) => match scope.out.as_ref() {
            Some(out) => scoped_path_type(Some(out), path),
            None => key_path(path)
                .filter(|keys| !keys.is_empty())
                .and_then(|keys| state.produced_type_for_ref(&keys))
                .unwrap_or(FieldType::JsonValue),
        },
        V2Ref::Item(path) => scoped_path_type(scope.item.as_ref(), path),
        V2Ref::Acc(path) => scoped_path_type(scope.acc.as_ref(), path),
        V2Ref::Local(name) => scope
            .locals
            .get(name)
            .cloned()
            .unwrap_or(FieldType::JsonValue),
    }
}
