use super::*;

pub(super) fn expr_to_json_for_v2_pipe_bounded(
    expr: &Expr,
    state: &mut InferenceState,
    depth: usize,
) -> Option<JsonValue> {
    if !state.enter_node(depth) {
        return None;
    }
    match expr {
        Expr::Literal(value @ JsonValue::Array(items)) => {
            if items.len().saturating_sub(1) > DTO_INFER_MAX_PIPE_STEPS {
                return None;
            }
            clone_json_bounded(value, state, depth + 1, JsonCloneContext::PipeRoot)
        }
        Expr::Literal(JsonValue::String(value))
            if is_v2_ref(value) || is_pipe_value(value) || is_literal_escape(value) =>
        {
            Some(JsonValue::String(value.clone()))
        }
        Expr::Ref(expr_ref)
            if is_v2_ref(&expr_ref.ref_path)
                || is_pipe_value(&expr_ref.ref_path)
                || is_literal_escape(&expr_ref.ref_path) =>
        {
            Some(JsonValue::Array(vec![JsonValue::String(
                expr_ref.ref_path.clone(),
            )]))
        }
        Expr::Chain(chain) => {
            if chain.chain.len().saturating_sub(1) > DTO_INFER_MAX_PIPE_STEPS {
                return None;
            }
            if !chain.chain.first().is_some_and(expr_starts_v2_pipe) {
                return None;
            }
            let mut values = Vec::with_capacity(chain.chain.len());
            for item in &chain.chain {
                values.push(expr_to_json_value_bounded(item, state, depth + 1)?);
            }
            Some(JsonValue::Array(values))
        }
        _ => None,
    }
}

pub(super) fn expr_starts_v2_pipe(expr: &Expr) -> bool {
    match expr {
        Expr::Ref(reference) => {
            is_v2_ref(&reference.ref_path)
                || is_pipe_value(&reference.ref_path)
                || is_literal_escape(&reference.ref_path)
        }
        Expr::Literal(JsonValue::String(value)) => {
            is_v2_ref(value) || is_pipe_value(value) || is_literal_escape(value)
        }
        _ => false,
    }
}

pub(super) fn expr_to_json_value_bounded(
    expr: &Expr,
    state: &mut InferenceState,
    depth: usize,
) -> Option<JsonValue> {
    if !state.enter_node(depth) {
        return None;
    }
    match expr {
        Expr::Ref(value_ref) => Some(JsonValue::String(value_ref.ref_path.clone())),
        Expr::Literal(value) => {
            clone_json_bounded(value, state, depth + 1, JsonCloneContext::ExprValue)
        }
        Expr::Op(op) => {
            if op.args.len() > DTO_INFER_MAX_ARRAY_ITEMS {
                return None;
            }
            let mut args = Vec::with_capacity(op.args.len());
            for arg in &op.args {
                args.push(expr_to_json_value_bounded(arg, state, depth + 1)?);
            }
            let mut object = serde_json::Map::new();
            object.insert(op.op.clone(), JsonValue::Array(args));
            Some(JsonValue::Object(object))
        }
        Expr::Chain(chain) => {
            if chain.chain.len().saturating_sub(1) > DTO_INFER_MAX_PIPE_STEPS {
                return None;
            }
            let mut values = Vec::with_capacity(chain.chain.len());
            for item in &chain.chain {
                values.push(expr_to_json_value_bounded(item, state, depth + 1)?);
            }
            Some(JsonValue::Array(values))
        }
    }
}

pub(super) fn clone_json_bounded(
    value: &JsonValue,
    state: &mut InferenceState,
    depth: usize,
    context: JsonCloneContext,
) -> Option<JsonValue> {
    if !state.enter_node(depth) {
        return None;
    }
    match value {
        JsonValue::Array(items) => {
            let max_items = match context {
                JsonCloneContext::PipeRoot => DTO_INFER_MAX_PIPE_STEPS + 1,
                JsonCloneContext::ExprValue => DTO_INFER_MAX_ARRAY_ITEMS,
            };
            if items.len() > max_items {
                return None;
            }
            let mut cloned = Vec::with_capacity(items.len());
            for item in items {
                cloned.push(clone_json_bounded(
                    item,
                    state,
                    depth + 1,
                    JsonCloneContext::ExprValue,
                )?);
            }
            Some(JsonValue::Array(cloned))
        }
        JsonValue::Object(map) => {
            if map.len() > DTO_INFER_MAX_OBJECT_FIELDS {
                return None;
            }
            let mut cloned = serde_json::Map::new();
            for (key, value) in map {
                cloned.insert(
                    key.clone(),
                    clone_json_bounded(value, state, depth + 1, JsonCloneContext::ExprValue)?,
                );
            }
            Some(JsonValue::Object(cloned))
        }
        scalar => Some(scalar.clone()),
    }
}

#[derive(Clone, Copy)]
pub(super) enum JsonCloneContext {
    PipeRoot,
    ExprValue,
}

pub(super) fn pipe_json_shape_is_bounded(value: &JsonValue) -> bool {
    !matches!(value, JsonValue::Array(items) if items.len().saturating_sub(1) > DTO_INFER_MAX_PIPE_STEPS)
}
