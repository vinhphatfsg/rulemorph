use serde_json::Value as JsonValue;

use crate::model::{CustomOpDef, Expr, Mapping, RuleFile, RuleType, RuleTypeKind};
use crate::path::{PathToken, parse_path};
use crate::v2_model::{
    V2CustomCallStep, V2Expr, V2IfStep, V2LetStep, V2MapStep, V2ObjectFieldValue, V2ObjectStep,
    V2OpStep, V2Pipe, V2Ref, V2Start, V2Step,
};
use crate::v2_parser::{
    custom_call_step_candidate, is_literal_escape, is_pipe_value, is_v2_ref,
    parse_custom_call_step, parse_v2_pipe_from_value,
};

use super::schema::{Field, FieldType, PrimitiveType, SchemaNode};

const DTO_INFER_MAX_DEPTH: usize = 64;
const DTO_INFER_MAX_NODES: usize = 4096;
const DTO_INFER_MAX_OBJECT_FIELDS: usize = 256;
const DTO_INFER_MAX_ARRAY_ITEMS: usize = 64;
const DTO_INFER_MAX_PIPE_STEPS: usize = 1024;
const DTO_INFER_MAX_GENERATED_TYPES: usize = 512;
const DTO_INFER_MAX_PATH_BYTES: usize = 1024;
const DTO_INFER_MAX_PATH_TOKENS: usize = 64;

mod custom;
mod expr_json;
mod merge;
mod op_dispatch;
mod path_helpers;
mod projection;
mod state;
mod step_helpers;

use custom::*;
use expr_json::*;
use merge::*;
use op_dispatch::*;
use path_helpers::*;
use projection::*;
pub(super) use state::InferenceState;
use state::*;
use step_helpers::*;

pub(super) fn infer_mapping_field_type(
    mapping: &Mapping,
    rule: &RuleFile,
    state: &mut InferenceState,
) -> FieldType {
    if let Some(explicit) = mapping
        .value_type
        .as_deref()
        .and_then(type_from_mapping_type)
    {
        return explicit;
    }

    let primary = if let Some(value) = &mapping.value {
        infer_json_value(value, state, 0)
    } else if let Some(expr) = &mapping.expr {
        infer_expr(expr, rule, state)
    } else {
        FieldType::JsonValue
    };

    match (&primary, &mapping.default) {
        (FieldType::JsonValue, _) => FieldType::JsonValue,
        (_, Some(default)) => merge_types(primary, infer_json_value(default, state, 0)),
        (_, None) => primary,
    }
}

pub(super) fn remember_mapping_type(
    state: &mut InferenceState,
    target_keys: &[String],
    field_type: &FieldType,
) {
    state
        .produced
        .insert(target_keys.to_vec(), field_type.clone());
}

pub(super) fn type_from_mapping_type(value_type: &str) -> Option<FieldType> {
    match value_type {
        "string" => Some(FieldType::Primitive(PrimitiveType::String)),
        "int" => Some(FieldType::Primitive(PrimitiveType::Int)),
        "float" => Some(FieldType::Primitive(PrimitiveType::Float)),
        "bool" => Some(FieldType::Primitive(PrimitiveType::Bool)),
        _ => None,
    }
}

fn infer_json_value(value: &JsonValue, state: &mut InferenceState, depth: usize) -> FieldType {
    if !state.enter_node(depth) {
        return FieldType::JsonValue;
    }

    match value {
        JsonValue::Null => FieldType::Nullable(Box::new(FieldType::JsonValue)),
        JsonValue::Bool(_) => FieldType::Primitive(PrimitiveType::Bool),
        JsonValue::Number(number) => {
            if number.as_i64().is_some() {
                FieldType::Primitive(PrimitiveType::Int)
            } else if number.as_u64().is_some() {
                FieldType::JsonValue
            } else {
                FieldType::Primitive(PrimitiveType::Float)
            }
        }
        JsonValue::String(_) => FieldType::Primitive(PrimitiveType::String),
        JsonValue::Array(items) => {
            if items.len() > DTO_INFER_MAX_ARRAY_ITEMS {
                return FieldType::Array(Box::new(FieldType::JsonValue));
            }
            let item_type = items
                .iter()
                .map(|item| infer_json_value(item, state, depth + 1))
                .reduce(merge_types)
                .unwrap_or(FieldType::JsonValue);
            FieldType::Array(Box::new(item_type))
        }
        JsonValue::Object(map) => {
            if map.len() > DTO_INFER_MAX_OBJECT_FIELDS || !state.reserve_generated_type() {
                return FieldType::JsonValue;
            }
            let fields = map
                .iter()
                .map(|(key, value)| Field {
                    key: key.clone(),
                    field_type: infer_json_value(value, state, depth + 1),
                    optional: false,
                    synthetic: false,
                })
                .collect();
            FieldType::Object(Box::new(SchemaNode { fields }))
        }
    }
}

fn infer_expr(expr: &Expr, rule: &RuleFile, state: &mut InferenceState) -> FieldType {
    infer_expr_with_scope(expr, rule, state, Scope::new(), 0)
}

fn infer_expr_with_scope(
    expr: &Expr,
    rule: &RuleFile,
    state: &mut InferenceState,
    scope: Scope,
    depth: usize,
) -> FieldType {
    if !state.enter_node(depth) {
        return FieldType::JsonValue;
    }
    let Some(value) = expr_to_json_for_v2_pipe_bounded(expr, state, 0) else {
        return FieldType::JsonValue;
    };
    if !pipe_json_shape_is_bounded(&value) {
        return FieldType::JsonValue;
    }
    let Ok(pipe) = parse_v2_pipe_from_value(&value) else {
        return FieldType::JsonValue;
    };
    if pipe.steps.len() > DTO_INFER_MAX_PIPE_STEPS {
        return FieldType::JsonValue;
    }
    infer_pipe(&pipe, rule, state, scope, depth + 1)
}

fn infer_v2_expr_with_scope(
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
        V2Expr::V1Fallback(expr) => infer_expr_with_scope(expr, rule, state, scope, depth + 1),
    }
}

fn infer_pipe(
    pipe: &V2Pipe,
    rule: &RuleFile,
    state: &mut InferenceState,
    mut scope: Scope,
    depth: usize,
) -> FieldType {
    if !state.enter_node(depth) {
        return FieldType::JsonValue;
    }

    let mut current = match parse_known_custom_call_literal_start(rule, &pipe.start) {
        Some(Ok(call)) => infer_custom_call(&call.op, rule, state, depth + 1),
        Some(Err(_)) => FieldType::JsonValue,
        None => infer_start(&pipe.start, state, &scope, depth + 1),
    };
    scope.pipe = current.clone();
    for step in &pipe.steps {
        if !state.enter_node(depth + 1) {
            return FieldType::JsonValue;
        }
        current = infer_step(step, rule, state, &mut scope, current, depth + 1);
        scope.pipe = current.clone();
    }
    current
}

fn infer_start(
    start: &V2Start,
    state: &mut InferenceState,
    scope: &Scope,
    depth: usize,
) -> FieldType {
    if !state.enter_node(depth) {
        return FieldType::JsonValue;
    }
    match start {
        V2Start::Ref(value_ref) => infer_ref(value_ref, state, scope, depth + 1),
        V2Start::PipeValue | V2Start::ImplicitPipeValue => scope.pipe.clone(),
        V2Start::Literal(value) => infer_json_value(value, state, depth + 1),
        V2Start::V1Expr(_) => FieldType::JsonValue,
    }
}

fn parse_known_custom_call_literal_start(
    rule: &RuleFile,
    start: &V2Start,
) -> Option<Result<V2CustomCallStep, crate::v2_parser::V2ParseError>> {
    let V2Start::Literal(value) = start else {
        return None;
    };
    let (op_name, args_val) = custom_call_step_candidate(value)?;
    if !rule.defs.contains_key(op_name) {
        return None;
    }
    match parse_custom_call_step(op_name, args_val) {
        Ok(Some(call)) => Some(Ok(call)),
        Ok(None) => Some(Err(crate::v2_parser::V2ParseError::InvalidStep(
            "custom op call must use with call options".to_string(),
        ))),
        Err(err) => Some(Err(err)),
    }
}

fn infer_step(
    step: &V2Step,
    rule: &RuleFile,
    state: &mut InferenceState,
    scope: &mut Scope,
    input_type: FieldType,
    depth: usize,
) -> FieldType {
    if !state.enter_node(depth) {
        return FieldType::JsonValue;
    }
    match step {
        V2Step::Op(op_step) => infer_op(op_step, rule, state, scope, input_type, depth + 1),
        V2Step::Object(object_step) => {
            infer_object_step(object_step, rule, state, scope, input_type, depth + 1)
        }
        V2Step::CustomCall(call_step) => infer_custom_call(&call_step.op, rule, state, depth + 1),
        V2Step::Let(let_step) => {
            infer_let_step(let_step, rule, state, scope, input_type, depth + 1)
        }
        V2Step::If(if_step) => infer_if_step(if_step, rule, state, scope, input_type, depth + 1),
        V2Step::Map(map_step) => {
            infer_map_step(map_step, rule, state, scope, input_type, depth + 1)
        }
        V2Step::Ref(value_ref) => infer_ref(value_ref, state, scope, depth + 1),
    }
}

fn infer_object_step(
    object_step: &V2ObjectStep,
    rule: &RuleFile,
    state: &mut InferenceState,
    scope: &Scope,
    input_type: FieldType,
    depth: usize,
) -> FieldType {
    if !state.enter_node(depth)
        || object_step.fields.len() > DTO_INFER_MAX_OBJECT_FIELDS
        || !state.reserve_generated_type()
    {
        return FieldType::JsonValue;
    }

    let mut field_scope = scope.clone();
    field_scope.pipe = input_type;
    let fields = object_step
        .fields
        .iter()
        .map(|field| {
            let field_type = match &field.value {
                V2ObjectFieldValue::Expr(expr) => {
                    infer_v2_expr_with_scope(expr, rule, state, field_scope.clone(), depth + 1)
                }
                V2ObjectFieldValue::Value(value) => infer_json_value(value, state, depth + 1),
            };
            Field {
                key: field.key.clone(),
                field_type,
                optional: true,
                synthetic: false,
            }
        })
        .collect();

    FieldType::Object(Box::new(SchemaNode { fields }))
}
