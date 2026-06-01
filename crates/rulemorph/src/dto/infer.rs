use std::collections::HashMap;

use serde_json::Value as JsonValue;

use crate::model::{Expr, Mapping, RuleFile};
use crate::path::{PathToken, parse_path};
use crate::v2_model::{
    V2Expr, V2IfStep, V2LetStep, V2MapStep, V2OpStep, V2Pipe, V2Ref, V2Start, V2Step,
};
use crate::v2_parser::{is_literal_escape, is_pipe_value, is_v2_ref, parse_v2_pipe_from_value};

use super::schema::{Field, FieldType, PrimitiveType, SchemaNode};

const DTO_INFER_MAX_DEPTH: usize = 64;
const DTO_INFER_MAX_NODES: usize = 4096;
const DTO_INFER_MAX_OBJECT_FIELDS: usize = 256;
const DTO_INFER_MAX_ARRAY_ITEMS: usize = 64;
const DTO_INFER_MAX_PIPE_STEPS: usize = 1024;
const DTO_INFER_MAX_GENERATED_TYPES: usize = 512;
const DTO_INFER_MAX_PATH_BYTES: usize = 1024;
const DTO_INFER_MAX_PATH_TOKENS: usize = 64;

#[derive(Clone)]
pub(super) struct InferenceState {
    produced: HashMap<Vec<String>, FieldType>,
    budget: InferenceBudget,
}

impl Default for InferenceState {
    fn default() -> Self {
        Self {
            produced: HashMap::new(),
            budget: InferenceBudget::default(),
        }
    }
}

impl InferenceState {
    fn enter_node(&mut self, depth: usize) -> bool {
        self.budget.enter_node(depth)
    }

    fn reserve_generated_type(&mut self) -> bool {
        self.budget.reserve_generated_type()
    }

    fn reserve_generated_types(&mut self, count: usize) -> bool {
        self.budget.reserve_generated_types(count)
    }

    fn produced_type(&self, keys: &[String]) -> Option<FieldType> {
        for prefix_len in (1..=keys.len()).rev() {
            let prefix = &keys[..prefix_len];
            let Some(field_type) = self.produced.get(prefix) else {
                continue;
            };
            if prefix_len == keys.len() {
                return Some(field_type.clone());
            }
            return field_type_at_keys(field_type, &keys[prefix_len..]);
        }
        None
    }

    fn produced_type_for_ref(&mut self, keys: &[String]) -> Option<FieldType> {
        let field_type = self.produced_type(keys)?;
        let generated_types = generated_type_count(&field_type);
        if generated_types > 0 && !self.reserve_generated_types(generated_types) {
            return None;
        }
        Some(field_type)
    }
}

#[derive(Clone)]
struct InferenceBudget {
    remaining_nodes: usize,
    remaining_generated_types: usize,
}

impl Default for InferenceBudget {
    fn default() -> Self {
        Self {
            remaining_nodes: DTO_INFER_MAX_NODES,
            remaining_generated_types: DTO_INFER_MAX_GENERATED_TYPES,
        }
    }
}

impl InferenceBudget {
    fn enter_node(&mut self, depth: usize) -> bool {
        if depth > DTO_INFER_MAX_DEPTH || self.remaining_nodes == 0 {
            return false;
        }
        self.remaining_nodes -= 1;
        true
    }

    fn reserve_generated_type(&mut self) -> bool {
        self.reserve_generated_types(1)
    }

    fn reserve_generated_types(&mut self, count: usize) -> bool {
        if count > self.remaining_generated_types {
            return false;
        }
        self.remaining_generated_types -= count;
        true
    }
}

#[derive(Clone)]
struct Scope {
    pipe: FieldType,
    item: Option<FieldType>,
    acc: Option<FieldType>,
    locals: HashMap<String, FieldType>,
}

impl Scope {
    fn new() -> Self {
        Self {
            pipe: FieldType::JsonValue,
            item: None,
            acc: None,
            locals: HashMap::new(),
        }
    }

    fn with_pipe(mut self, pipe: FieldType) -> Self {
        self.pipe = pipe;
        self
    }
}

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
            if number.as_i64().is_some() || number.as_u64().is_some() {
                FieldType::Primitive(PrimitiveType::Int)
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
    infer_pipe(&pipe, rule, state, Scope::new(), 0)
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

    let mut current = infer_start(&pipe.start, state, &scope, depth + 1);
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
        V2Start::PipeValue => scope.pipe.clone(),
        V2Start::Literal(value) => infer_json_value(value, state, depth + 1),
        V2Start::V1Expr(_) => FieldType::JsonValue,
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

fn infer_op(
    op_step: &V2OpStep,
    rule: &RuleFile,
    state: &mut InferenceState,
    scope: &Scope,
    input_type: FieldType,
    depth: usize,
) -> FieldType {
    if !state.enter_node(depth) {
        return FieldType::JsonValue;
    }

    match op_step.op.as_str() {
        "string" | "to_string" | "trim" | "lowercase" | "uppercase" | "concat" | "replace"
        | "pad_start" | "pad_end" | "date_format" | "to_base" => {
            FieldType::Primitive(PrimitiveType::String)
        }
        "int" | "len" | "to_unixtime" | "find_index" | "index_of" | "sign" => {
            FieldType::Primitive(PrimitiveType::Int)
        }
        "float" | "+" | "add" | "-" | "subtract" | "*" | "multiply" | "/" | "divide" | "round"
        | "abs" | "floor" | "ceil" | "trunc" | "sqrt" | "mod" | "pow" | "clamp" => {
            FieldType::Primitive(PrimitiveType::Float)
        }
        "range" => FieldType::Array(Box::new(FieldType::Primitive(PrimitiveType::Int))),
        "sum" | "avg" | "min" | "max" => {
            FieldType::Nullable(Box::new(FieldType::Primitive(PrimitiveType::Float)))
        }
        "bool" | "and" | "or" | "not" | "==" | "!=" | "<" | "<=" | ">" | ">=" | "~=" | "eq"
        | "ne" | "lt" | "lte" | "gt" | "gte" | "match" | "contains" => {
            FieldType::Primitive(PrimitiveType::Bool)
        }
        "split" | "keys" => FieldType::Array(Box::new(FieldType::Primitive(PrimitiveType::String))),
        "values" => FieldType::Array(Box::new(object_value_union(&input_type))),
        "entries" => FieldType::Array(Box::new(FieldType::Object(Box::new(SchemaNode {
            fields: vec![
                Field {
                    key: "key".to_string(),
                    field_type: FieldType::Primitive(PrimitiveType::String),
                    optional: false,
                    synthetic: false,
                },
                Field {
                    key: "value".to_string(),
                    field_type: object_value_union(&input_type),
                    optional: false,
                    synthetic: false,
                },
            ],
        })))),
        "from_entries" => FieldType::Map(Box::new(FieldType::JsonValue)),
        "map" => {
            let element = array_element_type(&input_type);
            let Some(arg) = op_step.args.first() else {
                return FieldType::Array(Box::new(FieldType::JsonValue));
            };
            let arg_scope = scope
                .clone()
                .with_pipe(element.clone())
                .with_item(Some(element));
            FieldType::Array(Box::new(infer_arg_expr(
                arg,
                rule,
                state,
                arg_scope,
                depth + 1,
            )))
        }
        "flat_map" => {
            let element = array_element_type(&input_type);
            let Some(arg) = op_step.args.first() else {
                return FieldType::Array(Box::new(FieldType::JsonValue));
            };
            let arg_scope = scope
                .clone()
                .with_pipe(element.clone())
                .with_item(Some(element));
            match infer_arg_expr(arg, rule, state, arg_scope, depth + 1) {
                FieldType::Array(inner) => FieldType::Array(inner),
                other => FieldType::Array(Box::new(other)),
            }
        }
        "filter" | "unique" | "distinct_by" | "sort_by" | "take" | "drop" | "slice" => input_type,
        "flatten" => match input_type {
            FieldType::Array(inner) => match *inner {
                FieldType::Array(nested) => FieldType::Array(nested),
                other => FieldType::Array(Box::new(other)),
            },
            other => other,
        },
        "chunk" => FieldType::Array(Box::new(FieldType::Array(Box::new(array_element_type(
            &input_type,
        ))))),
        "zip" => FieldType::Array(Box::new(FieldType::Array(Box::new(FieldType::JsonValue)))),
        "zip_with" => {
            let Some(arg) = op_step.args.last() else {
                return FieldType::Array(Box::new(FieldType::JsonValue));
            };
            let arg_scope = scope
                .clone()
                .with_pipe(FieldType::Array(Box::new(FieldType::JsonValue)))
                .with_item(Some(FieldType::Array(Box::new(FieldType::JsonValue))));
            FieldType::Array(Box::new(infer_arg_expr(
                arg,
                rule,
                state,
                arg_scope,
                depth + 1,
            )))
        }
        "group_by" => FieldType::Map(Box::new(FieldType::Array(Box::new(array_element_type(
            &input_type,
        ))))),
        "key_by" => FieldType::Map(Box::new(array_element_type(&input_type))),
        "partition" => FieldType::Array(Box::new(FieldType::Array(Box::new(array_element_type(
            &input_type,
        ))))),
        "find" | "first" | "last" => FieldType::Nullable(Box::new(array_element_type(&input_type))),
        "lookup" => FieldType::Array(Box::new(FieldType::JsonValue)),
        "lookup_first" => FieldType::Nullable(Box::new(FieldType::JsonValue)),
        "get" => infer_get(&input_type, &op_step.args),
        "pick" => infer_pick(&input_type, &op_step.args),
        "omit" => infer_omit(&input_type, &op_step.args),
        "merge" | "deep_merge" => {
            infer_merge(&input_type, &op_step.args, rule, state, scope, depth)
        }
        "object_flatten" => FieldType::Map(Box::new(FieldType::JsonValue)),
        "object_unflatten" => FieldType::JsonValue,
        "coalesce" => {
            let mut merged = input_type;
            for arg in &op_step.args {
                let arg_type = infer_arg_expr(
                    arg,
                    rule,
                    state,
                    scope.clone().with_pipe(merged.clone()),
                    depth + 1,
                );
                merged = merge_types(merged, arg_type);
            }
            merged
        }
        "reduce" => FieldType::JsonValue,
        "fold" => infer_fold(&input_type, &op_step.args, rule, state, scope, depth),
        _ => FieldType::JsonValue,
    }
}

fn infer_let_step(
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

fn infer_if_step(
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

fn infer_map_step(
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

fn infer_arg_expr(
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

fn infer_ref(
    value_ref: &V2Ref,
    state: &mut InferenceState,
    scope: &Scope,
    _depth: usize,
) -> FieldType {
    match value_ref {
        V2Ref::Input(_) | V2Ref::Context(_) => FieldType::JsonValue,
        V2Ref::Out(path) => key_path(path)
            .filter(|keys| !keys.is_empty())
            .and_then(|keys| state.produced_type_for_ref(&keys))
            .unwrap_or(FieldType::JsonValue),
        V2Ref::Item(path) => scoped_path_type(scope.item.as_ref(), path),
        V2Ref::Acc(path) => scoped_path_type(scope.acc.as_ref(), path),
        V2Ref::Local(name) => scope
            .locals
            .get(name)
            .cloned()
            .unwrap_or(FieldType::JsonValue),
    }
}

fn scoped_path_type(base: Option<&FieldType>, path: &str) -> FieldType {
    let Some(base) = base else {
        return FieldType::JsonValue;
    };
    if path.is_empty() {
        return base.clone();
    }
    field_type_at_path(base, path).unwrap_or(FieldType::JsonValue)
}

fn key_path(path: &str) -> Option<Vec<String>> {
    let tokens = parse_path_bounded(path)?;
    let mut keys = Vec::with_capacity(tokens.len());
    for token in tokens {
        match token {
            PathToken::Key(key) => keys.push(key),
            PathToken::Index(_) => return None,
        }
    }
    Some(keys)
}

fn parse_path_bounded(path: &str) -> Option<Vec<PathToken>> {
    if path.len() > DTO_INFER_MAX_PATH_BYTES {
        return None;
    }
    let tokens = parse_path(path).ok()?;
    if tokens.len() > DTO_INFER_MAX_PATH_TOKENS {
        return None;
    }
    Some(tokens)
}

fn field_type_at_path(field_type: &FieldType, path: &str) -> Option<FieldType> {
    let tokens = parse_path_bounded(path)?;
    field_type_at_tokens(field_type, &tokens)
}

fn field_type_at_keys(field_type: &FieldType, keys: &[String]) -> Option<FieldType> {
    if keys.is_empty() {
        return Some(field_type.clone());
    }
    match field_type {
        FieldType::Object(node) => node
            .fields
            .iter()
            .find(|field| field.key == keys[0])
            .and_then(|field| field_type_at_keys(&field.field_type, &keys[1..])),
        FieldType::Map(inner) => field_type_at_keys(inner, &keys[1..]),
        FieldType::Nullable(inner) => field_type_at_keys(inner, keys),
        _ => None,
    }
}

fn field_type_at_tokens(field_type: &FieldType, tokens: &[PathToken]) -> Option<FieldType> {
    if tokens.is_empty() {
        return Some(field_type.clone());
    }
    match (&tokens[0], field_type) {
        (PathToken::Key(key), FieldType::Object(node)) => node
            .fields
            .iter()
            .find(|field| field.key == *key)
            .and_then(|field| field_type_at_tokens(&field.field_type, &tokens[1..])),
        (PathToken::Key(_), FieldType::Map(inner)) => field_type_at_tokens(inner, &tokens[1..]),
        (PathToken::Index(_), FieldType::Array(inner)) => field_type_at_tokens(inner, &tokens[1..]),
        (_, FieldType::Nullable(inner)) => field_type_at_tokens(inner, tokens),
        _ => None,
    }
}

fn infer_get(input_type: &FieldType, args: &[V2Expr]) -> FieldType {
    let Some(path) = args.first().and_then(literal_string_arg) else {
        return FieldType::JsonValue;
    };
    field_type_at_path(input_type, &path).unwrap_or(FieldType::JsonValue)
}

fn infer_pick(input_type: &FieldType, args: &[V2Expr]) -> FieldType {
    let FieldType::Object(node) = input_type else {
        return FieldType::JsonValue;
    };
    let paths = literal_path_args(args);
    if paths.len() != args.len() {
        return FieldType::JsonValue;
    }
    let mut fields = Vec::new();
    for path in paths {
        let Some(keys) = key_path(&path) else {
            return FieldType::JsonValue;
        };
        let Some(field) = pick_field_at_keys(&node.fields, &keys) else {
            continue;
        };
        if !merge_projected_field(&mut fields, field) {
            return FieldType::JsonValue;
        }
    }
    FieldType::Object(Box::new(SchemaNode { fields }))
}

fn infer_omit(input_type: &FieldType, args: &[V2Expr]) -> FieldType {
    let FieldType::Object(node) = input_type else {
        return FieldType::JsonValue;
    };
    let paths = literal_path_args(args);
    if paths.len() != args.len() {
        return FieldType::JsonValue;
    }
    let mut omit_keys = Vec::new();
    for path in paths {
        let Some(keys) = key_path(&path) else {
            return FieldType::JsonValue;
        };
        if keys.is_empty() {
            return FieldType::JsonValue;
        }
        omit_keys.push(keys);
    }
    let mut fields = node.fields.clone();
    for keys in omit_keys {
        omit_path_from_fields(&mut fields, &keys);
    }
    FieldType::Object(Box::new(SchemaNode { fields }))
}

fn pick_field_at_keys(fields: &[Field], keys: &[String]) -> Option<Field> {
    let key = keys.first()?;
    let field = fields.iter().find(|field| field.key == *key)?;
    if keys.len() == 1 {
        return Some(field.clone());
    }

    let FieldType::Object(child) = &field.field_type else {
        return None;
    };
    let child_field = pick_field_at_keys(&child.fields, &keys[1..])?;
    Some(Field {
        key: field.key.clone(),
        field_type: FieldType::Object(Box::new(SchemaNode {
            fields: vec![child_field],
        })),
        optional: field.optional,
        synthetic: field.synthetic,
    })
}

fn merge_projected_field(fields: &mut Vec<Field>, projected: Field) -> bool {
    if let Some(existing) = fields.iter_mut().find(|field| field.key == projected.key) {
        existing.field_type =
            merge_projected_types(existing.field_type.clone(), projected.field_type);
        existing.optional = existing.optional || projected.optional;
        !matches!(existing.field_type, FieldType::JsonValue)
    } else if fields.len() >= DTO_INFER_MAX_OBJECT_FIELDS {
        false
    } else {
        fields.push(projected);
        true
    }
}

fn merge_projected_types(left: FieldType, right: FieldType) -> FieldType {
    match (&left, &right) {
        (FieldType::Object(_), FieldType::Object(_)) => {
            merge_object_types_for_operation(left, right)
        }
        _ => merge_types(left, right),
    }
}

fn omit_path_from_fields(fields: &mut Vec<Field>, keys: &[String]) {
    let Some(key) = keys.first() else {
        return;
    };
    if keys.len() == 1 {
        fields.retain(|field| field.key != *key);
        return;
    }

    let Some(field) = fields.iter_mut().find(|field| field.key == *key) else {
        return;
    };
    if let FieldType::Object(child) = &mut field.field_type {
        omit_path_from_fields(&mut child.fields, &keys[1..]);
    }
}

fn infer_merge(
    input_type: &FieldType,
    args: &[V2Expr],
    rule: &RuleFile,
    state: &mut InferenceState,
    scope: &Scope,
    depth: usize,
) -> FieldType {
    let mut merged = match input_type {
        FieldType::Object(_) => input_type.clone(),
        _ => return FieldType::JsonValue,
    };
    for arg in args {
        let arg_type = infer_arg_expr(
            arg,
            rule,
            state,
            scope.clone().with_pipe(merged.clone()),
            depth + 1,
        );
        if !matches!(arg_type, FieldType::Object(_)) {
            return FieldType::JsonValue;
        }
        merged = merge_object_types_for_operation(merged, arg_type);
        if matches!(merged, FieldType::JsonValue) {
            return FieldType::JsonValue;
        }
    }
    merged
}

fn infer_fold(
    input_type: &FieldType,
    args: &[V2Expr],
    rule: &RuleFile,
    state: &mut InferenceState,
    scope: &Scope,
    depth: usize,
) -> FieldType {
    let [initial_arg, fold_arg] = args else {
        return FieldType::JsonValue;
    };
    let element = array_element_type(input_type);
    let initial_type = infer_arg_expr(
        initial_arg,
        rule,
        state,
        scope.clone().with_pipe(input_type.clone()),
        depth + 1,
    );
    let fold_scope = scope
        .clone()
        .with_pipe(element.clone())
        .with_item(Some(element))
        .with_acc(Some(initial_type.clone()));
    let fold_type = infer_arg_expr(fold_arg, rule, state, fold_scope, depth + 1);
    merge_types(initial_type, fold_type)
}

fn literal_path_args(args: &[V2Expr]) -> Vec<String> {
    args.iter().filter_map(literal_string_arg).collect()
}

fn literal_string_arg(expr: &V2Expr) -> Option<String> {
    match expr {
        V2Expr::Pipe(pipe) if pipe.steps.is_empty() => match &pipe.start {
            V2Start::Literal(JsonValue::String(value)) => Some(value.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn array_element_type(field_type: &FieldType) -> FieldType {
    match field_type {
        FieldType::Array(inner) => (**inner).clone(),
        FieldType::Nullable(inner) => array_element_type(inner),
        _ => FieldType::JsonValue,
    }
}

fn object_value_union(field_type: &FieldType) -> FieldType {
    match field_type {
        FieldType::Object(node) => node
            .fields
            .iter()
            .map(|field| field.field_type.clone())
            .reduce(merge_types)
            .unwrap_or(FieldType::JsonValue),
        FieldType::Map(inner) => (**inner).clone(),
        FieldType::Nullable(inner) => object_value_union(inner),
        _ => FieldType::JsonValue,
    }
}

fn generated_type_count(field_type: &FieldType) -> usize {
    match field_type {
        FieldType::Object(node) => {
            1 + node
                .fields
                .iter()
                .map(|field| generated_type_count(&field.field_type))
                .sum::<usize>()
        }
        FieldType::Array(inner) | FieldType::Map(inner) | FieldType::Nullable(inner) => {
            generated_type_count(inner)
        }
        FieldType::Primitive(_) | FieldType::JsonValue => 0,
    }
}

fn merge_types(left: FieldType, right: FieldType) -> FieldType {
    use FieldType::*;
    match (left, right) {
        (JsonValue, _) | (_, JsonValue) => JsonValue,
        (Primitive(a), Primitive(b)) if a == b => Primitive(a),
        (Primitive(PrimitiveType::Int), Primitive(PrimitiveType::Float))
        | (Primitive(PrimitiveType::Float), Primitive(PrimitiveType::Int)) => {
            Primitive(PrimitiveType::Float)
        }
        (Nullable(a), Nullable(b)) => Nullable(Box::new(merge_types(*a, *b))),
        (Nullable(a), _) if *a == JsonValue => JsonValue,
        (_, Nullable(b)) if *b == JsonValue => JsonValue,
        (Nullable(a), b) | (b, Nullable(a)) => Nullable(Box::new(merge_types(*a, b))),
        (Array(a), Array(b)) => Array(Box::new(merge_types(*a, *b))),
        (Map(a), Map(b)) => Map(Box::new(merge_types(*a, *b))),
        (Object(a), Object(b)) => match merge_object_nodes(*a, *b) {
            Some(node) => Object(Box::new(node)),
            None => JsonValue,
        },
        (Primitive(_), Primitive(_)) => JsonValue,
        (Array(_), _)
        | (_, Array(_))
        | (Map(_), _)
        | (_, Map(_))
        | (Object(_), _)
        | (_, Object(_)) => JsonValue,
    }
}

fn merge_object_nodes(left: SchemaNode, right: SchemaNode) -> Option<SchemaNode> {
    let mut right_fields = right.fields;
    let mut fields = Vec::with_capacity(left.fields.len() + right_fields.len());

    for mut left_field in left.fields {
        if let Some(index) = right_fields
            .iter()
            .position(|right_field| right_field.key == left_field.key)
        {
            let right_field = right_fields.remove(index);
            left_field.field_type = merge_types(left_field.field_type, right_field.field_type);
            left_field.optional = left_field.optional || right_field.optional;
            fields.push(left_field);
        } else {
            left_field.optional = true;
            fields.push(left_field);
        }
    }

    for mut right_field in right_fields {
        right_field.optional = true;
        fields.push(right_field);
    }

    if fields.len() > DTO_INFER_MAX_OBJECT_FIELDS {
        None
    } else {
        Some(SchemaNode { fields })
    }
}

fn merge_object_types_for_operation(left: FieldType, right: FieldType) -> FieldType {
    match (left, right) {
        (FieldType::Object(left), FieldType::Object(right)) => {
            match merge_object_nodes_for_operation(*left, *right) {
                Some(node) => FieldType::Object(Box::new(node)),
                None => FieldType::JsonValue,
            }
        }
        _ => FieldType::JsonValue,
    }
}

fn merge_object_nodes_for_operation(left: SchemaNode, right: SchemaNode) -> Option<SchemaNode> {
    let mut right_fields = right.fields;
    let mut fields = Vec::with_capacity(left.fields.len() + right_fields.len());

    for mut left_field in left.fields {
        if let Some(index) = right_fields
            .iter()
            .position(|right_field| right_field.key == left_field.key)
        {
            let right_field = right_fields.remove(index);
            left_field.field_type = merge_types(left_field.field_type, right_field.field_type);
            left_field.optional = left_field.optional || right_field.optional;
        }
        fields.push(left_field);
    }

    fields.extend(right_fields);

    if fields.len() > DTO_INFER_MAX_OBJECT_FIELDS {
        None
    } else {
        Some(SchemaNode { fields })
    }
}

trait ScopeExt {
    fn with_item(self, item: Option<FieldType>) -> Self;
    fn with_acc(self, acc: Option<FieldType>) -> Self;
}

impl ScopeExt for Scope {
    fn with_item(mut self, item: Option<FieldType>) -> Self {
        self.item = item;
        self
    }

    fn with_acc(mut self, acc: Option<FieldType>) -> Self {
        self.acc = acc;
        self
    }
}

fn expr_to_json_for_v2_pipe_bounded(
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
            if expr_ref.ref_path.starts_with('@') || is_literal_escape(&expr_ref.ref_path) =>
        {
            Some(JsonValue::Array(vec![JsonValue::String(
                expr_ref.ref_path.clone(),
            )]))
        }
        Expr::Chain(chain) => {
            if chain.chain.len().saturating_sub(1) > DTO_INFER_MAX_PIPE_STEPS {
                return None;
            }
            let starts_with_v2_ref = matches!(
                chain.chain.first(),
                Some(Expr::Ref(first)) if first.ref_path.starts_with('@')
            );
            if !starts_with_v2_ref {
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

fn expr_to_json_value_bounded(
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

fn clone_json_bounded(
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
enum JsonCloneContext {
    PipeRoot,
    ExprValue,
}

fn pipe_json_shape_is_bounded(value: &JsonValue) -> bool {
    !matches!(value, JsonValue::Array(items) if items.len().saturating_sub(1) > DTO_INFER_MAX_PIPE_STEPS)
}
