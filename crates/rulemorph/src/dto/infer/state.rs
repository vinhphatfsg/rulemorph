use std::collections::HashMap;

use super::*;

#[derive(Clone, Default)]
pub(in crate::dto) struct InferenceState {
    pub(super) produced: HashMap<Vec<String>, FieldType>,
    budget: InferenceBudget,
}

impl InferenceState {
    pub(super) fn enter_node(&mut self, depth: usize) -> bool {
        self.budget.enter_node(depth)
    }

    pub(super) fn reserve_generated_type(&mut self) -> bool {
        self.budget.reserve_generated_type()
    }

    pub(super) fn reserve_generated_types(&mut self, count: usize) -> bool {
        self.budget.reserve_generated_types(count)
    }

    pub(super) fn produced_type(&self, keys: &[String]) -> Option<FieldType> {
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

    pub(super) fn produced_type_for_ref(&mut self, keys: &[String]) -> Option<FieldType> {
        let field_type = self.produced_type(keys)?;
        let generated_types = generated_type_count(&field_type);
        if generated_types > 0 && !self.reserve_generated_types(generated_types) {
            return None;
        }
        Some(field_type)
    }
}

#[derive(Clone)]
pub(super) struct InferenceBudget {
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
pub(super) struct Scope {
    pub(super) input: Option<FieldType>,
    pub(super) out: Option<FieldType>,
    pub(super) pipe: FieldType,
    pub(super) item: Option<FieldType>,
    pub(super) acc: Option<FieldType>,
    pub(super) locals: HashMap<String, FieldType>,
}

impl Scope {
    pub(super) fn new() -> Self {
        Self {
            input: None,
            out: None,
            pipe: FieldType::JsonValue,
            item: None,
            acc: None,
            locals: HashMap::new(),
        }
    }

    pub(super) fn with_input(mut self, input: FieldType) -> Self {
        self.input = Some(input);
        self
    }

    pub(super) fn with_out(mut self, out: FieldType) -> Self {
        self.out = Some(out);
        self
    }

    pub(super) fn with_pipe(mut self, pipe: FieldType) -> Self {
        self.pipe = pipe;
        self
    }
}

pub(super) trait ScopeExt {
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
