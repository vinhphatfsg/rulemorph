use serde_json::Value as JsonValue;

use super::super::options::Profile;
use super::super::{
    MAX_DYNAMODB_DEPTH, MAX_FIRESTORE_DEPTH, MAX_TYPED_VALUE_DEPTH, MAX_TYPED_VALUE_FIELDS,
    MAX_TYPED_VALUE_NODES, MAX_TYPED_VALUE_OUTPUT_BYTES,
};
use super::expr_error;
use crate::error::TransformError;

pub(in crate::v2_eval::operators::typed_value) struct ResourceGuard {
    profile: Profile,
    nodes: usize,
    fields: usize,
}

impl ResourceGuard {
    pub(in crate::v2_eval::operators::typed_value) fn new(profile: Profile) -> Self {
        Self {
            profile,
            nodes: 0,
            fields: 0,
        }
    }

    pub(in crate::v2_eval::operators::typed_value) fn visit_node(
        &mut self,
        path: &str,
        depth: usize,
    ) -> Result<(), TransformError> {
        self.nodes = self
            .nodes
            .checked_add(1)
            .ok_or_else(|| expr_error("typed value node count overflow", path))?;
        if self.nodes > MAX_TYPED_VALUE_NODES {
            return Err(expr_error(
                "typed value node count exceeds configured limit",
                path,
            ));
        }
        let max_depth = match self.profile {
            Profile::DynamoDbAttributeValue | Profile::DynamoDbItem => MAX_DYNAMODB_DEPTH,
            Profile::FirestoreValue | Profile::FirestoreFields | Profile::FirestoreDocument => {
                MAX_FIRESTORE_DEPTH
            }
            Profile::MongoExtendedJson => MAX_TYPED_VALUE_DEPTH,
        };
        if depth > max_depth {
            return Err(expr_error(
                "typed value depth exceeds configured limit",
                path,
            ));
        }
        Ok(())
    }

    pub(in crate::v2_eval::operators::typed_value) fn visit_field(
        &mut self,
        path: &str,
    ) -> Result<(), TransformError> {
        self.fields = self
            .fields
            .checked_add(1)
            .ok_or_else(|| expr_error("typed value field count overflow", path))?;
        if self.fields > MAX_TYPED_VALUE_FIELDS {
            return Err(expr_error(
                "typed value field count exceeds configured limit",
                path,
            ));
        }
        Ok(())
    }

    pub(in crate::v2_eval::operators::typed_value) fn check_output_bytes(
        &self,
        value: &JsonValue,
        path: &str,
    ) -> Result<(), TransformError> {
        let bytes = serde_json::to_vec(value)
            .map_err(|_| expr_error("typed value output could not be serialized", path))?;
        if bytes.len() > MAX_TYPED_VALUE_OUTPUT_BYTES {
            return Err(expr_error(
                "typed value output bytes exceed configured limit",
                path,
            ));
        }
        Ok(())
    }
}
