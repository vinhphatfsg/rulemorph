use super::super::{EvalValue, V2EvalContext};
use crate::error::TransformError;
use crate::v2_model::V2OpStep;
use serde_json::{Map as JsonMap, Value as JsonValue};

mod common;
mod dynamodb;
mod firestore;
mod mongo;
mod options;

use common::{PathElem, ResourceGuard, check_required_hints, expr_error, validate_dynamodb_key};
use dynamodb::{decode_dynamodb_attribute, encode_dynamodb_value};
use firestore::{
    decode_firestore_fields, decode_firestore_value, encode_firestore_fields,
    encode_firestore_value,
};
use mongo::{decode_mongo_value, encode_mongo_value};
use options::{CodecOptions, OnMissing, Profile, parse_options};

const MAX_TYPED_VALUE_DEPTH: usize = 64;
const MAX_DYNAMODB_DEPTH: usize = 32;
const MAX_FIRESTORE_DEPTH: usize = 20;
const MAX_TYPED_VALUE_NODES: usize = 100_000;
const MAX_TYPED_VALUE_FIELDS: usize = 20_000;
const MAX_TYPED_VALUE_HINTS: usize = 1024;
const MAX_TYPED_VALUE_OUTPUT_BYTES: usize = 10 * 1024 * 1024;
const MAX_TYPED_VALUE_INPUT_STRING_BYTES: usize = 8 * 1024 * 1024;
const MAX_TYPED_VALUE_HINT_PATH_BYTES: usize = 4096;
const MAX_TYPED_VALUE_HINT_PATH_TOKENS: usize = 256;
const MAX_TYPED_VALUE_CODECS: usize = 1024;
const MAX_TYPED_VALUE_CODEC_NAME_BYTES: usize = 256;
const DYNAMODB_NAME_MAX_BYTES: usize = 65_535;
const FIRESTORE_FIELD_NAME_MAX_BYTES: usize = 1_500;
const FIRESTORE_VALUE_MAX_BYTES: usize = 1_048_487;

pub(super) fn eval_typed_value_op<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    let input = match pipe_value {
        EvalValue::Missing => {
            let options = parse_options(op_step, record, context, out, path, ctx)?;
            return match options.on_missing {
                OnMissing::Error => Err(expr_error("typed value input is missing", path)),
                OnMissing::Propagate | OnMissing::Ignore => Ok(EvalValue::Missing),
            };
        }
        EvalValue::Value(value) => value,
    };
    let options = parse_options(op_step, record, context, out, path, ctx)?;
    let mut guard = ResourceGuard::new(options.profile);
    let value = match op_step.op.as_str() {
        "to_typed_value" => encode_root(&input, &options, path, &mut guard)?,
        "from_typed_value" => decode_root(&input, &options, path, &mut guard)?,
        _ => return Err(expr_error("unsupported typed value operation", path)),
    };
    guard.check_output_bytes(&value, path)?;
    Ok(EvalValue::Value(value))
}

fn encode_root(
    input: &JsonValue,
    options: &CodecOptions,
    path: &str,
    guard: &mut ResourceGuard,
) -> Result<JsonValue, TransformError> {
    check_required_hints(input, options, path)?;
    match options.profile {
        Profile::DynamoDbAttributeValue => {
            encode_dynamodb_value(input, options, &[], options.root_type, path, guard)
        }
        Profile::DynamoDbItem => {
            let obj = input
                .as_object()
                .ok_or_else(|| expr_error("dynamodb_item requires root object", path))?;
            let mut out = JsonMap::new();
            for (key, value) in obj {
                validate_dynamodb_key(key, path)?;
                guard.visit_field(path)?;
                out.insert(
                    key.clone(),
                    encode_dynamodb_value(
                        value,
                        options,
                        &[PathElem::Key(key)],
                        None,
                        path,
                        guard,
                    )?,
                );
            }
            Ok(JsonValue::Object(out))
        }
        Profile::FirestoreValue => {
            encode_firestore_value(input, options, &[], options.root_type, None, guard, path)
        }
        Profile::FirestoreFields => encode_firestore_fields(input, options, &[], guard, path),
        Profile::FirestoreDocument => {
            let fields = encode_firestore_fields(input, options, &[], guard, path)?;
            let mut out = JsonMap::new();
            out.insert("fields".to_string(), fields);
            Ok(JsonValue::Object(out))
        }
        Profile::MongoExtendedJson => {
            encode_mongo_value(input, options, &[], options.root_type, guard, path)
        }
    }
}

fn decode_root(
    input: &JsonValue,
    options: &CodecOptions,
    path: &str,
    guard: &mut ResourceGuard,
) -> Result<JsonValue, TransformError> {
    let decoded = match options.profile {
        Profile::DynamoDbAttributeValue => {
            decode_dynamodb_attribute(input, options, &[], path, guard)
        }
        Profile::DynamoDbItem => {
            let map = input
                .as_object()
                .ok_or_else(|| expr_error("dynamodb_item decode requires object", path))?;
            let mut out = JsonMap::new();
            for (key, value) in map {
                validate_dynamodb_key(key, path)?;
                guard.visit_field(path)?;
                out.insert(
                    key.clone(),
                    decode_dynamodb_attribute(value, options, &[PathElem::Key(key)], path, guard)?,
                );
            }
            Ok(JsonValue::Object(out))
        }
        Profile::FirestoreValue => decode_firestore_value(input, options, &[], false, guard, path),
        Profile::FirestoreFields => decode_firestore_fields(input, options, &[], guard, path),
        Profile::FirestoreDocument => {
            let map = input
                .as_object()
                .ok_or_else(|| expr_error("firestore_document decode requires object", path))?;
            let fields = map
                .get("fields")
                .ok_or_else(|| expr_error("firestore_document requires fields", path))?;
            decode_firestore_fields(fields, options, &[], guard, path)
        }
        Profile::MongoExtendedJson => decode_mongo_value(input, options, &[], guard, path, 0),
    }?;
    check_required_hints(&decoded, options, path)?;
    Ok(decoded)
}
