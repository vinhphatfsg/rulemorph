use super::super::expr_error;
use crate::error::TransformError;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(in crate::v2_eval::operators::typed_value) enum Profile {
    DynamoDbAttributeValue,
    DynamoDbItem,
    FirestoreValue,
    FirestoreFields,
    FirestoreDocument,
    MongoExtendedJson,
}

impl Profile {
    pub(in crate::v2_eval::operators::typed_value) fn parse(
        raw: &str,
        path: &str,
    ) -> Result<Self, TransformError> {
        match raw {
            "dynamodb_attribute_value" => Ok(Self::DynamoDbAttributeValue),
            "dynamodb_item" => Ok(Self::DynamoDbItem),
            "firestore_value" => Ok(Self::FirestoreValue),
            "firestore_fields" => Ok(Self::FirestoreFields),
            "firestore_document" => Ok(Self::FirestoreDocument),
            "mongo_extended_json" => Ok(Self::MongoExtendedJson),
            _ => Err(expr_error(
                format!("unknown typed value profile: {}", raw),
                path,
            )),
        }
    }

    pub(in crate::v2_eval::operators::typed_value) fn is_mongo(self) -> bool {
        matches!(self, Self::MongoExtendedJson)
    }

    pub(in crate::v2_eval::operators::typed_value) fn supports_root_type(self) -> bool {
        matches!(
            self,
            Self::DynamoDbAttributeValue | Self::FirestoreValue | Self::MongoExtendedJson
        )
    }

    pub(in crate::v2_eval::operators::typed_value) fn name(self) -> &'static str {
        match self {
            Self::DynamoDbAttributeValue => "dynamodb_attribute_value",
            Self::DynamoDbItem => "dynamodb_item",
            Self::FirestoreValue => "firestore_value",
            Self::FirestoreFields => "firestore_fields",
            Self::FirestoreDocument => "firestore_document",
            Self::MongoExtendedJson => "mongo_extended_json",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(in crate::v2_eval::operators::typed_value) enum OnMissing {
    Error,
    Propagate,
    Ignore,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(in crate::v2_eval::operators::typed_value) enum DecodeMode {
    SafeJson,
    JsonShapeRoundtrip,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(in crate::v2_eval::operators::typed_value) enum NumberPolicy {
    String,
    ParseJsonNumberIfSafe,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(in crate::v2_eval::operators::typed_value) enum MongoMode {
    Relaxed,
    Canonical,
}

#[derive(Debug, Clone)]
pub(in crate::v2_eval::operators::typed_value) struct CodecOptions {
    pub(in crate::v2_eval::operators::typed_value) profile: Profile,
    pub(in crate::v2_eval::operators::typed_value) root_type: Option<HintType>,
    pub(in crate::v2_eval::operators::typed_value) hints: Vec<Hint>,
    pub(in crate::v2_eval::operators::typed_value) on_missing: OnMissing,
    pub(in crate::v2_eval::operators::typed_value) decode_mode: DecodeMode,
    pub(in crate::v2_eval::operators::typed_value) number_policy: NumberPolicy,
    pub(in crate::v2_eval::operators::typed_value) mongo_mode: MongoMode,
    pub(in crate::v2_eval::operators::typed_value) extended_json_wrapper_objects:
        WrapperObjectPolicy,
    pub(in crate::v2_eval::operators::typed_value) allow_dollar_prefixed_fields: bool,
    pub(in crate::v2_eval::operators::typed_value) allow_extended_json_passthrough: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(in crate::v2_eval::operators::typed_value) enum WrapperObjectPolicy {
    RejectUnhinted,
}

#[derive(Debug, Clone)]
pub(in crate::v2_eval::operators::typed_value) struct Hint {
    pub(in crate::v2_eval::operators::typed_value) path: HintPath,
    pub(in crate::v2_eval::operators::typed_value) ty: HintType,
    pub(in crate::v2_eval::operators::typed_value) nullable: bool,
    pub(in crate::v2_eval::operators::typed_value) on_missing: OnMissing,
    pub(in crate::v2_eval::operators::typed_value) format: Option<String>,
    pub(in crate::v2_eval::operators::typed_value) input: Option<String>,
    pub(in crate::v2_eval::operators::typed_value) output_precision: Option<String>,
    pub(in crate::v2_eval::operators::typed_value) subtype: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(in crate::v2_eval::operators::typed_value) struct HintPath(
    pub(in crate::v2_eval::operators::typed_value) Vec<PathPart>,
);

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(in crate::v2_eval::operators::typed_value) enum PathPart {
    Key(String),
    AnyIndex,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(in crate::v2_eval::operators::typed_value) enum HintType {
    StringSet,
    NumberSet,
    BinarySetBase64,
    NumberString,
    NumberStringSet,
    BinaryBase64,
    Integer,
    Timestamp,
    BytesBase64,
    Reference,
    GeoPoint,
    ObjectId,
    Date,
    Decimal128,
    Int32,
    Int64,
    Double,
}

impl HintType {
    pub(in crate::v2_eval::operators::typed_value) fn name(self) -> &'static str {
        match self {
            Self::StringSet => "string_set",
            Self::NumberSet => "number_set",
            Self::BinarySetBase64 => "binary_set_base64",
            Self::NumberString => "number_string",
            Self::NumberStringSet => "number_string_set",
            Self::BinaryBase64 => "binary_base64",
            Self::Integer => "integer",
            Self::Timestamp => "timestamp",
            Self::BytesBase64 => "bytes_base64",
            Self::Reference => "reference",
            Self::GeoPoint => "geo_point",
            Self::ObjectId => "object_id",
            Self::Date => "date",
            Self::Decimal128 => "decimal128",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::Double => "double",
        }
    }
}
