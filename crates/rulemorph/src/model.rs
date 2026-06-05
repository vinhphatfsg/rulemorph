use serde::{Deserialize, Deserializer};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

use crate::custom_ops::{MAX_TYPE_DEPTH, MAX_TYPE_FIELDS};

mod input;

#[cfg_attr(
    any(not(feature = "html"), not(feature = "excel")),
    allow(unused_imports)
)]
pub use self::input::{
    Column, ExcelCellErrorPolicy, ExcelColumn, ExcelDatePolicy, ExcelEmptyCellPolicy,
    ExcelFormulaPolicy, ExcelInput, ExcelSheetRef, HtmlInput, HtmlValueKind, InputFormat,
    InputSpec, XmlInput, XmlNamespacePolicy,
};

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct RuleFile {
    pub version: u8,
    pub input: InputSpec,
    #[serde(default)]
    pub output: Option<OutputSpec>,
    #[serde(default)]
    pub defs: BTreeMap<String, CustomOpDef>,
    #[serde(default)]
    pub codecs: BTreeMap<String, JsonValue>,
    #[serde(default)]
    pub record_when: Option<Expr>,
    #[serde(default)]
    pub mappings: Vec<Mapping>,
    #[serde(default)]
    pub steps: Option<Vec<V2RuleStep>>,
    #[serde(default)]
    pub finalize: Option<FinalizeSpec>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct OutputSpec {
    pub name: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
    pub target: String,
    pub source: Option<String>,
    pub value: Option<JsonValue>,
    pub expr: Option<Expr>,
    pub when: Option<Expr>,
    #[serde(rename = "type")]
    pub value_type: Option<String>,
    #[serde(default)]
    pub required: bool,
    pub default: Option<JsonValue>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CustomOpDef {
    pub input: RuleType,
    #[serde(default)]
    pub returns: Option<RuleType>,
    #[serde(default)]
    pub expr: Option<Expr>,
    #[serde(default)]
    pub mappings: Option<Vec<Mapping>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleType {
    pub kind: RuleTypeKind,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RuleTypeKind {
    String,
    Int,
    Float,
    Number,
    Bool,
    Json,
    Array(Box<RuleType>),
    Object(BTreeMap<String, RuleTypeField>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleTypeField {
    pub ty: RuleType,
    pub optional: bool,
}

impl<'de> Deserialize<'de> for RuleType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = JsonValue::deserialize(deserializer)?;
        let mut field_count = 0usize;
        parse_rule_type_value(&value, 1, &mut field_count).map_err(serde::de::Error::custom)
    }
}

fn parse_rule_type_value(
    value: &JsonValue,
    depth: usize,
    field_count: &mut usize,
) -> Result<RuleType, String> {
    if depth > MAX_TYPE_DEPTH {
        return Err("type exceeds configured depth limit".to_string());
    }
    match value {
        JsonValue::String(name) => parse_rule_type_name(name),
        JsonValue::Array(items) => {
            if items.len() != 1 {
                return Err("array type must contain exactly one item type".to_string());
            }
            Ok(RuleType {
                kind: RuleTypeKind::Array(Box::new(parse_rule_type_value(
                    &items[0],
                    depth + 1,
                    field_count,
                )?)),
                nullable: false,
            })
        }
        JsonValue::Object(map) => parse_rule_type_object(map, depth, field_count),
        _ => Err("type literal must be a string, array, or object".to_string()),
    }
}

fn parse_rule_type_name(name: &str) -> Result<RuleType, String> {
    let (base, nullable) = match name.strip_suffix('?') {
        Some(base) => (base, true),
        None => (name, false),
    };
    let kind = match base {
        "string" => RuleTypeKind::String,
        "int" => RuleTypeKind::Int,
        "float" => RuleTypeKind::Float,
        "number" => RuleTypeKind::Number,
        "bool" => RuleTypeKind::Bool,
        "json" => RuleTypeKind::Json,
        other => return Err(format!("unknown type `{}`", other)),
    };
    Ok(RuleType { kind, nullable })
}

fn parse_rule_type_object(
    map: &serde_json::Map<String, JsonValue>,
    depth: usize,
    field_count: &mut usize,
) -> Result<RuleType, String> {
    *field_count = field_count.saturating_add(map.len());
    if *field_count > MAX_TYPE_FIELDS {
        return Err("type exceeds configured field limit".to_string());
    }

    let mut fields = BTreeMap::new();
    for (raw_key, value) in map {
        let (key, optional) = match raw_key.strip_suffix('?') {
            Some(key) => (key.to_string(), true),
            None => (raw_key.clone(), false),
        };
        if key.is_empty() {
            return Err("object field name must not be empty".to_string());
        }
        if fields.contains_key(&key) {
            return Err(format!("object field `{}` is duplicated", key));
        }
        let (ty, value_optional) = parse_rule_type_field_value(value, depth + 1, field_count)?;
        fields.insert(
            key,
            RuleTypeField {
                ty,
                optional: optional || value_optional,
            },
        );
    }

    Ok(RuleType {
        kind: RuleTypeKind::Object(fields),
        nullable: false,
    })
}

fn parse_rule_type_field_value(
    value: &JsonValue,
    depth: usize,
    field_count: &mut usize,
) -> Result<(RuleType, bool), String> {
    if let JsonValue::Object(map) = value
        && is_canonical_rule_type_object(map)
    {
        return parse_canonical_rule_type_field_object(map, depth, field_count);
    }
    Ok((parse_rule_type_value(value, depth, field_count)?, false))
}

fn is_canonical_rule_type_object(map: &serde_json::Map<String, JsonValue>) -> bool {
    map.contains_key("type")
        && (map.contains_key("optional") || map.contains_key("nullable"))
        && map
            .keys()
            .all(|key| matches!(key.as_str(), "type" | "optional" | "nullable"))
}

fn parse_canonical_rule_type_field_object(
    map: &serde_json::Map<String, JsonValue>,
    depth: usize,
    field_count: &mut usize,
) -> Result<(RuleType, bool), String> {
    for key in map.keys() {
        if !matches!(key.as_str(), "type" | "optional" | "nullable") {
            return Err(format!("unknown type option `{}`", key));
        }
    }
    let type_value = map
        .get("type")
        .ok_or_else(|| "canonical type object must include type".to_string())?;
    let mut ty = parse_rule_type_value(type_value, depth, field_count)?;
    if parse_type_option_bool(map, "nullable")? {
        ty.nullable = true;
    }
    let optional = parse_type_option_bool(map, "optional")?;
    Ok((ty, optional))
}

fn parse_type_option_bool(
    map: &serde_json::Map<String, JsonValue>,
    key: &str,
) -> Result<bool, String> {
    match map.get(key) {
        Some(JsonValue::Bool(value)) => Ok(*value),
        Some(_) => Err(format!("type option `{}` must be boolean", key)),
        None => Ok(false),
    }
}

// =============================================================================
// v2 Rule Steps / Finalize
// =============================================================================

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct V2RuleStep {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub mappings: Option<Vec<Mapping>>,
    #[serde(default)]
    pub record_when: Option<Expr>,
    #[serde(default)]
    pub asserts: Option<Vec<V2Assert>>,
    #[serde(default)]
    pub branch: Option<V2Branch>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct V2Assert {
    pub when: Expr,
    pub error: V2AssertError,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct V2AssertError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct V2Branch {
    pub when: Expr,
    pub then: String,
    #[serde(default)]
    pub r#else: Option<String>,
    #[serde(rename = "return", default)]
    pub return_: bool,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct FinalizeSpec {
    #[serde(default)]
    pub filter: Option<Expr>,
    #[serde(default)]
    pub sort: Option<FinalizeSort>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub wrap: Option<JsonValue>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct FinalizeSort {
    pub by: String,
    #[serde(default = "default_sort_order")]
    pub order: String,
}

fn default_sort_order() -> String {
    "asc".to_string()
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum Expr {
    Ref(ExprRef),
    Op(ExprOp),
    Chain(ExprChain),
    Literal(JsonValue),
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExprRef {
    #[serde(rename = "ref")]
    pub ref_path: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExprOp {
    pub op: String,
    #[serde(default)]
    pub args: Vec<Expr>,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExprChain {
    pub chain: Vec<Expr>,
}
