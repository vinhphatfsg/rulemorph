use serde::Deserialize;
use serde_json::Value as JsonValue;

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct RuleFile {
    pub version: u8,
    pub input: InputSpec,
    #[serde(default)]
    pub output: Option<OutputSpec>,
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
pub struct InputSpec {
    pub format: InputFormat,
    pub csv: Option<CsvInput>,
    pub json: Option<JsonInput>,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum InputFormat {
    Csv,
    Json,
}

fn default_true() -> bool {
    true
}

fn default_delimiter() -> String {
    ",".to_string()
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CsvInput {
    #[serde(default = "default_true")]
    pub has_header: bool,
    #[serde(default = "default_delimiter")]
    pub delimiter: String,
    pub columns: Option<Vec<Column>>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub value_type: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct JsonInput {
    pub records_path: Option<String>,
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
