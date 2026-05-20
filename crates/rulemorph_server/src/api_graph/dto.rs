use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ApiGraphResponse {
    pub nodes: Vec<ApiGraphNode>,
    pub edges: Vec<ApiGraphEdge>,
}

#[derive(Debug, Serialize)]
pub struct ApiGraphNode {
    pub id: String,
    pub label: String,
    pub kind: String,
    pub path: String,
    pub ops: Vec<ApiGraphOp>,
}

#[derive(Debug, Serialize)]
pub struct ApiGraphOp {
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub refs: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ApiGraphEdge {
    pub source: String,
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub kind: String,
}
