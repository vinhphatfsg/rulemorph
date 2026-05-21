use std::path::Path;

use anyhow::Result;

mod dto;
mod load;
mod ops;
mod primitives;

pub use self::dto::{ApiGraphEdge, ApiGraphNode, ApiGraphOp, ApiGraphResponse};

pub fn build_api_graph(data_dir: &Path) -> Result<ApiGraphResponse> {
    load::build_api_graph(data_dir)
}

#[cfg(test)]
mod tests;
