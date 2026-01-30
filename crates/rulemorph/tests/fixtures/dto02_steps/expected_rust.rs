use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub name: String,
    pub score: i64,
}
