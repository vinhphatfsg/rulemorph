mod docs;
mod inventory;

use serde_json::{Value, json};

pub(crate) fn run_list_ops_tool() -> Value {
    let ops = inventory::ops_json(docs::category_docs_json());

    let text = serde_json::to_string_pretty(&ops)
        .unwrap_or_else(|_| "{\"error\":\"failed to serialize ops\"}".to_string());

    json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "meta": {
            "ops": ops
        }
    })
}
