use rulemorph_server::{ApiKeyInfo, ApiKeyIssueResult};

pub(super) fn emit_api_key_issue(issued: &ApiKeyIssueResult, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(issued).unwrap_or_default()
        );
        return;
    }
    println!("id: {}", issued.id);
    println!("prefix: {}", issued.prefix);
    println!("key: {}", issued.key);
    if let Some(label) = issued.label.as_ref() {
        println!("label: {}", label);
    }
    println!("created_at: {}", issued.created_at);
}

pub(super) fn emit_api_key_list(keys: &[ApiKeyInfo], json: bool) {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(keys).unwrap_or_else(|_| "[]".to_string())
        );
        return;
    }
    if keys.is_empty() {
        println!("no api keys");
        return;
    }
    for key in keys {
        println!("id: {}", key.id);
        println!("prefix: {}", key.prefix);
        println!("created_at: {}", key.created_at);
        if let Some(revoked) = key.revoked_at.as_ref() {
            println!("revoked_at: {}", revoked);
        }
        if let Some(label) = key.label.as_ref() {
            println!("label: {}", label);
        }
        println!("---");
    }
}
