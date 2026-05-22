use serde_json::json;

use super::super::ErrorFormat;

pub(crate) fn emit_rules_dir_errors(
    errors: &rulemorph_server::RulesDirErrors,
    format: ErrorFormat,
) {
    match format {
        ErrorFormat::Text => {
            eprintln!("{}", errors);
        }
        ErrorFormat::Json => {
            let values: Vec<_> = errors
                .errors
                .iter()
                .map(|err| rules_dir_error_json(err))
                .collect();
            eprintln!("{}", serde_json::to_string(&values).unwrap_or_default());
        }
    }
}

fn rules_dir_error_json(err: &rulemorph_server::RulesDirError) -> serde_json::Value {
    let mut value = json!({
        "type": "rules_dir",
        "code": err.code,
        "message": err.message,
        "file": err.file.to_string_lossy(),
    });
    if let Some(path) = &err.path {
        value["path"] = json!(path);
    }
    if let Some(line) = err.line {
        value["line"] = json!(line);
    }
    if let Some(column) = err.column {
        value["column"] = json!(column);
    }
    value
}
