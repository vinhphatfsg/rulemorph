use rulemorph::{InputFormat, RuleFile};

pub(crate) fn apply_format_override(
    rule: &mut RuleFile,
    format: Option<&str>,
) -> Result<(), String> {
    let Some(format) = format else {
        return Ok(());
    };
    let normalized = format.to_lowercase();
    rule.input.format = match normalized.as_str() {
        "csv" => InputFormat::Csv,
        "json" => InputFormat::Json,
        "yaml" => InputFormat::Yaml,
        "toml" => InputFormat::Toml,
        "xml" => InputFormat::Xml,
        "html" => InputFormat::Html,
        "excel" => InputFormat::Excel,
        _ => return Err(format!("unknown format: {}", format)),
    };
    Ok(())
}
