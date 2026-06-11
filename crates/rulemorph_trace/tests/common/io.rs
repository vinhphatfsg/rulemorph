use std::fs;
use std::path::Path;

pub fn read_ndjson_lines(path: impl AsRef<Path>) -> anyhow::Result<Vec<String>> {
    let payload = fs::read_to_string(path)?;
    Ok(payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect())
}

pub fn read_ndjson_values(path: impl AsRef<Path>) -> anyhow::Result<Vec<serde_json::Value>> {
    read_ndjson_lines(path)?
        .into_iter()
        .map(|line| Ok(serde_json::from_str(&line)?))
        .collect()
}

pub fn write_ndjson_lines(path: impl AsRef<Path>, lines: &[String]) -> anyhow::Result<()> {
    fs::write(path, format!("{}\n", lines.join("\n")))?;
    Ok(())
}
