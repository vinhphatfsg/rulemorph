use std::fs;
use std::io::{IsTerminal, Read};
use std::path::{Path, PathBuf};

pub(crate) fn load_input_bytes_from_path_or_stdin(
    path: Option<&PathBuf>,
    max_input_bytes: usize,
) -> Result<Vec<u8>, i32> {
    match path {
        Some(path) if path != Path::new("-") => load_input_bytes_with_limit(path, max_input_bytes),
        Some(_) => load_stdin_bytes_with_limit(max_input_bytes),
        None => {
            if std::io::stdin().is_terminal() {
                eprintln!("input file is required unless stdin is piped");
                return Err(1);
            }
            load_stdin_bytes_with_limit(max_input_bytes)
        }
    }
}

pub(crate) fn load_input_bytes_with_limit(
    path: &PathBuf,
    max_input_bytes: usize,
) -> Result<Vec<u8>, i32> {
    match read_file_with_limit(path, max_input_bytes) {
        Ok(value) => Ok(value),
        Err(message) => {
            eprintln!("failed to read input: {}", message);
            Err(1)
        }
    }
}

fn read_file_with_limit(path: &PathBuf, max_bytes: usize) -> Result<Vec<u8>, String> {
    let metadata = fs::metadata(path).map_err(|err| err.to_string())?;
    if metadata.len() > max_bytes as u64 {
        return Err(format!("input exceeds max_input_bytes ({})", max_bytes));
    }
    let mut file = fs::File::open(path).map_err(|err| err.to_string())?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(max_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|err| err.to_string())?;
    if bytes.len() > max_bytes {
        return Err(format!("input exceeds max_input_bytes ({})", max_bytes));
    }
    Ok(bytes)
}

fn load_stdin_bytes_with_limit(max_input_bytes: usize) -> Result<Vec<u8>, i32> {
    let mut stdin = std::io::stdin().lock();
    let mut bytes = Vec::new();
    match Read::by_ref(&mut stdin)
        .take(max_input_bytes as u64 + 1)
        .read_to_end(&mut bytes)
    {
        Ok(_) if bytes.len() <= max_input_bytes => Ok(bytes),
        Ok(_) => {
            eprintln!(
                "failed to read input: input exceeds max_input_bytes ({})",
                max_input_bytes
            );
            Err(1)
        }
        Err(err) => {
            eprintln!("failed to read input: {}", err);
            Err(1)
        }
    }
}

pub(crate) fn load_context(path: &Option<PathBuf>) -> Result<Option<serde_json::Value>, i32> {
    match path {
        Some(path) => match fs::read_to_string(path) {
            Ok(data) => match serde_json::from_str(&data) {
                Ok(json) => Ok(Some(json)),
                Err(err) => {
                    eprintln!("failed to parse context JSON: {}", err);
                    Err(1)
                }
            },
            Err(err) => {
                eprintln!("failed to read context: {}", err);
                Err(1)
            }
        },
        None => Ok(None),
    }
}
