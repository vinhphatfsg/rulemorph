use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::errors::{CallError, io_error_json};

use super::allowed_existing_path;

const MCP_MAX_FILE_BYTES: usize = 64 * 1024 * 1024;

pub(crate) fn read_allowed_to_string(path: &str, label: &str) -> Result<String, CallError> {
    read_allowed_file(path, label).map(|(_, data)| data)
}

pub(crate) fn read_allowed_bytes(path: &str, label: &str) -> Result<Vec<u8>, CallError> {
    read_allowed_file_bytes(path, label).map(|(_, data)| data)
}

pub(crate) fn read_allowed_file(path: &str, label: &str) -> Result<(PathBuf, String), CallError> {
    let (path, bytes) = read_allowed_file_bytes(path, label)?;
    let data = String::from_utf8(bytes).map_err(|err| {
        let message = format!("failed to read {}: {}", label, err);
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![io_error_json(
                &message,
                Some(path.to_string_lossy().as_ref()),
            )]),
        }
    })?;
    Ok((path, data))
}

fn read_allowed_file_bytes(path: &str, label: &str) -> Result<(PathBuf, Vec<u8>), CallError> {
    let path = allowed_existing_path(path).map_err(|err| {
        let message = format!("failed to read {}: {}", label, err);
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![io_error_json(&message, Some(path))]),
        }
    })?;
    let data = read_file_with_limit(&path, MCP_MAX_FILE_BYTES).map_err(|err| {
        let message = format!("failed to read {}: {}", label, err);
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![io_error_json(
                &message,
                Some(path.to_string_lossy().as_ref()),
            )]),
        }
    })?;
    Ok((path, data))
}

fn read_file_with_limit(path: &Path, max_bytes: usize) -> Result<Vec<u8>, String> {
    let metadata = fs::metadata(path).map_err(|err| err.to_string())?;
    if metadata.len() > max_bytes as u64 {
        return Err(format!("file exceeds maximum size ({})", max_bytes));
    }
    let mut file = fs::File::open(path).map_err(|err| err.to_string())?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(max_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|err| err.to_string())?;
    if bytes.len() > max_bytes {
        return Err(format!("file exceeds maximum size ({})", max_bytes));
    }
    Ok(bytes)
}
