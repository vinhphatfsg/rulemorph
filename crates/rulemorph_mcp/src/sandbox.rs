use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::errors::{CallError, io_error_json};

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

pub(crate) fn write_allowed_output(path: &str, output: &str) -> Result<(), String> {
    let path = Path::new(path);
    ensure_allowed_output_path(path)?;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("failed to create output directory: {}", err))?;
        }
    }
    fs::write(path, output.as_bytes()).map_err(|err| format!("failed to write output: {}", err))
}

fn allowed_existing_path(path: &str) -> Result<PathBuf, String> {
    let path = Path::new(path);
    let canonical = path
        .canonicalize()
        .map_err(|err| format!("failed to resolve path: {}", err))?;
    ensure_allowed_canonical_path(&canonical)?;
    Ok(canonical)
}

fn ensure_allowed_output_path(path: &Path) -> Result<(), String> {
    let path = absolute_clean_path(path)?;
    if path.exists() {
        let check_path = path
            .canonicalize()
            .map_err(|err| format!("failed to resolve output path: {}", err))?;
        ensure_allowed_canonical_path(&check_path)
    } else {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let ancestor = nearest_existing_ancestor(parent)?;
        let check_path = ancestor
            .canonicalize()
            .map_err(|err| format!("failed to resolve output directory: {}", err))?;
        ensure_allowed_canonical_path(&check_path)
    }
}

fn absolute_clean_path(path: &Path) -> Result<PathBuf, String> {
    let mut clean = if path.is_absolute() {
        PathBuf::new()
    } else {
        std::env::current_dir()
            .map_err(|err| format!("failed to read current directory: {}", err))?
            .canonicalize()
            .map_err(|err| format!("failed to resolve current directory: {}", err))?
    };
    for component in path.components() {
        match component {
            std::path::Component::Prefix(prefix) => clean.push(prefix.as_os_str()),
            std::path::Component::RootDir => {
                push_root_dir(&mut clean);
            }
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if !clean.pop() {
                    return Err("output path escapes filesystem root".to_string());
                }
            }
            std::path::Component::Normal(value) => clean.push(value),
        }
    }
    Ok(clean)
}

fn push_root_dir(clean: &mut PathBuf) {
    #[cfg(windows)]
    {
        if matches!(
            clean.components().next(),
            Some(std::path::Component::Prefix(_))
        ) {
            let mut rooted = clean.as_os_str().to_os_string();
            rooted.push(std::path::MAIN_SEPARATOR.to_string());
            *clean = PathBuf::from(rooted);
            return;
        }
    }
    clean.push(std::path::MAIN_SEPARATOR.to_string());
}

fn nearest_existing_ancestor(path: &Path) -> Result<PathBuf, String> {
    let mut ancestor = path.to_path_buf();
    loop {
        if ancestor.exists() {
            return Ok(ancestor);
        }
        if !ancestor.pop() {
            return Err("failed to find existing output directory ancestor".to_string());
        }
    }
}

fn ensure_allowed_canonical_path(path: &Path) -> Result<(), String> {
    if allow_any_path() {
        return Ok(());
    }
    let roots = allowed_roots()?;
    if roots.iter().any(|root| path.starts_with(root)) {
        return Ok(());
    }
    Err(format!(
        "path is outside MCP allowed roots: {}",
        path.display()
    ))
}

fn allowed_roots() -> Result<Vec<PathBuf>, String> {
    if let Some(raw) = std::env::var_os("RULEMORPH_MCP_ALLOWED_ROOTS") {
        let roots = std::env::split_paths(&raw)
            .map(|path| {
                path.canonicalize().map_err(|err| {
                    format!(
                        "failed to resolve MCP allowed root {}: {}",
                        path.display(),
                        err
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if !roots.is_empty() {
            return Ok(roots);
        }
    }
    let cwd = std::env::current_dir()
        .map_err(|err| format!("failed to read current directory: {}", err))?
        .canonicalize()
        .map_err(|err| format!("failed to resolve current directory: {}", err))?;
    Ok(vec![cwd])
}

fn allow_any_path() -> bool {
    std::env::var("RULEMORPH_MCP_ALLOW_ANY_PATH")
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn allowed_roots_fallback_uses_canonical_current_dir() {
        let _guard = env_lock();
        let previous_cwd = std::env::current_dir().expect("current dir");
        let previous_allowed_roots = std::env::var_os("RULEMORPH_MCP_ALLOWED_ROOTS");
        let previous_allow_any = std::env::var_os("RULEMORPH_MCP_ALLOW_ANY_PATH");
        let temp = tempfile::tempdir().expect("tempdir");

        unsafe {
            std::env::remove_var("RULEMORPH_MCP_ALLOWED_ROOTS");
            std::env::remove_var("RULEMORPH_MCP_ALLOW_ANY_PATH");
        }
        std::env::set_current_dir(temp.path()).expect("set current dir");

        let roots = allowed_roots().expect("allowed roots");
        assert_eq!(
            roots,
            vec![temp.path().canonicalize().expect("canonical temp")]
        );

        std::env::set_current_dir(previous_cwd).expect("restore current dir");
        unsafe {
            match previous_allowed_roots {
                Some(value) => std::env::set_var("RULEMORPH_MCP_ALLOWED_ROOTS", value),
                None => std::env::remove_var("RULEMORPH_MCP_ALLOWED_ROOTS"),
            }
            match previous_allow_any {
                Some(value) => std::env::set_var("RULEMORPH_MCP_ALLOW_ANY_PATH", value),
                None => std::env::remove_var("RULEMORPH_MCP_ALLOW_ANY_PATH"),
            }
        }
    }

    #[cfg(windows)]
    #[test]
    fn absolute_clean_path_preserves_windows_drive_root() {
        let path = Path::new(r"C:\work\..\allowed\out.json");
        let clean = absolute_clean_path(path).expect("clean path");

        assert_eq!(clean, PathBuf::from(r"C:\allowed\out.json"));
    }
}
