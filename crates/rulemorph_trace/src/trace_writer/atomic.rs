use std::fs;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::Mutex;
#[cfg(test)]
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

pub(super) fn write_atomic(path: &Path, payload: &[u8]) -> Result<()> {
    #[cfg(test)]
    if should_fail_write(path) {
        return Err(anyhow::anyhow!("forced write failure"));
    }
    let temp_path = temp_path_for(path)?;
    fs::write(&temp_path, payload)
        .with_context(|| format!("failed to write temporary file: {}", temp_path.display()))?;
    if let Err(err) = fs::rename(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(anyhow::anyhow!(
            "failed to rename temp file {} -> {}: {}",
            temp_path.display(),
            path.display(),
            err
        ));
    }
    Ok(())
}

#[cfg(test)]
fn should_fail_write(path: &Path) -> bool {
    let config = fail_write_config()
        .lock()
        .expect("fail write config lock")
        .clone();
    let Some(config) = config else {
        return false;
    };
    if !path
        .components()
        .any(|component| component.as_os_str() == config.trace_id)
    {
        return false;
    }
    if let Some(filename) = config.filename.as_ref() {
        return path.file_name() == Some(filename);
    }
    true
}

#[cfg(test)]
#[derive(Clone, Debug)]
struct FailWriteConfig {
    trace_id: std::ffi::OsString,
    filename: Option<std::ffi::OsString>,
}

#[cfg(test)]
fn fail_write_config() -> &'static Mutex<Option<FailWriteConfig>> {
    static FAIL_WRITE_CONFIG: OnceLock<Mutex<Option<FailWriteConfig>>> = OnceLock::new();
    FAIL_WRITE_CONFIG.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
pub(super) struct FailWriteGuard;

#[cfg(test)]
impl Drop for FailWriteGuard {
    fn drop(&mut self) {
        let mut guard = fail_write_config().lock().expect("fail write config lock");
        *guard = None;
    }
}

#[cfg(test)]
pub(super) fn fail_write_for_trace_id(trace_id: &str, filename: Option<&str>) -> FailWriteGuard {
    let mut guard = fail_write_config().lock().expect("fail write config lock");
    *guard = Some(FailWriteConfig {
        trace_id: std::ffi::OsString::from(trace_id),
        filename: filename.map(std::ffi::OsString::from),
    });
    FailWriteGuard
}

fn temp_path_for(path: &Path) -> Result<PathBuf> {
    let filename = path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("missing file name for trace chunk"))?;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = std::process::id();
    let temp_name = format!("{}.tmp-{pid}-{nanos}", filename.to_string_lossy());
    Ok(path.with_file_name(temp_name))
}
