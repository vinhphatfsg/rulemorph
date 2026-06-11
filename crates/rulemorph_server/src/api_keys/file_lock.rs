use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};

const API_KEY_LOCK_STALE_AFTER: Duration = Duration::from_secs(300);

#[cfg(unix)]
unsafe extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

pub(super) struct ApiKeyFileLock {
    path: PathBuf,
}

impl ApiKeyFileLock {
    pub(super) fn acquire(path: &Path) -> Result<Self> {
        let lock_path = lock_path(path);
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create api key lock dir: {}", parent.display())
            })?;
        }
        let mut last_err = None;
        for _ in 0..250 {
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
            {
                Ok(mut handle) => {
                    writeln!(handle, "{}", std::process::id()).ok();
                    return Ok(Self { path: lock_path });
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    if remove_stale_lock_if_needed(&lock_path)? {
                        continue;
                    }
                    last_err = Some(err);
                    thread::sleep(Duration::from_millis(20));
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("failed to create api key lock: {}", lock_path.display())
                    });
                }
            }
        }
        Err(last_err.unwrap_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::TimedOut, "api key lock timeout")
        }))
        .with_context(|| {
            format!(
                "timed out waiting for api key lock: {}",
                lock_path.display()
            )
        })
    }
}

fn remove_stale_lock_if_needed(path: &Path) -> Result<bool> {
    match fs::read_to_string(path) {
        Ok(contents) => {
            if let Ok(pid) = contents.trim().parse::<u32>()
                && pid != std::process::id()
                && !process_is_running(pid)
            {
                remove_lock_file(path)?;
                return Ok(true);
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(_) => {}
    }

    let is_old = fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| modified.elapsed().ok())
        .is_some_and(|age| age >= API_KEY_LOCK_STALE_AFTER);
    if is_old {
        remove_lock_file(path)?;
        return Ok(true);
    }
    Ok(false)
}

fn remove_lock_file(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => {
            Err(err).with_context(|| format!("failed to remove stale lock: {}", path.display()))
        }
    }
}

#[cfg(unix)]
fn process_is_running(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    let rc = unsafe { kill(pid, 0) };
    if rc == 0 {
        return true;
    }
    match std::io::Error::last_os_error().raw_os_error() {
        Some(1) => true,
        Some(3) => false,
        _ => true,
    }
}

#[cfg(not(unix))]
fn process_is_running(_pid: u32) -> bool {
    true
}

impl Drop for ApiKeyFileLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub(super) fn lock_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("api_keys.json");
    path.with_file_name(format!("{}.lock", file_name))
}
