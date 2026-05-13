use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use zip::ZipArchive;

use super::{
    ApiError, IMPORT_ZIP_MAX_ENTRIES, IMPORT_ZIP_MAX_FILE_BYTES, IMPORT_ZIP_MAX_TOTAL_BYTES,
};

pub(super) fn validate_bundle_path(bundle_path: &Path) -> std::result::Result<PathBuf, ApiError> {
    let bundle_path = bundle_path
        .canonicalize()
        .map_err(|err| ApiError::bad_request(format!("invalid bundle_path: {}", err)))?;
    if !bundle_path.is_dir() {
        return Err(ApiError::bad_request("bundle_path must be a directory"));
    }
    let temp_dir = std::env::temp_dir()
        .canonicalize()
        .unwrap_or_else(|_| std::env::temp_dir());
    if !bundle_path.starts_with(&temp_dir) {
        return Err(ApiError::bad_request(format!(
            "bundle_path must be under {}",
            temp_dir.display()
        )));
    }
    Ok(bundle_path)
}

pub(super) fn extract_zip(path: &Path, dest: &Path) -> Result<(), String> {
    let file = File::open(path).map_err(|err| format!("failed to open zip: {}", err))?;
    let mut archive = ZipArchive::new(file).map_err(|err| format!("invalid zip: {}", err))?;
    let mut total_bytes: u64 = 0;

    if archive.len() > IMPORT_ZIP_MAX_ENTRIES {
        return Err("zip has too many entries".to_string());
    }

    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|err| format!("zip entry error: {}", err))?;
        let name = entry.name().to_string();
        let entry_path = Path::new(&name);
        for component in entry_path.components() {
            match component {
                std::path::Component::Normal(_) => {}
                _ => {
                    return Err(format!("invalid zip entry path: {}", name));
                }
            }
        }
        if let Some(mode) = entry.unix_mode() {
            if (mode & 0o170000) == 0o120000 {
                return Err(format!("zip entry is symlink: {}", name));
            }
        }
        let out_path = dest.join(entry_path);
        if entry.is_dir() {
            std::fs::create_dir_all(&out_path)
                .map_err(|err| format!("failed to create dir: {}", err))?;
            continue;
        }
        let size = entry.size();
        if size > IMPORT_ZIP_MAX_FILE_BYTES {
            return Err(format!("zip entry too large: {}", name));
        }
        total_bytes = total_bytes.saturating_add(size);
        if total_bytes > IMPORT_ZIP_MAX_TOTAL_BYTES {
            return Err("zip exceeds max total bytes".to_string());
        }
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| format!("failed to create dir: {}", err))?;
        }
        let mut outfile = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&out_path)
            .map_err(|err| format!("failed to create file: {}", err))?;
        let copied = copy_zip_entry_bounded(&mut entry, &mut outfile, IMPORT_ZIP_MAX_FILE_BYTES)
            .map_err(|err| format!("failed to write file: {}", err))?;
        total_bytes = total_bytes.saturating_sub(size).saturating_add(copied);
        if copied > IMPORT_ZIP_MAX_FILE_BYTES {
            return Err(format!("zip entry too large: {}", name));
        }
        if total_bytes > IMPORT_ZIP_MAX_TOTAL_BYTES {
            return Err("zip exceeds max total bytes".to_string());
        }
    }
    Ok(())
}

pub(super) fn copy_zip_entry_bounded<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    max_bytes: u64,
) -> std::io::Result<u64> {
    let mut copied = 0u64;
    let mut buffer = [0u8; 8192];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(copied);
        }
        copied = copied.saturating_add(read as u64);
        if copied > max_bytes {
            return Ok(copied);
        }
        writer.write_all(&buffer[..read])?;
    }
}

pub(super) fn resolve_bundle_root(base: &Path) -> std::result::Result<PathBuf, ApiError> {
    if base.join("traces").exists() || base.join("rules").exists() {
        return Ok(base.to_path_buf());
    }
    let mut entries = std::fs::read_dir(base)
        .map_err(|err| ApiError::bad_request(format!("invalid zip bundle: {}", err)))?
        .filter_map(|entry| entry.ok())
        .collect::<Vec<_>>();
    if entries.len() == 1 {
        let entry = entries.remove(0);
        let path = entry.path();
        if path.is_dir() && (path.join("traces").exists() || path.join("rules").exists()) {
            return Ok(path);
        }
    }
    Err(ApiError::bad_request(
        "zip bundle must include traces/ or rules/",
    ))
}
