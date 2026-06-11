use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use axum::http::HeaderMap;
use futures_util::TryStreamExt;
use http_body_util::{BodyExt, Limited};
use serde_json::{Value as JsonValue, json};
use zip::ZipArchive;

use super::{
    EndpointError, MULTIPART_IMPORT_MAX_ENTRIES, MULTIPART_IMPORT_MAX_FILE_BYTES,
    MULTIPART_IMPORT_MAX_TOTAL_BYTES,
};

pub(super) async fn build_multipart_import_body(
    headers: &HeaderMap,
    body: axum::body::Body,
) -> Result<(JsonValue, tempfile::TempDir), EndpointError> {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| EndpointError::bad_request("missing content-type"))?;
    let boundary = multer::parse_boundary(content_type)
        .map_err(|err| EndpointError::bad_request(format!("multipart error: {}", err)))?;
    let stream = Limited::new(body, MULTIPART_IMPORT_MAX_TOTAL_BYTES as usize)
        .into_data_stream()
        .map_err(|err| std::io::Error::other(err.to_string()));
    let mut multipart = multer::Multipart::new(stream, boundary);
    let mut zip_file: Option<tempfile::NamedTempFile> = None;
    let mut total_bytes: u64 = 0;

    while let Some(field) = multipart.next_field().await.map_err(multipart_error)? {
        if field.name() != Some("bundle") {
            continue;
        }
        let mut handle = tempfile::NamedTempFile::new()
            .map_err(|err| EndpointError::network(err.to_string()))?;
        let mut field = field;
        while let Some(chunk) = field.chunk().await.map_err(multipart_upload_error)? {
            total_bytes = total_bytes.saturating_add(chunk.len() as u64);
            if total_bytes > MULTIPART_IMPORT_MAX_TOTAL_BYTES {
                return Err(EndpointError::payload_too_large(
                    MULTIPART_IMPORT_MAX_TOTAL_BYTES as usize,
                ));
            }
            handle
                .write_all(&chunk)
                .map_err(|err| EndpointError::network(err.to_string()))?;
        }
        zip_file = Some(handle);
        break;
    }

    let zip_file = zip_file.ok_or_else(|| EndpointError::bad_request("missing bundle file"))?;
    let extract_dir =
        tempfile::TempDir::new().map_err(|err| EndpointError::network(err.to_string()))?;
    extract_zip(zip_file.path(), extract_dir.path()).map_err(EndpointError::bad_request)?;
    let bundle_root = resolve_bundle_root(extract_dir.path())?;
    Ok((
        json!({ "bundle_path": bundle_root.display().to_string() }),
        extract_dir,
    ))
}

fn multipart_error(err: multer::Error) -> EndpointError {
    let message = err.to_string();
    if message.contains("length limit") {
        EndpointError::payload_too_large(MULTIPART_IMPORT_MAX_TOTAL_BYTES as usize)
    } else {
        EndpointError::bad_request(format!("multipart error: {}", message))
    }
}

fn multipart_upload_error(err: multer::Error) -> EndpointError {
    let message = err.to_string();
    if message.contains("length limit") {
        EndpointError::payload_too_large(MULTIPART_IMPORT_MAX_TOTAL_BYTES as usize)
    } else {
        EndpointError::bad_request(format!("upload error: {}", message))
    }
}

pub(super) fn extract_zip(path: &Path, dest: &Path) -> Result<(), String> {
    let file = File::open(path).map_err(|err| format!("failed to open zip: {}", err))?;
    let mut archive = ZipArchive::new(file).map_err(|err| format!("invalid zip: {}", err))?;
    let mut total_bytes: u64 = 0;

    if archive.len() > MULTIPART_IMPORT_MAX_ENTRIES {
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
                _ => return Err(format!("invalid zip entry path: {}", name)),
            }
        }
        if let Some(mode) = entry.unix_mode()
            && (mode & 0o170000) == 0o120000
        {
            return Err(format!("zip entry is symlink: {}", name));
        }
        let out_path = dest.join(entry_path);
        if entry.is_dir() {
            std::fs::create_dir_all(&out_path)
                .map_err(|err| format!("failed to create dir: {}", err))?;
            continue;
        }
        let size = entry.size();
        if size > MULTIPART_IMPORT_MAX_FILE_BYTES {
            return Err(format!("zip entry too large: {}", name));
        }
        total_bytes = total_bytes.saturating_add(size);
        if total_bytes > MULTIPART_IMPORT_MAX_TOTAL_BYTES {
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
        let copied =
            copy_zip_entry_bounded(&mut entry, &mut outfile, MULTIPART_IMPORT_MAX_FILE_BYTES)
                .map_err(|err| format!("failed to write file: {}", err))?;
        total_bytes = total_bytes.saturating_sub(size).saturating_add(copied);
        if copied > MULTIPART_IMPORT_MAX_FILE_BYTES {
            return Err(format!("zip entry too large: {}", name));
        }
        if total_bytes > MULTIPART_IMPORT_MAX_TOTAL_BYTES {
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

fn resolve_bundle_root(base: &Path) -> Result<PathBuf, EndpointError> {
    if base.join("traces").exists() || base.join("rules").exists() {
        return Ok(base.to_path_buf());
    }
    let mut entries = std::fs::read_dir(base)
        .map_err(|err| EndpointError::bad_request(format!("invalid zip bundle: {}", err)))?
        .filter_map(|entry| entry.ok())
        .collect::<Vec<_>>();
    if entries.len() == 1 {
        let entry = entries.remove(0);
        let path = entry.path();
        if path.is_dir() && (path.join("traces").exists() || path.join("rules").exists()) {
            return Ok(path);
        }
    }
    Err(EndpointError::bad_request(
        "zip bundle must include traces/ or rules/",
    ))
}
