use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use sha2::{Digest, Sha256};

use super::super::{TraceCompression, TraceWriteOptions, reserve_budget, write_atomic};

pub(super) fn build_payload_preview(raw: &[u8], limit: usize) -> String {
    if limit == 0 || raw.is_empty() {
        return String::new();
    }
    if raw.len() <= limit {
        return String::from_utf8_lossy(raw).into_owned();
    }
    let mut preview = String::from_utf8_lossy(&raw[..limit]).into_owned();
    preview.push_str("...");
    preview
}

pub(super) enum WriteBlobResult {
    Written {
        blob_ref: String,
        blob_path: PathBuf,
    },
    BudgetExceeded,
}

pub(super) fn write_blob(
    trace_dir: &Path,
    raw: &[u8],
    options: &TraceWriteOptions,
    seen: &mut HashSet<String>,
    budget_remaining: &mut u64,
) -> Result<WriteBlobResult> {
    let hash = Sha256::digest(raw);
    let hash_hex = hex_encode(&hash);
    let extension = match options.compression {
        TraceCompression::Zstd => ".zst",
        TraceCompression::None => "",
    };
    let filename = format!("sha256-{hash_hex}.json{extension}");
    let rel_path = PathBuf::from("blobs").join(filename);
    let rel_string = rel_path.to_string_lossy().to_string();
    let full_path = trace_dir.join(&rel_path);
    if !seen.insert(rel_string.clone()) {
        return Ok(WriteBlobResult::Written {
            blob_ref: rel_string,
            blob_path: full_path,
        });
    }
    if let Some(parent) = full_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file_exists = full_path.exists();
    let mut needs_write = !file_exists;
    let mut payload: Option<Vec<u8>> = None;
    let bytes = if file_exists {
        match fs::metadata(&full_path) {
            Ok(metadata) => metadata.len(),
            Err(_) => {
                let computed = match options.compression {
                    TraceCompression::Zstd => zstd::stream::encode_all(raw, 3)?,
                    TraceCompression::None => raw.to_vec(),
                };
                let bytes = computed.len() as u64;
                payload = Some(computed);
                needs_write = true;
                bytes
            }
        }
    } else {
        let computed = match options.compression {
            TraceCompression::Zstd => zstd::stream::encode_all(raw, 3)?,
            TraceCompression::None => raw.to_vec(),
        };
        let bytes = computed.len() as u64;
        payload = Some(computed);
        bytes
    };
    if !reserve_budget(budget_remaining, bytes) {
        return Ok(WriteBlobResult::BudgetExceeded);
    }
    if needs_write {
        if let Some(payload) = payload {
            write_atomic(&full_path, payload.as_slice())?;
        }
    }
    Ok(WriteBlobResult::Written {
        blob_ref: rel_string,
        blob_path: full_path,
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push_str(&format!("{:02x}", byte));
    }
    output
}
