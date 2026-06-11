use std::fmt;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result};

use crate::trace_schema::{
    TRACE_CHUNK_BYTES_COMPRESSED_HARD_MAX, TRACE_CHUNK_BYTES_COMPRESSED_OVERHEAD_MAX,
    TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX, TraceChunkRef,
};

const HARD_MAX_CHUNK_BYTES_COMPRESSED: u64 = TRACE_CHUNK_BYTES_COMPRESSED_HARD_MAX as u64;
// Zstd frames produced with default settings expect at least an 8MB window.
const ZSTD_WINDOW_BYTES_MIN: usize = 8 * 1024 * 1024;

#[derive(Debug)]
pub(super) struct ChunkSizeExceeded {
    actual: u64,
    max: u64,
}

impl fmt::Display for ChunkSizeExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "trace chunk exceeds max bytes: {} > {}",
            self.actual, self.max
        )
    }
}

impl std::error::Error for ChunkSizeExceeded {}

pub(in crate::trace_store) fn resolve_chunk_path(
    base_dir: &Path,
    chunk_path: &str,
) -> Result<PathBuf> {
    let rel = Path::new(chunk_path);
    if rel.as_os_str().is_empty() {
        return Err(anyhow::anyhow!("trace chunk path is empty"));
    }
    if rel.is_absolute()
        || rel.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(anyhow::anyhow!(
            "trace chunk path must be relative without parent components: {}",
            chunk_path
        ));
    }

    let base_dir = base_dir.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize trace base dir: {}",
            base_dir.display()
        )
    })?;
    let path = base_dir.join(rel);
    let resolved = path.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize trace chunk path: {}",
            path.display()
        )
    })?;
    if !resolved.starts_with(&base_dir) {
        return Err(anyhow::anyhow!(
            "trace chunk path escapes base dir: {}",
            chunk_path
        ));
    }
    Ok(resolved)
}

pub(super) fn read_chunk_bytes(
    base_dir: &Path,
    chunk: &TraceChunkRef,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    let path = resolve_chunk_path(base_dir, &chunk.path)?;
    let compressed_bytes = std::fs::metadata(&path)
        .with_context(|| format!("failed to read trace chunk metadata: {}", path.display()))?
        .len();
    let max_compressed_bytes = std::cmp::min(
        HARD_MAX_CHUNK_BYTES_COMPRESSED,
        (max_bytes as u64).saturating_add(TRACE_CHUNK_BYTES_COMPRESSED_OVERHEAD_MAX as u64),
    );
    if compressed_bytes > max_compressed_bytes {
        return Err(anyhow::Error::new(ChunkSizeExceeded {
            actual: compressed_bytes,
            max: max_compressed_bytes,
        }));
    }
    let raw = std::fs::read(&path)
        .with_context(|| format!("failed to read trace chunk: {}", path.display()))?;
    match chunk.compression.as_str() {
        "zstd" => decode_zstd_limited(&raw, max_bytes),
        "none" => {
            if raw.len() > max_bytes {
                return Err(anyhow::Error::new(ChunkSizeExceeded {
                    actual: raw.len() as u64,
                    max: max_bytes as u64,
                }));
            }
            Ok(raw)
        }
        other => Err(anyhow::anyhow!("unsupported compression: {}", other)),
    }
}

fn decode_zstd_limited(raw: &[u8], max_bytes: usize) -> Result<Vec<u8>> {
    let mut decoder = zstd::stream::read::Decoder::new(raw)?;
    decoder.window_log_max(zstd_window_log_max(max_bytes))?;
    let mut limited = decoder.take((max_bytes as u64).saturating_add(1));
    let mut output = Vec::new();
    limited.read_to_end(&mut output)?;
    if output.len() > max_bytes {
        return Err(anyhow::Error::new(ChunkSizeExceeded {
            actual: output.len() as u64,
            max: max_bytes as u64,
        }));
    }
    Ok(output)
}

fn zstd_window_log_max(max_bytes: usize) -> u32 {
    let max_bytes = max_bytes
        .max(ZSTD_WINDOW_BYTES_MIN)
        .clamp(1, TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX) as u64;
    let pow2 = max_bytes.next_power_of_two();
    let log = 63u32.saturating_sub(pow2.leading_zeros());
    log.clamp(20, 31)
}
