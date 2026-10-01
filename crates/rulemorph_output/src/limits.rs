use crate::{OutputError, OutputErrorKind};
use serde_json::Value;
use std::io::{self, Write};

pub(crate) const MAX_BYTES: usize = 64 * 1024;
pub(crate) const MAX_DEPTH: usize = 32;
pub(crate) const MAX_NODES: usize = 4096;

pub(crate) fn limit(message: &str) -> OutputError {
    OutputError::new(OutputErrorKind::LimitExceeded, message)
}

pub(crate) fn check_text(text: &str) -> Result<(), OutputError> {
    if text.len() > MAX_BYTES {
        Err(limit("input exceeds 64 KiB"))
    } else {
        Ok(())
    }
}

pub(crate) fn check_value(
    value: &Value,
    max_nodes: usize,
    max_bytes: usize,
) -> Result<usize, OutputError> {
    let mut nodes = 0;
    walk(value, 0, &mut nodes, max_nodes)?;
    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        maximum: max_bytes,
    };
    serde_json::to_writer(&mut writer, value)
        .map_err(|_| limit("serialized JSON exceeds byte limit"))?;
    Ok(nodes)
}

fn walk(value: &Value, depth: usize, nodes: &mut usize, maximum: usize) -> Result<(), OutputError> {
    *nodes += 1;
    if depth > MAX_DEPTH || *nodes > maximum {
        return Err(limit("JSON depth/node limit exceeded"));
    }
    match value {
        Value::Array(items) => {
            if items.len() > 256 {
                return Err(limit("array length exceeds 256"));
            }
            for item in items {
                walk(item, depth + 1, nodes, maximum)?;
            }
        }
        Value::Object(fields) => {
            if fields.len() > 256 {
                return Err(limit("object fields exceed 256"));
            }
            for value in fields.values() {
                walk(value, depth + 1, nodes, maximum)?;
            }
        }
        _ => {}
    }
    Ok(())
}

struct BoundedWriter {
    bytes: Vec<u8>,
    maximum: usize,
}
impl Write for BoundedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() > self.maximum.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("byte limit"));
        }
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
