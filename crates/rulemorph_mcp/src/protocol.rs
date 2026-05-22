use std::io::{self, BufRead, Write};

use serde_json::Value;

const MCP_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
const MCP_MAX_HEADER_LINE_BYTES: usize = 8 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OutputMode {
    Line,
    ContentLength,
}

pub(crate) fn read_message(
    reader: &mut impl BufRead,
    output_mode: &mut OutputMode,
) -> io::Result<Option<String>> {
    let mut line: String;
    loop {
        line = match read_line_bounded(reader, MCP_MAX_HEADER_LINE_BYTES)? {
            Some(line) => line,
            None => return Ok(None),
        };

        if let Some(length) = line.strip_prefix("Content-Length:") {
            let length = length.trim().parse::<usize>().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid Content-Length")
            })?;
            if length > MCP_MAX_MESSAGE_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Content-Length exceeds MCP message limit",
                ));
            }

            loop {
                line = match read_line_bounded(reader, MCP_MAX_HEADER_LINE_BYTES)? {
                    Some(line) => line,
                    None => return Ok(None),
                };
                if line == "\r\n" || line == "\n" {
                    break;
                }
            }

            let mut buffer = vec![0u8; length];
            reader.read_exact(&mut buffer)?;
            *output_mode = OutputMode::ContentLength;
            return Ok(Some(String::from_utf8_lossy(&buffer).to_string()));
        }

        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        *output_mode = OutputMode::Line;
        return Ok(Some(trimmed.to_string()));
    }
}

fn read_line_bounded(reader: &mut impl BufRead, max_bytes: usize) -> io::Result<Option<String>> {
    let mut bytes = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            if bytes.is_empty() {
                return Ok(None);
            }
            break;
        }

        let take_len = match available.iter().position(|byte| *byte == b'\n') {
            Some(index) => index + 1,
            None => available.len(),
        };
        if bytes.len().saturating_add(take_len) > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "line exceeds MCP message limit",
            ));
        }
        bytes.extend_from_slice(&available[..take_len]);
        reader.consume(take_len);
        if bytes.last() == Some(&b'\n') {
            break;
        }
    }

    String::from_utf8(bytes)
        .map(Some)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

pub(crate) fn write_message(
    writer: &mut impl Write,
    output_mode: OutputMode,
    message: &Value,
) -> io::Result<()> {
    let text = serde_json::to_string(message)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;

    match output_mode {
        OutputMode::Line => {
            writeln!(writer, "{}", text)?;
        }
        OutputMode::ContentLength => {
            write!(writer, "Content-Length: {}\r\n\r\n{}", text.len(), text)?;
        }
    }

    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor};

    #[test]
    fn bounded_line_reader_rejects_oversized_line() {
        let mut reader = BufReader::new(Cursor::new(b"{\"jsonrpc\":\"2.0\"}\n".to_vec()));
        let err = read_line_bounded(&mut reader, 8).expect_err("oversized line should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn bounded_line_reader_reads_complete_line() {
        let mut reader = BufReader::new(Cursor::new(b"{\"jsonrpc\":\"2.0\"}\n".to_vec()));
        let line = read_line_bounded(&mut reader, 64)
            .expect("read line")
            .expect("line");
        assert_eq!(line, "{\"jsonrpc\":\"2.0\"}\n");
    }

    #[test]
    fn read_message_rejects_oversized_initial_header_line() {
        let mut input = b"Content-Length: ".to_vec();
        input.extend(std::iter::repeat_n(b'1', MCP_MAX_HEADER_LINE_BYTES));
        input.extend_from_slice(b"\r\n\r\n{}");
        let mut reader = BufReader::new(Cursor::new(input));
        let mut output_mode = OutputMode::Line;
        let err = read_message(&mut reader, &mut output_mode)
            .expect_err("oversized initial header should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
