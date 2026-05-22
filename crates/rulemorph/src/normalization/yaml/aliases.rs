use crate::error::{TransformError, TransformErrorKind};

use super::NormalizationOptions;

pub(super) fn enforce_yaml_alias_limit(
    input: &str,
    options: &NormalizationOptions,
) -> Result<(), TransformError> {
    let aliases = count_yaml_alias_tokens(input);
    if aliases > options.max_yaml_aliases {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "input exceeds max_yaml_aliases",
        ));
    }
    Ok(())
}

fn count_yaml_alias_tokens(input: &str) -> usize {
    let mut count = 0usize;
    let mut block_scalar_indent: Option<usize> = None;

    for line in input.lines() {
        let indent = line.chars().take_while(|value| *value == ' ').count();
        if let Some(block_indent) = block_scalar_indent {
            if line.trim().is_empty() || indent > block_indent {
                continue;
            }
            block_scalar_indent = None;
        }

        if starts_block_scalar(line) {
            block_scalar_indent = Some(indent);
        }
        count = count.saturating_add(count_yaml_alias_tokens_in_line(line));
    }

    count
}

fn starts_block_scalar(line: &str) -> bool {
    let mut in_single = false;
    let mut in_double = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => {
                in_double = !in_double;
                while in_double {
                    match chars.next() {
                        Some('\\') => {
                            chars.next();
                        }
                        Some('"') => in_double = false,
                        Some(_) => {}
                        None => break,
                    }
                }
            }
            '#' if !in_single && !in_double => break,
            '|' | '>' if !in_single && !in_double => {
                let tail = chars.collect::<String>();
                let tail = tail.trim();
                return tail.is_empty()
                    || tail
                        .chars()
                        .all(|value| matches!(value, '+' | '-' | '0'..='9'));
            }
            _ => {}
        }
    }
    false
}

fn count_yaml_alias_tokens_in_line(line: &str) -> usize {
    let mut count = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let bytes = line.as_bytes();
    let mut index = 0usize;

    while index < bytes.len() {
        let byte = bytes[index];
        match byte {
            b'\'' if !in_double => {
                in_single = !in_single;
                index += 1;
            }
            b'"' if !in_single => {
                in_double = !in_double;
                index += 1;
            }
            b'\\' if in_double => {
                index = (index + 2).min(bytes.len());
            }
            b'#' if !in_single && !in_double => break,
            b'*' if !in_single && !in_double => {
                if is_alias_token_boundary(bytes, index) {
                    count = count.saturating_add(1);
                }
                index += 1;
            }
            _ => index += 1,
        }
    }

    count
}

fn is_alias_token_boundary(bytes: &[u8], index: usize) -> bool {
    let previous = index
        .checked_sub(1)
        .and_then(|previous| bytes.get(previous))
        .copied();
    let next = bytes.get(index + 1).copied();
    previous.is_none_or(|value| {
        value.is_ascii_whitespace() || matches!(value, b'[' | b'{' | b',' | b':' | b'-')
    }) && next.is_some_and(is_yaml_anchor_char)
}

fn is_yaml_anchor_char(value: u8) -> bool {
    value.is_ascii_alphanumeric() || matches!(value, b'_' | b'-')
}
