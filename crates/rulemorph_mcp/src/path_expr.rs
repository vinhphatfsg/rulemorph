use serde_json::Value;

pub(crate) fn append_path(prefix: &str, key: &str) -> String {
    let needs_quote = key
        .chars()
        .any(|ch| ch == '.' || ch == '[' || ch == ']' || ch == '"' || ch == '\'' || ch == '\\');
    let segment = if needs_quote {
        let escaped = key.replace('\\', "\\\\").replace('"', "\\\"");
        format!("[\"{}\"]", escaped)
    } else {
        key.to_string()
    };
    if prefix.is_empty() {
        segment
    } else if segment.starts_with('[') {
        format!("{}{}", prefix, segment)
    } else {
        format!("{}.{}", prefix, segment)
    }
}

pub(crate) fn leaf_from_path(path: &str) -> Option<String> {
    match parse_path_tokens(path) {
        Ok(tokens) => {
            for token in tokens.iter().rev() {
                if let PathToken::Key(key) = token {
                    return Some(key.clone());
                }
            }
            None
        }
        Err(_) => Some(path.to_string()),
    }
}

pub(crate) fn get_value_at_path<'a>(
    value: &'a Value,
    path: &str,
) -> Result<Option<&'a Value>, String> {
    let tokens = parse_path_tokens(path)?;
    Ok(get_value_by_tokens(value, &tokens))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathToken {
    Key(String),
    Index(usize),
}

fn parse_path_tokens(path: &str) -> Result<Vec<PathToken>, String> {
    if path.is_empty() {
        return Err("path is empty".to_string());
    }

    let chars: Vec<char> = path.chars().collect();
    let mut tokens = Vec::new();
    let mut index = 0;

    while index < chars.len() {
        if chars[index] == '.' {
            return Err("path segment is empty".to_string());
        }

        if chars[index] == '[' {
            let (token, next) = parse_bracket(&chars, index)?;
            tokens.push(token);
            index = next;
        } else {
            let start = index;
            while index < chars.len() && chars[index] != '.' && chars[index] != '[' {
                index += 1;
            }
            if start == index {
                return Err("path segment is empty".to_string());
            }
            let key: String = chars[start..index].iter().collect();
            if key.is_empty() {
                return Err("path segment is empty".to_string());
            }
            tokens.push(PathToken::Key(key));
        }

        while index < chars.len() && chars[index] == '[' {
            let (token, next) = parse_bracket(&chars, index)?;
            tokens.push(token);
            index = next;
        }

        if index < chars.len() {
            if chars[index] == '.' {
                index += 1;
                if index == chars.len() {
                    return Err("path syntax is invalid".to_string());
                }
            } else {
                return Err("path syntax is invalid".to_string());
            }
        }
    }

    Ok(tokens)
}

fn parse_bracket(chars: &[char], start: usize) -> Result<(PathToken, usize), String> {
    if chars.get(start) != Some(&'[') {
        return Err("path syntax is invalid".to_string());
    }
    let index = start + 1;
    if index >= chars.len() {
        return Err("path syntax is invalid".to_string());
    }

    match chars[index] {
        '"' | '\'' => parse_quoted(chars, index),
        c if c.is_ascii_digit() => parse_index(chars, index),
        _ => Err("path syntax is invalid".to_string()),
    }
}

fn parse_index(chars: &[char], start: usize) -> Result<(PathToken, usize), String> {
    let mut index = start;
    let mut value: usize = 0;
    let mut has_digit = false;

    while index < chars.len() && chars[index].is_ascii_digit() {
        has_digit = true;
        value = value
            .saturating_mul(10)
            .saturating_add(chars[index].to_digit(10).unwrap_or(0) as usize);
        index += 1;
    }

    if !has_digit {
        return Err("path syntax is invalid".to_string());
    }
    if chars.get(index) != Some(&']') {
        return Err("path syntax is invalid".to_string());
    }
    index += 1;
    Ok((PathToken::Index(value), index))
}

fn parse_quoted(chars: &[char], start: usize) -> Result<(PathToken, usize), String> {
    let quote = chars[start];
    let mut index = start + 1;
    let mut value = String::new();
    while index < chars.len() {
        let ch = chars[index];
        if ch == '\\' {
            index += 1;
            if index >= chars.len() {
                return Err("path escape is invalid".to_string());
            }
            let escaped = chars[index];
            if escaped == '\\' || escaped == quote {
                value.push(escaped);
                index += 1;
                continue;
            }
            return Err("path escape is invalid".to_string());
        }

        if ch == '[' || ch == ']' {
            return Err("path syntax is invalid".to_string());
        }

        if ch == quote {
            index += 1;
            break;
        }

        value.push(ch);
        index += 1;
    }

    if value.is_empty() {
        return Err("path segment is empty".to_string());
    }
    if chars.get(index - 1) != Some(&quote) {
        return Err("path syntax is invalid".to_string());
    }
    if chars.get(index) != Some(&']') {
        return Err("path syntax is invalid".to_string());
    }
    index += 1;
    Ok((PathToken::Key(value), index))
}

fn get_value_by_tokens<'a>(value: &'a Value, tokens: &[PathToken]) -> Option<&'a Value> {
    let mut current = value;
    for token in tokens {
        match token {
            PathToken::Key(key) => match current {
                Value::Object(map) => current = map.get(key)?,
                _ => return None,
            },
            PathToken::Index(index) => match current {
                Value::Array(items) => current = items.get(*index)?,
                _ => return None,
            },
        }
    }
    Some(current)
}
