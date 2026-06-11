fn normalize_braced_text(text: &str, split_commas_in_parens: bool) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut in_string: Option<char> = None;
    let mut escape = false;
    let mut angle_depth = 0usize;
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            out.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }

        if in_block_comment {
            out.push(ch);
            if ch == '*' && matches!(chars.peek(), Some('/')) {
                out.push('/');
                chars.next();
                in_block_comment = false;
            }
            continue;
        }

        if let Some(quote) = in_string {
            out.push(ch);
            if escape {
                escape = false;
                continue;
            }
            if ch == '\\' {
                escape = true;
                continue;
            }
            if ch == quote {
                in_string = None;
            }
            continue;
        }

        if ch == '/'
            && let Some(next) = chars.peek()
        {
            if *next == '/' {
                out.push(ch);
                out.push(*next);
                chars.next();
                in_line_comment = true;
                continue;
            }
            if *next == '*' {
                out.push(ch);
                out.push(*next);
                chars.next();
                in_block_comment = true;
                continue;
            }
        }

        if ch == '"' || ch == '\'' {
            in_string = Some(ch);
            out.push(ch);
            continue;
        }

        match ch {
            '<' => {
                angle_depth += 1;
                out.push(ch);
            }
            '>' => {
                angle_depth = angle_depth.saturating_sub(1);
                out.push(ch);
            }
            '(' => {
                paren_depth += 1;
                out.push(ch);
            }
            ')' => {
                paren_depth = paren_depth.saturating_sub(1);
                out.push(ch);
            }
            '[' => {
                bracket_depth += 1;
                out.push(ch);
            }
            ']' => {
                bracket_depth = bracket_depth.saturating_sub(1);
                out.push(ch);
            }
            '{' => {
                out.push(ch);
                out.push('\n');
            }
            '}' => {
                out.push('\n');
                out.push(ch);
                out.push('\n');
            }
            ';' => {
                out.push(ch);
                out.push('\n');
            }
            ',' => {
                out.push(ch);
                if split_commas_in_parens
                    && paren_depth > 0
                    && angle_depth == 0
                    && bracket_depth == 0
                {
                    out.push('\n');
                }
            }
            _ => out.push(ch),
        }
    }

    out
}

pub(crate) fn normalize_java_text(text: &str) -> String {
    normalize_braced_text(text, true)
}

pub(crate) fn normalize_kotlin_text(text: &str) -> String {
    normalize_braced_text(text, true)
}

pub(crate) fn normalize_swift_text(text: &str) -> String {
    normalize_braced_text(text, false)
}
