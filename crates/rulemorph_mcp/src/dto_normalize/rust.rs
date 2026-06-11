pub(crate) fn normalize_rust_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut in_string: Option<char> = None;
    let mut escape = false;
    let mut angle_depth = 0usize;
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut last_newline = false;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            out.push(ch);
            last_newline = ch == '\n';
            if last_newline {
                in_line_comment = false;
            }
            continue;
        }

        if in_block_comment {
            out.push(ch);
            last_newline = ch == '\n';
            if ch == '*' && matches!(chars.peek(), Some('/')) {
                out.push('/');
                chars.next();
                in_block_comment = false;
                last_newline = false;
            }
            continue;
        }

        if let Some(quote) = in_string {
            out.push(ch);
            last_newline = ch == '\n';
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
                last_newline = false;
                continue;
            }
            if *next == '*' {
                out.push(ch);
                out.push(*next);
                chars.next();
                in_block_comment = true;
                last_newline = false;
                continue;
            }
        }

        if ch == '"' || ch == '\'' {
            out.push(ch);
            in_string = Some(ch);
            last_newline = false;
            continue;
        }

        match ch {
            '<' => {
                angle_depth += 1;
                out.push(ch);
                last_newline = false;
            }
            '>' => {
                angle_depth = angle_depth.saturating_sub(1);
                out.push(ch);
                last_newline = false;
            }
            '(' => {
                paren_depth += 1;
                out.push(ch);
                last_newline = false;
            }
            ')' => {
                paren_depth = paren_depth.saturating_sub(1);
                out.push(ch);
                last_newline = false;
            }
            '[' => {
                bracket_depth += 1;
                out.push(ch);
                last_newline = false;
            }
            ']' => {
                bracket_depth = bracket_depth.saturating_sub(1);
                out.push(ch);
                last_newline = false;
            }
            '{' => {
                out.push(ch);
                out.push('\n');
                last_newline = true;
            }
            '}' => {
                if !last_newline {
                    out.push('\n');
                }
                out.push(ch);
                out.push('\n');
                last_newline = true;
            }
            ',' | ';' => {
                out.push(ch);
                if angle_depth == 0 && paren_depth == 0 && bracket_depth == 0 {
                    out.push('\n');
                    last_newline = true;
                } else {
                    last_newline = false;
                }
            }
            _ => {
                out.push(ch);
                last_newline = false;
            }
        }
    }

    out
}
