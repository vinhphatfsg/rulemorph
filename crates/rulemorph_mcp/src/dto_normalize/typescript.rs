pub(crate) fn normalize_typescript_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    let mut in_line_comment = false;
    let mut in_block_comment = false;

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

        match ch {
            '{' => {
                out.push(ch);
                out.push('\n');
            }
            '}' => {
                out.push('\n');
                out.push(ch);
            }
            ';' => {
                out.push(ch);
                out.push('\n');
            }
            _ => out.push(ch),
        }
    }

    out
}
