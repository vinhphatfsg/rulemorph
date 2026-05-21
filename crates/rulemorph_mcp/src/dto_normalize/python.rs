pub(crate) fn normalize_python_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut in_string: Option<char> = None;
    let mut escape = false;

    for ch in text.chars() {
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

        if ch == '"' || ch == '\'' {
            in_string = Some(ch);
            out.push(ch);
            continue;
        }

        if ch == ';' {
            out.push(ch);
            out.push('\n');
            continue;
        }

        out.push(ch);
    }

    out
}
