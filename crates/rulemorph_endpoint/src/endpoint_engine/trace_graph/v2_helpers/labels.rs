use rulemorph::v2_model::{V2Ref, V2Start, V2Step};

pub(super) fn v2_start_label(start: &V2Start) -> String {
    match start {
        V2Start::Ref(reference) => v2_ref_label(reference),
        V2Start::PipeValue | V2Start::ImplicitPipeValue => "$".to_string(),
        V2Start::Literal(value) => value.to_string(),
        V2Start::V1Expr(_) => "v1_expr".to_string(),
    }
}

pub(super) fn v2_step_label(step: &V2Step) -> String {
    match step {
        V2Step::Op(op) => op.op.clone(),
        V2Step::CustomCall(call) => call.op.clone(),
        V2Step::Let(let_step) => format!(
            "let {}",
            let_step
                .bindings
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        V2Step::If(_) => "if".to_string(),
        V2Step::Map(_) => "map".to_string(),
        V2Step::Ref(reference) => v2_ref_label(reference),
    }
}

fn v2_ref_label(reference: &V2Ref) -> String {
    match reference {
        V2Ref::Input(path) => ref_path_label("@input", path),
        V2Ref::Context(path) => ref_path_label("@context", path),
        V2Ref::Out(path) => ref_path_label("@out", path),
        V2Ref::Pipe(path) => pipe_ref_label(path),
        V2Ref::Item(path) => ref_path_label("@item", path),
        V2Ref::Acc(path) => ref_path_label("@acc", path),
        V2Ref::Local(name) => format!("@{}", name),
    }
}

fn ref_path_label(prefix: &str, path: &str) -> String {
    if path.is_empty() {
        prefix.to_string()
    } else {
        format!("{}.{}", prefix, path)
    }
}

fn pipe_ref_label(path: &str) -> String {
    if path.is_empty() {
        "$".to_string()
    } else if path.starts_with('[') {
        format!("${}", path)
    } else {
        format!("$.{}", path)
    }
}

#[cfg(test)]
mod tests {
    use super::v2_ref_label;
    use rulemorph::v2_model::V2Ref;

    #[test]
    fn labels_empty_namespaced_paths_without_trailing_dot() {
        assert_eq!(v2_ref_label(&V2Ref::Input(String::new())), "@input");
        assert_eq!(v2_ref_label(&V2Ref::Context(String::new())), "@context");
        assert_eq!(v2_ref_label(&V2Ref::Out(String::new())), "@out");
        assert_eq!(v2_ref_label(&V2Ref::Item(String::new())), "@item");
        assert_eq!(v2_ref_label(&V2Ref::Acc(String::new())), "@acc");
    }

    #[test]
    fn labels_pipe_bracket_paths_without_dot() {
        assert_eq!(v2_ref_label(&V2Ref::Pipe(String::new())), "$");
        assert_eq!(v2_ref_label(&V2Ref::Pipe("name".to_string())), "$.name");
        assert_eq!(v2_ref_label(&V2Ref::Pipe("[0]".to_string())), "$[0]");
        assert_eq!(
            v2_ref_label(&V2Ref::Pipe("[\"a.b\"]".to_string())),
            "$[\"a.b\"]"
        );
    }
}
