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
        V2Ref::Input(path) => format!("@input.{}", path),
        V2Ref::Context(path) => format!("@context.{}", path),
        V2Ref::Out(path) => format!("@out.{}", path),
        V2Ref::Pipe(path) => {
            if path.is_empty() {
                "$".to_string()
            } else {
                format!("$.{}", path)
            }
        }
        V2Ref::Item(path) => format!("@item.{}", path),
        V2Ref::Acc(path) => format!("@acc.{}", path),
        V2Ref::Local(name) => format!("@{}", name),
    }
}
