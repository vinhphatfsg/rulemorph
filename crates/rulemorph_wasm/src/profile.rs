use rulemorph::{Expr, RuleFile, v2_model::*, v2_parser::*};

// A deliberately bounded HTTP profile. The local core keeps its full language.
pub fn check(rule: &RuleFile) -> Result<usize, String> {
    if !rule.defs.is_empty() || !rule.codecs.is_empty() || rule.finalize.is_some() {
        return Err("HTTP profile does not support defs, codecs or finalize".into());
    }
    if rule
        .steps
        .as_ref()
        .is_some_and(|steps| steps.iter().any(|s| s.branch.is_some()))
    {
        return Err("HTTP profile does not support file branches".into());
    }
    let mut cost = 0;
    let mut mappings = rule.mappings.iter().collect::<Vec<_>>();
    if let Some(steps) = &rule.steps {
        if steps.len() > 16 {
            return Err("step limit is 16".into());
        }
        for step in steps {
            mappings.extend(step.mappings.iter().flatten());
            if let Some(expr) = &step.record_when {
                check_when(expr, &mut cost)?;
            }
            for assertion in step.asserts.iter().flatten() {
                check_when(&assertion.when, &mut cost)?;
            }
        }
    }
    if mappings.len() > 64 {
        return Err("mapping limit is 64".into());
    }
    for mapping in mappings {
        let target = rulemorph::parse_path(&mapping.target).map_err(|e| e.message().to_string())?;
        if target.len() > 32
            || target.iter().any(|token| match token {
                rulemorph::PathToken::Index(index) => *index >= 256,
                rulemorph::PathToken::Key(key) => key.len() > 256,
            })
        {
            return Err(
                "target path exceeds HTTP limits (depth 32, index 255, key 256 bytes)".into(),
            );
        }
        cost += 1;
        if let Some(expr) = &mapping.expr {
            check_expr(expr, &mut cost)?;
        }
        if let Some(expr) = &mapping.when {
            check_when(expr, &mut cost)?;
        }
    }
    if let Some(expr) = &rule.record_when {
        check_when(expr, &mut cost)?;
    }
    if cost > 256 {
        return Err("expression limit is 256".into());
    }
    Ok(cost.max(1))
}

fn safe_op(op: &str) -> Result<(), String> {
    if matches!(
        op,
        "trim"
            | "lowercase"
            | "uppercase"
            | "coalesce"
            | "+"
            | "-"
            | "*"
            | "/"
            | "round"
            | "abs"
            | "floor"
            | "ceil"
            | "trunc"
            | "sqrt"
            | "sign"
            | "mod"
            | "pow"
            | "clamp"
            | "eq"
            | "ne"
            | "gt"
            | "gte"
            | "lt"
            | "lte"
            | "and"
            | "or"
            | "not"
    ) {
        Ok(())
    } else {
        Err(format!(
            "operator {op} is not supported by the bounded HTTP profile"
        ))
    }
}

fn check_expr(expr: &Expr, cost: &mut usize) -> Result<(), String> {
    *cost += 1;
    match expr {
        Expr::Ref(_) => Ok(()),
        Expr::Op(op) => {
            safe_op(&op.op)?;
            for arg in &op.args {
                check_expr(arg, cost)?;
            }
            Ok(())
        }
        Expr::Chain(chain) => {
            for expr in &chain.chain {
                check_expr(expr, cost)?;
            }
            Ok(())
        }
        Expr::Literal(value) if is_v2_expr(value) => {
            check_v2(&parse_v2_expr(value).map_err(|e| e.to_string())?, cost)
        }
        Expr::Literal(_) => Ok(()),
    }
}

fn check_v2(expr: &V2Expr, cost: &mut usize) -> Result<(), String> {
    *cost += 1;
    match expr {
        V2Expr::V1Fallback(expr) => check_expr(expr, cost),
        V2Expr::Pipe(pipe) => {
            if let V2Start::V1Expr(expr) = &pipe.start {
                check_expr(expr, cost)?;
            }
            for step in &pipe.steps {
                *cost += 1;
                match step {
                    V2Step::Ref(_) => {}
                    V2Step::Op(op) => {
                        safe_op(&op.op)?;
                        for arg in &op.args {
                            check_v2(arg, cost)?;
                        }
                    }
                    _ => {
                        return Err(
                            "HTTP profile supports only scalar pipe operators and references"
                                .into(),
                        );
                    }
                }
            }
            Ok(())
        }
    }
}

fn check_when(expr: &Expr, cost: &mut usize) -> Result<(), String> {
    match expr {
        Expr::Literal(value) if is_v2_expr(value) => {
            check_condition(&parse_v2_condition(value).map_err(|e| e.to_string())?, cost)
        }
        _ => check_expr(expr, cost),
    }
}

fn check_condition(condition: &V2Condition, cost: &mut usize) -> Result<(), String> {
    *cost += 1;
    match condition {
        V2Condition::All(items) | V2Condition::Any(items) => {
            for item in items {
                check_condition(item, cost)?;
            }
            Ok(())
        }
        V2Condition::Comparison(comparison) => {
            if comparison.op == V2ComparisonOp::Match {
                return Err("HTTP profile does not support regex conditions".into());
            }
            for arg in &comparison.args {
                check_v2(arg, cost)?;
            }
            Ok(())
        }
        V2Condition::Expr(expr) => check_v2(expr, cost),
    }
}
