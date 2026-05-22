use crate::error::ErrorCode;

use super::super::ValidationCtx;

#[derive(Clone, Copy)]
pub(super) enum ArgCountRule {
    ExactlyOne,
    ExactlyTwo,
    ExactlyThree,
    OneOrTwo,
    TwoOrThree,
    ThreeOrFour,
    OneToThree,
    TwoToFour,
    AtLeastTwo,
    AtLeastThree,
}

pub(super) fn require(
    actual: usize,
    rule: ArgCountRule,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    if !rule.accepts(actual) {
        push_invalid_args(rule.message(), base_path, ctx);
    }
}

impl ArgCountRule {
    fn accepts(self, actual: usize) -> bool {
        match self {
            ArgCountRule::ExactlyOne => actual == 1,
            ArgCountRule::ExactlyTwo => actual == 2,
            ArgCountRule::ExactlyThree => actual == 3,
            ArgCountRule::OneOrTwo => (1..=2).contains(&actual),
            ArgCountRule::TwoOrThree => (2..=3).contains(&actual),
            ArgCountRule::ThreeOrFour => (3..=4).contains(&actual),
            ArgCountRule::OneToThree => (1..=3).contains(&actual),
            ArgCountRule::TwoToFour => (2..=4).contains(&actual),
            ArgCountRule::AtLeastTwo => actual >= 2,
            ArgCountRule::AtLeastThree => actual >= 3,
        }
    }

    fn message(self) -> &'static str {
        match self {
            ArgCountRule::ExactlyOne => "expr.args must contain exactly one item",
            ArgCountRule::ExactlyTwo => "expr.args must contain exactly two items",
            ArgCountRule::ExactlyThree => "expr.args must contain exactly three items",
            ArgCountRule::OneOrTwo => "expr.args must contain one or two items",
            ArgCountRule::TwoOrThree => "expr.args must contain two or three items",
            ArgCountRule::ThreeOrFour => "expr.args must contain three or four items",
            ArgCountRule::OneToThree => "expr.args must contain one to three items",
            ArgCountRule::TwoToFour => "expr.args must contain two to four items",
            ArgCountRule::AtLeastTwo => "expr.args must contain at least two items",
            ArgCountRule::AtLeastThree => "expr.args must contain at least three items",
        }
    }
}

fn push_invalid_args(message: &'static str, base_path: &str, ctx: &mut ValidationCtx<'_>) {
    ctx.push(
        ErrorCode::InvalidArgs,
        message,
        format!("{}.args", base_path),
    );
}
