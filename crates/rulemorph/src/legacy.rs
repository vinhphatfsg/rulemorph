use crate::error::{TransformErrorKind, TransformWarning};
use crate::model::RuleFile;

pub const LEGACY_V1_RULE_DEPRECATION_MESSAGE: &str =
    "version: 1 rule files are deprecated; migrate the rule file to version: 2";

pub fn is_legacy_v1_rule(rule: &RuleFile) -> bool {
    rule.version == 1
}

pub fn legacy_v1_rule_warning(rule: &RuleFile) -> Option<TransformWarning> {
    is_legacy_v1_rule(rule).then(|| {
        TransformWarning::new(
            TransformErrorKind::InvalidInput,
            LEGACY_V1_RULE_DEPRECATION_MESSAGE,
        )
        .with_path("version")
    })
}
