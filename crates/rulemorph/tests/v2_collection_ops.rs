use rulemorph::{TransformErrorKind, parse_rule_file, transform, transform_with_warnings};

mod v2_collection_ops {
    pub(super) mod errors;
    pub(super) mod missing;
    pub(super) mod reduce_fold;
    pub(super) mod sort;
    pub(super) mod zip;
}

fn run_ok(yaml: &str, input: &str) -> serde_json::Value {
    let rule = parse_rule_file(yaml).expect("parse rule");
    let (output, warnings) =
        transform_with_warnings(&rule, input, None).expect("transform should succeed");
    assert!(warnings.is_empty(), "unexpected warnings: {warnings:?}");
    output
}

fn run_err(yaml: &str, input: &str) -> rulemorph::TransformError {
    let rule = parse_rule_file(yaml).expect("parse rule");
    transform(&rule, input, None).expect_err("transform should fail")
}
