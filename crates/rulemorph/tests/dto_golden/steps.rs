#[test]
fn dto02_steps_rust() {
    assert_golden_in_fixture(DtoLanguage::Rust, "dto02_steps", "expected_rust.rs");
}
