#[test]
fn dto01_rust() {
    assert_golden_in_fixture(DtoLanguage::Rust, "dto01_basic", "expected_rust.rs");
}

#[test]
fn dto01_typescript() {
    assert_golden_in_fixture(
        DtoLanguage::TypeScript,
        "dto01_basic",
        "expected_typescript.ts",
    );
}

#[test]
fn dto01_python() {
    assert_golden_in_fixture(DtoLanguage::Python, "dto01_basic", "expected_python.py");
}

#[test]
fn dto01_go() {
    assert_golden_in_fixture(DtoLanguage::Go, "dto01_basic", "expected_go.go");
}

#[test]
fn dto01_java() {
    assert_golden_in_fixture(DtoLanguage::Java, "dto01_basic", "expected_java.java");
}

#[test]
fn dto01_kotlin() {
    assert_golden_in_fixture(DtoLanguage::Kotlin, "dto01_basic", "expected_kotlin.kt");
}

#[test]
fn dto01_swift() {
    assert_golden_in_fixture(DtoLanguage::Swift, "dto01_basic", "expected_swift.swift");
}
