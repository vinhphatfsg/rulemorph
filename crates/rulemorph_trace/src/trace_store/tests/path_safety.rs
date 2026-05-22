#[test]
fn fallback_trace_id_uses_hash_when_stem_missing() {
    let path = Path::new("/");
    let trace_id = fallback_trace_id_for_path(path);
    assert!(trace_id.starts_with("trace-"));
}

#[test]
fn sanitize_trace_id_rejects_dot_only() {
    assert!(sanitize_trace_id(".").is_empty());
    assert!(sanitize_trace_id("..").is_empty());
}

#[test]
fn hash_uses_traces_relative_path_when_possible() {
    let path_a = Path::new("/tmp/a/traces/2026/01/trace.json");
    let path_b = Path::new("/var/b/traces/2026/01/trace.json");
    assert_eq!(
        path_hash_for_trace_id(path_a),
        path_hash_for_trace_id(path_b)
    );
}

#[test]
fn resolve_chunk_path_rejects_parent_dirs() {
    let temp = tempdir().expect("tempdir");
    let base = temp.path().join("trace");
    std::fs::create_dir_all(&base).expect("create base dir");

    let err = resolve_chunk_path(&base, "../escape.json").expect_err("should reject");
    assert!(err.to_string().contains("relative"));
}

#[test]
fn resolve_chunk_path_rejects_absolute_paths() {
    let temp = tempdir().expect("tempdir");
    let base = temp.path().join("trace");
    std::fs::create_dir_all(&base).expect("create base dir");

    let err = resolve_chunk_path(&base, "/tmp/escape.json").expect_err("should reject");
    assert!(err.to_string().contains("relative"));
}

#[test]
fn resolve_chunk_path_accepts_relative_paths() {
    let temp = tempdir().expect("tempdir");
    let base = temp.path().join("trace");
    std::fs::create_dir_all(&base).expect("create base dir");
    let path = base.join("records-0001.ndjson");
    std::fs::write(&path, b"{}").expect("write chunk");

    let resolved = resolve_chunk_path(&base, "records-0001.ndjson").expect("resolve");
    let base = base.canonicalize().expect("canonicalize base");
    assert!(resolved.starts_with(&base));
}
