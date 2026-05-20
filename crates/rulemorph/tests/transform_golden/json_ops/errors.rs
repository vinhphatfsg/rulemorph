#[test]
fn r02_json_ops_invalid_path_pick() {
    assert_transform_error_fixture("r02_json_ops_invalid_path_pick");
}

#[test]
fn r03_json_ops_non_object() {
    assert_transform_error_fixture("r03_json_ops_non_object");
}

#[test]
fn r04_json_ops_null_arg() {
    assert_transform_error_fixture("r04_json_ops_null_arg");
}

#[test]
fn r05_json_ops_unflatten_array_index() {
    assert_transform_error_fixture("r05_json_ops_unflatten_array_index");
}

#[test]
fn r06_json_ops_flatten_brackets() {
    assert_transform_error_fixture("r06_json_ops_flatten_brackets");
}

#[test]
fn r07_json_ops_flatten_empty_key() {
    assert_transform_error_fixture("r07_json_ops_flatten_empty_key");
}

#[test]
fn r08_json_ops_from_entries_single_pair() {
    assert_transform_error_fixture("r08_json_ops_from_entries_single_pair");
}
