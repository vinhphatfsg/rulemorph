use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use rulemorph::{
    InputData, TraceAttributeValue, TraceEvent, TraceEventKind, TransformTrace,
    TransformTraceOptions, parse_rule_file, transform, transform_input_with_trace,
    transform_input_with_trace_with_base_dir_and_options, transform_record,
    transform_record_with_trace, transform_with_base_dir,
};
use serde_json::{Value as JsonValue, json};

fn iter_trace_events(trace: &TransformTrace) -> Vec<&TraceEvent> {
    trace
        .records
        .iter()
        .flat_map(|record| record.events.iter())
        .chain(trace.finalize.iter().flatten())
        .collect()
}

fn assert_parent_ids_point_to_emitted_events(trace: &TransformTrace) {
    let ids = iter_trace_events(trace)
        .into_iter()
        .map(|event| event.id)
        .collect::<std::collections::BTreeSet<_>>();
    for event in iter_trace_events(trace) {
        if let Some(parent_id) = event.parent_id {
            assert!(
                ids.contains(&parent_id),
                "dangling parent_id {parent_id} on {:?}",
                event.kind
            );
        }
    }
}

fn assert_trace_paths_are_canonical(trace: &TransformTrace) {
    for event in iter_trace_events(trace) {
        if let Some(path) = event.input_path.as_deref() {
            assert!(
                path == "@input"
                    || path.starts_with("@input.")
                    || path.starts_with("@input[")
                    || path == "@item"
                    || path.starts_with("@item.")
                    || path.starts_with("@item[")
                    || path == "@acc"
                    || path.starts_with("@acc.")
                    || path.starts_with("@acc[")
                    || path == "@context"
                    || path.starts_with("@context.")
                    || path.starts_with("@context[")
                    || path == "@out"
                    || path.starts_with("@out.")
                    || path.starts_with("@out["),
                "non-canonical input_path: {path}"
            );
        }
        if let Some(path) = event.output_path.as_deref() {
            assert!(
                path == "$" || path.starts_with("$.") || path.starts_with("$["),
                "non-canonical output_path: {path}"
            );
        }
    }
}

fn assert_trace_shape(trace: &TransformTrace) {
    assert_parent_ids_point_to_emitted_events(trace);
    assert_trace_paths_are_canonical(trace);
}

fn assert_traced_output_matches_normal(yaml: &str, input: &str, expected: JsonValue) {
    let rule = parse_rule_file(yaml).expect("parse rule");
    let normal = transform(&rule, input, None).expect("normal transform");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(input),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(normal, expected);
    assert_eq!(traced.output, normal);
    assert_trace_shape(&traced.trace);
}

fn attr_number(event: &TraceEvent, key: &str) -> Option<u64> {
    match event.attributes.get(key) {
        Some(TraceAttributeValue::Number(number)) => number.as_u64(),
        _ => None,
    }
}

fn attr_bool(event: &TraceEvent, key: &str) -> Option<bool> {
    match event.attributes.get(key) {
        Some(TraceAttributeValue::Bool(flag)) => Some(*flag),
        _ => None,
    }
}

fn unique_temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("rulemorph-trace-{name}-{nanos}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

#[test]
fn trace_v2_eager_operator_emits_arg_eval_for_actual_args() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "label"
    expr:
      - "@input.first"
      - concat: ["@input.second"]
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"first":"A","second":"B"}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(traced.output, json!([{ "label": "AB" }]));
    assert!(
        iter_trace_events(&traced.trace).into_iter().any(|event| {
            event.kind == TraceEventKind::ArgEval
                && event.operator.as_deref() == Some("concat")
                && attr_number(event, "arg_index") == Some(0)
                && event
                    .output
                    .as_ref()
                    .and_then(|snapshot| snapshot.value.as_ref())
                    == Some(&json!("B"))
        }),
        "v2 eager operator args should be traced when actually evaluated"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_output_write_uses_output_snapshot_not_input_slot() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    source: "name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"name":"alice"}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    let output_write = iter_trace_events(&traced.trace)
        .into_iter()
        .find(|event| event.kind == TraceEventKind::OutputWrite)
        .expect("output_write event");
    assert!(
        output_write.inputs.is_empty(),
        "output_write must not put the written value in inputs"
    );
    assert_eq!(
        output_write
            .output
            .as_ref()
            .and_then(|snapshot| snapshot.value.as_ref()),
        Some(&json!("alice"))
    );
}

#[test]
fn trace_v2_collection_operators_emit_item_level_events() {
    let cases = [
        (
            "filter",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - filter: ["@item.active"]
"#,
            json!([{ "out": [
                {"id":"u2","role":"member","active":true,"tags":["c"],"score":1},
                {"id":"u3","role":"admin","active":true,"tags":[],"score":3}
            ] }]),
        ),
        (
            "flat_map",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - flat_map: ["@item.tags"]
"#,
            json!([{ "out": ["a", "b", "c"] }]),
        ),
        (
            "reduce",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.scores"
      - reduce: [["@acc", { "+": "@item" }]]
"#,
            json!([{ "out": 6.0 }]),
        ),
        (
            "fold",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.scores"
      - fold:
          - 0
          - ["@acc", { "+": "@item" }]
"#,
            json!([{ "out": 6.0 }]),
        ),
        (
            "group_by",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - group_by: ["@item.role"]
"#,
            json!([{ "out": {
                "admin": [
                    {"id":"u1","role":"admin","active":false,"tags":["a","b"],"score":2},
                    {"id":"u3","role":"admin","active":true,"tags":[],"score":3}
                ],
                "member": [
                    {"id":"u2","role":"member","active":true,"tags":["c"],"score":1}
                ]
            } }]),
        ),
        (
            "key_by",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - key_by: ["@item.id"]
"#,
            json!([{ "out": {
                "u1": {"id":"u1","role":"admin","active":false,"tags":["a","b"],"score":2},
                "u2": {"id":"u2","role":"member","active":true,"tags":["c"],"score":1},
                "u3": {"id":"u3","role":"admin","active":true,"tags":[],"score":3}
            } }]),
        ),
        (
            "partition",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - partition: ["@item.active"]
"#,
            json!([{ "out": [
                [
                    {"id":"u2","role":"member","active":true,"tags":["c"],"score":1},
                    {"id":"u3","role":"admin","active":true,"tags":[],"score":3}
                ],
                [
                    {"id":"u1","role":"admin","active":false,"tags":["a","b"],"score":2}
                ]
            ] }]),
        ),
        (
            "distinct_by",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - distinct_by: ["@item.role"]
"#,
            json!([{ "out": [
                {"id":"u1","role":"admin","active":false,"tags":["a","b"],"score":2},
                {"id":"u2","role":"member","active":true,"tags":["c"],"score":1}
            ] }]),
        ),
        (
            "sort_by",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - sort_by: ["@item.score"]
"#,
            json!([{ "out": [
                {"id":"u2","role":"member","active":true,"tags":["c"],"score":1},
                {"id":"u1","role":"admin","active":false,"tags":["a","b"],"score":2},
                {"id":"u3","role":"admin","active":true,"tags":[],"score":3}
            ] }]),
        ),
        (
            "find",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - find: ["@item.active"]
"#,
            json!([{ "out": {"id":"u2","role":"member","active":true,"tags":["c"],"score":1} }]),
        ),
        (
            "find_index",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - find_index: ["@item.active"]
"#,
            json!([{ "out": 1 }]),
        ),
    ];
    let input = r#"[{
        "users":[
            {"id":"u1","role":"admin","active":false,"tags":["a","b"],"score":2},
            {"id":"u2","role":"member","active":true,"tags":["c"],"score":1},
            {"id":"u3","role":"admin","active":true,"tags":[],"score":3}
        ],
        "scores":[1,2,3]
    }]"#;

    for (operator, yaml, expected) in cases {
        let rule = parse_rule_file(yaml).unwrap_or_else(|err| panic!("{operator} parse: {err:?}"));
        let normal = transform(&rule, input, None)
            .unwrap_or_else(|err| panic!("{operator} normal transform: {err:?}"));
        let traced = transform_input_with_trace(
            &rule,
            InputData::Text(input),
            None,
            &TransformTraceOptions::raw(),
        )
        .unwrap_or_else(|err| panic!("{operator} traced transform: {err:?}"));

        assert_eq!(normal, expected, "{operator} normal output");
        assert_eq!(traced.output, normal, "{operator} traced output");
        let events = iter_trace_events(&traced.trace);
        assert!(
            events.iter().any(|event| {
                event.kind == TraceEventKind::CollectionItemStart
                    && event.operator.as_deref() == Some(operator)
            }),
            "{operator} should emit per-item start events"
        );
        assert!(
            events.iter().any(|event| {
                event.kind == TraceEventKind::CollectionItemEnd
                    && event.operator.as_deref() == Some(operator)
                    && event.attributes.contains_key("item_index")
            }),
            "{operator} should emit per-item end/decision events"
        );
        assert_trace_shape(&traced.trace);
    }
}

#[test]
fn trace_record_when_and_mapping_when_include_internal_condition_trace() {
    let yaml = r#"
version: 2
input:
  format: json
record_when:
  eq: ["@input.kind", "keep"]
mappings:
  - target: "name"
    when:
      eq: ["@input.enabled", true]
    source: "name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"kind":"keep","enabled":true,"name":"alice"}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(traced.output, json!([{ "name": "alice" }]));
    let events = iter_trace_events(&traced.trace);
    assert!(
        events.iter().any(|event| {
            event.kind == TraceEventKind::RefRead
                && event.input_path.as_deref() == Some("@input.kind")
        }),
        "record_when should expose internal ref evaluation"
    );
    assert!(
        events.iter().any(|event| {
            event.kind == TraceEventKind::RefRead
                && event.input_path.as_deref() == Some("@input.enabled")
        }),
        "mapping.when should expose internal ref evaluation"
    );
    assert!(
        events.iter().any(|event| {
            event.kind == TraceEventKind::ArgEval
                && event.operator.as_deref() == Some("eq")
                && attr_number(event, "arg_index") == Some(1)
        }),
        "condition comparison should expose actual argument evaluation"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_finalize_filter_and_sort_emit_item_level_decisions() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "id"
    source: "id"
  - target: "active"
    source: "active"
  - target: "score"
    source: "score"
finalize:
  filter:
    eq: ["@item.active", true]
  sort:
    by: "score"
    order: "desc"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(
            r#"[
                {"id":"a","active":true,"score":2},
                {"id":"b","active":false,"score":5},
                {"id":"c","active":true,"score":3}
            ]"#,
        ),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(
        traced.output,
        json!([
            {"id":"c","active":true,"score":3},
            {"id":"a","active":true,"score":2}
        ])
    );
    let finalize_events = traced.trace.finalize.as_ref().expect("finalize trace");
    assert!(
        finalize_events.iter().any(|event| {
            event.kind == TraceEventKind::FinalizeFilter
                && attr_number(event, "item_index") == Some(1)
                && attr_bool(event, "kept") == Some(false)
        }),
        "finalize.filter should emit per-item kept decisions"
    );
    assert!(
        finalize_events.iter().any(|event| {
            event.kind == TraceEventKind::FinalizeSort
                && attr_number(event, "from_index") == Some(1)
                && attr_number(event, "to_index") == Some(0)
        }),
        "finalize.sort should emit item movement decisions"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_v2_operator_fixture_table_covers_valid_inventory_representatives() {
    let cases = [
        ("concat", r#"["@input.s", {"concat": "!"}]"#),
        ("to_string", r#"["@input.n", "to_string"]"#),
        ("trim", r#"["@input.spaced", "trim"]"#),
        ("lowercase", r#"["@input.upper", "lowercase"]"#),
        ("uppercase", r#"["@input.s", "uppercase"]"#),
        (
            "replace",
            r#"["@input.text", {"replace": ["world", "there"]}]"#,
        ),
        ("split", r#"["@input.csv", {"split": ","}]"#),
        ("pad_start", r#"["@input.pad", {"pad_start": [3, "0"]}]"#),
        ("pad_end", r#"["@input.pad", {"pad_end": [3, "0"]}]"#),
        (
            "coalesce",
            r#"["@input.missing", {"coalesce": "@input.s"}]"#,
        ),
        (
            "lookup",
            r#"["@input.lookup_rows", {"lookup": ["id", "b"]}]"#,
        ),
        (
            "lookup_first",
            r#"["@input.lookup_rows", {"lookup_first": ["id", "b"]}]"#,
        ),
        ("+", r#"["@input.n", {"+": 2}]"#),
        ("-", r#"["@input.n", {"-": 1}]"#),
        ("*", r#"["@input.n", {"*": 2}]"#),
        ("/", r#"["@input.n", {"/": 2}]"#),
        ("multiply", r#"["@input.n", {"multiply": 2}]"#),
        ("add", r#"["@input.n", {"add": 2}]"#),
        ("subtract", r#"["@input.n", {"subtract": 1}]"#),
        ("divide", r#"["@input.n", {"divide": 2}]"#),
        ("round", r#"["@input.float", {"round": 2}]"#),
        ("to_base", r#"["@input.base", {"to_base": 16}]"#),
        (
            "date_format",
            r#"["@input.date", {"date_format": "%Y/%m/%d"}]"#,
        ),
        ("to_unixtime", r#"["@input.unix", "to_unixtime"]"#),
        ("and", r#"["@input.truth", {"and": true}]"#),
        ("or", r#"["@input.falsehood", {"or": true}]"#),
        ("not", r#"["@input.falsehood", "not"]"#),
        ("==", r#"["@input.n", {"==": 3}]"#),
        ("!=", r#"["@input.n", {"!=": 4}]"#),
        ("<", r#"["@input.n", {"<": 4}]"#),
        ("<=", r#"["@input.n", {"<=": 3}]"#),
        (">", r#"["@input.n", {">": 2}]"#),
        (">=", r#"["@input.n", {">=": 3}]"#),
        ("~=", r#"["@input.s", {"~=": "^h"}]"#),
        ("eq", r#"["@input.n", {"eq": 3}]"#),
        ("ne", r#"["@input.n", {"ne": 4}]"#),
        ("lt", r#"["@input.n", {"lt": 4}]"#),
        ("lte", r#"["@input.n", {"lte": 3}]"#),
        ("gt", r#"["@input.n", {"gt": 2}]"#),
        ("gte", r#"["@input.n", {"gte": 3}]"#),
        ("match", r#"["@input.s", {"match": "^h"}]"#),
        ("merge", r#"["@input.obj", {"merge": {"b":2}}]"#),
        (
            "deep_merge",
            r#"["@input.deep_left", {"deep_merge": "@input.deep_right"}]"#,
        ),
        ("get", r#"["@input.obj", {"get": "a"}]"#),
        ("pick", r#"["@input.obj", {"pick": "a"}]"#),
        ("omit", r#"["@input.obj", {"omit": "b"}]"#),
        ("keys", r#"["@input.obj", "keys"]"#),
        ("values", r#"["@input.obj", "values"]"#),
        ("entries", r#"["@input.obj", "entries"]"#),
        ("len", r#"["@input.arr", "len"]"#),
        ("from_entries", r#"["@input.entries", "from_entries"]"#),
        (
            "object_flatten",
            r#"["@input.deep_left", "object_flatten"]"#,
        ),
        (
            "object_unflatten",
            r#"["@input.flat_obj", "object_unflatten"]"#,
        ),
        ("map", r#"["@input.arr", {"map": ["@item", {"add": 1}]}]"#),
        ("filter", r#"["@input.items", {"filter": "@item.keep"}]"#),
        (
            "flat_map",
            r#"["@input.items", {"flat_map": "@item.tags"}]"#,
        ),
        ("flatten", r#"["@input.nested", {"flatten": 2}]"#),
        ("take", r#"["@input.arr", {"take": 2}]"#),
        ("drop", r#"["@input.arr", {"drop": 1}]"#),
        ("slice", r#"["@input.arr", {"slice": [1, 3]}]"#),
        ("chunk", r#"["@input.arr", {"chunk": 2}]"#),
        ("zip", r#"["@input.arr", {"zip": "@input.arr2"}]"#),
        (
            "zip_with",
            r#"["@input.arr", {"zip_with": ["@input.arr2", "@item"]}]"#,
        ),
        ("unzip", r#"["@input.pairs", "unzip"]"#),
        (
            "group_by",
            r#"["@input.items", {"group_by": "@item.role"}]"#,
        ),
        ("key_by", r#"["@input.items", {"key_by": "@item.id"}]"#),
        (
            "partition",
            r#"["@input.items", {"partition": "@item.keep"}]"#,
        ),
        ("unique", r#"["@input.dups", "unique"]"#),
        (
            "distinct_by",
            r#"["@input.items", {"distinct_by": "@item.role"}]"#,
        ),
        ("sort_by", r#"["@input.items", {"sort_by": "@item.v"}]"#),
        ("find", r#"["@input.items", {"find": "@item.keep"}]"#),
        (
            "find_index",
            r#"["@input.items", {"find_index": "@item.keep"}]"#,
        ),
        ("index_of", r#"["@input.arr", {"index_of": 2}]"#),
        ("contains", r#"["@input.arr", {"contains": 3}]"#),
        ("sum", r#"["@input.arr", "sum"]"#),
        ("avg", r#"["@input.arr", "avg"]"#),
        ("min", r#"["@input.arr", "min"]"#),
        ("max", r#"["@input.arr", "max"]"#),
        (
            "reduce",
            r#"["@input.arr", {"reduce": [["@acc", {"+": "@item"}]]}]"#,
        ),
        (
            "fold",
            r#"["@input.arr", {"fold": [0, ["@acc", {"+": "@item"}]]}]"#,
        ),
        ("first", r#"["@input.arr", "first"]"#),
        ("last", r#"["@input.arr", "last"]"#),
        ("string", r#"["@input.n", "string"]"#),
        ("int", r#"["@input.int_text", "int"]"#),
        ("float", r#"["@input.float_text", "float"]"#),
        ("bool", r#"["@input.bool_text", "bool"]"#),
    ];
    let expected_inventory = [
        "concat",
        "to_string",
        "trim",
        "lowercase",
        "uppercase",
        "replace",
        "split",
        "pad_start",
        "pad_end",
        "coalesce",
        "lookup",
        "lookup_first",
        "+",
        "-",
        "*",
        "/",
        "multiply",
        "add",
        "subtract",
        "divide",
        "round",
        "to_base",
        "date_format",
        "to_unixtime",
        "and",
        "or",
        "not",
        "==",
        "!=",
        "<",
        "<=",
        ">",
        ">=",
        "~=",
        "eq",
        "ne",
        "lt",
        "lte",
        "gt",
        "gte",
        "match",
        "merge",
        "deep_merge",
        "get",
        "pick",
        "omit",
        "keys",
        "values",
        "entries",
        "len",
        "from_entries",
        "object_flatten",
        "object_unflatten",
        "map",
        "filter",
        "flat_map",
        "flatten",
        "take",
        "drop",
        "slice",
        "chunk",
        "zip",
        "zip_with",
        "unzip",
        "group_by",
        "key_by",
        "partition",
        "unique",
        "distinct_by",
        "sort_by",
        "find",
        "find_index",
        "index_of",
        "contains",
        "sum",
        "avg",
        "min",
        "max",
        "reduce",
        "fold",
        "first",
        "last",
        "string",
        "int",
        "float",
        "bool",
    ];
    assert_eq!(
        cases
            .iter()
            .map(|(op, _)| *op)
            .collect::<std::collections::BTreeSet<_>>(),
        expected_inventory
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        "trace fixture table must drift with valid v2 operator inventory"
    );
    let input = r#"[{
        "s":"hi",
        "spaced":" hi ",
        "upper":"HI",
        "text":"hello world",
        "csv":"a,b",
        "pad":"7",
        "n":3,
        "float":1.2345,
        "base":255,
        "date":"2024-01-02 03:04:05",
        "unix":"1970-01-01T00:00:01Z",
        "truth":true,
        "falsehood":false,
        "obj":{"a":1,"b":2},
        "deep_left":{"a":1,"nested":{"x":1}},
        "deep_right":{"nested":{"y":2},"c":3},
        "flat_obj":{"a.b":1,"c":2},
        "entries":[["a",1],["b",2]],
        "arr":[1,2,3],
        "arr2":[4,5,6],
        "nested":[1,[2,[3]],4],
        "pairs":[[1,"a"],[2,"b"]],
        "dups":["a","b","a"],
        "items":[
            {"id":"a","role":"admin","keep":true,"v":2,"tags":["x"]},
            {"id":"b","role":"member","keep":false,"v":1,"tags":["y","z"]},
            {"id":"c","role":"admin","keep":true,"v":3,"tags":[]}
        ],
        "lookup_rows":[{"id":"a","value":1},{"id":"b","value":2}],
        "int_text":"42",
        "float_text":"1.5",
        "bool_text":"true"
    }]"#;

    for (operator, expr) in cases {
        let yaml = format!(
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr: {expr}
"#
        );
        let rule = parse_rule_file(&yaml).unwrap_or_else(|err| panic!("{operator} parse: {err:?}"));
        let normal = transform(&rule, input, None)
            .unwrap_or_else(|err| panic!("{operator} normal transform: {err:?}"));
        let traced = transform_input_with_trace(
            &rule,
            InputData::Text(input),
            None,
            &TransformTraceOptions::raw(),
        )
        .unwrap_or_else(|err| panic!("{operator} traced transform: {err:?}"));

        assert_eq!(traced.output, normal, "{operator}");
        let events = iter_trace_events(&traced.trace);
        assert!(
            events.iter().any(|event| {
                event.kind == TraceEventKind::OpStart && event.operator.as_deref() == Some(operator)
            }),
            "{operator} missing op_start"
        );
        assert!(
            events.iter().any(|event| {
                event.kind == TraceEventKind::OpEnd && event.operator.as_deref() == Some(operator)
            }),
            "{operator} missing op_end"
        );
        assert_trace_shape(&traced.trace);
    }
}

#[test]
fn trace_v1_coalesce_does_not_evaluate_short_circuited_arg() {
    let yaml = r#"
version: 1
input:
  format: json
mappings:
  - target: "name"
    expr:
      op: coalesce
      args:
        - { ref: "input.name" }
        - { ref: "item.value" }
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"name":"alice"}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(traced.output, json!([{ "name": "alice" }]));
    let arg_eval_indexes = iter_trace_events(&traced.trace)
        .into_iter()
        .filter(|event| event.kind == TraceEventKind::ArgEval)
        .filter_map(|event| event.attributes.get("arg_index"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        arg_eval_indexes,
        vec![TraceAttributeValue::Number(0.into())]
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_v1_and_does_not_evaluate_short_circuited_arg() {
    let yaml = r#"
version: 1
input:
  format: json
mappings:
  - target: "flag"
    expr:
      op: and
      args:
        - false
        - { ref: "item.value" }
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(traced.output, json!([{ "flag": false }]));
    let arg_eval_indexes = iter_trace_events(&traced.trace)
        .into_iter()
        .filter(|event| event.kind == TraceEventKind::ArgEval)
        .filter_map(|event| event.attributes.get("arg_index"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        arg_eval_indexes,
        vec![TraceAttributeValue::Number(0.into())]
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_v1_map_ref_read_preserves_item_namespace() {
    let yaml = r#"
version: 1
input:
  format: json
mappings:
  - target: "names"
    expr:
      op: map
      args:
        - { ref: "input.users" }
        - { ref: "item.value.name" }
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"users":[{"name":"alice"},{"name":"bob"}]}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(traced.output, json!([{ "names": ["alice", "bob"] }]));
    assert!(
        iter_trace_events(&traced.trace).into_iter().any(|event| {
            event.kind == TraceEventKind::RefRead
                && event.input_path.as_deref() == Some("@item.value.name")
        }),
        "item-scoped ref reads must not be reported as @input paths"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_v1_reduce_ref_read_preserves_acc_namespace() {
    let yaml = r#"
version: 1
input:
  format: json
mappings:
  - target: "total"
    expr:
      op: reduce
      args:
        - { ref: "input.values" }
        - { op: "+", args: [ { ref: "acc.value" }, { ref: "item.value" } ] }
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"values":[1,2,3]}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(traced.output, json!([{ "total": 6 }]));
    assert!(
        iter_trace_events(&traced.trace).into_iter().any(|event| {
            event.kind == TraceEventKind::RefRead
                && event.input_path.as_deref() == Some("@acc.value")
        }),
        "acc-scoped ref reads must not be reported as @input paths"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_v2_and_short_circuits_false_pipe_without_evaluating_invalid_arg() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "flag"
    expr:
      - "@input.enabled"
      - and: ["@item.enabled"]
"#;

    assert_traced_output_matches_normal(yaml, r#"[{"enabled":false}]"#, json!([{ "flag": false }]));
}

#[test]
fn trace_v2_or_short_circuits_true_pipe_without_evaluating_invalid_arg() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "flag"
    expr:
      - "@input.enabled"
      - or: ["@item.enabled"]
"#;

    assert_traced_output_matches_normal(yaml, r#"[{"enabled":true}]"#, json!([{ "flag": true }]));
}

#[test]
fn trace_v2_coalesce_short_circuits_after_first_present_arg() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    expr:
      - "@input.primary"
      - coalesce: ["@input.secondary", "@item.name"]
"#;

    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"secondary":"fallback"}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(traced.output, json!([{ "name": "fallback" }]));
    let arg_eval_indexes = iter_trace_events(&traced.trace)
        .into_iter()
        .filter(|event| {
            event.kind == TraceEventKind::ArgEval && event.operator.as_deref() == Some("coalesce")
        })
        .filter_map(|event| event.attributes.get("arg_index"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        arg_eval_indexes,
        vec![TraceAttributeValue::Number(0.into())],
        "coalesce should trace evaluated fallback args but not short-circuited args"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_v2_short_circuited_args_are_not_reported_as_arg_eval() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "flag"
    expr:
      - "@input.enabled"
      - and: ["@item.enabled"]
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"enabled":false}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert!(
        iter_trace_events(&traced.trace)
            .into_iter()
            .all(|event| event.kind != TraceEventKind::ArgEval),
        "short-circuited v2 args must not be reported as evaluated"
    );
}

#[test]
fn trace_v2_non_short_circuited_invalid_arg_errors_like_normal() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "flag"
    expr:
      - "@input.enabled"
      - and: ["@item.enabled"]
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"[{"enabled":true}]"#;

    let normal = transform(&rule, input, None).expect_err("normal error");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(input),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect_err("traced error");

    assert_eq!(traced.error, normal);
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_v2_map_nested_error_closes_collection_and_map_spans() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "items"
    expr:
      - "@input.items"
      - map:
          - divide: [0]
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        transform_input_with_trace(
            &rule,
            InputData::Text(r#"[{"items":[1]}]"#),
            None,
            &TransformTraceOptions::raw(),
        )
    }));

    assert!(result.is_ok(), "map nested error must not leave open spans");
    let err = result.expect("no panic").expect_err("traced error");
    assert_eq!(err.error.kind, rulemorph::TransformErrorKind::ExprError);
    assert_trace_shape(&err.trace);
}

#[test]
fn trace_record_api_matches_normal_finalize_behavior() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    source: "name"
finalize:
  wrap:
    data: "@out"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({"name":"alice"});

    let normal = transform_record(&rule, &record, None).expect("normal record transform");
    let traced = transform_record_with_trace(&rule, &record, None, &TransformTraceOptions::raw())
        .expect("traced record transform");

    assert_eq!(normal, Some(json!({"data":[{"name":"alice"}]})));
    assert_eq!(traced.output, normal);
    assert!(
        iter_trace_events(&traced.trace)
            .into_iter()
            .any(|event| event.kind == TraceEventKind::FinalizeStart),
        "single-record trace API must mirror normal finalize behavior"
    );
}

#[test]
fn trace_record_api_skips_finalize_when_record_is_dropped() {
    let yaml = r#"
version: 2
input:
  format: json
record_when: false
mappings:
  - target: "name"
    source: "name"
finalize:
  wrap:
    data: "@out"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({"name":"alice"});

    let normal = transform_record(&rule, &record, None).expect("normal record transform");
    let traced = transform_record_with_trace(&rule, &record, None, &TransformTraceOptions::raw())
        .expect("traced record transform");

    assert_eq!(normal, None);
    assert_eq!(traced.output, normal);
    assert!(
        iter_trace_events(&traced.trace)
            .into_iter()
            .all(|event| event.kind != TraceEventKind::FinalizeStart),
        "dropped record must not run finalize in trace mode"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_branch_return_preserves_child_finalize_null_output() {
    let dir = unique_temp_dir("branch-null-finalize");
    fs::write(
        dir.join("child.yaml"),
        r#"
version: 2
input:
  format: json
mappings:
  - target: "ignored"
    value: true
finalize:
  wrap: "@context.missing"
"#,
    )
    .expect("write child rule");
    let yaml = r#"
version: 2
input:
  format: json
steps:
  - branch:
      when: true
      then: child.yaml
      return: true
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"[{"name":"alice"}]"#;

    let normal = transform_with_base_dir(&rule, input, None, &dir).expect("normal transform");
    let traced = transform_input_with_trace_with_base_dir_and_options(
        &rule,
        InputData::Text(input),
        None,
        Some(&dir),
        &Default::default(),
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(normal, json!([null]));
    assert_eq!(traced.output, normal);
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_branch_child_rule_applies_finalize_like_normal_execution() {
    let dir = unique_temp_dir("branch-child-finalize");
    fs::write(
        dir.join("child.yaml"),
        r#"
version: 2
input:
  format: json
mappings:
  - target: "final_name"
    source: "name"
finalize:
  wrap:
    data: "@out"
"#,
    )
    .expect("write child rule");
    let yaml = r#"
version: 2
input:
  format: json
steps:
  - mappings:
      - target: "name"
        source: "name"
  - branch:
      when: true
      then: child.yaml
      return: true
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"[{"name":"alice"}]"#;

    let normal = transform_with_base_dir(&rule, input, None, &dir).expect("normal transform");
    let traced = transform_input_with_trace_with_base_dir_and_options(
        &rule,
        InputData::Text(input),
        None,
        Some(&dir),
        &Default::default(),
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(normal, json!([{ "data": [{ "final_name": "alice" }] }]));
    assert_eq!(traced.output, normal);
    assert!(
        iter_trace_events(&traced.trace)
            .into_iter()
            .any(|event| event.kind == TraceEventKind::FinalizeStart),
        "branch child finalize should be visible in the record trace"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_branch_merge_error_closes_branch_and_step_spans() {
    let dir = unique_temp_dir("branch-merge-error");
    fs::write(
        dir.join("child.yaml"),
        r#"
version: 2
input:
  format: json
mappings:
  - target: "ignored"
    value: true
finalize:
  wrap: "@context.missing"
"#,
    )
    .expect("write child rule");
    let yaml = r#"
version: 2
input:
  format: json
steps:
  - branch:
      when: true
      then: child.yaml
      return: false
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        transform_input_with_trace_with_base_dir_and_options(
            &rule,
            InputData::Text(r#"[{}]"#),
            None,
            Some(&dir),
            &Default::default(),
            &TransformTraceOptions::raw(),
        )
    }));

    assert!(
        result.is_ok(),
        "branch merge error must not leave open spans"
    );
    let err = result.expect("no panic").expect_err("traced error");
    assert_eq!(err.error.kind, rulemorph::TransformErrorKind::InvalidTarget);
    assert_trace_shape(&err.trace);
}

#[test]
fn trace_v2_item_scoped_collection_ops_match_normal() {
    let cases = [
        (
            "flat_map",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "tags"
    expr:
      - "@input.users"
      - flat_map: ["@item.tags"]
"#,
            json!([{ "tags": ["a", "b", "c"] }]),
        ),
        (
            "group_by",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "by_role"
    expr:
      - "@input.users"
      - group_by: ["@item.role"]
"#,
            json!([{ "by_role": {
                "admin": [
                    {"id":"u1","name":"Alice","role":"admin","active":false,"tags":["a","b"]},
                    {"id":"u3","name":"Carol","role":"admin","active":true,"tags":[]}
                ],
                "member": [
                    {"id":"u2","name":"Bob","role":"member","active":true,"tags":["c"]}
                ]
            } }]),
        ),
        (
            "key_by",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "by_id"
    expr:
      - "@input.users"
      - key_by: ["@item.id"]
"#,
            json!([{ "by_id": {
                "u1": {"id":"u1","name":"Alice","role":"admin","active":false,"tags":["a","b"]},
                "u2": {"id":"u2","name":"Bob","role":"member","active":true,"tags":["c"]},
                "u3": {"id":"u3","name":"Carol","role":"admin","active":true,"tags":[]}
            } }]),
        ),
        (
            "partition",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "active_groups"
    expr:
      - "@input.users"
      - partition: ["@item.active"]
"#,
            json!([{ "active_groups": [
                [
                    {"id":"u2","name":"Bob","role":"member","active":true,"tags":["c"]},
                    {"id":"u3","name":"Carol","role":"admin","active":true,"tags":[]}
                ],
                [
                    {"id":"u1","name":"Alice","role":"admin","active":false,"tags":["a","b"]}
                ]
            ] }]),
        ),
        (
            "distinct_by",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "first_by_role"
    expr:
      - "@input.users"
      - distinct_by: ["@item.role"]
"#,
            json!([{ "first_by_role": [
                {"id":"u1","name":"Alice","role":"admin","active":false,"tags":["a","b"]},
                {"id":"u2","name":"Bob","role":"member","active":true,"tags":["c"]}
            ] }]),
        ),
        (
            "find",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "first_active"
    expr:
      - "@input.users"
      - find: ["@item.active"]
"#,
            json!([{ "first_active": {"id":"u2","name":"Bob","role":"member","active":true,"tags":["c"]} }]),
        ),
        (
            "find_index",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "first_active_index"
    expr:
      - "@input.users"
      - find_index: ["@item.active"]
"#,
            json!([{ "first_active_index": 1 }]),
        ),
    ];
    let input = r#"[{"users":[
        {"id":"u1","name":"Alice","role":"admin","active":false,"tags":["a","b"]},
        {"id":"u2","name":"Bob","role":"member","active":true,"tags":["c"]},
        {"id":"u3","name":"Carol","role":"admin","active":true,"tags":[]}
    ]}]"#;

    for (name, yaml, expected) in cases {
        assert_traced_output_matches_normal(yaml, input, expected);
        let rule = parse_rule_file(yaml).unwrap_or_else(|err| panic!("{name} parse: {err:?}"));
        let traced = transform_input_with_trace(
            &rule,
            InputData::Text(input),
            None,
            &TransformTraceOptions::raw(),
        )
        .unwrap_or_else(|err| panic!("{name} traced transform: {err:?}"));
        assert!(
            iter_trace_events(&traced.trace).iter().any(|event| {
                event.kind == TraceEventKind::OpStart && event.operator.as_deref() == Some(name)
            }),
            "missing op_start for {name}"
        );
    }
}

#[test]
fn trace_v2_fold_preserves_acc_and_item_scope() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "sum"
    expr:
      - "@input.numbers"
      - fold:
          - 0
          - ["@acc", { "+": "@item" }]
"#;

    assert_traced_output_matches_normal(yaml, r#"[{"numbers":[1,2,3]}]"#, json!([{ "sum": 6.0 }]));
}

#[test]
fn trace_v2_let_binding_survives_nested_map_steps() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "totals"
    expr:
      - "@input.prices"
      - map:
          - let: { tax: 1.1 }
          - multiply: ["@tax"]
"#;

    assert_traced_output_matches_normal(
        yaml,
        r#"[{"prices":[100,200]}]"#,
        json!([{ "totals": [110.00000000000001, 220.00000000000003] }]),
    );
}

#[test]
fn trace_v2_if_does_not_evaluate_unselected_branch() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "label"
    expr:
      - "@input.enabled"
      - if:
          cond:
            eq: ["$", true]
          then:
            - "enabled"
          else:
            - "@item.label"
"#;

    assert_traced_output_matches_normal(
        yaml,
        r#"[{"enabled":true}]"#,
        json!([{ "label": "enabled" }]),
    );
}
