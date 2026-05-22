# Semantic Trace Guide

## When to read
- trace / transform / v2_eval / branch / finalize を変更する前に読む。
- `TransformTrace`、trace event、trace option、trace error、WASM 向け trace surface に触る場合も読む。

## Do not break
- trace は observational API。通常 transform の意味論を変更しない。
- trace 有効化で通常 transform の output、warnings、error kind を変えない。
- `parent_id` が emitted event を指す構造を壊さない。
- error path でも open span を残さない。
- `input_path` / `output_path` の canonical path を壊さない。
- missing / null / empty string の区別を壊さない。
- `OutputWrite` は input slot ではなく output snapshot を表す。
- branch child rule は同じ record trace 内で branch event の下に nest する。
- collection operator は item / accumulator scope を混ぜない。
- `finalize` section と record-level events を混ぜない。
- raw value leakage を増やさない。`Debug` / `Display` / error / attributes / messages / export boundary に raw trace を逃がさない。

## Required tests
- `cargo fmt`
- `cargo test -p rulemorph --test transform_trace`
- `cargo test -p rulemorph --test transform_trace_semantics`
- 必要に応じて `cargo test -p rulemorph`

## Notes
- 詳細仕様は `docs/rules_spec_ja.md` の Semantic trace API section と、対応する `docs/rules_spec_en.md` を参照する。
- trace API は raw-first の local opt-in だが、外部 surface は `MetadataOnly` を既定にし、`contains_raw_values == false` を確認する。
