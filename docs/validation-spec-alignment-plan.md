# Validation Spec Alignment Plan

## 棚卸しステータス

2026-06-11 時点では、削除せず tracked docs として保持する。本文には完了済みの修正方針と、現行実装に対する再確認が必要な coverage / validation 強化項目が混在している。`docs/plans/` へ退避するのは、完了条件を実装・tests・docs で再照合し、残タスクが roadmap または issue に移された後にする。

## 目的

本書は、v2 validation が仕様上正しい rule を reject した問題を起点に、validation / runtime / docs / tests の整合性を取り直すための修正方針をまとめる。

対象は core validation を中心に、入力正規化、branch rule graph、CLI validate、DTO inference、direct mode synthetic rule までを含む。実装は TDD で進める。

## 基本方針

- validation は docs に書かれた仕様を reject しない。
- validation は runtime で fail することが静的に分かる rule を fail-closed にする。
- runtime と validation の operator 引数契約は同じ source of truth に寄せる。
- branch は traceability の中核なので、branch 先 rule も validation graph の解析対象にする。
- `docs/rules_spec_ja.md` と `docs/rules_spec_en.md` は仕様更新時に必ず同期する。
- 既存 fixture が transform で通るだけでは十分ではない。代表 fixture は `validate_rule_file` と CLI `validate` でも固定する。

## 決定済み仕様

### v2 pipe の暗黙入力

v2 pipe では、前段の pipe value が operator の暗黙入力になる。

したがって validation の arg count は、pipe value を `op_step.args` の明示引数とは別に扱う。

例:

```yaml
expr:
  - "@input.flat"
  - object_unflatten
```

この場合、`object_unflatten` の明示引数は `0` で正しい。

対象 operator:

- `object_flatten`
- `object_unflatten`
- `from_entries`

対応方針:

- v2 operator metadata は v2 pipe 表記の明示引数数を表す。
- validation は pipe value を implicit operand として扱い、明示引数だけで過剰 reject しない。
- docs の args 表も v2 pipe 表記の明示引数数に合わせる。
- `object_flatten` / `object_unflatten` は pipe object に対する `0` args を正とする。
- `from_entries` は pipe pair array に対する `0` args と、pipe key + `value` arg の `1` arg を正とする。

現状:

- 本問題の RED/GREEN patch は既に入っている。
- 今後は operator inventory と validation positive coverage を広げて再発を防ぐ。

### `records_path` の record は object 必須

`records_path` は JSON/YAML/TOML/XML 入力から record を切り出すための設定である。

record は JSON object として扱う。scalar を record として扱わない。

仕様:

```text
records_path が object を指す:
  その object を単一 record として扱う

records_path が array を指す:
  array の各要素が object なら、それぞれを record として扱う

records_path が scalar/null を指す:
  invalid

records_path が array を指すが、要素に object 以外が混ざる:
  invalid
```

OK:

```json
{
  "items": [
    { "id": 1 },
    { "id": 2 }
  ]
}
```

NG:

```json
{
  "items": [
    1,
    { "id": 2 }
  ]
}
```

対応方針:

- JSON/YAML/TOML の normalization で、array element が object か検査する。
- 非 object element は normalization error にする。
- JSON/YAML/TOML それぞれに RED fixture を追加する。
- docs は現行の「record は JSON object」を維持する。

### `return:false` branch は child rule graph を解析する

`return:false` branch は、分岐先 rule の output object を現在の `@out` に merge して親 rule の後続 step を続行する。

traceability を核にするため、validation も branch 先 rule を解析する。

現状の問題:

- trace 実行時は branch 先 rule が読み込まれ、child rule event は `BranchTaken` の下に nest される。
- しかし validation 時は branch 先 rule の output target を解析していない。
- 現在は `return:false` branch 後に `allow_any_out_ref = true` としており、後続 `@out.*` forward reference が広く緩む。

目標仕様:

```text
return:false branch の後続 step では、
親 rule で既に生成された output target と、
branch child rule が生成しうる output target のみ @out.* 参照可能。
```

例:

```yaml
steps:
  - mappings:
      - target: base
        value: 10
  - branch:
      when: { eq: ["@input.kind", "a"] }
      then: then.yaml
      else: else.yaml
      return: false
  - mappings:
      - target: after
        expr: "@out.branch_value"
```

`then.yaml` と `else.yaml` がどちらも `branch_value` を出すなら、`@out.branch_value` は valid。

一方、child rule が出さない `@out.never_created` は `ForwardOutReference` として invalid。

Output contract:

```text
possible_outputs:
  どれかの実行経路で生成されうる output target

guaranteed_outputs:
  すべての実行経路で生成される output target
```

forward reference validation はまず `possible_outputs` を使う。`guaranteed_outputs` は UI、trace metadata、preflight diagnostics で使える情報として保持する。

`return:false` mergeability:

- child output は object として merge 可能でなければならない。
- child rule に `finalize.wrap` がない場合は、mappings / steps が作る output object を merge 対象とみなす。
- child rule に `finalize.wrap` がある場合は、root が statically object と分かる形だけ `return:false` mergeable とする。
- root が scalar、array、または object と証明できない expression の場合は validation error にする。必要なら将来 explicit output contract を追加する。

実装方針:

- `validate_rule_file_with_base_dir` 相当の graph validation API を追加する。
- CLI `validate -r path` は rule file の parent directory を base dir として graph validation を呼ぶ。
- branch path 解決、base directory 制約、depth limit、cycle detection は transform 側と同じ安全性 invariant に合わせる。
- child rule 自体も validate し、その errors は親 rule の `steps[n].branch.then` / `else` に紐づける。
- `allow_any_out_ref = true` は廃止または branch graph 解析不能時の内部 fallback に限定する。

### v1 chain の `concat` / `coalesce`

v1 chain でも、`concat` / `coalesce` の追加 args なしは invalid に寄せる。

理由:

- v2 pipe の docs/metadata は追加 args `>=1` を要求する。
- `{ op: concat }` や `{ op: coalesce }` は identity 的に振る舞うだけで、書き間違いを見逃しやすい。
- identity 用途は `ref` または pipe value 自体で表現できる。

対応方針:

- v1 chain validator に `concat` / `coalesce` の min arg guard を追加する。
- invalid validation fixture を追加する。
- 互換性影響があるため、release note で破壊的変更として明記する。

## 追加で見つかった不整合と対応

### `sort_by` docs args

症状:

- runtime / validator は `sort_by` の第2引数 `asc|desc` を許容する。
- docs の args 表は `1` のまま。

対応:

- `docs/rules_spec_ja.md` / `docs/rules_spec_en.md` の `sort_by` を `1-2` に修正する。
- 第2引数は `asc|desc`、省略時 `asc` と明記する。

### Excel input の静的 validation

症状:

- `range` 形式、`columns[].column` の Excel column letter、`header_row` と `range` 開始行の関係は docs 上の制約だが、rule validation が十分に検査していない。

対応:

- validator で Excel range parser と column letter validation を使う。
- invalid range、numeric column、`header_row < range.start_row` の validation tests を追加する。
- normalization まで遅らせず、data-independent な設定エラーは validate で返す。

### `pick` literal path の invalid validation fixture

症状:

- `pick` の literal path validation は validator で検出できる。
- 既存 fixture は transform error golden に寄っており、validation invalid case が不足している。

対応:

- `r02_json_ops_invalid_path_pick` 相当を validation invalid fixture に追加する。
- `InvalidArgs` と error path を固定する。

### typed-value inline option の validation parity

症状:

- inline literal options の `decode`、hint option、hint path は validator 実装がある。
- しかし transform/runtime 寄りの tests が中心で、`validate_rule_file` が同じ fail-closed を保証する tests が薄い。

対応:

- inline literal の invalid case を validation tests に追加する。
- 対象:
  - `decode` が object でない
  - unknown decode key
  - unsupported hint `format`
  - unsupported hint `output_precision`
  - malformed hint path

### v2 runtime と metadata の arg guard

症状:

- 通常の CLI/API flow では validation が先に効く。
- ただし runtime API を validation bypass で呼ぶと、metadata より lenient な operator がある。

例:

- `lookup` / `lookup_first` は metadata が `2-4` args だが、runtime が余分な args を無視しうる。
- `trim` / `lowercase` / `uppercase` / casts / `first` / `last` など zero-arg ops が extra args を無視しうる。

対応:

- v2 runtime entrypoint でも shared operator metadata による arg guard を適用する。
- 少なくとも lookup max args と zero-arg op extra args の runtime reject tests を追加する。
- validation と runtime の二重防御にする。

### comparison aliases の docs 表

症状:

- `eq` / `ne` / `lt` / `lte` / `gt` / `gte` / `match` は metadata/runtime にある。
- docs 本文には alias 利用可とあるが、operator args 表は symbol op 中心で inventory と完全一致しない。

対応:

- 表に alias 行を足す、または alias が symbol op と同じ `1` arg contract を共有することを明記する。

## テスト coverage 改善

### valid fixture validation

transform golden にある valid rule が validation valid list に入っていない。

追加対象:

- typed-value provider/profile:
  - `tv47_typed_value_dynamodb_item_shorthand`
  - `tv48_typed_value_dynamodb_item_codec_binding`
  - `tv49_typed_value_firestore_document`
  - `tv50_typed_value_mongo_extended_json`
- branch / finalize:
  - `tv32_steps_finalize`
  - `tv33_branch_return`
  - `tv34_branch_return_true`
  - `tv35_finalize_wrap`
  - `tv38_finalize_filter_offset`
  - `tv40_branch_return_filter`
  - `tv41_branch_finalize_wrap`
  - `tv42_branch_deep_merge`
- custom ops:
  - `tv45_custom_ops_dot_path_body`
  - `tv46_custom_ops_body_input_and_pipe_refs`
- input-format rich fixtures:
  - `t36_spreadsheets_plugin_products`
  - `t37_spreadsheets_plugin_orders`
  - `t38_spreadsheets_plugin_survey`
  - `t39_pyproject_dependency_inventory`
  - `t40_cargo_dependency_feature_inventory`
  - `t41_github_actions_matrix`
  - `t42_openapi_endpoint_catalog`
- math ops:
  - `t44_math_ops`
  - `tv44_math_ops`

方針:

- 個別 list 追加に加えて、`version: 2` fixture を batch validate する smoke test を検討する。
- 既知 invalid fixture は allowlist で除外する。

### operator inventory と validation positive の接続

trace semantics には operator inventory representative cases があるが、validation positive と接続されていない。

対応:

- `OPERATOR_CASES` 相当の representative v2 pipe を `validate_rule_file` にも通す test を追加する。
- 共有が重い場合は、fixture generator を共通化せず、validation 側に最小 representative table を持つ。

### CLI validate smoke

CLI `validate` は user-facing entrypoint なので、core validation だけでは不十分。

対応:

- `crates/rulemorph_cli/tests/cli/validate.rs` を table 化する。
- 代表として typed-value、branch/finalize、input-format を通す。
- JSON error output の path/code も代表ケースで固定する。

### DTO inference

DTO inference tests は parse + generate に寄っており、rule が validation を通ることを保証していない。

対応:

- DTO test helper で `validate_rule_file` を先に呼ぶ。
- validation と DTO inference の受理範囲が drift したら test で検知する。

### direct mode synthetic rule

direct mode は synthetic rule を生成して実行するが、生成 rule が core validate を通ることを固定していない。

対応:

- direct rule builder に近い unit/integration test で、生成後 rule を `validate_rule_file` に通す。
- `--output-map` の if step、Excel range、records_path を含む representative cases を追加する。

## 実装順序

### Phase 1: 小さい docs / coverage 修正

1. `sort_by` docs を ja/en 同期で修正する。
2. valid fixture validation list を拡張する。
3. operator representative validation test を追加する。
4. CLI validate smoke を table 化する。

Verify:

```bash
cargo fmt
cargo test -p rulemorph --test validation
cargo test -p rulemorph_cli --test cli validate
git diff --check
```

### Phase 2: normalization / input validation hardening

1. JSON/YAML/TOML `records_path` array element object-only の RED tests を追加する。
2. normalization で non-object element を reject する。
3. Excel data-independent validation の RED tests を追加する。
4. Excel validator に range / column / header relation check を追加する。

Verify:

```bash
cargo fmt
cargo test -p rulemorph --test validation
cargo test -p rulemorph --test transform_golden
cargo test -p rulemorph
git diff --check
```

### Phase 3: branch graph validation

1. graph validation API を追加する。
2. validation 用 branch resolver を追加し、base dir / depth / cycle / base-dir escape を固定する。
3. child rule output contract を実装する。
4. `return:false` branch 後の `@out` forward reference を child output contract で判定する。
5. `return:false` mergeability validation を追加する。
6. CLI `validate` を graph validation API に切り替える。
7. trace tests を再実行し、observational API としての trace semantics が変わらないことを確認する。

RED tests:

- child が出す `@out.branch_value` は valid。
- child が出さない `@out.never_created` は `ForwardOutReference`。
- then/else の union は `possible_outputs` として扱う。
- then/else の intersection は `guaranteed_outputs` として保持する。
- branch cycle は validation error。
- base dir escape は validation error。
- `return:false` child が scalar/array output になる場合は validation error。
- child rule parse/validation error は親の `steps[n].branch.then` / `else` に紐づく。

Verify:

```bash
cargo fmt
cargo test -p rulemorph --test validation
cargo test -p rulemorph --test transform_trace
cargo test -p rulemorph --test transform_trace_semantics
cargo test -p rulemorph
git diff --check
```

### Phase 4: runtime parity / peripheral surfaces

1. v2 runtime entrypoint に metadata arg guard を追加する。
2. typed-value inline option validation tests を追加する。
3. `pick` literal path invalid validation fixture を追加する。
4. v1 chain `concat` / `coalesce` zero-arg invalid tests と validator guard を追加する。
5. DTO inference helper で validation を通す。
6. direct mode synthetic rule validation tests を追加する。

Verify:

```bash
cargo fmt
cargo test
git diff --check
```

## 完了条件

- docs と validation が一致している。
- runtime-valid な documented notation を validation が reject しない。
- statically invalid な rule は validation で reject される。
- branch child rule は trace だけでなく validation graph でも解析される。
- `allow_any_out_ref` に依存した branch 後の広い `@out` 許容が残っていない。
- JSON/YAML/TOML `records_path` は object record contract を満たす。
- Excel の data-independent constraint は validation で検出される。
- representative fixtures は transform golden、core validation、CLI validate の少なくとも該当 surface で固定される。
