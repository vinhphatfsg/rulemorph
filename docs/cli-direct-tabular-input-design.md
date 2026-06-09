# CLI direct mode の CSV / Excel 入力と複数出力詳細設計

## 目的

`rulemorph --rule` の direct mode で、rule file を作らずに CSV / Excel を扱えるようにする。あわせて、複数の出力フィールドを rule file なしで定義できる `-F/--field` と `--output-map` を追加する。

現在の core library は `input.format=csv` / `input.format=excel` の normalization を持つが、CLI direct mode は synthetic rule を `json` / `csv` の最小設定でしか生成できない。そのため headerless CSV、CSV header 付与、Excel sheet / header row / range 指定を direct mode から扱えない。

また、既存 direct mode の `--rule <EXPR>` は 1 つの expr の評価結果だけを返す。object を返すことはできるが、複数 target を作るには `from_entries` / `merge` などを組み合わせる必要があり、CLI の一時変換としては書きづらい。

この設計では core normalization / expr / mappings の契約を変えず、CLI layer だけで direct mode 用の入力設定と mappings を生成する。

## スコープ

対象:

- `rulemorph --rule <EXPR>` / `rulemorph -rule <EXPR>` の direct mode
- CSV direct input
- Excel `.xlsx` direct input
- direct mode の入力形式推定
- direct mode の context JSON
- direct mode の複数 target 出力 sugar
- direct mode docs / tests

対象外:

- `rulemorph transform -r ...` / `preflight` の rule file override 拡張
- core normalization の挙動変更
- CSV / Excel cell の自動型推論
- Excel formula evaluation
- raw string output の導入
- `@input.age|int` のような CLI 独自ショートハンド
- `--output-map` の再帰テンプレート展開
- literal array 専用 sugar

## 受け入れ条件

以下を満たす。

```sh
echo "a,test,1" | rulemorph -rule "@input.0"
# => "a"

echo "a,test,1" | rulemorph -H "id,name,age" -rule "@input.id"
# => "a"

rulemorph -H "id,name,age" -rule "@input.id" -i non_header.csv
# => "a"

rulemorph -rule "@input.id" -i with_header.csv
# => "a"

rulemorph -rule "@input.id" -i users.xlsx --excel-header-row 1 --excel-data-range A2:D2
# => 1

rulemorph -rule "@input.id" -i users.xlsx --excel-header-row 1 --excel-data-range A2:D3
# => [1,2]

echo "u1,Alice,42" | rulemorph -H "id,name,age" \
  -F id="@input.id" \
  -F name='["@input.name","trim","uppercase"]' \
  -F kind="lit:user"
# => {"id":"u1","name":"ALICE","kind":"user"}

echo "u1,Alice,42" | rulemorph -H "id,name,age" \
  --output-map '{"user.id":"@input.id","user.name":["@input.name","trim"],"kind":"lit:user"}'
# => {"user":{"id":"u1","name":"Alice"},"kind":"user"}

echo '{"tenant_id":"t1"}' > context.json
echo "u1,Alice" | rulemorph -H "id,name" -c context.json \
  -F id="@input.id" \
  -F tenant="@context.tenant_id"
# => {"id":"u1","tenant":"t1"}
```

出力は既存 direct mode と同じ JSON serialization を維持する。したがって string は raw `a` ではなく `"a"` として出力される。raw 出力が必要な場合は、別機能として `--raw-output` を追加する。

## CLI surface

direct mode の output spec として以下を追加する。

```text
-F, --field <TARGET=EXPR>
    1 つの output mapping を追加する。複数指定可。

--output-map <JSON_OBJECT>
    JSON object の key を output target path、value を rulemorph expr として mappings を生成する。
    再帰的 JSON template ではない。
```

direct mode の入力 option として以下を追加する。

```text
-H, --headers <CSV_HEADERS>
    Headerless CSV に与える field name。1 つの CSV record として parse する。

-c, --context <JSON_FILE>
    @context として参照する JSON file。既存 transform と同じ読み込み規則を使う。

--excel-data-range <RANGE>
    Excel の data range。例: A2:C100。header row は含めない。

--excel-header-row <ROW>
    Excel header row。1-based。Excel direct input では必須。

--excel-sheet <NAME>
    Excel sheet name。省略時は最初の sheet。

--excel-sheet-index <INDEX>
    Excel sheet index。0-based。--excel-sheet と同時指定不可。
```

`-h` は clap の help と衝突するため、headers の short option は `-H` とする。

direct mode は `--rule`、`--output-map`、`-F/--field` のいずれか 1 種類だけを受け付ける。併用した場合は CLI validation error とする。`-F/--field` は同じ option の繰り返しだけを許可する。

`echo non_header.csv | rulemorph ...` はファイルパス扱いにしない。stdin は常に入力データ本体として読む。ファイル入力は `-i non_header.csv` を使う。

`-i` / `-o` / `-c` は通常の local file path として扱う。これはローカル CLI として自然な挙動だが、CI / server / MCP / Web UI などで第三者が path を制御できる運用では、呼び出し側で sandbox / allowlist / stdin 経由入力を使い、任意 local file read/write surface にしない。

## 入力形式推定

direct mode では `DirectInputFormat` を新設し、`transform` / `preflight` で使う `FormatOverride` とは分ける。

推定順:

1. `-f/--format` があればそれを使う。
2. `-i -` は stdin と同じ扱いにする。
3. `-i` の拡張子で判定する。
   - `.csv` -> `csv`
   - `.xlsx` -> `excel`
   - `.json` -> `json`
4. `-i` があり、拡張子が未知または拡張子なしの場合は、既存互換のため `json` とする。
5. stdin の場合:
   - UTF-8 BOM と ASCII whitespace を読み飛ばした最初のバイトが `{` または `[` なら `json`
   - それ以外なら `csv`

stdin の Excel bytes は拡張子がないため、`-f excel` を必須にする。`-f excel` で stdin を読む場合は `InputData::Bytes` として core に渡す。

stdin の自動判定では JSON 全体を parse しない。判定は先頭バイトの分類だけに留める。JSON 形式に見える stdin input が parse に失敗した場合は CSV fallback せず、既存 JSON parser の error を返す。CSV が `{` または `[` で始まる場合は `-f csv` を明示する。

`transform` / `preflight` の `-f` は既存どおり `csv` / `json` のままにする。direct mode 側だけ `excel` を受け付ける。

CSV / Excel 専用 option は対象 format と一致しない場合に拒否する。たとえば `--headers` は CSV direct input 以外では使えず、`--excel-data-range` / `--excel-header-row` / `--excel-sheet` / `--excel-sheet-index` は Excel direct input 以外では使えない。指定を無視する fallback は作らない。

## context 設計

direct mode でも既存 `transform` / `preflight` と同じ `-c, --context <JSON_FILE>` を受け付ける。context file は `crates/rulemorph_cli/src/input/files.rs` の既存 `load_context` を再利用して JSON として読み込む。

`@context` は通常の expr 参照として扱う。したがって以下のすべてで利用できる。

- `--rule '@context.tenant_id'`
- `-F tenant='@context.tenant_id'`
- `--output-map '{"tenant":"@context.tenant_id"}'`

context は synthetic rule には埋め込まない。`transform_input_with_warnings_with_base_dir_and_options` の context 引数に `context_value.as_ref()` を渡す。context が指定されない場合は既存 direct mode と同じく `None` を渡す。

`@context` は `@out` と違って mapping order に依存しないため、`--output-map` でも禁止しない。headerless CSV の numeric field reference detection では `@context.*` を入力列推定に使わない。

context file の read / parse error は既存 `transform -c` と同じ扱いにする。`load_context` の現行 contract に合わせ、read error と JSON parse error は exit code `1` で返し、代表メッセージも既存の `failed to read context: ...` / `failed to parse context JSON: ...` を維持する。

context file については、現在 direct mode 専用の size limit や strict JSON parser は追加していない。これは `transform -c/--context` と direct mode の parse behavior / error message を揃えるためであり、context だけ direct mode で異なる duplicate key handling や size error を持たせない。一般的なローカル CLI と同じく、ユーザー自身が明示した local file の大きさは基本的に OS / runner の resource limit に委ねる。

ただし、rulemorph CLI を CI、server、MCP、Web UI などから呼び出し、第三者が context path または context file content を制御できる運用では、巨大 context による memory DoS や duplicate key による解釈差が security risk になりうる。その hardening は direct mode 専用ではなく、既存 `transform -c` も含めた共有 `load_context` contract の変更として扱う。将来対応する場合は `MAX_CONTEXT_BYTES` と strict JSON duplicate-key rejection を shared helper に追加し、direct / transform の両方で同じ error contract に更新する。

## direct rule 生成

`crates/rulemorph_cli/src/direct.rs` の `build_direct_rule` を、入力形式、direct input option、direct output spec を受け取る形に拡張する。

direct output spec は内部的に次の 3 種類として扱う。

- `Rule(expr)`: 既存 `--rule <EXPR>`
- `Fields(Vec<FieldSpec>)`: `-F/--field <TARGET=EXPR>` の繰り返し
- `OutputMap(Vec<FieldSpec>)`: `--output-map <JSON_OBJECT>` を key/value から展開したもの

### `--rule`

既存互換のため、`--rule` は synthetic target `__rulemorph_direct_value` に 1 mapping だけを書き込む。

```json
{
  "version": 2,
  "input": {
    "format": "<format>",
    "<format>": { "...": "..." }
  },
  "mappings": [
    {
      "target": "__rulemorph_direct_value",
      "expr": "<parsed inline expr>"
    }
  ]
}
```

`parse_rule_file_with_format(..., RuleFormat::Json)` は継続利用する。これにより validation / serde の `deny_unknown_fields` を通した同じ rule contract を使える。

### `-F/--field`

`-F/--field` は `TARGET=EXPR` を受け取る。split は最初の `=` だけで行う。

例:

```sh
rulemorph -H "id,name,age" \
  -F id="@input.id" \
  -F name='["@input.name","trim","uppercase"]' \
  -F age='["@input.age","int"]'
```

生成する mappings:

```json
[
  { "target": "id", "expr": "@input.id" },
  { "target": "name", "expr": ["@input.name", "trim", "uppercase"] },
  { "target": "age", "expr": ["@input.age", "int"] }
]
```

RHS は既存 `--rule` と同じ inline expr semantics に寄せる。つまり、JSON として parse できる値は JSON expr として扱い、JSON でなければ文字列 expr として扱う。`@input.age|int` のような CLI 独自ショートハンドは導入しない。

ただし `-F/--field` の RHS が BOM / whitespace 後に `{` または `[` で始まる場合は JSON-looking expr とみなし、strict JSON parse に失敗したら literal string fallback せず CLI validation error にする。`["@input.age","int"` のような typo が文字列 literal として silently 出力されるのを避けるためである。`{` または `[` で始まる literal string が必要な場合は、既存 expr と同じく `lit:{...` / `lit:[...]` を使う。

target に `=` を含めたい場合、`-F` では separator と衝突するため `--output-map` を使う。

```sh
rulemorph --output-map '{"user[\"a=b\"]":"@input.value"}'
```

`-F/--field` の指定順は synthetic `mappings` の評価順として維持する。したがって `@out` 参照を使う場合は、先に指定した field だけを参照できる。

```sh
rulemorph \
  -F subtotal='["@input.items",{"map":["@item.price"]},"sum"]' \
  -F total='["@out.subtotal",{"*":[1.1]}]'
```

文字列の扱いも既存 v2 expr に合わせる。

- `@input.id` は参照
- `["@input.age","int"]` は pipe
- `lit:@input.id` は文字列リテラル `@input.id`
- `lit:$` は文字列リテラル `$`
- plain string は既存 expr の literal string として扱う

### `--output-map`

`--output-map` は strict JSON object を受け取り、object の key を `target`、value を `expr` として mappings を生成する。これは再帰的 JSON template ではなく、通常の `mappings` を短く書くための target-to-expr map である。

例:

```sh
rulemorph -H "id,name,age" \
  --output-map '{"user.id":"@input.id","user.name":["@input.name","trim"],"kind":"lit:user"}'
```

生成する mappings:

```json
[
  { "target": "user.id", "expr": "@input.id" },
  { "target": "user.name", "expr": ["@input.name", "trim"] },
  { "target": "kind", "expr": "lit:user" }
]
```

ネスト出力は既存 `target` path で表現する。たとえば `user.id` は `{ "user": { "id": ... } }` を作る。ドットを含む key を出力したい場合は既存 path と同じ bracket quote を使う。

```json
{
  "user[\"full.name\"]": "@input.name"
}
```

`--output-map` の value は通常の `expr` contract で解釈する。plain object は literal object として扱われるが、既存 `Expr` の shape に一致する `{"ref":"..."}` / `{"op":"...","args":[...]}` / `{"chain":[...]}` は v1 expr として扱われる。したがって `{"user":{"id":"@input.id"}}` は nested template ではなく、top-level target `user` の plain object literal expr であり、内部の `"@input.id"` は参照評価されず文字列値として出力される。混乱を避けるため、docs では nested output は target path で書くことを推奨する。

expr shape に一致する object 自体を literal object として出力する direct sugar は今回追加しない。たとえば `{"ref":"input.id"}` という object literal をそのまま出したい場合は、direct mode の `--output-map` では表現せず、rule file の正式な mapping / expr 表現で扱う。将来必要になった場合は `--value TARGET=JSON` のような別 surface か、core expr 側の literal object 表現として設計する。

top-level array は既存 v2 expr では pipe として解釈される。したがって `--output-map '{"tags":["a","b"]}'` は literal array ではなく pipe expr として扱われる。literal array 専用 sugar は今回追加しない。literal array が必要な場合は、既存 v2 expr の正式形として literal array を pipe start に置く。

```json
{
  "tags": [["a", "b"]]
}
```

### output spec validation

CLI 側で以下を fail-closed にする。

- `--rule`、`--output-map`、`-F/--field` の併用
- `-F/--field` の `=` 不足
- 空 target
- 空 expr
- `--output-map` が JSON object ではない
- `--output-map` の空 object
- strict JSON parse で拒否される output-map
- output field 数の上限超過
- output spec byte 数の上限超過
- target byte 数の上限超過
- target byte 数合計の上限超過
- target path depth / target token 総数の上限超過
- direct output cell 数の上限超過
- expr JSON depth / node / string byte 数の上限超過
- canonical target の重複
- target の親子衝突
- `--output-map` value 内の評価される `@out` 参照

target の parse は core の `parse_path` と同じ規則を使う。array index を含む target は通常 mapping と同じく拒否する。`user.name` と `user["name"]` のように表記が違っても同じ path tokens になる場合は duplicate として拒否する。`user` と `user.name` のような親子関係は、評価順に依存する上書きや `target path conflicts with non-object value` を避けるため、CLI の synthetic rule 生成前に拒否する。

output spec にも direct mode 専用上限を適用する。初期値は `MAX_DIRECT_OUTPUT_FIELDS = 10_000`、`MAX_DIRECT_OUTPUT_SPEC_BYTES = 8 MiB`、`MAX_DIRECT_OUTPUT_TARGET_BYTES = 256 KiB`、`MAX_DIRECT_OUTPUT_TARGET_BYTES_TOTAL = 8 MiB`、`MAX_DIRECT_OUTPUT_TARGET_DEPTH = 256`、`MAX_DIRECT_OUTPUT_TARGET_TOKENS_TOTAL = 1_000_000`、`MAX_DIRECT_OUTPUT_EXPR_DEPTH = 256`、`MAX_DIRECT_OUTPUT_EXPR_NODES = 1_000_000`、`MAX_DIRECT_OUTPUT_EXPR_STRING_BYTES = 8 MiB`、`MAX_DIRECT_OUTPUT_CELLS = 10_000_000` とする。これは巨大な `-F` 繰り返しや巨大 output-map key / expr から synthetic rule JSON / nested output path validation / `records * mappings` 評価を過剰に肥大化させないための guard である。

`MAX_DIRECT_OUTPUT_CELLS` は record object output、つまり `-F/--field` と `--output-map` にだけ適用する。transform 前に、resolved input format と既存 `NormalizationOptions` の effective record limit から `effective_max_records * output_field_count` を見積もり、上限を超える場合は CLI validation error とする。JSON object input のように 1 record と判定できる入力は `1 * output_field_count` として扱う。現在は output byte 数の精密な上限は追加せず、必要になった場合は shared `max_output_bytes` として別途設計する。

`--output-map` は JSON object なので、target-to-expr map 自体に評価順の意味を持たせない。既存 `mappings` の `@out` は「前の mapping」への参照であり、object key order に依存させると validation / runtime の意味が揺れる。そのため `--output-map` value 内で評価される `@out` 参照は CLI 側で拒否する。順序依存が必要な場合は `-F/--field` を使う。plain literal object 内の文字列 `"@out.x"` は評価される ref ではないため、この禁止の対象外である。

## CSV 設計

### header あり CSV

条件:

- `--headers` がない
- direct output spec の expr 群に root numeric field reference がない

生成する input:

```json
{
  "format": "csv",
  "csv": {
    "has_header": true
  }
}
```

例:

```sh
rulemorph -rule "@input.id" -i with_header.csv
```

### headerless CSV + 明示 headers

条件:

- `--headers id,name,age` がある

生成する input:

```json
{
  "format": "csv",
  "csv": {
    "has_header": false,
    "columns": [
      { "name": "id" },
      { "name": "name" },
      { "name": "age" }
    ]
  }
}
```

validation:

- `--headers` は 1 つの CSV record として parse する。空白は field name の一部として扱い、trim しない。たとえば `-H "id, name, age"` は `id` / ` name` / ` age` になるため、通常は `-H "id,name,age"` を使う。
- header 名に comma を含めたい場合は CSV と同じ quote を使う。例: `-H 'id,"display,name",age'`
- 空 header は CLI 側で拒否する。
- 重複 header は CLI 側でも拒否してよいが、core validation / normalization でも拒否される。
- header 数は direct mode 専用上限 `MAX_DIRECT_TABULAR_FIELDS` 以下でなければならない。
- header 名は 1 個あたり `MAX_DIRECT_TABULAR_HEADER_BYTES` 以下、合計で `MAX_DIRECT_TABULAR_HEADER_BYTES_TOTAL` 以下でなければならない。
- delimiter は今回 CLI surface に追加しない。既存 default `,` を使う。

### headerless CSV + numeric field 推定

条件:

- `--headers` がない
- direct output spec の expr 群に root numeric field reference がある

例:

- `@input.0`
- `["@input.0", { "concat": ["-", "@input.1"] }]`

この場合、input bytes の先頭 CSV record を読み、field 数から `"0"`, `"1"`, ... の columns を生成する。

先頭 record の読み取りは core CSV normalization と同じ `csv` crate semantics を使う。実装は、`rulemorph_cli` に `csv = "1.3"` を追加して direct mode の推定にだけ使う。CSV reader が record を読めない入力は推定成功として扱わず、transform 前の入力エラーとして fail-closed にする。

生成例:

```json
{
  "format": "csv",
  "csv": {
    "has_header": false,
    "columns": [
      { "name": "0" },
      { "name": "1" },
      { "name": "2" }
    ]
  }
}
```

`@input.0` は path parser 上は key `"0"` への参照になるため、既存 evaluator を変更せず動作する。

先頭 record が読めない、または field 数が 0 の場合は direct mode の入力エラーとして終了する。

推定した field 数が direct mode 専用上限 `MAX_DIRECT_TABULAR_FIELDS` を超える場合も入力エラーとして終了する。これは大量 delimiter を含む 1 行入力から巨大な synthetic rule / `columns` vector を生成しないための fail-closed guard である。

`MAX_DIRECT_TABULAR_FIELDS` は初期実装では `10_000` とする。これは public API には出さず、`crates/rulemorph_cli/src/direct.rs` の direct mode guard として持つ。将来、core normalization 全体の CSV field 数上限が必要になった場合は `NormalizationOptions` 側に `max_csv_fields` を追加して統合する。

`MAX_DIRECT_TABULAR_HEADER_BYTES` は初期実装では `256 KiB`、`MAX_DIRECT_TABULAR_HEADER_BYTES_TOTAL` は `8 MiB` とする。これらも direct mode guard として持ち、synthetic rule JSON の過剰な肥大化を防ぐ。

### numeric field reference の検出

direct output spec に含まれる expr から、実際に評価される root numeric input reference だけを検出する。

- `--rule` は 1 つの expr
- `-F/--field` は各 field RHS の expr
- `--output-map` は各 target value の expr

検出は raw JSON string の再帰走査ではなく、既存 expr semantics に寄せる。実装では、expr value を `rulemorph::Expr` として読める場合は `Expr` を走査し、v2-looking literal は `rulemorph::v2_parser::parse_v2_expr` で `V2Expr` にしてから参照を走査する。plain literal object の内側にある `"@input.0"` のような文字列は評価される ref ではないため、numeric field inference には使わない。

検出対象:

- v2 `@input.<digits>`
- v2 `@input.<digits>.` で始まる参照
- v2 `@input.<digits>[` で始まる参照
- v1 `{"ref":"input.<digits>"}` 系の input ref

`<digits>` は canonical decimal とし、`0` または non-zero digit で始まる digit sequence だけを numeric field inference 対象にする。`@input.01` のような leading zero は列名 `"01"` への通常参照として扱い、headerless numeric inference は起動しない。

`@input["0"]` は今回の必須条件には含めない。ただし将来拡張してもよい。

この検出は headerless 推定のためだけに使い、実際の参照解決は既存 parser/evaluator に任せる。

## Excel 設計

Excel direct input では以下を必須にする。

- `--excel-header-row <ROW>`
- `--excel-data-range <RANGE>`

`--excel-data-range` はユーザー向けには data range として定義する。たとえば `--excel-header-row 1 --excel-data-range A2:C100` は「1 行目を header、2-100 行目を data」と読む。

core の `input.excel.range` は header row を含む selected cell window として扱われるため、CLI では data range を core range に変換する。

例:

```sh
rulemorph -i users.xlsx -rule "@input.id" \
  --excel-header-row 1 \
  --excel-data-range A2:C100
```

生成する input:

```json
{
  "format": "excel",
  "excel": {
    "has_header": true,
    "header_row": 1,
    "data_start_row": 2,
    "range": "A1:C100"
  }
}
```

sheet 指定:

- `--excel-sheet Users` -> `"sheet": "Users"`
- `--excel-sheet-index 0` -> `"sheet": 0`
- 両方指定された場合は CLI parse / validation error
- どちらもない場合は core default の最初の sheet

### Excel data range 変換

CLI は `A2:C100` 形式を受ける。列だけの `A:C` は data start row が決まらないため direct mode では拒否する。

変換:

1. data range を start column / start row / end column / end row に parse する。
2. `header_row` が 1-based positive であることを確認する。
3. `header_row < data_start_row` であることを確認する。
4. core range を `<start_col><header_row>:<end_col><end_row>` にする。
5. `data_start_row` に data range の start row を入れる。

`--excel-data-range A2:C100 --excel-header-row 1` は core range `A1:C100` に変換される。

`--excel-data-range B5:F20 --excel-header-row 4` は core range `B4:F20` に変換される。

`--excel-data-range A1:C100 --excel-header-row 1` は header row と data start row が同じなので拒否する。

## direct output unwrap

既存 direct mode は JSON object input だけ単一出力を unwrap し、array input は配列を維持する。

`--rule` は既存 direct value output として扱う。CSV / Excel direct convenience mode では、tabular input の結果を以下のようにする。

- 0 records -> `[]`
- 1 record -> direct value に unwrap
- 2 records 以上 -> direct value の配列

convenience mode とは、以下のいずれかに該当する場合を指す。

- `-f` なしで CSV / Excel と推定された場合
- `--headers` を指定した CSV direct input
- `--excel-*` を指定した Excel direct input
- `-f excel` を指定した Excel direct input

既存互換のため、`-f csv` だけを指定し、新しい tabular option を指定しない direct mode は legacy CSV mode として扱い、現行どおり単一 record でも配列を維持する。

例:

```sh
echo "a,test,1" | rulemorph -rule "@input.0"
# => "a"

printf "a,test,1\nb,demo,2\n" | rulemorph -rule "@input.0"
# => ["a","b"]

printf "id\na\n" | rulemorph -f csv -rule "@input.id"
# => ["a"]
```

`-F/--field` と `--output-map` は record object output として扱う。この場合、synthetic target `__rulemorph_direct_value` は使わず、通常 mappings の出力 object をそのまま返す。

convenience mode では以下を返す。

- 0 records -> `[]`
- 1 record -> object
- 2 records 以上 -> object array

legacy CSV mode、つまり `-f csv` だけを指定し、新しい tabular input option を指定しない direct mode では、`-F/--field` / `--output-map` でも単一 record を unwrap せず object array を維持する。これは `--rule` の legacy CSV presentation と揃え、出力 shape を安定させたい場合の逃げ道にする。

例:

```sh
printf "u1,Alice,42\nu2,Bob,30\n" | rulemorph -H "id,name,age" \
  -F id="@input.id" \
  -F name="@input.name"
# => [{"id":"u1","name":"Alice"},{"id":"u2","name":"Bob"}]

printf "id,name\nu1,Alice\n" | rulemorph -f csv \
  -F id="@input.id" \
  -F name="@input.name"
# => [{"id":"u1","name":"Alice"}]
```

JSON array input に対する `-F/--field` / `--output-map` も record object array を返す。JSON object input は 1 record として object を返す。

この unwrap は direct mode の presentation layer のみで行い、core transform result は変更しない。

## エラー設計

CLI option validation のエラーは exit code `2` とする。

互換性維持のため、既存 `--rule` inline expr parse failure は現行 direct mode と同じ exit code `1` を維持する。一方、`-F/--field` と `--output-map` の output spec validation、format-specific option mismatch、Excel data range validation は exit code `2` とする。

代表的なメッセージ:

- `exactly one of --rule, --output-map, or --field is required`
- `--field must be TARGET=EXPR`
- `--field target must not be blank`
- `--field expr must not be blank`
- `--output-map must be a JSON object`
- `--output-map must define at least one field`
- `direct output target is duplicated`
- `direct output target conflicts with another target`
- `direct output has too many fields`
- `direct output spec must be at most ... bytes`
- `direct output target path is invalid`
- `direct output target path must not include indexes`
- `direct output target names must be at most ... bytes each`
- `direct output target names total size must be at most ... bytes`
- `direct output target path exceeds configured depth limit`
- `direct output target paths have too many tokens`
- `direct output cells must be at most ...`
- `direct output expr exceeds configured depth limit`
- `direct output expr has too many nodes`
- `direct output expr string values must be at most ... bytes each`
- `--field expr looks like JSON but failed to parse`
- `--output-map does not support @out references; use --field for ordered mappings`
- `--headers must not contain blank field names`
- `--headers must be unique`
- `--headers requires CSV direct input`
- `--headers has too many fields`
- `--headers field names must be at most ... bytes each`
- `--headers total size must be at most ... bytes`
- `--excel-data-range is required for Excel direct input`
- `--excel-header-row is required for Excel direct input`
- `--excel-data-range is the data range and must start after --excel-header-row; use A2:C100 when --excel-header-row is 1`
- `--excel-* options require Excel direct input`
- `--excel-sheet and --excel-sheet-index cannot be used together`
- `failed to infer CSV columns: input has no records`
- `failed to infer CSV columns: ...`

core normalization / transform のエラーは既存どおり `emit_transform_error` を通す。JSON 形式に見える stdin input が parse に失敗した場合は、形式推定後に JSON normalization が失敗するため、既存の transform error として exit code `3` で返す。`-f csv` を明示すれば CSV として処理できることは docs に明記する。

context file の read / parse error は既存 `transform -c` と同じく exit code `1` とする。代表メッセージは `failed to read context: ...` / `failed to parse context JSON: ...` を維持する。

## 実装手順

1. direct mode 専用 option 型を追加する。
   - `DirectInputFormat`: `Json` / `Csv` / `Excel`
   - `DirectInputOptions`: headers / excel data range / header row / sheet
   - `DirectOutputSpec`: rule / fields / output-map
   - `DirectArgs`: context path を追加する。
   - top-level `Cli` に direct-only options を追加する。

2. `has_direct_options` を更新する。
   - 新 direct-only options が subcommand 前に置かれた場合、`direct-mode options require --rule, --output-map, or --field and cannot be used before a subcommand` のように direct output spec を要求する message で拒否する。
   - `--output-map` / `-F/--field` / `-c/--context` も direct mode 判定に含める。
   - top-level dispatch は `rule: Option<String>` だけでなく `DirectOutputSpec` の有無で direct mode に入る。`-F` または `--output-map` 単独でも direct mode として処理し、direct-only option だけで output spec がない場合は help fallback ではなく validation error にする。

3. direct output spec parser を追加する。
   - `direct.rs` 本体が入力検出 / 実行 / presentation を持ち、`direct/output_spec.rs` が output spec の parsing / validation / evaluated-ref scan を持つ構成にする。
   - `--rule` / `--output-map` / `-F` の排他 validation
   - `-F TARGET=EXPR` parser
   - `--output-map` strict JSON object parser
   - target parse / duplicate / parent-child conflict validation
   - output spec / target byte / target path depth / target token total / output cell / expr の direct mode 専用 resource guard
   - `--output-map` の評価される `@out` 参照拒否
   - `scan_evaluated_refs(expr) -> { input_numeric_refs, out_refs }` のような helper を作り、CSV numeric inference と `--output-map` の `@out` rejection で共有する。raw JSON string の再帰走査には戻さない。
   - `--rule` は既存 inline expr parser を再利用する。
   - `-F` RHS は inline expr semantics に寄せつつ、`{` / `[` で始まる JSON-looking value の strict parse failure は literal fallback せず error にする。
   - `--output-map` value は strict JSON parse 済みの `serde_json::Value` をそのまま expr value として使う。

4. direct rule builder を output spec 対応にする。
   - `--rule` は `__rulemorph_direct_value` target を生成する。
   - `-F` / `--output-map` は通常 mappings を生成する。
   - `DirectOutputMode::Value` / `DirectOutputMode::RecordObject` を分け、unwrap に渡す。

5. direct mode context loading を追加する。
   - top-level `-c/--context` を `DirectArgs` に渡す。
   - 既存 `load_context` を再利用する。
   - `transform_input_with_warnings_with_base_dir_and_options` に `context_value.as_ref()` を渡す。
   - context 未指定時は `None` を維持する。
   - 現在は direct mode 専用の context size limit / strict JSON duplicate-key rejection は追加しない。将来 hardening は `load_context` の共有 contract 変更として direct / transform の両方に適用する。

6. format 推定を `direct.rs` に寄せる。
   - 明示 `-f`
   - `-i -` は stdin 扱い
   - `-i` extension
   - unknown extension / no extension は JSON default
   - stdin leading-token classification
   - JSON-looking stdin input は parse failure で fail-closed
   - CSV / Excel 専用 option と resolved format の不一致は fail-closed

7. CSV rule config builder を追加する。
   - `--headers` parser
   - `MAX_DIRECT_TABULAR_FIELDS` guard
   - `MAX_DIRECT_TABULAR_HEADER_BYTES` / `MAX_DIRECT_TABULAR_HEADER_BYTES_TOTAL` guard
   - `Expr` / `V2Expr` semantics に沿った root numeric field reference detector
   - headerless numeric columns inference
   - core と同じ CSV parser semantics で先頭 record を読む

8. Excel rule config builder を追加する。
   - Excel direct input required options validation
   - data range parser
   - core range conversion
   - sheet / sheet index serialization

9. direct output unwrap を tabular input と object output spec に拡張する。
   - JSON object は既存どおり unwrap
   - CSV / Excel convenience mode は transform output record 数で unwrap
   - legacy `-f csv` は単一 record でも配列を維持
   - `-F` / `--output-map` は 1 record object、multi record object array を返す。

10. docs を更新する。
   - `README.md`
   - `docs/rules_spec_ja.md`
   - `docs/rules_spec_en.md`
   - `-F/--field`、`--output-map`、direct `-c/--context`、`--output-map` での `@out` 禁止、literal array の正式形、JSON/CSV/Excel の output shape を同期する。

## テスト計画

Rust 実装を変更するため、最終確認は以下を実行する。

```sh
cargo fmt
cargo test
```

追加する CLI integration tests:

- stdin headerless CSV:
  - `echo "a,test,1" | rulemorph -rule "@input.0"` -> `"a"`
  - `echo "a,test,1" | rulemorph -F id=@input.0` -> `{"id":"a"}`
  - `echo "a,test,1" | rulemorph --output-map '{"id":"@input.0"}'` -> `{"id":"a"}`
  - v1 ref expr `{"ref":"input.0"}` also triggers headerless inference
  - `@input.01` does not trigger headerless inference and remains a normal key ref
- stdin CSV with explicit headers:
  - `echo "a,test,1" | rulemorph -H "id,name,age" -rule "@input.id"` -> `"a"`
- `-i` CSV with explicit headers:
  - headerless temp file + `-H` -> `"a"`
- `-i` CSV with file header:
  - header temp file without `-H` -> `"a"`
- multiple CSV rows:
  - output is array
- legacy explicit `-f csv`:
  - `printf "id\na\n" | rulemorph -f csv -rule "@input.id"` -> `["a"]`
- unknown extension / no extension `-i`:
  - `-i data.txt` without `-f` uses JSON default
  - `-i data` without `-f` uses JSON default
- `-i -`:
  - uses stdin leading-token classification
- JSON-looking malformed stdin:
  - returns transform error exit code `3`
- invalid `--headers`:
  - blank name
  - duplicate name
  - too many names
  - oversized name / total header bytes
  - spaces are significant: `-H "id, name"` does not trim the second field
  - quoted comma is parsed as one header name
- format-specific option mismatch:
  - `--headers` with JSON / Excel is rejected
  - `--excel-*` with JSON / CSV is rejected
- headerless CSV inference with too many fields:
  - excessive comma count is rejected before synthetic rule generation
- malformed CSV headerless inference:
  - CSV reader が先頭 record を読めない入力は synthetic rule generation 前に拒否する
- `.xlsx` direct input with header row / data range:
  - existing `t34_excel_input/input.xlsx` fixture with a single-row data range returns scalar
  - existing `t34_excel_input/input.xlsx` fixture with a multi-row data range returns array
  - stdin Excel bytes require explicit `-f excel`
  - sheet selection by name works
  - sheet selection by index works
  - offset data range such as `B5:F20` converts to the expected core range
  - column-only range such as `A:C` is rejected
  - reversed range such as `C2:A10` or `A10:C2` is rejected
- direct context:
  - `--rule '@context.tenant_id' -c context.json` -> context value
  - `-F tenant=@context.tenant_id -c context.json` -> object with context value
  - `--output-map '{"tenant":"@context.tenant_id"}' -c context.json` -> object with context value
  - `@context.*` does not trigger headerless CSV numeric inference
  - invalid context JSON returns existing `failed to parse context JSON:` error and exit code `1`
  - missing context file returns existing `failed to read context:` error and exit code `1`
  - duplicate-key context JSON follows existing `load_context` behavior and observes the same value as `transform -c`
  - context size limit is not added in direct mode only
  - `-c/--context` before a subcommand without direct output spec is rejected as a direct-only option placement error
- `-F/--field` output:
  - single CSV row + `-F id=@input.id -F name=@input.name` -> object
  - multiple CSV rows + repeated `-F` -> object array
  - JSON object input + `-F` -> object
  - JSON array input + `-F` -> object array
  - pipe expr RHS: `-F name='["@input.name","trim","uppercase"]'`
  - JSON-looking malformed RHS such as `-F name='["@input.name","trim"'` is rejected instead of emitted as a literal string
  - literal string starting with `{` or `[` is represented through `lit:` and is not rejected
  - literal string escape: `-F label=lit:@input.id` -> `"@input.id"`
  - ordered `@out` dependency: second `-F` can read first `-F`
  - nested target path: `-F user.id=@input.id`
  - bracket-quoted target path: `-F 'user["full.name"]=@input.name'`
  - target containing `=` is represented through `--output-map`, not `-F`
  - explicit `-f csv` without new tabular input option returns object array even for one record
- `--output-map` output:
  - target-to-expr map with scalar refs -> object
  - target-to-expr map with pipe array -> object
  - JSON object input + `--output-map` -> object
  - JSON array input + `--output-map` -> object array
  - nested output via target path key: `"user.id"`
  - bracket-quoted target path key: `"user[\"full.name\"]"`
  - plain object value is literal expr, not nested template:
    `--output-map '{"user":{"id":"@input.id"}}'` outputs `{"user":{"id":"@input.id"}}`
  - expr-shaped object values follow existing expr contract:
    `{"ref":"input.id"}` and `{"op":"uppercase","args":[{"ref":"input.name"}]}` are evaluated as expr shapes, not plain literal objects
  - expr-shaped object literal collision is documented as unsupported in direct sugar
  - literal array uses existing pipe-start form:
    `--output-map '{"tags":[["a","b"]]}'` outputs `{"tags":["a","b"]}`
  - evaluated `@out` reference is rejected in `--output-map`
  - evaluated `@out` rejection covers v1 `{"ref":"out.x"}` and v2 nested pipe / condition / map argument positions
  - evaluated `@context` reference is allowed in `--output-map`
  - literal object containing `"@out.x"` as plain string is not rejected as an evaluated ref
  - literal object containing `"@input.0"` as plain string does not trigger headerless numeric inference
- direct output spec conflicts:
  - `--rule` + `-F` is rejected
  - `--rule` + `--output-map` is rejected
  - `--output-map` + `-F` is rejected
  - no `--rule` / `--output-map` / `-F` before direct-only input options is rejected
- invalid `-F/--field`:
  - missing `=`
  - blank target
  - blank expr
  - duplicate canonical target
  - parent-child target conflict
  - array index target
  - too many fields
  - oversized output spec bytes
  - oversized target / total target bytes
  - excessive target path depth / target token total
  - excessive direct output cells
  - excessive expr depth / nodes / string bytes
- invalid `--output-map`:
  - invalid JSON
  - non-object JSON
  - empty object
  - duplicate key rejected by strict JSON parser
  - duplicate canonical target
  - parent-child target conflict
  - array index target
  - too many fields
  - oversized output spec bytes
  - oversized target / total target bytes
  - excessive target path depth / target token total
  - excessive direct output cells
  - excessive expr depth / nodes / string bytes
- Excel missing required option:
  - missing `--excel-data-range`
  - missing `--excel-header-row`
- Excel invalid range/header combination:
  - header row is same as data start row
- Excel invalid range parser inputs:
  - huge column letters / row numbers are rejected without panic
- `--excel-sheet` and `--excel-sheet-index` conflict
- direct-only options before subcommand are rejected

## 互換性

- 既存 direct JSON behavior は維持する。
- 既存 `--rule` behavior は維持する。`--output-map` / `-F` は新しい output spec として追加する。
- 既存 `-f csv` direct behavior は維持する。`-f csv` だけを指定し、新しい tabular option を指定しない場合は、1 row でも配列を出す。
- `--output-map` は既存 `mappings` の target / expr contract に寄せる。key は `target`、value は `expr` であり、再帰的 JSON template ではない。
- plain string は既存 expr と同じく literal string として扱う。`@...` や `$` を literal string にしたい場合は既存 v2 expr の `lit:` を使う。`$literal` / `$expr` のような direct mode 専用 wrapper は追加しない。
- direct mode の `-c/--context` は既存 `transform -c/--context` と同じ JSON file 読み込みを使う。context の parse behavior や error message は既存 helper に揃え、direct mode 専用の strict JSON parser は導入しない。
- direct mode の `-c/--context` に direct 専用 size limit は追加しない。context hardening が必要になった場合は、既存 `transform -c` との挙動差を作らず、共有 `load_context` の contract として導入する。
- `-F` RHS だけは typo が output value に silent fallback しやすいため、`{` / `[` で始まる JSON-looking malformed value を新規 error として扱う。`--rule` の既存 inline expr 互換は維持する。
- `@input.age|int` のような CLI 独自ショートハンドは追加しない。
- `-f` なし stdin で JSON 形式に見えない入力は、従来の JSON parse error ではなく CSV direct convenience mode として処理される。これは今回の受け入れ条件に含める。
- `-i` の未知拡張子 / 拡張子なしは既存互換のため JSON default を維持する。`-i -` は stdin と同じ判定を使う。
- `transform` / `preflight` の `-f` は変更しないため、rule file based workflow への影響はない。
- raw output は追加しないため、string output は JSON string のまま。

## リスクと対策

### stdin auto-detect が JSON typo を CSV と誤認する

JSON parse の成否で形式判定すると、JSON typo や limit 超過を CSV として処理してしまう可能性がある。また、判定目的の全体 parse が本処理とは別の CPU / memory 消費を作る。

対策:

- stdin auto-detect は全体 parse ではなく、BOM / whitespace 後の先頭バイトだけを見る。
- `{` または `[` で始まる入力は JSON として扱い、parse failure 時は CSV fallback せず error にする。
- CSV が `{` または `[` で始まる場合は `-f csv` を明示する。

### headerless CSV inference が巨大な synthetic rule を作る

headerless CSV の numeric field 推定では、先頭 record の field 数から `columns` を生成する。大量 delimiter を含む 1 行入力を許すと、巨大な synthetic rule / `columns` vector を作る可能性がある。

対策:

- `--headers` の header 数と inferred field 数に direct mode 専用上限 `MAX_DIRECT_TABULAR_FIELDS` を適用する。
- header 名 1 個あたりの byte 数と header 名合計 byte 数にも direct mode 専用上限を適用する。
- 初期値は `MAX_DIRECT_TABULAR_FIELDS = 10_000`、`MAX_DIRECT_TABULAR_HEADER_BYTES = 256 KiB`、`MAX_DIRECT_TABULAR_HEADER_BYTES_TOTAL = 8 MiB` とし、超過時は transform 前の CLI validation error とする。
- テストに excessive field count、oversized header、malformed first CSV record の拒否を追加する。

### `-F` / `--output-map` が巨大な synthetic mappings を作る

大量の `-F` や巨大な output-map key を許すと、core validation 前に synthetic rule JSON / mappings vector が肥大化する。

対策:

- output field 数に `MAX_DIRECT_OUTPUT_FIELDS = 10_000` を適用する。
- output spec 全体に `MAX_DIRECT_OUTPUT_SPEC_BYTES = 8 MiB` を適用する。
- target 名 1 個あたり `MAX_DIRECT_OUTPUT_TARGET_BYTES = 256 KiB`、target 名合計 `MAX_DIRECT_OUTPUT_TARGET_BYTES_TOTAL = 8 MiB` を適用する。
- target path 1 個あたり `MAX_DIRECT_OUTPUT_TARGET_DEPTH = 256`、target path token 合計 `MAX_DIRECT_OUTPUT_TARGET_TOKENS_TOTAL = 1_000_000` を適用する。
- direct output cell 数に `MAX_DIRECT_OUTPUT_CELLS = 10_000_000` を適用する。これは `effective_max_records * output_field_count` で見積もる。
- expr JSON に `MAX_DIRECT_OUTPUT_EXPR_DEPTH` / `MAX_DIRECT_OUTPUT_EXPR_NODES` / `MAX_DIRECT_OUTPUT_EXPR_STRING_BYTES` を適用する。
- 超過時は transform 前の CLI validation error とする。
- output byte 数は実データ依存で事前見積もりしづらいため、現在は byte 専用の新上限は追加しない。必要になった場合は shared `max_output_bytes` として設計する。
- テストに excessive output fields、oversized spec、oversized target、total target bytes、excessive target depth / token total、excessive output cells、excessive expr shape の拒否を追加する。

### context JSON の size / duplicate key は既存挙動に合わせる

一般的なローカル CLI では、ユーザーが明示した file の大きさを必ず application-level limit で拒否するとは限らない。`rulemorph` direct mode でも、context file はユーザーが `-c/--context` で明示する local file であり、現在は既存 `transform -c` と同じ `load_context` behavior を使う。

対策:

- direct mode 専用の context size limit / duplicate-key rejection は追加しない。
- この判断は「ローカル CLI で本人が実行する」前提では主に robustness / UX の問題であり、ただちに高 severity の脆弱性とは扱わない。
- CI、server、MCP、Web UI などで第三者が context path または context content を制御できる場合は DoS / parser ambiguity risk になりうるため、運用側では OS / runner の memory / file size 制限を併用する。
- 将来 hardening する場合は direct mode だけでなく `transform -c` も含め、共有 `load_context` に `MAX_CONTEXT_BYTES` と strict JSON duplicate-key rejection を追加する。

### `-i` / `-o` / `-c` path を未信頼入力として渡す

ローカル CLI では `-i` / `-o` / `-c` が local file path を読む / 書くのは期待どおりである。一方、CI、server、MCP、Web UI などの wrapper が第三者入力をそのまま path option に渡すと、任意 local file read や意図しない output overwrite の surface になる。

対策:

- stdin の内容は file path として扱わない。`echo non_header.csv | rulemorph ...` はあくまで文字列 input であり、file read しない。
- 未信頼ユーザーに path option を直接渡させる運用は scope 外とする。
- wrapper 経由で使う場合は stdin / temporary sandbox file / allowlist path に限定する。
- output path も sandbox 内に制限し、既存 file 上書きの扱いは呼び出し側 policy で決める。

### JSON-looking な `-F` RHS typo が literal string に fallback する

既存 inline expr parser と完全に同じ fallback を `-F` RHS に適用すると、`["@input.age","int"` のような壊れた JSON array が literal string として出力され、変換漏れに気づきにくい。

対策:

- `-F/--field` の RHS は BOM / whitespace 後の先頭 byte が `{` または `[` の場合、strict JSON parse failure を CLI validation error とする。
- `--rule` は既存互換のため現行 inline expr parser behavior を維持する。
- `{` または `[` で始まる literal string は `lit:{...` / `lit:[...]` と書く。
- テストに malformed JSON-looking RHS の拒否と `lit:` による literal escape を追加する。

### headerless numeric detection が literal 文字列を ref と誤認する

numeric field reference detection が raw JSON string を再帰走査すると、plain literal object 内の `"@input.0"` まで評価される ref と誤認し、header あり CSV を headerless として扱う可能性がある。

対策:

- detection は `Expr` / `V2Expr` semantics に寄せ、評価される参照だけを見る。
- plain literal object の内側にある ref 風文字列は検出対象外にする。
- 実際の rule parse / eval は既存 parser/evaluator に任せる。
- detection 対象は `@input.0` 系と v1 `input.0` 系に限定し、曖昧な構文には踏み込まない。

### Excel CLI range と core range の意味が違う

CLI の `--excel-data-range` は data range、core の `input.excel.range` は selected cell window である。

対策:

- CLI builder で必ず core range に変換する。
- docs では direct mode の `--excel-data-range` を data range と明記する。

### `--output-map` が nested template と誤読される

`--output-map '{"user":{"id":"@input.id"}}'` を `{ "user": { "id": "u1" } }` と期待する可能性がある。しかし、既存構文に寄せるなら `--output-map` の key は target、value は expr であり、plain object value は object literal expr である。

対策:

- docs / examples では nested output を `"user.id": "@input.id"` と書く。
- `--output-map` は「JSON template」ではなく「target-to-expr map」と説明する。
- 再帰テンプレート展開は今回入れない。

### `--output-map` で `@out` の順序依存が発生する

既存 `mappings` の `@out` は「前に評価済みの output」だけを参照できる。`--output-map` は JSON object の target-to-expr map であり、object key order を評価順として意味づけると、parser / serializer / 将来実装の差で挙動が揺れる。

対策:

- `--output-map` value 内の評価される `@out` 参照は拒否する。
- 順序依存が必要な場合は `-F/--field` を使い、CLI 指定順を mappings order として維持する。
- plain literal object 内の `"@out.x"` は評価される ref ではないため拒否対象外とする。

### literal array と pipe array が衝突する

既存 v2 expr では top-level array は pipe として解釈される。そのため `--output-map '{"tags":["a","b"]}'` は literal array ではなく pipe expr になる。

対策:

- 今回は literal array 専用 sugar を追加しない。
- array literal が必要な場合は、既存 v2 expr の正式形として `{"tags":[["a","b"]]}` のように literal array を pipe start に置く。
- 将来より読みやすい記法を入れる場合は、CLI 独自 wrapper より先に core expr 側の literal array 表現を設計する。

### target 表記ゆれで duplicate / conflict が見えづらい

`user.name` と `user["name"]` は同じ target だが、単純な文字列比較では重複を検出できない。また `user` と `user.name` は実行順で挙動が変わる。

対策:

- CLI output spec parser で target を `PathToken` に正規化して比較する。
- duplicate canonical target はエラーにする。
- 親子 target conflict もエラーにする。
- array index target は既存 mapping と同じく拒否する。

## 将来拡張

- `--raw-output` による raw string output
- `--delimiter` による CSV delimiter 指定
- headerless Excel direct input
- `@input["0"]` を含めた numeric field detector 拡張
- `transform` / `preflight` への input option override
- `--output-map` の再帰テンプレート展開
- literal array を安全に表す expr / output-map 記法
- `@input.age|int` などの CLI shorthand
