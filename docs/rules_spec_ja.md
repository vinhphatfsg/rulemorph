# 変換ルール仕様（実装準拠）

このドキュメントは v2 のルール仕様、参照、式構文、評価ルールを説明します。
英語版は `docs/rules_spec_en.md` を参照してください。

`endpoint`/`network` ルールの仕様は別ドキュメントに分離しています。
- `docs/rules_spec_endpoint_ja.md`
- `docs/rules_spec_network_ja.md`

## この仕様の読み方

Rulemorph の通常変換（`normal` ルール）は、入力をまず **JSON record の配列**に正規化し、その各 record に対して出力 JSON を組み立てます。

```text
raw input
  -> input normalization
  -> records: [JSON object, ...]
  -> mappings または steps
  -> output records
  -> finalize
  -> JSON output
```

初めてルールを書く場合は、以下の順で読むと理解しやすくなります。

1. `input` で入力形式と record の切り出し方を決める
2. `mappings` で出力フィールドを作る
3. `expr` と `conditions` で値の加工や条件分岐を足す
4. 複数段階の処理が必要になったら `steps` を使う
5. 出力配列全体を加工したい場合だけ `finalize` を使う

## 仕様の全体像

| 領域 | 役割 | 主なキー |
| --- | --- | --- |
| 入力正規化 | CSV/JSON/YAML/TOML/XML/HTML/Excel を JSON record にそろえる | `input` |
| レコード単位の除外 | record を処理するか決める | `record_when` |
| 出力生成 | 1 record から 1 output object を作る | `mappings` |
| 段階実行 | mapping、filter、assert、branch を順序付きで実行する | `steps` |
| 再利用可能な式 | v2 pipe / mappings に名前を付けて再利用する | `defs` |
| 配列全体の後処理 | filter/sort/limit/wrap を出力配列へ適用する | `finalize` |
| 参照と式 | 入力、context、途中出力を参照し、値を加工する | `@input`, `@context`, `@out`, `expr` |

`mappings` と `steps` はどちらも出力を作るための構文です。単純な変換では `mappings` を使い、途中で assert や branch を挟みたい場合に `steps` を使います。

## Semantic trace API

core library は opt-in の semantic trace API を提供します。

- `transform_input_with_trace(...)`
- `transform_input_with_trace_with_base_dir_and_options(...)`
- `transform_record_with_trace(...)`

trace は正規化済み record から `record_when`、`mappings` / `steps`、`expr`、operator、branch、`finalize` までの評価イベントを `TransformTrace` として返します。通常の `transform` / `transform_input` / `transform_record` の出力や warning は trace 有効化で変わってはいけません。

`TransformTraceOptions::default()` は `Raw` mode です。trace API は値を観測する目的で明示的に呼び出す API なので、既定では input / output / context 由来の raw 値を snapshot に含めます。raw trace を保存、ログ出力、network 送信、`postMessage`、共有 URL へ出す処理は core には含めません。

値の扱いは `TraceValueMode` で選択します。

| mode | 挙動 | 主な用途 |
| --- | --- | --- |
| `Raw` | raw 値を snapshot に含める | ローカル UI の透明性表示、デバッグ |
| `Redacted` | secret-like path や上限超過値を値なし snapshot に落とす | 人間確認用の安全寄り表示 |
| `MetadataOnly` | 値を持たず state/type/bytes などのメタデータだけ返す | export / share / network / postMessage の既定 |

`Redacted` の `redaction_reason` は secret-like path では `"secret_like_path"` を使います。object / array の composite snapshot は初期実装では metadata-only として扱います。

各 value snapshot は `state` と `type` を持ち、`missing` / `null` / empty string を区別します。`default` は `missing` の場合だけ適用されます。`null` fallback を表現したい場合は `default` ではなく `coalesce` を使います。

trace event の `input_path` / `output_path` は canonical form を使います。入力側は `@input.*`、`@item.*`、`@acc.*`、`@context.*`、`@out.*`、出力側は `$.*` です。bracket notation は `@input["@id"]`、`@input[0].name`、`@item[0].name`、`$.items[0].name` のように保持されます。

trace は `max_events` / `max_trace_bytes` / `max_snapshot_bytes` で明示的に制限できます。event stream が打ち切られた場合は `complete=false` と `truncation.reason` が設定されます。`max_snapshot_bytes` は snapshot value のみを落とす制限であり、event の親子構造は維持されます。

trace API の error は `TransformTraceError` として partial trace を返します。ただし `Debug` / `Display` / `std::error::Error` は raw trace 本体や underlying `TransformError` の full message を出しません。外部 surface に trace を渡す adapter は `MetadataOnly` を既定にし、`contains_raw_values == false` を確認してください。

WASM binding follow-up の想定 shape は `wasm_transform_trace({ rule, input, inputFormat?, traceMode? })` です。`traceMode` は `"raw" | "redacted" | "metadata_only"` とし、local animation は `raw`、export / share / `postMessage` は `metadata_only` を既定にします。

## ルールファイル構成

```yaml
version: 2
input:
  format: json
  json:
    records_path: "items"

record_when:
  all:
    - { gt: ["@input.score", 10] }
    - { eq: ["@input.active", true] }

mappings:
  - target: "user.id"
    source: "id"
    type: "string"
    required: true
  - target: "user.name"
    expr:
      - "@input.name"
      - trim
  - target: "meta.source"
    value: "api"
```

- `version`（必須）: `2` 固定
- `input`（必須）: 入力形式とオプション
- `mappings`（必須）: 変換ルール（上から順に評価）
- `defs`（任意）: v2 pipe / mappings を名前付き関数OPとして定義
- `output`（任意）: メタデータ（DTO 名など）
- `record_when`（任意）: レコードの採用/除外条件
- `steps`（任意）: 段階実行（`mappings` / `record_when` と併用不可）
- `finalize`（任意）: 出力配列の最終加工（`mappings` / `steps` どちらでも利用可）

### DTO 型推論

`generate_dto` は `mapping.type` がある場合、その明示型を最優先します。
`mapping.type` がない場合は、literal value、v2 pipe の終端 op、object/array 操作から
`string` / `int` / `float` / `bool` / array / map / nested object を静的に推測します。
推測できない dynamic reference、v1 expr、互換性のない union は各言語の JSON fallback 型
（Rust `serde_json::Value`、TypeScript `unknown` など）として出力します。

推論は untrusted rule input に対して bounded に実行されます。過度に深い object、巨大な array、
大量 field、または生成 type 数の上限を超える shape は、狭い型にせず JSON fallback 型へ戻します。
巨大な path や dynamic path を含む `get` / `pick` / `omit` も JSON fallback 型へ戻します。
`default` / `coalesce` は dynamic/unknown input を具体型へ狭める根拠には使いません。

`optional` は field が省略される可能性、`nullable` は field 値が `null` になり得る可能性を表します。
JSON integer literal が signed 64-bit integer に収まらない場合は、Rust / Go / JVM 系 DTO の `i64` / `int64` / `Long` で安全に表現できないため JSON fallback 型として扱います。

`defs` の `returns` は DTO 推論にも使われます。`returns` が object の場合は nested DTO shape として伝播します。`mappings` body の関数OPで `returns` を省略した場合は、body の `target` から object shape を合成します。推論できない field は JSON fallback 型になります。

## defs（関数OP）

`defs` は rule file 内だけで使える名前付きOPです。外部コード、IO、network、time/random などの side effect は実行せず、既存の v2 pipe / mappings に型付きの名前を付ける機能です。

```yaml
defs:
  slug:
    input: string
    returns: string
    expr:
      - "$"
      - trim
      - lowercase

mappings:
  - target: slug
    expr:
      - "@input.title"
      - slug
```

各定義は `input` を必須にし、`expr` または `mappings` のどちらか一方を持ちます。`expr` body では `returns` が必須です。`mappings` body では `returns` を省略でき、その場合は `target` から object return contract を合成します。

### 型

`defs.*.input` と `defs.*.returns` では次の型を使えます。

| 型 | 説明 |
| --- | --- |
| `string` | JSON string |
| `int` | JSON integer |
| `float` | finite JSON number。integer も受け入れる |
| `number` | integer / float を区別しない finite JSON number |
| `bool` | JSON boolean |
| `json` | 任意 JSON。内側の shape check はしない |
| `[T]` | homogeneous array |
| `{ field: T }` | object field map |

object field は `?` で optional と nullable を区別します。

```yaml
input:
  {
    name: string,
    nickname?: string,
    note: string?,
    memo?: string?
  }
```

canonical form も使えます。

```yaml
input:
  {
    nickname: { type: string, optional: true },
    note: { type: string, nullable: true },
    memo: { type: string, optional: true, nullable: true }
  }
```

`{ type: string }` だけの object は canonical form ではなく、`type` という field を持つ object 型として扱われます。canonical form として解釈されるのは `optional` または `nullable` を含む場合です。

object input の direct call は required field が揃っていれば extra field を許可します。`with` adapter と object output contract は exact match です。`json` field の内側だけは shape check しません。数値型は文字列を暗黙 parse しないため、`"2"` は `int` / `float` / `number` としては通りません。

### 呼び出し

引数なしの custom OP は current pipe value `$` をそのまま受け取ります。

```yaml
expr:
  - "@input.title"
  - slug
```

呼び出し方の使い分けは次の通りです。

| 呼び出し方 | 使う場面 | 例 |
| --- | --- | --- |
| `- slug` | `input` が primitive、または current pipe の object をそのまま渡す | `["@input.title", slug]` |
| `- line_total: [{ with: ... }]` | caller 側の field 名や shape を `defs.*.input` に合わせる。公式推奨の引数付き形式 | `with: { qty: "$.quantity" }` |
| `value` wrapper | `$.field` のような文字列を参照ではなく literal として渡す | `{ value: "$.quantity" }` |
| `expr` wrapper | object の中で値が expr であることを明示する | `{ expr: "$.quantity" }` |

custom OP call が pipe の最初の要素になる場合、外側から渡された current pipe がないことがあります。`input` が current pipe を必要とする場合は、先に明示的な start value を置くか、`with` で必要な field を渡してください。

入力元の field 名が `input` shape と違う場合は、公式推奨形として `with` adapter を使います。

```yaml
expr:
  - "@input.line"
  - line_total:
      - with: { qty: "$.quantity", unit_price: "$.price" }
```

custom OP body で input object の field を読む場合は、公式例では dot path 形式の `$.field` を推奨します。`get` OP を使う `["$", { get: ["field"] }]` 形式も有効ですが、field access だけなら `$.field` のほうが短く読みやすいです。

```yaml
defs:
  line_total:
    input: { qty: int, unit_price: number }
    returns: number
    expr:
      - "$"
      - let:
          qty: ["$.qty", float]
          price: ["$.unit_price", float]
      - "@qty"
      - "*": ["@price"]
```

`with` の値は caller scope の v2 expr として評価されます。文字列を literal として渡したい場合は `value`、明示的に expr として渡したい場合は `expr` wrapper を使います。

```yaml
- decorate:
    - with:
        label:
          value: "$.quantity"
        qty:
          expr: "$.quantity"
```

direct adapter object は無効です。

```yaml
# invalid
- line_total:
    qty: "$.quantity"
```

custom OP body の `$` と `@input` は custom OP input を指します。outer `@input` は暗黙 capture できません。`@context` capture、再帰、built-in OP の shadowing、import、generic、overload は MVP では無効です。

object を返す関数OPでは `returns` に object 型を書けます。`expr` body なら返す値がその object contract と一致する必要があります。

```yaml
defs:
  public_line:
    input: { sku: string, qty: int, secret?: string }
    returns: { sku: string, qty: int }
    expr:
      - "$"
      - pick: ["sku", "qty"]
```

computed field を持つ object を返す場合は `mappings` body が読みやすいです。`mappings` body では `returns` を省略でき、`target` から object return contract が合成されます。

```yaml
defs:
  line_summary:
    input: { qty: int, unit_price: number }
    mappings:
      - target: qty
        expr: "$.qty"
      - target: total
        expr: ["$.unit_price", { "*": ["$.qty"] }]
```

### 検証と trace

validation は unknown custom OP、built-in shadowing、invalid identifier、`expr` / `mappings` の重複、`returns` missing、cycle、unknown/duplicate call option、`with` shape mismatch を fail-closed にします。runtime でも `input` / `returns` contract を検査し、contract error は raw input value を既定では message に含めません。

semantic trace では custom OP call が `kind=custom_op` の span として出ます。span には `name`、`def_path`、`call_path`、`input_type`、`output_type`、`with_adapter`、`body_truncated` が入ります。値 snapshot は既存の `TraceValueMode` に従います。
`with` args は body より先に caller scope で評価され、body の `expr` / `mappings` は custom OP span の子として展開されます。body 内で error が起きた場合も custom OP span の内側に記録されます。`MetadataOnly` / `Redacted` mode でも、この span/event 構造は保ち、raw value だけを出さないようにします。

## Input

`input` は raw input を Rulemorph の共通データモデルである JSON record 配列へ変換します。
以降の `mappings` / `steps` / `finalize` は、元のファイル形式ではなく、正規化後の JSON record に対して動作します。

### 共通
- `input.format`（必須）: `csv` / `json` / `yaml` / `toml` / `xml` / `html` / `excel`

| format | record の切り出し方 | 主な用途 |
| --- | --- | --- |
| `json` | root または `records_path` | API response、JSON export |
| `csv` | 行ごとに record | CSV import、業務データ |
| `yaml` | root または `records_path` | 設定ファイル、YAML export |
| `toml` | root または `records_path` | dependency / inventory data |
| `xml` | `records_path` で element を選択 | XML feed、legacy API |
| `html` | `records_selector` と field selector | table や list からの抽出 |
| `excel` | sheet の行を record | `.xlsx` import |

### 入力正規化の共通契約

- record は JSON object として扱います。
- `records_path` が配列を指す場合は、各要素が record になります。
- `records_path` が object を指す場合は、単一 record として扱います。
- scalar を record として扱うことはできません。
- 参照先が存在しない場合は `missing` として扱います。`null` とは区別されます。
- `mappings` / `steps` / `finalize` は、入力形式ごとの差異を直接扱いません。差異は parser 層で JSON record に正規化されます。

安全性 invariant として、JSON/YAML の duplicate key、XML DTD/entity/processing instruction、HTML の JavaScript 実行/URL 取得、Excel macro/external relationship/formula evaluation は許可されません。これらは CLI の resource limit override では緩和できません。

### CSV
- `input.csv` は `format=csv` のとき必須

| option | 必須 | 既定 | 説明 |
| --- | --- | --- | --- |
| `has_header` | 任意 | `true` | `true` では最初の行を header として使います。`false` では `columns` が field name になります。 |
| `delimiter` | 任意 | `","` | CSV delimiter。1 byte 文字のみ指定できます。`"||"` や全角カンマのような multi-byte delimiter は無効です。 |
| `columns` | `has_header=false` のとき必須 | なし | header なし CSV の field 定義。各要素は `name` 必須、`type` 任意です。 |

- header / `columns[].name` は空文字不可、重複不可です。
- `has_header=true` の場合、先頭 header の UTF-8 BOM は取り除かれます。
- `columns[].type` は受け取れますが、現行の validation / normalization では使いません。CSV cell はすべて string として JSON record に入ります。
- 各 data row の field 数は header / `columns` の数と一致する必要があります。

```yaml
input:
  format: csv
  csv:
    has_header: false
    delimiter: ","
    columns:
      - { name: "id" }
      - { name: "price", type: "float" }
```

### JSON
- `input.json` は `format=json` のとき必須

| option | 必須 | 既定 | 説明 |
| --- | --- | --- | --- |
| `records_path` | 任意 | ルート | レコード配列または単一 record object を指すドットパス。 |

- root / `records_path` が配列の場合、各要素を record として扱います。
- root / `records_path` が object の場合、単一 record として扱います。
- scalar は record として扱えません。
- `records_path` は通常の Rulemorph path と同じ構文です。存在しない path、または scalar を指す path はエラーです。
- duplicate key と non-finite number は拒否されます。

```yaml
input:
  format: json
  json:
    records_path: "items"
```

### YAML / TOML
- `input.yaml` / `input.toml` は対応する `format` のとき必須

| option | 必須 | 既定 | 説明 |
| --- | --- | --- | --- |
| `records_path` | 任意 | ルート | レコード配列または単一 record object を指すドットパス。 |

- YAML/TOML は JSON record に正規化されてから mapping / steps / finalize に渡されます。
- `records_path` は通常の Rulemorph path と同じ構文です。存在しない path、または scalar を指す path はエラーです。
- YAML stream は 1 document のみ受け付けます。duplicate key、string 以外の mapping key、custom tag は拒否されます。
- YAML alias / anchor は展開できますが、alias 数と展開後 node 数は resource limit の対象です。
- TOML datetime は string に正規化されます。TOML table / array-of-tables / inline table は JSON object / array に正規化されます。

```yaml
input:
  format: yaml
  yaml:
    records_path: "users"
```

```yaml
input:
  format: toml
  toml:
    records_path: "users"
```

### XML
- `input.xml` は `format=xml` のとき必須

| option | 必須 | 既定 | 説明 |
| --- | --- | --- | --- |
| `records_path` | 必須 | なし | root element を含むドット区切り element path。`[]` index は使えません。 |
| `attr_prefix` | 任意 | `"@"` | attribute field 名の prefix。空文字不可。 |
| `text_key` | 任意 | `"#text"` | direct text を格納する field 名。空文字不可。`attr_prefix` と同一不可。 |
| `child_policy` | 任意 | `array` | child element の格納形式。現行は `array` のみ有効で、子要素は常に配列になります。 |
| `trim_text` | 任意 | `true` | direct text / CDATA の前後空白を取り除きます。 |
| `collapse_whitespace` | 任意 | `true` | direct text / CDATA 内の連続空白を 1 space に畳みます。 |
| `namespaces` | 任意 | `qualified` | `qualified` は prefix 付き名を保持します。`strip` は local name のみにします。 |

- attribute は `attr_prefix + attribute_name` の string field になります。
- direct text / CDATA は正規化後に空でなければ `text_key` に入ります。
- child element は名前ごとに配列 field になります。単一 child でも配列です。
- mixed content では child element の text は child 側に入り、record element の direct text は `text_key` に入ります。
- `namespaces: strip` で local name が衝突する場合や、attribute / text / child の key が衝突する場合はエラーです。
- XML は single root document のみ受け付けます。DTD、processing instruction、root 外の非空 text は拒否されます。

```yaml
input:
  format: xml
  xml:
    records_path: "users.user"
    attr_prefix: "@"
    text_key: "#text"
    child_policy: array
    namespaces: strip
```

### HTML
- `input.html` は `format=html` のとき必須

| option | 必須 | 既定 | 説明 |
| --- | --- | --- | --- |
| `records_selector` | 必須 | なし | record element を選択する CSS selector。空文字不可。 |
| `fields` | 必須 | なし | 出力 field 名から field 抽出設定への map。空 map 不可。 |
| `trim_text` | 任意 | `true` | `value: text` / `attr` の前後空白を取り除きます。 |
| `collapse_whitespace` | 任意 | `true` | `value: text` / `attr` の連続空白を 1 space に畳みます。 |

`fields.<name>`:

| option | 必須 | 既定 | 説明 |
| --- | --- | --- | --- |
| `selector` | 任意 | record element 自身 | record element からの相対 CSS selector。省略時は record element 自身を対象にします。 |
| `value` | 任意 | `text` | `text` / `html` / `attr`。 |
| `attr` | `value=attr` のとき必須 | なし | 抽出する attribute 名。空文字不可。 |
| `multiple` | 任意 | `false` | `true` では一致した要素の値を array として返します。 |

- field 名は空文字不可です。
- `multiple=false` では最初に一致した element だけを使います。一致しない場合、その field は `missing` です。
- `multiple=true` では一致した値だけを配列に入れます。一致しない場合は空配列です。`value=attr` で attribute がない element は配列に入りません。
- `value: html` は raw inner HTML string です。空白の trim / collapse は行わず、sanitize も実行もしません。
- HTML parser は JavaScript 実行や URL 取得を行いません。

```yaml
input:
  format: html
  html:
    records_selector: "table#users tbody tr"
    fields:
      id: { selector: "td:nth-child(1)", value: text }
      name: { selector: "td:nth-child(2)", value: text }
      profile_url: { selector: "a.profile", value: attr, attr: href }
      tags: { selector: ".tag", value: text, multiple: true }
```

### Excel
- `input.excel` は `format=excel` のとき必須

| option | 必須 | 既定 | 説明 |
| --- | --- | --- | --- |
| `sheet` | 任意 | 最初の sheet | sheet 名 string、または 0-based sheet index。 |
| `has_header` | 任意 | `true` | `true` では `header_row` の cell を field name にします。`false` では `columns` が field name と対象列を決めます。 |
| `header_row` | 任意 | `1` | header 行番号。1-based。`has_header=true` のとき使います。 |
| `data_start_row` | 任意 | header ありなら `header_row + 1`、header なしなら `range` の開始行または 1 行目 | data row の開始行番号。1-based。 |
| `range` | 任意 | sheet の有効範囲 | 対象 cell 範囲。`A:D` または `A1:D100` 形式。 |
| `columns` | `has_header=false` のとき必須 | なし | header なし Excel の field 定義。各要素は `name` と `column` が必須です。 |
| `empty_cell` | 任意 | `missing` | 現行は `missing` のみ有効。空 cell は field から省略されます。 |
| `formula` | 任意 | `cached` | `cached` / `formula` / `error`。formula cell は計算しません。 |
| `date` | 任意 | `iso8601` | `iso8601` / `serial` / `string`。Excel datetime cell の正規化方法。 |
| `cell_error` | 任意 | `error` | 現行は `error` のみ有効。Excel error cell は拒否されます。 |

- `range` の列指定は Excel column letter です。`A:D` は列だけを絞り、`B3:F100` は行列の window を絞ります。
- `header_row` は `range` の開始行より前にできません。`range: "B3:F100"` で header を使う場合は通常 `header_row: 3` を指定します。
- `has_header=true` では選択列の header cell が field name になります。header は空文字不可、重複不可です。
- `has_header=false` の `columns[].column` は Excel column letter です。`columns[].name` は空文字不可、重複不可です。
- 空 cell は `missing` として扱います。空文字列が実値として存在する場合は `""` として扱い、`missing` とは区別します。
- formula cell は計算しません。`formula: cached` では cached value を読み、cached value がない formula cell はエラーです。`formula: formula` では式文字列を値として読み、`formula: error` では formula cell を拒否します。
- `date: iso8601` と `date: string` は `YYYY-MM-DDTHH:MM:SS` string、`date: serial` は Excel serial number を JSON number として出力します。
- `.xlsx` のみ対応。macro、external relationship、shared formula metadata、ambiguous workbook structure は拒否されます。

```yaml
input:
  format: excel
  excel:
    sheet: "Users"
    has_header: true
    header_row: 1
```

```yaml
input:
  format: excel
  excel:
    sheet: 0
    has_header: true
    header_row: 3
    data_start_row: 4
    range: "B3:F100"
```

```yaml
input:
  format: excel
  excel:
    sheet: "Users"
    has_header: false
    data_start_row: 2
    columns:
      - { name: "id", column: "A" }
      - { name: "name", column: "B" }
```

## Output

- 出力は JSON 配列が既定
- CLI `transform --ndjson` は 1 行 1 JSON（ストリーミング）
- `records_path` がオブジェクトを指す場合は単一レコード

## Resource limits

CLI は大きなローカル入力向けに有限の resource limit を緩和できます。

```sh
rulemorph transform -r rules.yaml -i huge.csv --limit records=500000 --limit input-bytes=536870912
rulemorph transform -r rules.yaml -i trusted.json --limit range-items=50000
rulemorph transform -r rules.yaml -i trusted.json --limit range-items=unlimited
rulemorph transform -r rules.yaml -i huge.csv --limits-profile large
rulemorph transform -r rules.yaml -i workbook.xlsx --limits-file limits.toml
```

`range` OP の生成数は既定で 10,000 要素までです。`range-items=<integer>` で上限を変更できます。`range-items=unlimited` は trusted なローカル入力/ルール向けに `range` 単体の上限制約を外します。`--limits-file` で指定する場合は `range-items = "unlimited"` のように文字列で書きます。`range-items=unlimited` の場合でも、`range`/`map`/`flat_map`/`flatten` などが生成する配列の総量は `array-len` で制限されます。

これらは処理量の上限を広げるだけです。duplicate key rejection、XML DTD/entity rejection、HTML no-network/no-JS、Excel no-macro/no-formula-evaluation、MCP pathless branch guard などの安全性 invariant は変更できません。

## Record filter（`record_when`）

`record_when` は各レコードに対してマッピング前に 1 回評価されます。
`false` の場合はそのレコードを出力しません。
評価に失敗した場合もスキップされ、警告が出ます。

- `record_when` は `when`/`if.cond` と同じ条件構文を使用
- `@input.*` と `@context.*` を参照可能
- `@out.*` は出力が未生成のため利用不可
- `steps` を使う場合は `record_when` ステップを使用し、トップレベルの `record_when` は使用しません

## Mapping

各 mapping は `target` に 1 つの値を書き込みます。

```yaml
- target: "user.id"
  source: "id"
  type: "string"
  required: true
```

項目:
- `target`（必須）: 出力 JSON のドットパス（配列インデックスは不可）
- `source` | `value` | `expr`（必須・排他）
  - `source`: 参照パス（Reference 参照）
  - `value`: JSON リテラル
  - `expr`: v2 パイプ式
- `when`（任意）: 条件。`false` または評価エラーでスキップ（警告）
- `type`（任意）: `string|int|float|bool`
- `required`（任意）: 既定 `false`
- `default`（任意）: 値が `missing` のときのみ使用

### `when` の挙動
- `when` は mapping の先頭で評価
- `false` または評価エラーで mapping をスキップ（エラーは警告扱い）
- スキップ時は `required/default/type` を評価しない
- `missing` は `false` 扱い

### `required`/`default` の挙動
- 値が `missing` の場合、`default` があれば使用
- 値が `missing` かつ `required=true` はエラー
- `null` は `missing` ではない。`required=true` ならエラー、それ以外は `null` を保持

### `target` の制約
- `target` はオブジェクトキーのみ（配列インデックス不可）
- 中間パスがオブジェクトでない場合はエラー

## Steps（段階実行）

`steps` は **段階的に評価順を制御**するための構文です。
`steps` が存在する場合、`mappings` / `record_when` は **併用不可**です。

```yaml
version: 2
input: { format: json, json: { records_path: "items" } }

steps:
  - mappings:
      - target: "total"
        expr: ["@input.a", { "+": ["@input.b"] }]
  - record_when:
      gt: ["@out.total", 0]
  - asserts:
      - when: { gt: ["@out.total", 10] }
        error:
          code: "INVALID_TOTAL"
          message: "total must be > 10"
  - branch:
      when: { eq: ["@input.type", "premium"] }
      then: ./rules/premium.yaml
      else: ./rules/basic.yaml
      return: true

finalize:
  sort: { by: "total", order: "desc" }
```

### ステップ要素
各ステップは **1つのキー**のみを持ちます（`name` は例外）。

| 要素 | 説明 |
| --- | --- |
| `mappings` | v2 mappings と同じ構文 |
| `record_when` | v2 条件。false ならレコードを除外 |
| `asserts` | 条件バリデーション（false でエラー） |
| `branch` | 条件分岐 |

### データフロー
- `@input` は **元の入力レコード**（固定）
- `@out` は **累積出力**（ステップごとにマージされる）
- `mappings` の結果は `@out` に上書きマージされます

### record_when（steps 内）
- `record_when` が false の場合、そのレコードは出力されません
- 評価エラーはステップエラーとして扱われます

### asserts
`asserts` は配列で記述します。`when` が false の場合にエラーを発生させ、
以降のステップは実行されません。

```yaml
asserts:
  - when: { gt: ["@input.age", 0] }
    error:
      code: "INVALID_AGE"
      message: "age must be > 0"
```

### branch
`branch` は条件により他のルールに分岐します。
`then` / `else` は **rule 参照のみ**（inline は不可）。

- `return: true` の場合、分岐先の出力で終了（以後の steps は実行しない）
- `return: false`（省略）なら、分岐先の出力を `@out` にマージして続行
- 分岐先ルールの `@input` は **現在の `@out`** が渡されます

```yaml
branch:
  when: { eq: ["@input.type", "premium"] }
  then: ./rules/premium.yaml
  else: ./rules/basic.yaml
  return: true
```

## Finalize（出力配列の最終加工）

`finalize` は **すべてのレコード処理後**に出力配列へ適用されます。
`mappings` / `steps` どちらでも利用可能です。
`finalize` は **複数の処理を併用可能**で、上から順に適用されます。
`finalize` は **ストリーミング出力では利用できません**（エラー）。

### 対応要素（MVP）
- `filter`: v2 条件（`@item` を参照）
- `sort`: 並び替え
- `limit` / `offset`: ページング
- `wrap`: 出力をオブジェクトで包む

### filter
`filter` は v2 条件で、`@item` が現在の要素を指します。

```yaml
finalize:
  filter:
    eq: ["@item.status", "active"]
```

### sort

```yaml
finalize:
  sort:
    by: "created_at"
    order: "desc"  # asc | desc
```

### limit / offset

```yaml
finalize:
  limit: 10
  offset: 20
```

### wrap
`wrap` は **v2 expr** で値を定義します。
`@out` は現在の出力配列を表します。
配列は **v2 expr（pipe）として解釈**されるため、
リテラル配列を直接書く用途には向きません（MVPでは未対応）。

```yaml
finalize:
  wrap:
    data: "@out"
    meta:
      total:
        - "@out"
        - len
```

### 併用例（MVP）
`sort` と `limit` を組み合わせて上位N件を返せます。

```yaml
finalize:
  sort:
    by: "score"
    order: "desc"
  limit: 5
```

## Reference

参照は `@` 付きの名前空間 + ドットパスです。
- `@input.*`: 入力レコード
- `@context.*`: 外部コンテキスト
- `@out.*`: 同一レコード内で先に生成された出力
- `@input`: 入力レコード全体
- `@context`: 外部コンテキスト全体
- `@out`: 現在の出力全体
- `@item.*`: `map` ステップ内の現在要素（`@item.index` は 0 始まり）
- `@item`: `map` ステップ内の現在要素（全体）
- `@acc.*`: `reduce`/`fold` ステップ内の累積値
- `@acc`: `reduce`/`fold` ステップ内の累積値（全体）
- `@<var>`: `let` で束縛した変数（例: `@total`）

全体参照は `@item`/`@acc`（ドットなし）です。`@item.` や `@acc.` の末尾ドットはエラーになります。

`source` は **単一キーのみ** 省略可（`input.*` が既定）。
ドットパスや配列インデックスを使う場合は `input.*` を明示してください。

例:
- `source: "id"` は `input.id`
- `source: "input.user.name"`
- `source: "input.items[0].id"`
- `source: "context.tenant_id"`
- `expr: "@out.text"`

### ドットパス
- 配列インデックス対応: `input.items[0].id`, `context.matrix[1][0]`
- ドットを含むキーは括弧付きクオート: `input.user["profile.name"]`
- 括弧内では `\\` とクオート（`\"` / `\'`）のみ許可
- 括弧内に `[` `]` は不可
- 非配列や範囲外インデックスは `missing`

## Expr（v2 パイプ）

`expr` はパイプ配列、または単一の開始値です。

```yaml
expr:
  - "@input.name"
  - trim
  - uppercase
```

単一参照の例:

```yaml
expr: "@input.name"
```

### パイプ形式

パイプは `[start, step1, step2, ...]` の配列です。

開始値は以下を利用できます:
- `@` 参照（`@input.*`, `@context.*`, `@out.*`, `@item.*`, `@var`）
- `$`（現在のパイプ値）
- 文字列/数値/真偽値/null/オブジェクト/配列のリテラル

`@` や `$` をリテラル文字列として扱うには `lit:` を使います。

```yaml
expr:
  - "lit:@input.name"
  - trim
```

### ステップ

- **Op ステップ**: 文字列の op 名、またはオブジェクト形式
  - `{ op: "trim", args: [...] }`
  - `{ concat: ["@out.name"] }`（短縮形）
- **Let ステップ**: `{ let: { varName: <expr>, ... } }`
- **If ステップ**: `{ if: <cond>, then: <pipe>, else: <pipe?> }`
- **Map ステップ**: `{ map: [ <step>, <step>, ... ] }`

`let` と `if` の例:

```yaml
expr:
  - "@input.price"
  - let: { base: "$" }
  - if:
      cond:
        gt: ["@base", 100]
      then:
        - "$"
        - multiply: [0.9]
      else:
        - "$"
```

`map` の例:

```yaml
expr:
  - "@input.items"
  - map:
    - "@item.value"
    - multiply: [2]
```

## Conditions（条件構文）

`record_when`、`when`、`if.cond` で使用します。

対応形式:
- `all: [ <cond>, ... ]`
- `any: [ <cond>, ... ]`
- 比較オブジェクト: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `match`

比較の挙動（v2）:
- `eq`/`ne` は **JSON の厳密一致**（型も含めて比較）です。例: `"1"` と `1` は一致しません。
- `gt`/`gte`/`lt`/`lte` はまず数値比較（数値 or 数値文字列）を試みます。両方が非数値文字列の場合は字句順比較（Rust の `str` の順序: UTF-8 バイト順 / Unicode code point 順）を行い、それ以外はエラーになります。

例:

```yaml
record_when:
  all:
    - { gt: ["@input.score", 10] }
    - { eq: ["@input.active", true] }

when:
  match: ["@input.email", ".+@example\\.com$"]
```

## オペレーション一覧（v2）

オペレーションはパイプのステップとして適用されます。
現在のパイプ値が暗黙の第 1 引数です。
`args` は追加の引数のみを列挙します。

対応状況:
- `runtime`: v2 実行時に実装済み

### カテゴリ

- 文字列系: `concat`, `to_string`, `trim`, `lowercase`, `uppercase`, `replace`, `split`, `pad_start`, `pad_end`
- JSON 操作: `merge`, `deep_merge`, `get`, `pick`, `omit`, `keys`, `values`, `entries`, `len`, `from_entries`, `object_flatten`, `object_unflatten`
- 配列 op: `map`, `filter`, `flat_map`, `flatten`, `take`, `drop`, `slice`, `chunk`, `zip`, `zip_with`, `unzip`, `group_by`, `key_by`, `partition`, `unique`, `distinct_by`, `sort_by`, `find`, `find_index`, `index_of`, `contains`, `sum`, `avg`, `min`, `max`, `reduce`, `fold`, `first`, `last`
- 数値系: `+` / `add`, `-` / `subtract`, `*` / `multiply`, `/` / `divide`, `round`, `abs`, `floor`, `ceil`, `trunc`, `sqrt`, `sign`, `mod`, `pow`, `clamp`, `range`, `to_base`, `sum`, `avg`, `min`, `max`
- 日付系: `date_format`, `to_unixtime`
- 論理演算: `and`, `or`, `not`
- 比較演算: `==`, `!=`, `<`, `<=`, `>`, `>=`, `~=`（エイリアス: `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `match`）
- 型変換: `string`, `int`, `float`, `bool`

### 命名規則

- `to_*`: 変換系（`to_string`, `to_base`, `to_unixtime`）
- `*_by`: キー指定の派生（`group_by`, `key_by`, `distinct_by`, `sort_by`）
- `object_*`: object 構造専用（`object_flatten`, `object_unflatten`）

### コアオペレーション

OP は current pipe value を暗黙の第 1 引数として受け取るものが中心です。追加引数が不要な OP は文字列で書けます。追加引数がある OP は object 形式で書きます。

```yaml
expr:
  - "@input.name"
  - trim
  - lowercase
  - replace: [" ", "-", "all"]
```

数値 OP も同じ pipe 形式で使えます。`range` だけは値を変換する OP ではなく配列生成 OPなので、pipe-first shorthand ではなく explicit form を使います。

| やりたいこと | 書き方 | 結果 |
| --- | --- | --- |
| 絶対値 | `[-7.5, abs]` | `7.5` |
| 平方根を整数境界にする | `[81, sqrt, floor]` | `9` |
| べき乗 | `[2, { pow: 8 }]` | `256` |
| Euclidean 剰余 | `[-5, { mod: 3 }]` | `1` |
| 範囲内に丸める | `["@input.score", { clamp: [0, 100] }]` | `0..100` の値 |
| 昇順 range | `[{ range: [2, 8] }]` | `[2,3,4,5,6,7]` |
| 降順 range | `[{ range: [8, 2] }]` | `[8,7,6,5,4,3]` |

| op | args | 説明 | 対応 |
| --- | --- | --- | --- |
| `concat` | `>=1` | 文字列連結（パイプ値 + args）。 | `runtime` |
| `coalesce` | `>=1` | pipe + args から最初の非 null を返す。 | `runtime` |
| `to_string` | `0` | 文字列化。 | `runtime` |
| `trim` | `0` | 先頭/末尾の空白を除去。 | `runtime` |
| `lowercase` | `0` | 小文字化。 | `runtime` |
| `uppercase` | `0` | 大文字化。 | `runtime` |
| `replace` | `2-3` | 文字列置換（`pattern`, `replacement`, `mode?`）。 | `runtime` |
| `split` | `1` | 区切り文字で分割。 | `runtime` |
| `pad_start` | `1-2` | 指定長まで先頭を埋める（`length`, `pad?`）。 | `runtime` |
| `pad_end` | `1-2` | 指定長まで末尾を埋める（`length`, `pad?`）。 | `runtime` |
| `lookup` | `2-4` | 配列から全一致を取得。 | `runtime` |
| `lookup_first` | `2-4` | 配列から最初の一致を取得。 | `runtime` |
| `+` / `add` | `>=1` | 数値加算。 | `runtime` |
| `-` / `subtract` | `>=1` | 数値減算（pipe - arg）。 | `runtime` |
| `*` / `multiply` | `>=1` | 数値乗算。 | `runtime` |
| `/` / `divide` | `>=1` | 数値除算。 | `runtime` |
| `round` | `0-1` | 数値を丸める（`scale`）。 | `runtime` |
| `abs` | `0` | 絶対値を返す。 | `runtime` |
| `floor` | `0` | 小数点以下を負の無限大方向へ丸める。 | `runtime` |
| `ceil` | `0` | 小数点以下を正の無限大方向へ丸める。 | `runtime` |
| `trunc` | `0` | 小数点以下を 0 方向へ切り捨てる。 | `runtime` |
| `sqrt` | `0` | 平方根を返す。負数はエラー。 | `runtime` |
| `sign` | `0` | 負数は `-1`、ゼロは `0`、正数は `1` を返す。 | `runtime` |
| `mod` | `1` | Euclidean 剰余を返す。除数 `0` はエラー。 | `runtime` |
| `pow` | `1` | べき乗を返す。結果が有限でない場合はエラー。 | `runtime` |
| `clamp` | `2` | 値を `min..max` に収める。`min > max` はエラー。 | `runtime` |
| `range` | `2-3` | 整数列を生成する（`start`, `end`, `step?`。`end` は排他）。 | `runtime` |
| `to_base` | `1` | 整数を指定進数の文字列に変換（2-36）。 | `runtime` |
| `date_format` | `1-3` | 日時文字列をフォーマット変換。 | `runtime` |
| `to_unixtime` | `0-2` | 日時文字列を unix time へ。 | `runtime` |
| `and` | `>=1` | boolean AND。条件は `all` を推奨。 | `runtime` |
| `or` | `>=1` | boolean OR。条件は `any` を推奨。 | `runtime` |
| `not` | `0` | boolean NOT。 | `runtime` |
| `==` | `1` | 等価比較。条件は `eq` を推奨。 | `runtime` |
| `!=` | `1` | 非等価比較。条件は `ne` を推奨。 | `runtime` |
| `<` | `1` | 数値比較。条件は `lt` を推奨。 | `runtime` |
| `<=` | `1` | 数値比較。条件は `lte` を推奨。 | `runtime` |
| `>` | `1` | 数値比較。条件は `gt` を推奨。 | `runtime` |
| `>=` | `1` | 数値比較。条件は `gte` を推奨。 | `runtime` |
| `~=` | `1` | 正規表現マッチ。条件は `match` を推奨。 | `runtime` |

`range` は pipe-first ではなく explicit form で使います。現在のパイプ値を境界値に使う場合は `$` を明示してください。

```yaml
expr:
  - "@input.n"
  - sqrt
  - floor
  - { "+": 1 }
  - range: [2, "$"]
```

`range: [start, end, step?]` は `end` 排他です。`start` / `end` / `step` は integer である必要があります。`step` 省略時は `start < end` なら `1`、`start > end` なら `-1` です。`start == end`、または向きと `step` が合わない場合は空配列を返します。`step: 0` はエラーです。既定では 10,000 要素まで生成し、CLI では `--limit range-items=...` で変更できます。`range-items=unlimited` でも、生成配列の総量は `array-len`（既定 1,000,000）に従います。

### JSON 操作

パス引数:
- `pick`/`omit` はパス文字列を複数引数で指定できます。
- 1 つの引数で文字列配列（例: `@context.paths`）も指定可能です。

例:

```yaml
- pick:
  - "name"
  - "price"
```

| op | args | 説明 | 対応 |
| --- | --- | --- | --- |
| `merge` | `>=1` | 浅い merge（右勝ち）。 | `runtime` |
| `deep_merge` | `>=1` | object は再帰 merge、配列は置換。 | `runtime` |
| `get` | `1` | パスの値を取得。存在しない場合は `missing`。 | `runtime` |
| `pick` | `>=1` | 指定パスのみ残す。 | `runtime` |
| `omit` | `>=1` | 指定パスを削除する。 | `runtime` |
| `keys` | `0` | キーの配列。 | `runtime` |
| `values` | `0` | 値の配列。 | `runtime` |
| `entries` | `0` | `{key, value}` の配列。 | `runtime` |
| `len` | `0` | string/array/object の長さを返す。 | `runtime` |
| `from_entries` | `>=1` | ペア配列や key/value から object を生成。 | `runtime` |
| `object_flatten` | `1` | オブジェクトを path キーで平坦化。 | `runtime` |
| `object_unflatten` | `1` | path キーからオブジェクトを再構成。 | `runtime` |

### 配列オペレーション

述語式の注意:
- `filter` / `partition` / `find` / `find_index` は v2 の式（boolean を返すパイプ式）を受け取ります。
- 条件オブジェクトではありません。`@item` と比較 op（`==`, `!=`, `>`, `>=`, `<`, `<=`, `~=`）を使います。
- 比較 op のエイリアス（`eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `match`）も利用できます。
- 述語が `missing` / `null` の場合は false 扱いで、boolean 以外はエラーになります。

例:

```yaml
- filter:
  - ["@item", {"!=": null}]
```

別例（partition）:

```yaml
- partition:
  - ["@item.price", {">": 80}]
```

| op | args | 説明 | 対応 |
| --- | --- | --- | --- |
| `map` | `1` | 要素を変換する（`map` ステップ推奨）。 | `runtime` |
| `filter` | `1` | 条件に一致した要素を残す。 | `runtime` |
| `flat_map` | `1` | `map` + `flatten(1)`。 | `runtime` |
| `flatten` | `0-1` | 指定深さで平坦化する。 | `runtime` |
| `take` | `1` | 先頭/末尾から取得する。 | `runtime` |
| `drop` | `1` | 先頭/末尾から除外する。 | `runtime` |
| `slice` | `1-2` | 範囲抽出（`end` は排他）。 | `runtime` |
| `chunk` | `1` | 固定サイズで分割する。 | `runtime` |
| `zip` | `>=1` | 最短の配列長で束ねる。 | `runtime` |
| `zip_with` | `>=2` | 要素ごとに式で合成する。 | `runtime` |
| `unzip` | `0` | 配列の配列を列配列に変換する。 | `runtime` |
| `group_by` | `1` | キーでグルーピングする。 | `runtime` |
| `key_by` | `1` | キーで map 化する（重複は後勝ち）。 | `runtime` |
| `partition` | `1` | 条件で 2 配列に分割する。 | `runtime` |
| `unique` | `0` | 等価な要素を除去する。 | `runtime` |
| `distinct_by` | `1` | キーで重複を除去する。 | `runtime` |
| `sort_by` | `1` | キーでソートする。 | `runtime` |
| `find` | `1` | 最初の一致要素を返す。 | `runtime` |
| `find_index` | `1` | 最初の一致インデックスを返す。 | `runtime` |
| `index_of` | `1` | 最初の一致インデックスを返す。 | `runtime` |
| `contains` | `1` | 含まれているかを返す。 | `runtime` |
| `sum` | `0` | 合計値を返す。 | `runtime` |
| `avg` | `0` | 平均値を返す。 | `runtime` |
| `min` | `0` | 最小値を返す。 | `runtime` |
| `max` | `0` | 最大値を返す。 | `runtime` |
| `reduce` | `1` | 累積式で縮約する。 | `runtime` |
| `fold` | `2` | 初期値付きで縮約する。 | `runtime` |
| `first` | `0` | 先頭要素を返す。 | `runtime` |
| `last` | `0` | 末尾要素を返す。 | `runtime` |

### 型変換

| op | args | 説明 | 対応 |
| --- | --- | --- | --- |
| `string` | `0` | 文字列に変換。 | `runtime` |
| `int` | `0` | 整数に変換。 | `runtime` |
| `float` | `0` | 浮動小数点に変換。 | `runtime` |
| `bool` | `0` | 真偽値に変換。 | `runtime` |

### Lookup の引数

`from` を明示する場合:

```yaml
expr:
  - lookup_first:
    - "@context.users"
    - id
    - "@input.user_id"
    - name
```

`from` を省略する場合（パイプ値を配列として利用）:

```yaml
expr:
  - "@context.users"
  - lookup_first:
    - id
    - "@input.user_id"
    - name
```

## 評価ルール（補足）

### missing と null
- `missing`: 参照先が存在しない
- `null`: 参照先が存在し値が null

### パイプ評価
- パイプは左から右へ評価
- `@out.*` は同一レコード内の既存出力のみ参照可能

### Map ステップ
- パイプ値が `missing` の場合は `missing` を返す
- パイプ値が配列以外ならエラー
- `map` は `missing` の結果を配列に含めない

### Lookup
- `lookup`/`lookup_first` の `from` は配列必須
- `match_key` と `get` は文字列
- `lookup` は配列を返し、`lookup_first` は最初の一致を返す（未一致は `missing`）

## 実行時の挙動

- `record_when` はマッピング前に評価し、`false` またはエラーでスキップ
- `mappings` は上から順に評価し、`@out.*` は前の出力のみ参照可
- `source/value/expr` が `missing` の場合は `default/required` を適用
- `type` は式評価後に適用され、失敗はエラー
- `when` の評価エラーは警告として扱う

## Preflight 検証

`preflight` は実入力を使って事前に実行時エラーを検出します。
入力解析と評価ルールは `transform` と同じです。
