# LLM出力の明示正規化と最終JSON Schema検証

`rulemorph_output`ライブラリは、LLM出力などのJSONを保守的な構文修復候補、明示したルールによる型・構造正規化、最終値のJSON Schema検証の順に処理します。成功payloadは完全な最終値を検証済みの場合だけ返します。Schema適合は元の意味の保存を証明しません。

既存のMCP `validate_rules` / core `validate_rule_file`は**ルールの検証**です。新しい`validate_output(value, schema)`は**最終値の検証**であり、用途を区別してください。既存CLI/stdio MCP/Sites HTTP版のツールは追加・変更していません。新ライブラリは現時点でworkspaceのpath dependencyとして利用できます。

## API

- `validate_output(&Value, &Value) -> Result<(), OutputError>`: 最終値全体を検証します。
- `normalize_to_schema(RawInput, rules_text, &schema) -> Result<ValidatedOutput, OutputError>`: strict JSONのみを受け付ける既定APIです。
- `normalize_to_schema_with_repair(..., RepairMode::MissingCommas)`: 明示opt-inで欠落カンマ候補を受け入れます。
- `repair_json_candidate(raw_input) -> Result<RepairCandidate, OutputError>`: 構文候補と修復位置を返します。これはschema検証済みの成功payloadではありません。

`RawInput.completion`は`Complete`、`Truncated`、`Unknown`を指定します。呼び出し側がproviderの終了状態を伝え、`Complete`以外は有効なJSON文字列であっても拒否します。JSONの見た目だけでは、完全な返答か、必要な内容が途中で途切れたかを判断できません。LLM再生成・再試行ループは呼び出し側の責務です。provider依存や認証APIはありません。

`ValidatedOutput.value()`は検証済みの値を不変参照で返し、`repairs()`がtraceを返します。`into_value()`後に値を変更した場合は、変更後の値を再検証してください。関数はストレージやネットワークにアクセスせず、raw input/outputをログに記録しません。

## object listからstring listへ明示したtextを抽出する

LLMが`[{"text":"first","title":"other"},{"text":"第二"}]`を返しても、`title`や`description`を勝手に採用しません。抽出先はルールで指定します。

```yaml
version: 2
input:
  format: json
  json: {}
mappings:
  - target: text
    source: text
    type: string
    required: true
finalize:
  wrap: ['@out', {map: ['@item.text']}]
```

```rust
use rulemorph_output::{normalize_to_schema, RawInput, InputCompletion};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
let rules = "version: 2\ninput: {format: json, json: {}}\nmappings:\n - target: text\n   source: text\n   type: string\n   required: true\nfinalize:\n wrap: ['@out', {map: ['@item.text']}]\n";
let schema = json!({"type":"array", "items":{"type":"string"}});
let result = normalize_to_schema(
    RawInput { text: r#"[{"text":"first"},{"text":"第二"}]"#, completion: InputCompletion::Complete },
    rules,
    &schema,
)?;
assert_eq!(result.value(), &json!(["first", "第二"]));
Ok(()) }
```

`validate_output`は解析済みのValueを受け取るため、呼び出し側が先に重複keyを失う解析をした場合は検出できません。raw JSONにはpipelineまたはcoreのstrict parserを使ってください。

すでに正常なstring listがある場合は、`validate_output`だけで確認できます。正規化時は既存coreのJSONレコード仕様を使うため、mappingsの入力はobject recordsです。

数値文字列を整数にするには`type: int`を明示します。`required: true`の値が欠損、null、不正型なら失敗です。Schemaに合わせてnullや0を自動創作しません。ルールで明示した`value`/`default`はユーザーが指定した値として扱います。変換warningが出た場合も成功payloadを返しません。

この正規化APIはJSON入力、最大64の直接mappings（source/value/type/default/required）、および上記の必須targetの最終投影だけに対応します。defs/codecs/steps/record_when、mapping expr/when、別ファイルbranch、任意のfinalize式、filter/sort/limit/offsetは拒否します。入力内容の削除や切詰めを成功に見せません。既存coreの全演算が必要な場合は、呼び出し側でcoreのtransformを行い、完成した値を`validate_output`で検証する方法もあります。

## 安全な修復範囲

修復候補は**JSONの空白で区切られた、すでに完全な値やフィールド間のカンマ欠落**だけです。例：`["a" "b"]` → `["a" ,"b"]`、`{"a":1 "b":2}` → `{"a":1 ,"b":2}`。既存の文字列、値、key、空白は変えません。

traceは元入力のUTF-8 **byte offset**と挿入した`,`を記録します。Unicodeやescaped quoteを保持し、後の変換/schema失敗でもtraceを返します。成功値・候補本文・traceを外部保存する場合の取り扱いは呼び出し側の責任です。

引用符の欠落や境界が曖昧なもの、`["a""b"]`、括弧の不一致、閉じ括弧欠落、colon欠落、trailing comma、コードフェンス、説明文混入は自動修復しません。閉じ括弧や引用符をEOFで補完すると、生成が途中で切れた不完全な内容を成功扱いする恐れがあるため、確認または再生成が必要な失敗として返します。重複keyは元JSON・修復後とも拒否します。Schemaを見て複数候補から意味を推測して選びません。

## JSON Schemaの方針

成熟した[Rust jsonschema](https://docs.rs/jsonschema/0.58.3/jsonschema/) 0.58.3を、HTTP/file resolver機能を無効にして使用します。さらにoffline compilerと明示keyword検査を設けています。Draftは**2020-12固定**で、`$schema`を指定する場合は`https://json-schema.org/draft/2020-12/schema`です。

| 対象 | 対応 |
| --- | --- |
| 根 | boolean schema、object schema |
| 型/値 | type、enum、const |
| object | properties、required、additionalProperties、minProperties、maxProperties |
| array | items、prefixItems、minItems、maxItems |
| string | minLength、maxLength（Unicode文字数のSchema意味論） |
| number | minimum、maximum、exclusiveMinimum、exclusiveMaximum、multipleOf |
| 組合せ | allOf、anyOf、oneOf、not |
| annotation | title、description、$comment。変換の選択には使いません |
| format | **全て拒否**。annotationとして黙って無視しません |
| 参照 | **全$ref拒否**（外部HTTP/file、相対path、ローカルfragmentも含む）。$defs、$id、dynamic refsも未対応 |
| その他 | pattern/patternProperties、uniqueItems、contains、if/then/else、unevaluated系、未知keyword等は拒否 |

未知keywordや未対応機能を含むschemaは`UnsupportedSchema`です。Schemaのproperty名やenum/const内のデータはkeywordとして扱いません。対応範囲を広げる際は、DoSの計算量とresolver境界を検証してから追加してください。

## 上限とエラー

| 対象 | 上限 |
| --- | --- |
| raw input、修復候補、最終値のserialized JSON | 64 KiB。超過を切り詰めません |
| JSON全体 | 深さ32、4096ノード、array/objectの要素/field数256 |
| schema | 16 KiB、256ノード、schema深さ16 |
| schema処理量 | instanceノード × schemaノードが262144以下 |
| rules | 16 KiB、YAML展開2048ノード、深さ32 |
| 正規化 | 128 records、64 mappings、target深さ16/index255/key256 bytes |
| 修復 | 最大128カンマ挿入 |
| schema失敗の診断 | 最大16のinstance/schema JSON Pointer位置。値は含めません |

`OutputError`には`kind`、説明、必要に応じたbyte位置、`repairs`、`diagnostics`があります。種類は`InputSyntax`、`IncompleteInput`、`AmbiguousRepair`、`DuplicateKey`、`LimitExceeded`、`UnsupportedSchema`、`InvalidSchema`、`UnsupportedRules`、`InvalidRules`、`Transform`、`OutputValidation`です。core正規化内のrecords制限等のエラーは`Transform`として返します。エラーには検証前の成功値を付けません。

## 検証

```sh
cargo fmt --check
cargo test --workspace
cargo +1.88.0 test -p rulemorph_output --locked
python3 -m pip install -r scripts/output-schema-test-requirements.txt
python3 scripts/verify-output-schema.py
```

Rust testsはカンマ/括弧/引用符欠落、Unicode、escaped quote、重複key、曖昧候補、必須欠損、型変換、生成打切り、深さ/サイズ/出力超過を検証します。性質テストは明示抽出された値の保存と、任意Unicode入力の修復候補がstrict JSONであることを検証します。別実装のPython `jsonschema` Draft202012Validatorが生成した全Ok payloadを独立再検証します。既存core/CLI/stdioの動作は変更しません。
