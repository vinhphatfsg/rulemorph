# 用語集（v2）

この用語集は v2 ルール仕様の「用語の揺れ」を抑える目的で定義します。
仕様の詳細は `docs/rules_spec_ja.md` を参照してください。

## 推奨呼称（今回の整理）

- **フィールド** → 「**出力フィールド** / **target**」
- **値（source/value/expr の総称）** → 「**値定義**」
- **OP（expr の各要素）** → 「**パイプステップ**」
  - OP[0] 相当は **開始値（start）**
  - OP[1..] 相当は **ステップ**（関数/演算）

> 指摘："OP" は UI 的には広く使いやすい一方、仕様としては **「パイプステップ」** の方が誤解が少ないため推奨します。

## 用語一覧

### ルール全体

- **ルールファイル**: 1つの YAML ファイル（version, input, mappings, steps, finalize などを持つ）。
- **ルール種別**: `normal` / `network` / `endpoint`。
- **レコード**: 変換対象の 1 件（入力レコード / 出力レコード）。
- **コンテキスト**: `@context` 参照で使える値（外部から与える入力）。

### 変換（mappings）

- **mapping（マッピング）**: 1 つの出力フィールドを作る単位。`mappings` 配列の 1 要素。
- **出力フィールド（target）**: `target` に指定する出力先パス。
- **値定義**: `source` / `value` / `expr` のいずれか 1 つ（排他）。
  - **source**: 入力やコンテキストから値を参照して出力。
  - **value**: リテラル値。
  - **expr**: パイプ式（複数ステップの評価）。

### expr（パイプ式）

- **パイプ式（expr）**: `expr` 配列で表現される評価パイプ。
- **開始値（start）**: `expr` の先頭要素（OP[0] 相当）。
- **パイプステップ（step）**: `expr` の 2 要素目以降（OP[1..] 相当）。
- **演算ステップ**: `trim` / `uppercase` / `len` / `concat` などの処理。

### v2 steps（steps:）

- **ステップ（steps）**: v2 の `steps:` における実行単位（mappings/record_when/asserts/branch を持つ）。
- **アサート（asserts）**: 条件に一致したらエラーを返すチェック。
- **分岐（branch）**: 条件で次のルールへ遷移。

### finalize

- **finalize**: 変換の最終処理。`filter/sort/limit/offset/wrap` を持つ。

### 参照（@）

- **@input**: 入力レコード。
- **@context**: 外部コンテキスト。
- **@out**: 途中出力。

## 例（用語の対応）

```yaml
version: 2
input:
  format: json
  json: {}

mappings:
  - target: name        # 出力フィールド
    expr:              # 値定義（expr）
      - "@input.path.name" # 開始値（start）
      - trim               # パイプステップ
      - uppercase          # パイプステップ
  - target: message
    expr:
      - "Hello, "
      - concat: ["@out.name"]
  - target: length
    expr:
      - "@out.name"
      - len
```
