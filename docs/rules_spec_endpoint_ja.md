# Endpoint ルール仕様（v2・MVP）

このドキュメントは v2 の `endpoint` ルールの最小仕様（MVP）を定義します。
共通仕様（参照/条件/expr など）は `docs/rules_spec_ja.md` を参照してください。

## 概要
`endpoint` ルールは HTTP エンドポイントを宣言し、
`input` → `steps` → `reply` の順で処理を行います。
`steps` の各ルールは順に実行され、出力が次ステップの入力になります。

## ルール構成（最小）

```yaml
version: 2
type: endpoint

endpoints:
  - method: GET
    path: /users/{id}
    input:
      - target: "user_id"
        source: "input.path.id"
        type: "int"

    steps:
      - rule: ./rules/validate_user.yaml
      - rule: ./rules/fetch_user.yaml

    reply:
      status: 200
      headers:
        Content-Type: application/json
      body: "@input"
```

## フィールド一覧（MVP）

### 必須
- `version`: `2` 固定
- `type`: `endpoint` 固定
- `endpoints`: エンドポイント配列
  - `method`: `GET|POST|PUT|PATCH|DELETE`
  - `path`: ルートパス（`/users/{id}` 形式）
  - `steps`: 実行するルールの配列
  - `reply`: レスポンス定義

### 任意
- `input`: リクエスト整形用の mapping（v2 `mappings` と同形式）
- `catch`: エラー分岐
- `reply.headers`: 固定ヘッダ（MVPではリテラルのみ）
- `steps[].with`: ルール呼び出し時のパラメータ
- `steps[].when`: v2条件（falseならそのステップをスキップ）
- `steps[].catch`: そのステップ専用のエラー分岐

### 保留（MVP外）
- inline ルール
- 高度な認証/認可
- レート制限、キャッシュ、監査ログ

## input
`input` は v2 `mappings` と同じ構文でリクエストを整形します。
`@input` は HTTP リクエストから構成される仮想入力です。

### @input の構造（MVP固定）
- `@input.method`: HTTP method（例: `"GET"`）
- `@input.path`: パスパラメータのマップ（`/users/{id}` → `@input.path.id`）
- `@input.query`: クエリパラメータのマップ（値は文字列）
- `@input.body`: JSON body（存在しない場合は `missing`）
- `@input.headers`: ヘッダのマップ（小文字キー）

`input` の評価後は **`input` の出力が新しい `@input` になります**。
元のリクエストは自動保持されないため、必要なら `input` で明示的に写してください。

### クエリ/ヘッダの扱い（MVP）
- `@input.query` は **単一値のみ**。同一キーの複数指定はエラーとして `catch` に渡します。
- `@input.headers` はキーを小文字化して格納します。同名ヘッダが複数ある場合は **カンマ連結** します。

```yaml
input:
  - target: "user_id"
    source: "input.path.id"
    type: "int"
  - target: "include_orders"
    source: "input.query.include"
    default: false
```

## steps
`steps` は上から順に実行され、
各ステップの出力が次ステップの `@input` になります。
`input` がある場合、`steps` に渡る `@input` は整形後の値です。

```yaml
steps:
  - rule: ./rules/validate_user.yaml
  - rule: ./rules/fetch_user.yaml
    with:
      fields: ["name", "email"]
    when:
      eq: ["@input.include", true]
```

### when
`when` は v2 条件として評価されます。
- `false` の場合、そのステップは **スキップ** され、入力はそのまま次へ渡されます。
- 評価エラーはステップのエラーとして扱い、`catch` に渡されます。

### with
`with` は呼び出し先ルールの `@context.params` として参照します。

```yaml
# 呼び出し側
- rule: ./rules/format_user.yaml
  with:
    fields: ["name", "email"]

# 呼び出し先
mappings:
  - target: "user"
    expr:
      - "@input"
      - pick: ["@context.params.fields"]
```

### catch
`catch` は HTTP ステータスや `timeout` をキーに分岐します。
マッチング優先順位:
1. 完全一致（`404` など）
2. パターン（`4xx`, `5xx`）
3. `timeout`
4. `default`

```yaml
catch:
  404: ./rules/not_found.yaml
  4xx: ./rules/client_error.yaml
  5xx: ./rules/server_error.yaml
  timeout: ./rules/timeout.yaml
  default: ./rules/error.yaml
```

ステップ内でエラーが起きた場合は **ステップの `catch` を優先** し、
マッチしなければ `endpoint` の `catch` にフォールバックします。

## reply
MVPでは `status` / `headers` / `body` を定義できます。
`status` と `body` は **v2 expr** として扱います（リテラルも可）。
`@` をリテラル文字列として扱いたい場合は `lit:` を使用します。

- `status`: v2 expr（整数を返す）
- `headers`: 固定値のみ（キーは小文字化）
- `body`: v2 expr（省略可）

`body` が存在し、`headers` に `content-type` が無い場合は
`application/json` を自動付与します（MVP）。

### status / body の評価ルール（MVP）
- `status` の評価結果は **100〜599 の整数** である必要があります。それ以外はエラーです。
- `body` の評価結果が `missing` の場合は `null` として扱います。
- `status` / `body` の評価エラーは `catch` に渡されます。

```yaml
reply:
  status: 200
  headers:
    Content-Type: application/json
  body: "@input"
```

```yaml
reply:
  status:
    - "@input.found"
    - if:
        cond: { eq: ["$", true] }
        then: 200
        else: 404
  body:
    - "@input"
```

## 配列レスポンス
レスポンスを配列で返したい場合は、**最後の normal ルール側で `finalize` を有効化**してください。
`reply.body: "@input"` の場合、最終ステップの出力がそのままレスポンスになります。

- `finalize` は **配列出力のみを許可**します（`wrap` を使うとオブジェクトになるため、配列レスポンス目的では使いません）。
- 単一レコードでも `finalize` を使うと **`[ { ... } ]` の配列**になります。

例（単一オブジェクトを配列レスポンス化）:
```yaml
# hello.yaml
finalize: {}
# input: { "name": "test" }
# ouput: [{ "name": "test" }]

# または
finalize:
  limit: 1000
```

## 実行モデル
1. HTTP リクエストを `@input` として読み込む
2. `input` を適用し、整形された入力を `steps` へ渡す
3. `steps` を順に実行し、出力を次の `@input` とする
4. `reply` を構築して返す

## エラー時の挙動
- パイプラインでエラーが発生した場合、
  `catch` または該当ステップの `catch` で分岐します。
- マッチがなければエラー応答を返します。

## MVPでの制約
- `status` / `body` は v2 expr（リテラルも expr として許可）
- `headers` は固定値のみ
- 元リクエストは `input` 実行後に自動保持されない
- `inline` / `auth` / `rate_limit` などは後続フェーズ
