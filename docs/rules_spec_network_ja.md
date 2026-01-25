# Network ルール仕様（v2・MVP）

このドキュメントは v2 の `network` ルールの最小仕様（MVP）を定義します。
共通仕様（参照/条件/expr など）は `docs/rules_spec_ja.md` を参照してください。

## 概要
`network` ルールは外部 HTTP API を呼び出し、
レスポンスを入力として次のステップへ渡します。

## ルール構成（最小）

```yaml
version: 2
type: network

request:
  method: GET
  url:
    - "@context.config.api_base"
    - concat: ["/users/", "@input.user_id"]
  headers:
    Authorization: "Bearer TOKEN"

timeout: 5s
select: "data"
```

## フィールド一覧（MVP）

### 必須
- `version`: `2` 固定
- `type`: `network` 固定
- `request.method`: `GET|POST|PUT|PATCH|DELETE`
- `request.url`: v2 expr
- `timeout`: 例 `5s`, `500ms`

### 任意
- `request.headers`: 文字列のマップ（MVPではリテラルのみ）
- `body`: v2 expr
- `body_map`: v2 `mappings`（入力からボディを組み立てる）
- `body_rule`: 外部ルール参照
- `retry`: リトライ設定
- `select`: レスポンス抽出パス
- `catch`: エラー分岐

### 保留（MVP外）
- 高度な認証（OIDC/SAML）
- キャッシュ、レート制限
- 監査ログ、メトリクス拡張

## request
### url
`url` は **v2 expr** として扱います（リテラルも可）。
評価結果は文字列である必要があります。
`missing` や非文字列はエラーとして `catch` に渡します。

### headers
`headers` は固定文字列のみ（MVPでは expr 非対応）。

```yaml
request:
  method: GET
  url: "https://api.example.com/users"
```

```yaml
request:
  method: GET
  url:
    - "https://api.example.com/users/"
    - concat: ["@input.user_id"]
```

## body
`body` / `body_map` / `body_rule` は **排他**。
`body` は **v2 expr** として扱います（リテラルも可）。

`body` の評価結果が `missing` の場合は **ボディ無し** として扱います。
`null` は JSON の `null` として送信されます。

### Content-Type の既定
`body` が存在し、`request.headers` に `content-type` が無い場合は
`application/json` を自動付与します（MVP）。

### method と body の関係（MVP）
- `GET` で `body` / `body_map` / `body_rule` を指定するのは禁止（バリデーションエラー）

```yaml
body:
  - "@input"
```

```yaml
body_map:
  - target: "userId"
    source: "input.user_id"
  - target: "action"
    value: "fetch"
```

```yaml
body_rule: ./rules/build_body.yaml
```

## timeout / retry
- `timeout` は必須。文字列で指定（例: `5s`, `500ms`）。
- `retry` は任意。

### timeout の単位（MVP）
- 受け付ける単位は `ms` と `s` のみ
- 0 以下はエラー

```yaml
retry:
  max: 3
  backoff: exponential
  initial_delay: 100ms
```

### retry の意味（MVP）
- `max`: 失敗後の **再試行回数**（`0` なら再試行なし）
- `backoff`: `fixed | linear | exponential`（省略時は `fixed`）
- `initial_delay`: 省略時は `100ms`

## select
レスポンスJSONから抽出するパスです（expr ではなく **ドットパス文字列**）。
MVPではドットパスと配列インデックスを許可します。
抽出先が存在しない場合はエラーとして `catch` に渡します。

### パス仕様（MVP）
- ドットと配列インデックスのみ（例: `data.items[0].id`）
- エスケープ付きキーは未対応

```yaml
select: "data.users[0]"
```

## catch
`catch` は HTTP ステータスや `timeout` をキーに分岐します。
マッチング優先順位:
1. 完全一致（`404` など）
2. パターン（`4xx`, `5xx`）
3. `timeout`
4. `default`

`default` はステータスが無いエラー（通信失敗、JSONパース失敗など）も扱います。

### catch 対象となるエラー例（MVP）
- 通信失敗 / タイムアウト
- 非JSONレスポンスのパース失敗
- `select` の抽出失敗
- `request.url` / `body` の expr 評価エラーや型不一致

```yaml
catch:
  404: ./rules/not_found.yaml
  4xx: ./rules/client_error.yaml
  5xx: ./rules/server_error.yaml
  timeout: ./rules/timeout.yaml
  default: ./rules/error.yaml
```

## 入出力
- 入力: 直前ステップの `@input`
- 出力: HTTPレスポンスの JSON（`select` があれば抽出後の値）
  - MVPでは JSON レスポンスを前提とし、非JSONはエラーとして `catch` に渡ります。
  - レスポンスボディが空の場合は `null` として扱います。

## MVPでの制約
- `headers` は固定値のみ
- `url` 内でテンプレート展開は行わない（expr を使う）
- 高度な認証やキャッシュは後続フェーズ
