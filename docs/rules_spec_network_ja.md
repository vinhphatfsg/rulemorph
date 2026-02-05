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
    - "@context.config.internal_base"
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
- `request.headers`: 文字列 or v2 expr のマップ（`missing` はヘッダを送らない）
- `body`: v2 expr
- `body_map`: v2 `mappings`（入力からボディを組み立てる）
- `body_rule`: 外部ルール参照
- `retry`: リトライ設定
- `select`: レスポンス抽出パス
- `catch`: エラー分岐
- `internal_auth`: `true` の場合のみ internal_base 宛てリクエストに `x-api-key`/`x-tenant-id` を自動付与（internal_base 宛てのみ有効。内部認証が有効な環境に限る。サーバ設定で許可パスが制限される場合あり）

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
`headers` は **固定文字列または v2 expr** を指定できます。
`expr` の評価結果が `missing` の場合は **そのヘッダを送信しません**。
`Host` / `Forwarded` / `X-Forwarded-*` は SSRF 対策のため指定不可です。

### context
- `@context.config.internal_api_key` は internal_auth が有効で internal_base 宛てのネットワークルールでのみ提供されます（未設定時は `missing`）。

```yaml
request:
  method: GET
  url: "https://api.example.com/users"
  headers:
    Authorization: "Bearer TOKEN"
```

```yaml
request:
  method: GET
  url:
    - "https://api.example.com/users/"
    - concat: ["@input.user_id"]
  headers:
    x-tenant-id: "@context.tenant_id"
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

## 運用向けメモ（SSRF対策）
network ルールのリクエストは SSRF 対策のバリデーションを通過する必要があります。

- 許可スキーム: `http` / `https` のみ
- IPリテラル（例: `http://127.0.0.1` / `http://[::1]`）は拒否
- allowlist 未設定時は **ホスト名は許可**（ただし IP リテラルは拒否）
- Cloud/APIキー運用では allowlist の指定を必須とし、例外的に許可する場合は `--ssrf-allow-any` を明示
- allowlist 設定時は **完全一致 or サブドメイン一致** のみ許可
- リダイレクトは無効化（外部への誘導を防止）

allowlist はサーバ起動時に指定します。

```sh
rulemorph ui --ssrf-allowlist api.example.com --ssrf-allowlist auth.example.com
# もしくは
rulemorph-server --ssrf-allowlist api.example.com
# allowlist を明示的に不要とする場合
rulemorph-server --ssrf-allow-any
```

ローカル検証を行う場合は `localhost` を使用してください（`127.0.0.1` など IP リテラルは拒否されます）。
内部IP/localhost を許可したい場合は明示的に `--ssrf-allow-private` を指定してください（本番では非推奨）。

### SSRF監査ログ
SSRF判定でブロックされた場合は `rulemorph_endpoint::ssrf` ターゲットで警告ログを出力します。
ログには `tenant_id` / `rule_ref` / `method` / `url` / `reason` が含まれます。
