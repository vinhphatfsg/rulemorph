# Sitesで使うHTTP MCP

Sites用のHTTPアダプターは、Rust coreをWASMとして実行します。従来のstdio MCP、CLI、ローカルサーバーはそのまま利用できます。HTTP版は`POST /mcp`で`initialize`、`tools/list`、`tools/call`、`ping`を受け付け、セッションやユーザーデータを保存しません。ブラウザUIはありません。

## 構築とローカル実行

Rust 1.88以上、`wasm32-unknown-unknown` target、wasm-pack 0.13.1、Node.js 22以上とnpmが必要です。

```sh
rustup target add wasm32-unknown-unknown
cargo install wasm-pack --locked --version 0.13.1
npm --prefix sites ci
npm --prefix sites run build
npm --prefix sites test
npm --prefix sites run dev
```

構築結果はリポジトリ直下の`dist/server/index.js`と隣接する`rulemorph_wasm_bg.wasm`です。ESMのdefault exportに`fetch(request, env, ctx)`相当のハンドラーを備え、WASMはWorkersのCompiledWasm module importで読み込みます。起動時に外部URLからWASMを取得しません。`sites/pkg`と`dist`は生成物です。

ローカルサーバーは`127.0.0.1:8787`だけにbindします。例はローカルテスト専用であり、Sites認証を検証するものではありません。

```sh
curl http://127.0.0.1:8787/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'oai-authenticated-user-id: local-test-user' \
  --data '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"analyze_input","arguments":{"input_json":[{"name":"太郎"}]}}}'
```

`.openai/hosting.json`は`capabilities: ["mcp"]`を宣言します。Sitesに登録済みのcheckoutへ導入する際は既存の`project_id`やcapabilitiesを維持して統合してください。本変更にはSite登録、公開、デプロイ、プラグイン登録、権限変更は含まれません。Sitesの実ホストでの接続は未検証です。

## 認証境界

OAuthはSitesが担当します。全`tools/call`で、Sitesから渡される`oai-authenticated-user-id`を必須とし、欠損時にHTTP 401を返します。email、full-name、ツール引数を代用しません。`initialize`と`tools/list`は静的な仕様だけを返し、個人データを含めません。

追加の利用者制限が必要な場合は、Sitesのruntime設定で`ALLOWED_SITE_USER_IDS`に許可するSite-scoped IDをカンマ区切りで設定できます。設定済みで空文字の場合は全員拒否、許可されないIDはHTTP 403です。未設定ならSitesが認証した利用者が利用できます。ユーザー間で共有する記録・キャッシュ・外部接続はありません。

このヘッダーは**Sitesが唯一の外部入口で、クライアントが偽装した同名ヘッダーを除去/置換する境界**でのみ信頼できます。Workerをそのまま別の公開URLへ配置すると認証を偽装できます。直接Workersへの公開には別途入口の認証が必要です。公開Siteの可視性と、他者へのprivate plugin配布・アクセス範囲は別問題であり、今回確認していません。

`Origin`があればリクエストURLと同一originだけを許可します。外部originは403です。認証・入出力の本文をログに記録しません。

## 提供ツール

| ツール | 引数 | 結果 |
| --- | --- | --- |
| `transform` | `rules_text`、`input_text`または`input_json` | `output`に変換結果 |
| `validate_rules` | `rules_text` | `valid`とルール診断。出力JSON Schemaの検証ではありません |
| `generate_dto` | `rules_text`、`language`、任意の`name` | `dto`にコード文字列。実行はしません |
| `analyze_input` | `input_text`または`input_json`、任意の`format`と`records_path` | レコード数、パス、型。値のサンプルなし |

`rules_text`はYAML/JSONです。変換入力はJSON、CSV、YAML、TOML、XMLに対応します。解析入力はJSON（既定）とCSVのみです。`input_json`はJSON形式限定で、`input_text`と排他的です。DTO言語はrust/typescript/python/go/java/kotlin/swiftです。

HTTP版の制限付きルールは、mappings、steps内のmappings/asserts/record_when、通常のsource/value/type/default/required、およびスカラーpipe/referenceを使えます。演算は`trim`、`lowercase`、`uppercase`、`coalesce`、四則演算、`round`、`abs`、`floor`、`ceil`、`trunc`、`sqrt`、`sign`、`mod`、`pow`、`clamp`、比較とboolean演算に限定します。条件のall/anyと比較も使えます。defs、codecs、finalize、ファイルbranch、regex条件、カスタム演算、map/reduce/range等の反復・拡張演算は拒否します。これは処理量を静的に制限するHTTPプロファイルであり、ローカルルールの全機能と同等ではありません。

`input_path`、`rules_path`、`output_path`、`base_dir`、context、利用者による上限overrideは公開APIにありません。未知の引数を拒否し、coreにはfilesystem base dirを渡しません。ローカルファイルの読み書き、任意コード実行、network fetchは提供しません。DTO/変換結果はリクエストした利用者だけへ返します。

## 上限と失敗

| 対象 | 固定上限 |
| --- | --- |
| HTTP本文（UTF-8） | 96 KiB。Content-Lengthなしのstreamにも適用 |
| rules_text | 16 KiB、深さ32、展開ノード2048、配列256 |
| 入力 | 64 KiB、128レコード、深さ32、配列256 |
| YAML入力 / XML入力 | 展開ノード4096 / XMLノード4096、YAML alias 0 |
| ルール処理量 | 16 steps、64 mappings、256式ノード、16384レコード×式単位 |
| 書込先パス | 深さ32、配列index 255、key 256 bytes |
| 生成object | 深さ32、fields 256、ノード4096、生成値256 KiB |
| 結果 | ツールpayload 256 KiB、HTTP tool content 512 KiB |
| 入力解析 | 256パス |

本文サイズ超過は413、JSON/UTF-8/重複キーの不正は400、media typeは415、Accept不足は406です。JSON-RPCでは不正paramsが`-32602`、未対応methodが`-32601`、内部実行失敗が`-32603`です。ツールの解析・検証・変換・上限エラーは`isError: true`と説明を返し、結果を切り詰めて成功扱いしません。元入力の生成打切り判定などはこのHTTP版の責務ではありません。

リクエストごとの制限に加えて、実ホストのCPU/メモリー上限が適用されます。構造・演算制限を解除するには、coreの全演算を覆う評価budgetを追加してから検証する必要があります。

## プロトコルと検証

[Streamable HTTP仕様](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)に沿ってJSON応答を返します。対応versionは2025-03-26、2025-06-18、2025-11-25。未知のinitialize versionには最新対応版を提示し、未知の`MCP-Protocol-Version`ヘッダーは400です。Acceptは`application/json, text/event-stream`、Content-Typeは`application/json`を指定してください。SSEは提供せず、GET/DELETEは405、通知は202（空本文）、batchは拒否します。

`cargo test --workspace`がstdio/CLI/coreを含む回帰を実行します。追加のRust境界テストと`npm --prefix sites test`がHTTPプロトコル、認証401/403、実Miniflare/workerd内のWASM、4ツール、上限・パス拒否を検証します。CIはWASM構築と実Workerテストも実行します。Miniflareは開発専用で、undici/sharpの修正済みバージョンをoverrideしています。ブラウザUIは変更していません。
