# UI起動・確認ガイド

Rulemorph UIサーバの起動からブラウザでの動作確認までをまとめたガイドです。

## 前提

- Rust/Cargo が使えること
- UIをビルドする場合は Node.js / npm が使えること
- GitHub Releases の `rulemorph-server` を使う場合はビルド不要（`ui/dist` 同梱）

## UIビルド（初回のみ）

> Release版 `rulemorph-server` を使う場合はこの手順は不要です。

開発時はUIの静的ファイルを手動でビルドする必要があります。

```sh
cd crates/rulemorph_ui/ui
npm install
npm run build
```

ビルド後、`crates/rulemorph_ui/ui/dist` が生成されます。

## 起動方法

### ui-only モード

内部APIのみを提供するモードです。UIは `/api/*` を利用するため、UIを閲覧する場合は rules モードを使用してください。

### rules モード（デフォルト）

UIに加えて、YAMLで定義したカスタムAPIを `/api/*` で提供するモードです。
デフォルトUI用ルールは `assets/api_rules/` を利用します。

```sh
# 開発時
cargo run -p rulemorph_server -- \\
  --api-mode rules \\
  --rules-dir ./assets/api_rules \\
  --allow-unauth-internal \\
  --ssrf-allow-private

# Release バイナリ
rulemorph-server --api-mode rules --rules-dir ./assets/api_rules --allow-unauth-internal --ssrf-allow-private
```

### オプション一覧

| オプション | 説明 | デフォルト |
|-----------|------|-----------|
| `--api-mode <MODE>` | `ui-only` または `rules` | `rules` |
| `--port <PORT>` | リッスンポート | `8080` |
| `--data-dir <PATH>` | データディレクトリ | `./.rulemorph` |
| `--rules-dir <PATH>` | APIルールディレクトリ | `./.rulemorph/api_rules` |
| `--no-ui` | UIを無効化（APIのみ提供） | - |
| `--internal-api-key <KEY>` | `/internal/*` と `/api/import` 用の内部キー | - |
| `--allow-unauth-internal` | 内部APIを鍵なしで許可 | - |
| `--ssrf-allow-private` | private IP/localhost を許可 | - |

## ブラウザ確認

サーバ起動後、ブラウザで以下にアクセスします。

```
http://127.0.0.1:8080
```

- トレース一覧が表示される
- トレースをクリックすると詳細が確認できる
- `/api` 利用時はポーリングで更新される

APIキーが必要な構成では `api_key` をクエリで渡します（初回アクセス時に localStorage へ保存され、URL から削除されます）。

```
http://127.0.0.1:8080/?api_key=<API_KEY>
```

ZIPインポートや内部操作を行う場合は `internal_key` を併用します。

```
http://127.0.0.1:8080/?api_key=<API_KEY>&internal_key=<INTERNAL_KEY>
```

`/api/import` は既定の `assets/api_rules/endpoint.yaml` ではYAMLルール経由で処理されます。endpoint側で `multipart/form-data` の `bundle` を一時展開し、`input.body.bundle_path` を network ルールへ渡して `/internal/import` を呼びます。
UI からのZIPインポートは `x-rulemorph-import: zip` ヘッダ付きで送信されます。このヘッダは既存クライアント互換のため維持されます。
`/api/import` は `--no-ui`（UI無効）時でも利用できますが、この場合は `internal_key` 必須です（`--allow-unauth-internal` だけでは許可されません）。
`endpoint.yaml` に `POST /api/import` を定義している場合、ZIPヘッダ付きのリクエストもルール側が優先されます。定義がない場合のみ専用axumハンドラへfallbackします。
`--api-key-store` などでテナント認証を有効化している場合、この優先判定は認証済みテナントの `endpoint.yaml` に対して評価されます。
`--rate-limit-per-sec` を有効化している場合、`/api/import` の上記優先判定では pre-auth レート制限がテナント解決より先に適用され、上限超過時は resolver 実行前に 429 が返ります。

## 破壊的変更メモ（2026-02-06）

- 旧: `POST /internal/import-zip`
- 新: `POST /api/import`（`multipart/form-data`, `bundle` フィールド, ZIP用途は `x-rulemorph-import: zip` 推奨）

既存クライアント/スクリプトで `POST /internal/import-zip` を呼んでいる場合は、`POST /api/import` へ移行してください。

## サンプルトレース投入

UIは `data_dir/traces` 配下の `trace.json` マニフェストとチャンクをトレースとして読み込みます（旧形式の単一JSONも読み込み可能）。

```sh
mkdir -p ./.rulemorph/traces/2025/01/01/demo-001
cat <<'JSON' > ./.rulemorph/traces/2025/01/01/demo-001/trace.json
{
  "trace_schema_version": 1,
  "trace_id": "demo-001",
  "timestamp": "2025-01-01T00:00:00Z",
  "status": "ok",
  "summary": {
    "record_total": 1,
    "record_success": 1,
    "record_failed": 0
  },
  "max_chunk_bytes_uncompressed": 4194304
}
JSON
```

> 日付フォルダは任意ですが、`YYYY/MM/DD` 形式で整理するのがおすすめです。

旧形式（単一JSON）でも動作しますが、新形式のマニフェスト + チャンクが推奨です。

ディレクトリ構成の詳細は [ui-data-dir-usage.md](ui-data-dir-usage.md) を参照してください。

## サンプルAPIルール

rules モードでは `./.rulemorph/api_rules/` 配下のYAMLでカスタムAPIを定義できます。

例：
- `endpoint.yaml`: エンドポイント定義
- `network_fetch.yaml`: 外部API呼び出し（`type: network`）
- `network_body.yaml`: リクエストボディ生成ルール

## よくあるエラー

| 症状 | 原因と対処 |
|------|-----------|
| 画面が真っ白 | `ui/dist` が存在しない。`npm run build` を実行 |
| 404が返る | `endpoint.yaml` が見つからない。`--rules-dir` を確認 |
| ポートが使用中 | `lsof -nP -iTCP:8080 -sTCP:LISTEN` で確認し、プロセスを終了 |
