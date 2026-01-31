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

UIのみを提供するモードです。内部APIは `/internal/*` で提供されます。

```sh
# 開発時
cargo run -p rulemorph_server

# Release バイナリ
rulemorph-server --api-mode ui-only
```

### rules モード（デフォルト）

UIに加えて、YAMLで定義したカスタムAPIを `/api/*` で提供するモードです。

```sh
# 開発時
cargo run -p rulemorph_server -- --api-mode rules

# Release バイナリ
rulemorph-server --api-mode rules
```

### オプション一覧

| オプション | 説明 | デフォルト |
|-----------|------|-----------|
| `--api-mode <MODE>` | `ui-only` または `rules` | `rules` |
| `--port <PORT>` | リッスンポート | `8080` |
| `--data-dir <PATH>` | データディレクトリ | `./.rulemorph` |
| `--rules-dir <PATH>` | APIルールディレクトリ | `./.rulemorph/api_rules` |
| `--no-ui` | UIを無効化（APIのみ提供） | - |

## ブラウザ確認

サーバ起動後、ブラウザで以下にアクセスします。

```
http://127.0.0.1:8080
```

- トレース一覧が表示される
- トレースをクリックすると詳細が確認できる
- トレース更新は SSE (`/internal/stream`) で自動反映される

## サンプルトレース投入

UIは `data_dir/traces` 配下のJSONファイルをトレースとして読み込みます。

```sh
mkdir -p ./.rulemorph/traces/2025/01/01
cat <<'JSON' > ./.rulemorph/traces/2025/01/01/demo-001.json
{
  "id": "demo-001",
  "title": "Demo Trace",
  "created_at": "2025-01-01T00:00:00Z",
  "summary": {
    "input": {"foo": "bar"},
    "output": {"ok": true}
  },
  "nodes": []
}
JSON
```

> 日付フォルダは任意ですが、`YYYY/MM/DD` 形式で整理するのがおすすめです。

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
