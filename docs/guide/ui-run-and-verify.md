# UI起動〜確認ガイド

このガイドは、Rulemorph UIサーバの起動からブラウザでの確認までをまとめたものです。
`--api-mode ui-only`（UI専用）と `--api-mode rules`（ユーザーAPIをYAMLで定義）に対応しています。
トレース保存は **ファイル保存（file backend）** を前提とした構成です（将来的にDB/オブジェクトへ差し替え可能）。

> UI自身は **/internal/** の組み込みAPIを使います。ユーザー向けAPIは /api/* で提供します（rules モードのみ）。

## 前提
- Rust/Cargo が使えること
- UIをビルドする場合は Node.js / npm が使えること

## UIビルド（初回のみ）
UIの静的ファイルは `crates/rulemorph_ui/ui/dist` を参照します。
ビルド済みでない場合は以下を実行してください。

```sh
cd crates/rulemorph_ui/ui
npm install
npm run build
```

## 起動（ui-only モード）
組み込みAPIのみを提供するモードです。UI表示に必要なAPIは /internal で提供されます。
トレース更新は `/internal/stream`（SSE）で自動反映されます。

```sh
cargo run -p rulemorph_cli -- ui --api-mode ui-only
```

- 既定ポート: `8080`
- 既定データディレクトリ: `./.rulemorph`

## 起動（rules モード）
`endpoint.yaml` / `network` ルールで `/api/*` を処理するモードです（ユーザーAPI）。
UI表示に必要なAPIは引き続き /internal が使われます。

```sh
cargo run -p rulemorph_cli -- ui --api-mode rules
```

- 既定 `--api-mode` は `rules` です。
- 既定 `--rules-dir` は `./.rulemorph/api_rules`（カレント配下）です。
- `--rules-dir` でユーザーAPIルールのディレクトリを指定できます。
- UIを無効化する場合は `--no-ui` を付けます（UI/内部API/静的配信を停止）。

## グラフの紐仕様（概要/詳細 共通）
**参照ファイルがある OP / Step は必ず概要ノードへの紐を持つ**ことを仕様とします。
また、**概要ノードを展開した場合は、そのノードを source に持つ概要→概要の紐は非表示**にします
（詳細ノードから概要ノードへの紐で代替表示）。

- 対象（必須で紐を張るもの）
  - endpoint の steps（全step）
  - network の body_rule
  - normal rule の branch（then / else）
  - trace 詳細では `meta.rule_ref` を持つ step / op
- ラベル
  - endpoint: `METHOD PATH`
  - branch: `branch: then` / `branch: else`
  - body_rule: `body_rule`

## サンプルAPIルール（network body_rule）
`./.rulemorph/api_rules/` に実運用想定の network + body_rule を組み込んでいます。

- `endpoint.yaml`: 新規 `GET /api/profile/{id}` を追加
- `network_fetch.yaml`: `type: network`（POST + body_rule）
- `network_body.yaml`: body_rule（入力からJSONを組み立てる）

## UI起動と確認（network body_rule を使う）
デフォルト設定でUIを起動します。

```sh
cargo run -p rulemorph_cli -- ui
```

ブラウザで `http://127.0.0.1:8080` を開き、以下を確認します。

- APIグラフ詳細で `branch` / `body_rule` / `endpoint steps` の詳細OPから概要ノードへ紐が表示される
- トレース詳細で `meta.rule_ref` を持つ step/op から概要ノードへの紐が表示される

> 外部ネットワークが必要な場合があります（`https://httpbin.org/anything` を使用）。

## サンプルトレース投入（手動）
UIは data_dir の `traces` 配下からトレースを読み込みます。
以下のようにJSONを配置すると一覧に表示されます。

例（今日のフォルダに `demo-001.json` を置く）:

```sh
mkdir -p ./.rulemorph/traces/2026/01/26
cat <<'JSON' > ./.rulemorph/traces/2026/01/26/demo-001.json
{
  "id": "demo-001",
  "title": "Demo Trace",
  "created_at": "2026-01-26T00:00:00Z",
  "summary": {
    "input": {"foo": "bar"},
    "output": {"ok": true}
  },
  "nodes": []
}
JSON
```

> 日付フォルダは任意ですが、`YYYY/MM/DD` 形式で整理するのがおすすめです。

## ブラウザ確認
UIサーバ起動後、以下にアクセスします。

- `http://127.0.0.1:8080`

一覧にトレースが表示され、クリックすると詳細が確認できればOKです。

## よくあるエラー
- **画面が真っ白**: `crates/rulemorph_ui/ui/dist` が存在しない可能性があります。`npm run build` を実行してください。
- **404が返る**: `--api-mode rules` で `endpoint.yaml` が見つからない可能性があります。`--rules-dir` を確認してください。
