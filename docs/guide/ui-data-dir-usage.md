# UIデータディレクトリ構成

Rulemorph UIが使用するデータディレクトリの構成と配置ルールをまとめたガイドです。

## ディレクトリ構成

デフォルトのデータディレクトリは `./.rulemorph` です。

```
./.rulemorph/
├── traces/          # トレースマニフェストとチャンク（JSON/NDJSON）
├── rules/           # トレース参照用ルール（YAML）
├── api_rules/       # カスタムAPI用ルール（YAML）
└── auth/            # APIキー情報（api_keys.json）
```

| ディレクトリ | 用途 |
|-------------|------|
| `traces/` | 変換実行のトレースログ（`trace.json` マニフェスト + チャンク、旧形式の単一JSONも可） |
| `rules/` | トレース内で参照されるルールファイル |
| `api_rules/` | `/api/*` エンドポイントを定義するルール |
| `auth/` | APIキー情報（`api_keys.json`） |

> `.rulemorph/` は `.gitignore` に追加することを推奨します。

## ファイル配置ルール

### traces/

トレースは `trace.json` マニフェストとチャンクを `traces/` 配下に配置します。サブディレクトリは任意ですが、日付形式での整理を推奨します。

```
./.rulemorph/traces/
├── 2025/01/01/
│   ├── trace-users-001/
│   │   ├── trace.json
│   │   ├── records-0001.ndjson.zst
│   │   ├── nodes-0001.ndjson.zst
│   │   ├── finalize.json.zst
│   │   └── blobs/
│   │       └── sha256-<hash>.json.zst
│   └── trace-users-002/
│       └── trace.json
└── 2025/01/02/
    └── trace-orders-001/
        └── trace.json
```

`trace.json` はトレースのマニフェストで、必要に応じて `records-*.ndjson` / `nodes-*.ndjson` / `finalize.json` などのチャンクを参照します。読み込み時の展開上限を通知したい場合は `max_chunk_bytes_uncompressed`（bytes）を指定できます。未指定の場合はハード上限（16MB）が適用され、指定値も 16MB でクランプされます。書き込み時も同じ上限でクランプされ、マニフェストにはクランプ後の値が記録されます。読み込み時は合計チャンク数/合計バイト数/レコード数/ノード数にも上限があり（records 200k / nodes 500k）、超過時は detail が `basic` に落ち、reason に `budget_exceeded` が追加されます。チャンクのI/O/パース/復号に失敗した場合も detail が `basic` に落ち、reason に `chunk_error` が追加されます。単一の record/node/finalize が `max_chunk_bytes_uncompressed` を超える場合は detail が `basic` に降格し、reason に `chunk_too_large` が追加されます。`trace.json`/旧形式JSONは 20MB を上限とし、超過時は読み取り対象から除外されます（書き込み時は `rule_source` を落として再計算し、それでも超過する場合は書き込みが失敗します）。`summary.record_total` 超過による detail 降格は `/internal/traces/{id}` と `/internal/traces/{id}/manifest` の両方で同一に適用されます。
`bytes_uncompressed` が欠落しているチャンクは `max_chunk_bytes_uncompressed`（未指定時は 16MB）を上限として合計バイト数を推定します。圧縮チャンクで `max_chunk_bytes_uncompressed` が欠落している場合は `bytes` を推定に使います。推定に必要な情報が欠落している場合は安全側に倒し、detail を `basic` に降格します。`record_start`/`record_end` や `node_start`/`node_end` が欠落し、`summary.record_total` もない場合も同様に安全側で `budget_exceeded` として扱われます。
`nodes` チャンク互換: node オブジェクトが自身の `record_index` を持つ場合、writer は `{ "record_index": ..., "node": {...} }` 形式で保存して値の上書きを避けます。reader はトップレベルキーが `node` と任意の `record_index` のみのときだけ wrapper として展開し、`node` の sibling キーがある場合は通常ノードとしてそのまま保持します。
旧形式として `traces/` 配下に単一JSON（1ファイル=1トレース）を置く方式も読み込み可能です。
デフォルトは Zstd 圧縮で `.zst` が付与されます。大きなペイロードは `blobs/` に外出しされます。

### rules/

トレース内で参照される `rule.path` / `meta.rule_ref` に一致するパスで配置します。

```
./.rulemorph/rules/
├── users/
│   ├── endpoint.yaml
│   ├── get.yaml
│   └── list.yaml
└── orders/
    └── transform.yaml
```

### api_rules/

rules モードで `/api/*` を提供するルールを配置します。

```
./.rulemorph/api_rules/
├── endpoint.yaml           # ルートエンドポイント定義
└── network/
    ├── list.yaml
    └── get.yaml
```

### auth/

APIキーは `auth/api_keys.json` に保存されます。

```
./.rulemorph/auth/
└── api_keys.json
```

## テナント有効時の構成

`--api-key-store` などでテナント分離を有効にすると、`tenants/<tenant_id>/` 配下にデータが作成されます。

```
./.rulemorph/tenants/
└── <tenant_id>/
    ├── traces/
    ├── rules/
    ├── api_rules/
    └── auth/
        └── api_keys.json
```

## トラブルシューティング

トレースが反映されない場合：

1. `--data-dir` の指定が正しいか確認
2. `traces/` に `trace.json`（または旧形式JSON）が存在するか確認
3. ポートを掴んでいる古いプロセスがないか確認

```sh
lsof -nP -iTCP:8080 -sTCP:LISTEN
```

起動方法の詳細は [ui-run-and-verify.md](ui-run-and-verify.md) を参照してください。

## 内部APIキーとUIアクセス

Cloud/APIキー運用では `/api/*` と `/internal/*` の両方にキーが必要になります。
UI は `/api/*` を利用するため、`api_key` と `internal_key` をクエリで渡してください。

```sh
rulemorph-server --api-key-store --internal-api-key <INTERNAL_KEY> ...
```

UI:

```
http://localhost:8080/?api_key=<API_KEY>&internal_key=<INTERNAL_KEY>
```

`api_key` / `internal_key` は初回アクセス時に localStorage に保存され、URL から削除されます（履歴/リファラでの漏えいを避けるため）。以降はクエリを付けずにアクセスできます。

`/api` 利用時は UI のトレース一覧更新はポーリングで行われます（既定 5 秒間隔）。

## 運用向け: 保持期限の削除（purge-traces）

古いトレースを削除する場合は `rulemorph purge-traces` を利用します。

```sh
rulemorph purge-traces --retention-days 30 --dry-run
rulemorph purge-traces --retention-days 30
```

- `--retention-days` は必須です（0 はエラー）。
- 期限判定は `trace.json` の `timestamp`（RFC3339）を優先し、無い場合はファイルの更新日時を利用します。
- `trace.json` が存在するディレクトリはディレクトリごと削除します（旧形式単一JSONはファイル単位で削除）。
- `--dry-run` は削除対象の列挙のみで、実際の削除は行いません。

定期実行は運用側のスケジューラで行ってください（cron など）。
