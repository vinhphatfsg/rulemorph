# テナント構成

Rulemorph でテナント分離を有効にした場合のデータ構成と API キー管理をまとめます。

## ディレクトリ構成

`data_dir` 配下に `tenants/<tenant_id>/` が作成され、テナントごとにデータが分離されます。

```
<data_dir>/tenants/
└── <tenant_id>/
    ├── traces/
    ├── rules/
    ├── api_rules/
    └── auth/
        └── api_keys.json
```

- `traces/`: テナントのトレースデータ
- `rules/`: トレース参照用ルール
- `api_rules/`: `/api/*` 用のルール
- `auth/`: APIキー情報

### 初期化タイミング

- テナント領域は遅延初期化です。`tenant_id` が実際に参照された時点で `tenants/<tenant_id>/` が作成されます。
- 起動時に `default` テナントは必須ではありません。`tenants/tenant-1/api_rules` のみが存在する構成でも動作します。

## APIキー形式

- 形式: `rmk_<tenant_id>.<secret>`
- `<secret>` は 32byte 以上のランダム値（base64url）
- サーバ側はハッシュのみ保存します（平文は保存しません）

## api_keys.json

`auth/api_keys.json` のフォーマット:

```json
{
  "version": 1,
  "salt": "...",
  "keys": [
    {
      "id": "...",
      "prefix": "rmk_<tenant_id>....",
      "hash": "...",
      "created_at": "2026-02-05T00:00:00Z",
      "revoked_at": null,
      "label": "..."
    }
  ]
}
```

- `prefix`: 表示用の先頭文字列（秘密部は保持しません）
- `hash`: `sha256(salt + api_key)`

## CLI 操作

発行:

```sh
rulemorph api-keys issue --tenant-id <tenant_id>
```

一覧:

```sh
rulemorph api-keys list --tenant-id <tenant_id>
```

失効:

```sh
rulemorph api-keys revoke --tenant-id <tenant_id> --id <key_id>
```

ローテーション:

```sh
rulemorph api-keys rotate --tenant-id <tenant_id> --id <key_id>
```

## 運用（推奨）

1. 新しいキーを `issue` で発行
2. クライアントを新キーへ切り替え
3. 旧キーを `revoke` で失効

## 内部API（管理）

内部APIの API キー操作は `internal_api_key` と `x-tenant-id` が揃っている場合のみ許可されます。

- `GET /internal/api-keys`
- `POST /internal/api-keys`
- `POST /internal/api-keys/:id/revoke`
- `POST /internal/api-keys/:id/rotate`

同一テナントの `api_keys.json` 更新（issue/revoke/rotate）はサーバ内で直列化され、同時更新時の上書きロストを防ぎます。
