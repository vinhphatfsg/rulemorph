# UIデータディレクトリ構成

Rulemorph UIが使用するデータディレクトリの構成と配置ルールをまとめたガイドです。

## ディレクトリ構成

デフォルトのデータディレクトリは `./.rulemorph` です。

```
./.rulemorph/
├── traces/          # トレースファイル（JSON）
├── rules/           # トレース参照用ルール（YAML）
└── api_rules/       # カスタムAPI用ルール（YAML）
```

| ディレクトリ | 用途 |
|-------------|------|
| `traces/` | 変換実行のトレースログ（1ファイル = 1トレース） |
| `rules/` | トレース内で参照されるルールファイル |
| `api_rules/` | `/api/*` エンドポイントを定義するルール |

> `.rulemorph/` は `.gitignore` に追加することを推奨します。

## ファイル配置ルール

### traces/

トレースファイルは `traces/` 配下に配置します。サブディレクトリは任意ですが、日付形式での整理を推奨します。

```
./.rulemorph/traces/
├── 2025/01/01/
│   ├── trace-users-001.json
│   └── trace-users-002.json
└── 2025/01/02/
    └── trace-orders-001.json
```

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

## トラブルシューティング

トレースが反映されない場合：

1. `--data-dir` の指定が正しいか確認
2. `traces/` にJSONファイルが存在するか確認
3. ポートを掴んでいる古いプロセスがないか確認

```sh
lsof -nP -iTCP:8080 -sTCP:LISTEN
```

起動方法の詳細は [ui-run-and-verify.md](ui-run-and-verify.md) を参照してください。
