# UI運用ガイド（data_dir 直置き）

Rulemorph UI は `data_dir` 配下の **traces / rules** を読み込みます。
ユーザーAPI用のルールは **api_rules** に配置します。
サンプル自動生成は行わないため、運用時はファイルを直接配置してください。

UIは `/internal/stream`（SSE）で更新通知を受け取り、トレースの追加を自動反映します。
トレース保存は **ファイル保存（file backend）** を前提とした構成です（将来的にDB/オブジェクトへ差し替え可能）。

## 既定の保存先

- 既定 `data_dir`: `./.rulemorph`
- trace 保存先: `./.rulemorph/traces`
- rule 保存先: `./.rulemorph/rules`（トレースに紐づくルール参照用）
- user API ルール保存先（既定）: `./.rulemorph/api_rules`

## プロジェクトローカルで運用する場合（推奨）

- `data_dir`: `./.rulemorph`
- git 管理外にするため `.rulemorph/` は `.gitignore` に追加済み

## ファイル配置ルール

### traces

- `./.rulemorph/traces/**/*.json`
- JSON 1ファイル = 1 trace

例:
```
./.rulemorph/traces/2026/01/27/trace-users-001.json
./.rulemorph/traces/2026/01/27/trace-users-002.json
```

### rules（トレース参照用）

- `./.rulemorph/rules/**.yaml`
- trace 内で参照される `rule.path` / `meta.rule_ref` に一致するパスで配置

例:
```
./.rulemorph/rules/users/endpoint.yaml
./.rulemorph/rules/users/get.yaml
./.rulemorph/rules/users/list.yaml
```

### api_rules（ユーザーAPI用）

- `./.rulemorph/api_rules/**/endpoint.yaml`
- UI を rules モードで起動した時に `/api/*` を提供するルール群

例:
```
./.rulemorph/api_rules/endpoint.yaml
./.rulemorph/api_rules/network/list.yaml
./.rulemorph/api_rules/network/get.yaml
```

## 起動コマンド

```sh
cargo run -p rulemorph_cli -- ui --api-mode ui-only --port 8080 --data-dir ./.rulemorph
```

ローカル運用（推奨）:

```sh
cargo run -p rulemorph_cli -- ui --api-mode ui-only --port 8080 --data-dir ./.rulemorph
```

ユーザーAPIを有効にする場合（rules モード）:

```sh
cargo run -p rulemorph_cli -- ui --api-mode rules --port 8080 --data-dir ./.rulemorph
```

UIを無効化する場合:

```sh
cargo run -p rulemorph_cli -- ui --api-mode rules --no-ui --port 8080 --data-dir ./.rulemorph
```

## 反映されないとき

1. `data_dir` が合っているか確認  
2. `traces/` に JSON があるか確認  
3. 8080 を掴んでいる古いプロセスがいないか確認  

```
lsof -nP -iTCP:8080 -sTCP:LISTEN
```
