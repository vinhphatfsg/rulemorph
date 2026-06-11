# Rulemorph ドキュメント

このディレクトリは、README では省いた詳細仕様と運用手順を置く場所です。
初めて読む場合は、まず「変換ルール仕様」の全体像を読み、必要に応じて endpoint / network / UI guide へ進んでください。

## 読む順番

| 目的 | ドキュメント |
| --- | --- |
| 変換ルールの基本構造を理解する | [変換ルール仕様](rules_spec_ja.md) |
| `source` / `expr` / `when` / `steps` の挙動を確認する | [変換ルール仕様](rules_spec_ja.md) |
| CLI の stdin 入力、direct mode、resource limit を確認する | [変換ルール仕様: CLI input](rules_spec_ja.md#cli-input) / [Resource limits](rules_spec_ja.md#resource-limits) |
| 関数OPや数値OPなどのOP一覧と書き方を確認する | [変換ルール仕様: defs](rules_spec_ja.md#defs関数op) / [オペレーション一覧](rules_spec_ja.md#オペレーション一覧v2) |
| DTO 型推論の対象と fallback を確認する | [変換ルール仕様: DTO 型推論](rules_spec_ja.md#dto-型推論) |
| 入力形式ごとの正規化ルールを確認する | [変換ルール仕様: Input](rules_spec_ja.md#input) |
| YAML で HTTP endpoint を定義する | [Endpoint ルール仕様](rules_spec_endpoint_ja.md) |
| HTTP request の `@input` 構造、`steps`、`reply` を確認する | [Endpoint ルール仕様: input](rules_spec_endpoint_ja.md#input) / [steps](rules_spec_endpoint_ja.md#steps) / [reply](rules_spec_endpoint_ja.md#reply) |
| YAML から外部 HTTP request を行う | [Network ルール仕様](rules_spec_network_ja.md) |
| network rule の request/body/select/catch と SSRF 制約を確認する | [Network ルール仕様: request](rules_spec_network_ja.md#request) / [body](rules_spec_network_ja.md#body) / [select](rules_spec_network_ja.md#select) / [SSRF対策](rules_spec_network_ja.md#運用向けメモssrf対策) |
| ローカル UI/API サーバーを起動する | [UI Server Guide](guide/ui-run-and-verify.md) |
| `.rulemorph` の data directory を理解する | [UI Data Directory](guide/ui-data-dir-usage.md) |
| 用語の揺れを確認する | [用語集](glossary.md) |

## 仕様の分担

Rulemorph の仕様は、用途ごとに分けています。

- `normal` 変換: 入力を JSON record に正規化し、`mappings` / `steps` / `finalize` で出力を作る。詳細は [変換ルール仕様](rules_spec_ja.md)。
- `endpoint` ルール: HTTP request を受け、YAML で処理と reply を定義する。詳細は [Endpoint ルール仕様](rules_spec_endpoint_ja.md)。
- `network` ルール: YAML から外部 HTTP request を実行し、結果を選択して返す。詳細は [Network ルール仕様](rules_spec_network_ja.md)。

共通の参照構文、条件構文、v2 pipe expression、`missing` / `null` の扱いは [変換ルール仕様](rules_spec_ja.md) を正とします。

## endpoint / network と UI/API server

`crates/rulemorph_endpoint` は endpoint / network rule の実行、HTTP request 正規化、reply 構築、network request 実行、trace graph 生成を担当します。
`crates/rulemorph_server` は local/API server、tenant dispatch、UI 配信、rules mode の `/api/*` routing を担当します。

- endpoint rule の最小構成、`@input` の JSON shape、`steps` / `catch` / `reply` の評価順は [Endpoint ルール仕様](rules_spec_endpoint_ja.md) を確認してください。
- network rule の `request.url` / `request.headers` / `body` / `select` / `catch` と timeout / retry / SSRF 制約は [Network ルール仕様](rules_spec_network_ja.md) を確認してください。
- UI server での `/api/import`、`endpoint.yaml` 優先、専用 handler fallback、tenant ごとの rule dispatch は [UI Server Guide](guide/ui-run-and-verify.md) と [UI Data Directory](guide/ui-data-dir-usage.md) を確認してください。
- Trace Console / API graph は endpoint/network 実行結果の診断面です。表示や import flow を変更する場合は `docs/agent-guides/endpoint.md` と `docs/agent-guides/release-and-ui.md` を先に読んでください。

## README に置かない情報

README は「何を解決するか」「最初にどう動かすか」「どの binary を使うか」に絞っています。
以下のような情報は、README ではなく docs 側で管理します。

- 入力形式ごとの細かな正規化ルール
- safety invariant と resource limit の詳細
- `steps` / `branch` / `finalize` の評価順
- endpoint / network の現在の制約
- UI server の全オプション
- data directory と trace / import の運用詳細
