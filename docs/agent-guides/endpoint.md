# Endpoint / Network Agent Guide

## When to read
- `crates/rulemorph_endpoint`、`crates/rulemorph_server/src/server/rules_api.rs`、`assets/api_rules/`、endpoint / network rule docs に触る前に読む。
- HTTP request / reply、network request、SSRF、internal auth、tenant routing、trace graph 生成、ZIP import の rule 経由処理を変更する前に読む。
- endpoint / network の trace 表示や UI import flow に触る場合は、この guide と `release-and-ui.md` の両方を読む。

## Do not break
- `endpoint` ルールは HTTP request を `@input` に正規化し、`steps` の結果から `reply` を構築する。request の method/path/query/header/body の既存 JSON shape を変えない。
- `network` ルールは外部 HTTP request という副作用を持つ。SSRF allowlist、scheme/host/private IP 判定、redirect、timeout、retry、internal auth の制御を弱めない。
- `/api/*` は rules mode では `endpoint.yaml` が優先される。専用 axum handler fallback との優先順位、tenant ごとの rule dispatch、`x-rulemorph-import: zip` の扱いを変えない。
- tenant / API key / internal key の境界をまたいで rules、trace、import bundle、data directory を共有しない。
- multipart / ZIP import は、展開先、bundle path、trace/rule import、temporary directory cleanup の順序と containment を維持する。archive member path を trust しない。
- trace graph は診断用出力であり、runtime の成否や reply 生成の truth source にしない。trace node / edge の追加は既存 UI と API graph consumer が読める shape を保つ。
- docs-only 変更では endpoint/network runtime、assets、tests、workflow を変更しない。

## Required tests
- endpoint/network runtime を変更したら最低限 `cargo test -p rulemorph_endpoint` を実行する。
- server routing、tenant dispatch、rules API、import flow を変更したら `cargo test -p rulemorph_server` も実行する。
- trace graph 生成を変更したら endpoint trace graph tests と UI graph/import e2e の影響を確認する。
- UI import flow や Trace Console へ波及する変更では `npm --prefix crates/rulemorph_ui/ui run test` と `npm --prefix crates/rulemorph_ui/ui run test:e2e` を実行し、ブラウザで確認する。
- security boundary に触れたら、変更範囲に対して codex-security の差分レビューを行う。

## Notes
- 仕様の入口は `docs/rules_spec_endpoint_ja.md` と `docs/rules_spec_network_ja.md`。
- UI/API server の起動と ZIP import の運用確認は `docs/guide/ui-run-and-verify.md` と `docs/guide/ui-data-dir-usage.md`。
- `endpoint` / `network` も参照構文、条件構文、v2 pipe expression は `docs/rules_spec_ja.md` を正とする。
