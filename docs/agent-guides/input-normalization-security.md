# Input Normalization Security Guide

## When to read
- input normalization / parser / `records_path` / resource limits / input docs を変更する前に読む。
- JSON / CSV / YAML / TOML / XML / HTML / Markdown / Excel 入力、MCP input handling、input docs に触る場合も読む。

## Do not break
- normalized JSON records が `@input` になる。
- format-specific behavior は normalization layer に閉じ込め、mapping / steps / finalize に持ち込まない。
- missing / null / empty string の区別を壊さない。
- CSV / XML / HTML text に自動型推論を追加しない。
- scalar record selection は error のままにする。
- duplicate key、duplicate header、namespace/key collision、column collision は error のままにする。
- XML DTD / entity / processing instruction rejection を弱めない。
- HTML は JavaScript 実行、network fetch、unbounded DOM expansion を許可しない。
- Markdown は parser AST を public JSON contract にしない。raw HTML は文字列として保持してよいが、parse / sanitize / render / execute / network fetch は行わない。
- Markdown frontmatter は document 先頭だけを扱い、object root 以外、duplicate key、string 以外の key、custom tag を安全側で拒否する。
- Markdown resource limits は parse 後 count だけに依存しない。軽量 preflight または parse-time budget と parse 後 count の両方を維持する。
- Excel は macro、external relationship、formula evaluation、ambiguous workbook structure を許可しない。
- MCP pathless input や resource-limit override で parser security invariant を緩和しない。

## Required tests
- `cargo fmt`
- `cargo test -p rulemorph`
- Markdown を触る場合は `cargo test -p rulemorph --features markdown --test transform_golden markdown` と `cargo test -p rulemorph --no-default-features --test feature_flags markdown` も実行する。
- MCP behavior も変える場合は `cargo test -p rulemorph_mcp`
- release / embedded-ui surface も変える場合は `docs/agent-guides/release-and-ui.md` も読む。

## Notes
- 詳細仕様は既存の input normalization spec を参照する。主に `docs/rules_spec_ja.md` の Input normalization section と `docs/rules_spec_en.md`。
- 大きい変更では、format ごとの parser test と golden fixture の両方で確認する。
