# MCP Guide

## When to read
- MCP server / tools / resources / prompts / stdio tests を変更する前に読む。
- `crates/rulemorph_mcp`、MCP manifest、MCP-facing schemas、MCP input handling に触る場合も読む。

## Do not break
- MCP server は stdio。
- tools / resources / prompts の JSON-RPC behavior と schemas を不用意に変えない。
- pathless `rules_text` に filesystem base dir を与えない。
- allowed roots、path validation、inline input handling の security boundary を弱めない。
- core behavior を変える場合は、MCP adapter だけでなく core crate の contract も確認する。

## Required tests
- `cargo fmt`
- `cargo test -p rulemorph_mcp`
- core behavior も変えるなら `cargo test -p rulemorph`
- workspace surface が広い場合は `cargo test`

## Notes
- MCP stdio behavior は `crates/rulemorph_mcp/tests/stdio.rs` を優先して確認する。
- input normalization を MCP 経由で変える場合は `docs/agent-guides/input-normalization-security.md` も読む。
