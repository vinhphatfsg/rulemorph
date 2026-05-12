# Repository Guidelines

## Language
- 返答、docs、plans は日本語 first。英語は明示された場合だけ使う。
- 用語は `docs/glossary.md` を優先する。

## Always
- 既存の仕様、fixtures、tests、README と矛盾する変更をしない。
- Rust 実装を変更したら必ず `cargo fmt` と `cargo test` を実行する。
- release workflow / packaging / version bump / embedded-ui を変更したら `scripts/verify-release-build.sh` を実行する。
- UI を変更したら `npm --prefix crates/rulemorph_ui/ui run test` とブラウザ確認を実行する。
- UI の end-to-end behavior / Trace Console / import flow を変更したら `npm --prefix crates/rulemorph_ui/ui run test:e2e` も実行する。
- 大きい挙動変更では、先に該当 guide を読み、必要な invariant と tests を確認する。
- docs-only 変更では、実装・tests・workflow の挙動を変えない。

## Project Map
- `crates/rulemorph`: core library。rule parsing、validation、normalization、transform engine。
- `crates/rulemorph_cli`: CLI binary。
- `crates/rulemorph_mcp`: MCP stdio server。tools、resources、prompts。
- `crates/rulemorph_server`: local/API server と UI 配信。
- `crates/rulemorph_ui`: embedded-ui 用 frontend assets。
- `docs/`: rule specs、design docs、roadmap、agent guides。
- `crates/*/tests`: crate ごとの integration tests と fixtures。

## Common Commands
- `cargo fmt`: Rust formatting。
- `cargo fmt --check`: CI / release preflight と同じ format check。
- `cargo test`: workspace test suite。
- `cargo test -p rulemorph`: core crate tests。
- `cargo test -p rulemorph_mcp`: MCP-specific tests。
- `cargo clippy --workspace`: workspace lint。
- `npm --prefix crates/rulemorph_ui/ui run test`: UI unit tests。`cargo test` では実行されない。
- `npm --prefix crates/rulemorph_ui/ui run test:e2e`: UI Playwright tests。`cargo test` では実行されない。
- `npm --prefix crates/rulemorph_ui/ui run build`: UI asset build。`cargo test` では実行されない。
- `cargo run -p rulemorph_cli -- --help`: CLI dev run。
- `cargo build -p rulemorph_cli --release`: release CLI build。
- `cargo build -p rulemorph_mcp --release`: release MCP build。
- `cargo build -p rulemorph_server --features embedded-ui --release --locked`: embedded-ui server release build。`cargo test` では実行されない。
- `scripts/verify-release-build.sh`: release preflight。`cargo test` では実行されない UI build と release builds も含む。
- `scripts/verify-release-build.sh <target> [...]`: explicit Rust targets の preflight。

## Task-Specific Guides
| 変更対象 | 先に読む guide |
| --- | --- |
| trace / transform / v2_eval / branch / finalize | `docs/agent-guides/semantic-trace.md` |
| input normalization / parser / records_path / resource limits / input docs | `docs/agent-guides/input-normalization-security.md` |
| release workflow / packaging / version bump / embedded-ui / UI assets | `docs/agent-guides/release-and-ui.md` |
| MCP server / tools / resources / prompts / stdio tests | `docs/agent-guides/mcp.md` |
