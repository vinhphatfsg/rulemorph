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

## Coding Style
- Rust は rustfmt default に従う。手動で独自整形しない。
- 命名は Rust 慣例を優先する。functions/modules は `snake_case`、types は `CamelCase`、constants は `SCREAMING_SNAKE_CASE`。
- rule specs、examples、fixtures は `docs/` の仕様と矛盾させない。

## Design & Code Organization
- code は simple and direct に保つ。clever abstraction より、小さく読める関数と明示的な control flow を優先する。
- file が読みにくくなる前に責務を分割する。parsing、validation、execution、tracing、persistence、presentation が同じ module に混ざる場合は、明確な名前の focused submodule へ抽出する。
- file length は maintainability signal として扱う。通常の source file は実用上 200-400 行程度を目安にし、400 行を超えたら責務分割を積極的に検討する。600 行超は、cohesive ownership、generated/schema-like content、tests/fixtures などの理由がある場合だけ例外とする。
- file hierarchy は実装上の偶然ではなく domain responsibility に合わせる。reader が feature / concept 名から owning module を見つけられる構成にする。
- broad utility modules や catch-all files を避ける。shared helper は narrow purpose にし、真に cross-cutting でない限り primary caller の近くに置く。
- abstraction は、実際の重複を減らす、ownership を明確にする、public/internal boundary を安定させる場合だけ追加する。generalized に見せるための layer は追加しない。
- public API surface は小さく保つ。internal seam は `pub(crate)` を優先し、user や他 crate 向けに意図した型・関数だけを expose する。
- large file refactor では、可能な限り mechanical move と behavior change を分ける。split 後も既存 semantics を証明する tests を維持する。

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

## Testing Guidelines
- unit / integration tests は該当 crate の近くに追加する。
- new rule behavior では `crates/rulemorph/tests/fixtures` の fixture を追加または更新する。
- MCP behavior は `crates/rulemorph_mcp/tests/stdio.rs` などの stdio JSON-RPC tests で固定する。
- refactor では test を弱めない。削除ではなく移動であること、semantic case が残っていることを確認する。

## Task-Specific Guides
| 変更対象 | 先に読む guide |
| --- | --- |
| trace / transform / v2_eval / branch / finalize | `docs/agent-guides/semantic-trace.md` |
| input normalization / parser / records_path / resource limits / input docs | `docs/agent-guides/input-normalization-security.md` |
| release workflow / packaging / version bump / embedded-ui / UI assets | `docs/agent-guides/release-and-ui.md` |
| MCP server / tools / resources / prompts / stdio tests | `docs/agent-guides/mcp.md` |

## Commit & Pull Request Guidelines
- commit message は短く imperative にする。例: `Add DTO parsing for additional languages`、`Fix single-line TypeScript DTO parsing`。
- commit は 1 つの logical change に絞る。
- PR には brief summary、実行した tests、docs updates の有無を含める。CLI / MCP behavior を変えた場合は特に明記する。

## Configuration Tips
- MCP server は stdio で動く。client config 例は README を参照する。
