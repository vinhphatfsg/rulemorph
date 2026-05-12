# Repository Guidelines

## Language
- Japanese-first for docs, plans, and communication. Provide English only when explicitly requested.
- 用語集は `docs/glossary.md` を参照。

## Project Structure & Module Organization
- `crates/rulemorph`: core library (rule parsing, validation, transform engine).
- `crates/rulemorph_cli`: CLI binary wrapping the core library.
- `crates/rulemorph_mcp`: MCP stdio server with tools/resources/prompts.
- `docs/`: rule specifications and documentation (`rules_spec_en.md`, `rules_spec_ja.md`).
- `docs/roadmap/roadmap.md`: product roadmap (YAML API platform).
- Tests live under each crate (for example, `crates/rulemorph/tests` with fixtures in `crates/rulemorph/tests/fixtures` and MCP tests in `crates/rulemorph_mcp/tests/stdio.rs`).

## Build, Test, and Development Commands
- `cargo build -p rulemorph_cli --release`: build the CLI binary.
- `cargo build -p rulemorph_mcp --release`: build the MCP server binary.
- `cargo test`: run the full workspace test suite.
- `cargo test -p rulemorph_mcp`: run MCP-specific tests.
- `cargo run -p rulemorph_cli -- --help`: run the CLI in dev mode.
- `scripts/verify-release-build.sh`: run the release preflight that mirrors the release workflow on the host target, including `cargo fmt --check`, `cargo metadata --locked`, `cargo test`, UI asset build, and release builds for CLI/MCP/server with `embedded-ui`.
- `scripts/verify-release-build.sh <target> [...]`: run the same release preflight for explicit Rust targets when checking release matrix compatibility.
- `cargo fmt` and `cargo clippy --workspace`: format and lint (recommended before PRs).
- ** Rustのソースを実装/修正後 ** は必ず `cargo fmt` と `cargo test` を実行すること。
- ** release workflow / release packaging / version bump / embedded-ui 周辺を修正した場合 ** は必ず `scripts/verify-release-build.sh` を実行すること。`cargo test` だけでは `rulemorph_server --features embedded-ui` の release build 不具合を検知できない。
- UIを修正した場合は必ずブラウザ上で挙動を確認すること。

## Coding Style & Naming Conventions
- Use standard Rust formatting (rustfmt defaults, 4-space indentation).
- Naming: `snake_case` for functions/modules, `CamelCase` for types, `SCREAMING_SNAKE_CASE` for constants.
- Keep rule specs and examples consistent with the docs in `docs/`.

## Design & Code Organization
- Keep code simple and direct. Prefer small, readable functions and explicit control flow over clever abstractions.
- Split responsibilities before files become hard to scan. When a module mixes parsing, validation, execution, tracing, persistence, or presentation concerns, extract focused submodules with clear names.
- Treat file length as a maintainability signal. Aim to keep ordinary source files around 200-400 lines when practical; when a file exceeds roughly 400 lines, actively look for a responsibility split. Files above 600 lines should be exceptional and justified by cohesive ownership, generated/schema-like content, or tests/fixtures.
- Keep file hierarchy aligned with domain responsibilities, not implementation accidents. A reader should be able to find the owning module from the feature or concept name.
- Avoid broad utility modules and catch-all files. Shared helpers should have a narrow purpose and live near their primary caller unless they are truly cross-cutting.
- Add abstractions only when they reduce real duplication, clarify ownership, or stabilize a public/internal boundary. Do not introduce layers just to make code look generalized.
- Preserve a small public API surface. Prefer `pub(crate)` for internal seams, and expose only the types/functions that are intended for users or other crates.
- When refactoring large files, keep behavior changes separate from mechanical moves where practical, and maintain tests that prove the split preserved existing semantics.

## Testing Guidelines
- Add unit/integration tests alongside the relevant crate.
- For new rule behavior, add or extend fixtures in `crates/rulemorph/tests/fixtures`.
- MCP behavior should include stdio JSON-RPC tests in `crates/rulemorph_mcp/tests/stdio.rs`.

## Commit & Pull Request Guidelines
- Commit messages are short, imperative, and sentence case (examples: "Add DTO parsing for additional languages", "Fix single-line TypeScript DTO parsing").
- Keep commits focused on one logical change.
- PRs should include: a brief summary, tests run, and any doc updates (especially when CLI/MCP behavior changes).

## Configuration Tips
- MCP server runs over stdio; see the README for client config examples.
