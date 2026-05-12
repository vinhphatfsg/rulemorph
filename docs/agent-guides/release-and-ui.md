# Release And UI Guide

## When to read
- release workflow / release packaging / version bump / embedded-ui / UI assets を変更する前に読む。
- `scripts/verify-release-build.sh`、GitHub Actions release workflow、asset embedding、UI build inputs に触る場合も読む。

## Do not break
- release workflow と local release preflight の対応をずらさない。
- version bump は manifest 1 個だけで済ませず、workspace の versioned files をまとめて確認する。
- embedded-ui build が release build に含まれる前提を壊さない。
- UI asset の build output、server embedding、runtime route の整合を壊さない。

## Required tests
- `cargo fmt`
- `cargo test`
- `scripts/verify-release-build.sh`
- UI 変更時はブラウザ確認

## Notes
- `cargo test` だけでは embedded-ui release build 問題を検知できない。
- `scripts/verify-release-build.sh` は release workflow に近い preflight として、fmt、metadata、tests、UI asset build、CLI / MCP / server release builds をまとめて確認する。
- sandbox で local bind 制約に当たった場合は、環境制約と実装不具合を切り分けて報告する。
