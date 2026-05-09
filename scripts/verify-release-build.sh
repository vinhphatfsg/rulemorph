#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ "$#" -gt 0 ]; then
  TARGETS=("$@")
else
  TARGETS=("$(rustc -vV | awk '/^host:/ { print $2 }')")
fi

echo "==> cargo fmt --check"
cargo fmt --check

echo "==> cargo metadata --locked"
cargo metadata --locked --format-version 1 --no-deps > /tmp/rulemorph-release-metadata.json

echo "==> cargo test"
cargo test

echo "==> npm build for embedded UI"
(
  cd crates/rulemorph_ui/ui
  npm ci
  npm run build
)

for TARGET in "${TARGETS[@]}"; do
  echo "==> release builds for ${TARGET}"
  cargo build -p rulemorph_cli --release --locked --target "$TARGET"
  cargo build -p rulemorph_mcp --release --locked --target "$TARGET"
  cargo build -p rulemorph_server --features embedded-ui --release --locked --target "$TARGET"
done
