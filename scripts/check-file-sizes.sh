#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WARN_LIMIT="${WARN_LIMIT:-400}"
HARD_LIMIT="${HARD_LIMIT:-600}"
FAIL_MODE=0

if [[ "${1:-}" == "--fail" ]]; then
  FAIL_MODE=1
fi

over_warn=0
over_hard=0

echo "Rulemorph source file size report"
echo "warn: >${WARN_LIMIT} lines; report threshold: >${HARD_LIMIT} lines"
echo

while IFS= read -r file; do
  lines="$(wc -l < "$file" | tr -d ' ')"
  rel="${file#"$ROOT_DIR"/}"
  if (( lines > WARN_LIMIT )); then
    over_warn=$((over_warn + 1))
    marker="WARN"
    if (( lines > HARD_LIMIT )); then
      over_hard=$((over_hard + 1))
      marker="OVER"
    fi
    printf "%5d  %-4s  %s\n" "$lines" "$marker" "$rel"
  fi
done < <(find "$ROOT_DIR/crates" -path "*/src/*.rs" -type f | sort)

echo
echo "summary: ${over_warn} files over ${WARN_LIMIT} lines; ${over_hard} files over ${HARD_LIMIT} lines"

if (( FAIL_MODE == 1 && over_hard > 0 )); then
  exit 1
fi
