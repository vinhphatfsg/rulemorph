#!/usr/bin/env python3
"""Audit test fixture directory references without deleting fixtures."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "crates" / "rulemorph" / "tests" / "fixtures"
SOURCE_GLOBS = (
    ("crates/rulemorph/tests", ("*.rs",)),
    ("crates/rulemorph_cli/tests", ("*.rs",)),
    ("crates/rulemorph_mcp/tests", ("*.rs",)),
    ("crates/rulemorph/tests/fixtures", ("*.yaml", "*.yml", "*.json", "*.toml")),
)

STRING_LITERAL_RE = re.compile(
    r'r#*"(?P<raw>.*?)"#*|"(?P<quoted>(?:[^"\\]|\\.)*)"', re.DOTALL
)
FIXTURE_PATH_RE = re.compile(r"tests/fixtures/([A-Za-z0-9_-]+)(?:/|$)")


def iter_source_files() -> list[Path]:
    files: list[Path] = []
    for root, patterns in SOURCE_GLOBS:
        source_root = REPO_ROOT / root
        if not source_root.exists():
            continue
        for pattern in patterns:
            files.extend(path for path in source_root.rglob(pattern) if path.is_file())
    return sorted(set(files))


def line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def decode_string(match: re.Match[str]) -> str:
    raw = match.group("raw")
    if raw is not None:
        return raw
    quoted = match.group("quoted") or ""
    try:
        return bytes(quoted, "utf-8").decode("unicode_escape")
    except UnicodeDecodeError:
        return quoted


def fixture_dirs() -> tuple[set[str], list[str], int]:
    dirs: set[str] = set()
    ignored_files: list[str] = []
    file_count = 0
    for child in sorted(FIXTURE_ROOT.iterdir()):
        if child.is_dir():
            dirs.add(child.name)
            continue
        if child.name.startswith("."):
            ignored_files.append(str(child.relative_to(REPO_ROOT)))
            continue
        file_count += 1
    for path in FIXTURE_ROOT.rglob("*"):
        if not path.is_file() or path.parent == FIXTURE_ROOT:
            continue
        if any(part.startswith(".") for part in path.relative_to(FIXTURE_ROOT).parts):
            ignored_files.append(str(path.relative_to(REPO_ROOT)))
            continue
        file_count += 1
    return dirs, sorted(ignored_files), file_count


def collect_refs(known_dirs: set[str]) -> tuple[dict[str, list[dict[str, object]]], list[str]]:
    refs: dict[str, list[dict[str, object]]] = {name: [] for name in known_dirs}
    ambiguous: list[str] = []

    for path in iter_source_files():
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        rel = str(path.relative_to(REPO_ROOT))

        for match in STRING_LITERAL_RE.finditer(text):
            value = decode_string(match)
            if value not in known_dirs:
                continue
            refs[value].append(
                {
                    "source": rel,
                    "line": line_number(text, match.start()),
                    "kind": "string-literal",
                }
            )

        for match in FIXTURE_PATH_RE.finditer(text):
            name = match.group(1)
            if name in known_dirs:
                refs[name].append(
                    {
                        "source": rel,
                        "line": line_number(text, match.start()),
                        "kind": "fixture-path",
                    }
                )
            else:
                ambiguous.append(f"{rel}:{line_number(text, match.start())}:{name}")

    return refs, sorted(set(ambiguous))


def build_report() -> dict[str, object]:
    dirs, ignored_files, file_count = fixture_dirs()
    refs, ambiguous = collect_refs(dirs)
    unused = sorted(name for name, entries in refs.items() if not entries)
    used = sorted(name for name, entries in refs.items() if entries)
    return {
        "summary": {
            "dirs": len(dirs),
            "used": len(used),
            "unused": len(unused),
            "files": file_count,
            "ignored": len(ignored_files),
        },
        "unused_dirs": unused,
        "refs_by_dir": {name: refs[name] for name in sorted(refs)},
        "ambiguous_refs": ambiguous,
        "ignored_files": ignored_files,
    }


def print_text(report: dict[str, object]) -> None:
    summary = report["summary"]
    print(
        "fixture refs: "
        f"dirs={summary['dirs']} "
        f"used={summary['used']} "
        f"unused={summary['unused']} "
        f"files={summary['files']} "
        f"ignored={summary['ignored']}"
    )
    print()

    print("UNUSED_DIRS:")
    unused = report["unused_dirs"]
    if unused:
        for name in unused:
            print(f"  {name}")
    else:
        print("  (none)")
    print()

    print("AMBIGUOUS_REFS:")
    ambiguous = report["ambiguous_refs"]
    if ambiguous:
        for ref in ambiguous:
            print(f"  {ref}")
    else:
        print("  (none)")
    print()

    print("IGNORED:")
    ignored = report["ignored_files"]
    if ignored:
        for path in ignored:
            print(f"  {path}")
    else:
        print("  (none)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    parser.add_argument(
        "--deny-unused",
        action="store_true",
        help="exit non-zero when fixture directories are not referenced",
    )
    args = parser.parse_args()

    report = build_report()
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print_text(report)

    if args.deny_unused and report["unused_dirs"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
