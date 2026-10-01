#!/usr/bin/env python3
"""Independent Draft 2020-12 validation of generated Rust success payloads."""
import json
import pathlib
import subprocess
from jsonschema import Draft202012Validator

root = pathlib.Path(__file__).resolve().parent.parent
process = subprocess.run(
    ["cargo", "run", "--quiet", "--locked", "-p", "rulemorph_output", "--example", "independent_cases"],
    cwd=root, text=True, capture_output=True, check=True,
)
successes = failures = 0
for line in process.stdout.splitlines():
    case = json.loads(line)
    if case["ok"]:
        Draft202012Validator.check_schema(case["schema"])
        Draft202012Validator(case["schema"]).validate(case["value"])
        successes += 1
    else:
        failures += 1
assert successes >= 400, (successes, failures)
assert failures >= 20, (successes, failures)
print(f"Independent Python validator: {successes} complete Ok payloads valid; {failures} explicit failures")
