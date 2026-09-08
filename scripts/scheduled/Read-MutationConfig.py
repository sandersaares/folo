"""Decode trusted cargo-mutants configuration using Python's standard TOML parser."""

import json
import sys
import tomllib

config = tomllib.loads(sys.stdin.read())
# Baseline-relevant defaults follow the pinned cargo-mutants options implementation:
# https://github.com/sourcefrog/cargo-mutants/blob/v27.1.0/src/options.rs
defaults = {
    "additional_cargo_args": [],
    "additional_cargo_test_args": [],
    "all_features": False,
    "cap_lints": False,
    "features": [],
    "no_default_features": False,
    "profile": None,
    "test_tool": "cargo",
}
result = {}
for key, default in defaults.items():
    value = config.get(key, default)
    expected = str if key == "profile" else type(default)
    if value is not None or key != "profile":
        if type(value) is not expected:
            raise ValueError(f"Invalid cargo-mutants configuration field: {key}")
    if isinstance(value, list) and any(not isinstance(item, str) for item in value):
        raise ValueError(f"Invalid cargo-mutants argument list: {key}")
    result[key] = value
if result["test_tool"] not in ("cargo", "nextest"):
    raise ValueError("Unsupported cargo-mutants test tool")
json.dump(result, sys.stdout)
