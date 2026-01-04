#!/usr/bin/env python3
# scripts/data_collector.py
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Any

BIN_ENV = "XTRADER_BIN"
DEFAULT_BIN = Path("/root/X-Trader/bin/demo")
SCRIPT_DIR = Path(__file__).resolve().parent
BATCH_FILE = SCRIPT_DIR / "batch.json"
CODE_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


def load_codes(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"`{path}` not found")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    # Support direct list, or dict containing a list under common keys, or first list found
    if isinstance(data, list):
        raw = data
    elif isinstance(data, dict):
        for key in ("codes", "products", "list"):
            if key in data and isinstance(data[key], list):
                raw = data[key]
                break
        else:
            # find first list value
            lists = [v for v in data.values() if isinstance(v, list)]
            if lists:
                raw = lists[0]
            else:
                raise ValueError("No list of codes found in JSON")
    else:
        raise ValueError("Unsupported JSON structure for codes")
    # Normalize to strings
    return [str(x).strip() for x in raw]


def sanitize(code: str) -> bool:
    return bool(CODE_PATTERN.fullmatch(code))


def get_bin_path() -> Path:
    env = os.environ.get(BIN_ENV)
    if env:
        return Path(env)
    return DEFAULT_BIN


def run_for_code(bin_path: Path, code: str) -> subprocess.CompletedProcess:
    # The target expects `-c期货产品编号` without space per requirement
    cmd = [str(bin_path), f"-c{code}"]
    return subprocess.run(cmd, capture_output=True, text=True)


def main():
    try:
        codes = load_codes(BATCH_FILE)
    except Exception as e:
        print(f"Error loading `batch.json`: {e}", file=sys.stderr)
        sys.exit(2)

    if not codes:
        print("No codes found in `batch.json`", file=sys.stderr)
        sys.exit(0)

    bin_path = get_bin_path()
    if not bin_path.exists() or not os.access(bin_path, os.X_OK):
        print(f"Executable not found or not executable: `{bin_path}`", file=sys.stderr)
        # do not exit — allow user to see which codes would have been run
    for code in codes:
        if not code:
            print("Skipping empty code entry")
            continue
        if not sanitize(code):
            print(f"Skipping invalid code: {code}")
            continue
        if not bin_path.exists() or not os.access(bin_path, os.X_OK):
            print(f"Would run: `{bin_path}` -c{code}  (executable missing)", file=sys.stderr)
            continue
        print(f"Running for code: {code}")
        try:
            result = run_for_code(bin_path, code)
        except Exception as e:
            print(f"Execution failed for {code}: {e}", file=sys.stderr)
            continue
        print(f"Return code: {result.returncode}")
        if result.stdout:
            print("Stdout:")
            print(result.stdout.strip())
        if result.stderr:
            print("Stderr:")
            print(result.stderr.strip())


if __name__ == "__main__":
    main()
