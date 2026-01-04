#!/usr/bin/env python3
# scripts/data_collector.py
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List

BIN_ENV = "XTRADER_BIN"
DEFAULT_BIN = Path("/root/X-Trader/bin/demo")
BIN_DIR = Path("/root/X-Trader/bin")
SCRIPT_DIR = Path(__file__).resolve().parent
BATCH_FILE = SCRIPT_DIR / "batch.json"
CODE_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


def load_codes(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"`{path}` not found")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        raw = data
    elif isinstance(data, dict):
        for key in ("codes", "products", "list"):
            if key in data and isinstance(data[key], list):
                raw = data[key]
                break
        else:
            lists = [v for v in data.values() if isinstance(v, list)]
            if lists:
                raw = lists[0]
            else:
                raise ValueError("No list of codes found in JSON")
    else:
        raise ValueError("Unsupported JSON structure for codes")
    return [str(x).strip() for x in raw]


def sanitize(code: str) -> bool:
    return bool(CODE_PATTERN.fullmatch(code))


def get_bin_path() -> Path:
    env = os.environ.get(BIN_ENV)
    if env:
        return Path(env)
    return DEFAULT_BIN


def process_running(bin_path: Path, code: str) -> bool:
    # 匹配完整命令行包含二进制路径和 -c{code}
    pattern = f"{str(bin_path)}.*-c{code}"
    try:
        res = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True)
        return res.returncode == 0
    except FileNotFoundError:
        # pgrep 不可用，回退到 ps 检查
        res = subprocess.run(["ps", "aux"], capture_output=True, text=True)
        for line in res.stdout.splitlines():
            if str(bin_path) in line and f"-c{code}" in line and "grep" not in line:
                return True
        return False


def start_in_screen(bin_path: Path, code: str) -> subprocess.CompletedProcess:
    session_name = f"xtrader_{code}"
    # screen -dmS <name> <cmd> <args...>
    cmd = ["screen", "-dmS", session_name, str(bin_path), f"-c{code}"]
    return subprocess.run(cmd, capture_output=True, text=True)


def start_background(bin_path: Path, code: str) -> subprocess.Popen:
    # 作为回退，在后台启动进程（不阻塞当前脚本）
    cmd = [str(bin_path), f"-c{code}"]
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


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
        # 继续以便输出将要运行的内容

    screen_available = shutil.which("screen") is not None
    if not BIN_DIR.exists():
        print(f"Warning: working directory `{BIN_DIR}` does not exist; will not change cwd before execution", file=sys.stderr)

    for code in codes:
        if not code:
            print("Skipping empty code entry")
            continue
        if not sanitize(code):
            print(f"Skipping invalid code: {code}")
            continue

        if process_running(bin_path, code):
            print(f"Skipping {code}: same command already running")
            continue

        if not bin_path.exists() or not os.access(bin_path, os.X_OK):
            print(f"Would run: `{bin_path}` -c{code}  (executable missing)", file=sys.stderr)
            continue

        print(f"Starting for code: {code}")
        old_cwd = Path.cwd()
        try:
            if BIN_DIR.exists():
                os.chdir(str(BIN_DIR))
            if screen_available:
                try:
                    result = start_in_screen(bin_path, code)
                    if result.returncode == 0:
                        print(f"Started in screen session: xtrader_{code}")
                    else:
                        print(f"screen start failed for {code}, returncode={result.returncode}", file=sys.stderr)
                        if result.stdout:
                            print(result.stdout.strip(), file=sys.stderr)
                        if result.stderr:
                            print(result.stderr.strip(), file=sys.stderr)
                except Exception as e:
                    print(f"Failed to start screen for {code}: {e}", file=sys.stderr)
            else:
                # 回退到简单的后台启动
                try:
                    proc = start_background(bin_path, code)
                    print(f"Started in background (fallback), pid={proc.pid}")
                except Exception as e:
                    print(f"Failed to start background process for {code}: {e}", file=sys.stderr)
        finally:
            try:
                os.chdir(str(old_cwd))
            except Exception:
                pass


if __name__ == "__main__":
    main()