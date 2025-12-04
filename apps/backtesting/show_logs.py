# python
from pathlib import Path
import argparse
import time
from typing import Dict, Tuple, Optional
from datetime import datetime
import sys

def parse_args() -> Tuple[Path, Optional[str], float]:
    epilog = (
        "示例:\n"
        "  python show_logs.py                # 监控当前目录下的 ./logs，间隔 1 秒\n"
        "  python show_logs.py /var/logs -i 5 # 每 5 秒轮询一次 /var/logs\n"
        "  python show_logs.py -f app_%YYYY%-%MM%-%DD%.log\n"
        "说明:\n"
        "  支持的占位符: %YYYY% 年 (4 位), %MM% 月 (2 位), %DD% 日 (2 位)\n"
        "  当使用 -f 指定模板时，程序每次轮询都会按当前日期解析模板并监控对应文件的新增行。\n"
    )
    parser = argparse.ArgumentParser(
        description="监控日志目录变化并可按文件模板打印新增行（默认目录: ./logs）",
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "logs_dir",
        nargs="?",
        default="./logs",
        help="要监控的日志目录，默认为 ./logs",
    )
    parser.add_argument(
        "-f",
        "--file",
        dest="target_file",
        help="仅监控指定的文件名或模板（支持 %YYYY% %MM% %DD%），只匹配文件名，不含路径",
    )
    parser.add_argument(
        "-i",
        "--interval",
        dest="interval",
        type=float,
        default=1.0,
        help="轮询间隔（秒），支持浮点数，必须大于 0，默认为 1.0",
    )
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("参数 --interval 必须大于 0")
    return Path(args.logs_dir).expanduser().resolve(), args.target_file, float(args.interval)

def ensure_dir(p: Path) -> None:
    if not p.exists():
        p.mkdir(parents=True, exist_ok=True)

def resolve_template(template: Optional[str], dt: Optional[datetime] = None) -> Optional[str]:
    if template is None:
        return None
    if dt is None:
        dt = datetime.now()
    s = template
    s = s.replace("%YYYY%", dt.strftime("%Y"))
    s = s.replace("%MM%", dt.strftime("%m"))
    s = s.replace("%DD%", dt.strftime("%d"))
    return s

def scan_files(dir_path: Path, target_name: Optional[str] = None) -> Dict[Path, Tuple[float, int]]:
    result: Dict[Path, Tuple[float, int]] = {}
    try:
        for entry in dir_path.iterdir():
            if not entry.is_file():
                continue
            if target_name is not None and entry.name != target_name:
                continue
            try:
                st = entry.stat()
                result[entry] = (st.st_mtime, st.st_size)
            except OSError:
                continue
    except OSError:
        pass
    return result

def _print_new_content(path: Path, prev_pos: int) -> int:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fp:
            fp.seek(0, 2)
            curr_size = fp.tell()
            if curr_size < prev_pos:
                prev_pos = 0
            fp.seek(prev_pos)
            data = fp.read()
            if data:
                for line in data.splitlines():
                    print(line)
            return fp.tell()
    except OSError:
        return prev_pos

def monitor(dir_path: Path, target_template: Optional[str] = None, interval: float = 1.0) -> None:
    print(f"开始监控目录: {dir_path}")
    if target_template:
        resolved = resolve_template(target_template)
        print(f"仅监控文件模板: `{target_template}` -> 当前解析为: `{resolved}`")
    else:
        resolved = None
        print("不使用文件模板，监控目录下所有文件的新增/修改/删除事件。")
    print(f"轮询间隔: {interval} 秒")

    known = scan_files(dir_path, resolved)
    positions: Dict[Path, int] = {}
    for p, (mtime, size) in sorted(known.items()):
        print(f"已存在: {p.name} (mtime={mtime})")
        positions[p] = size

    try:
        while True:
            time.sleep(interval)
            resolved = resolve_template(target_template) if target_template else None
            current = scan_files(dir_path, resolved)

            new_files = set(current.keys()) - set(known.keys())
            for f in sorted(new_files):
                print(f"新文件: {f.name}")
                if target_template:
                    pos = _print_new_content(f, 0)
                    positions[f] = pos

            removed = set(known.keys()) - set(current.keys())
            for f in sorted(removed):
                print(f"已删除: {f.name}")
                positions.pop(f, None)

            for f in set(current.keys()) & set(known.keys()):
                prev_mtime, prev_size = known[f]
                curr_mtime, curr_size = current[f]
                if curr_mtime != prev_mtime or curr_size != prev_size:
                    if target_template:
                        prev_pos = positions.get(f, 0)
                        new_pos = _print_new_content(f, prev_pos)
                        positions[f] = new_pos
                    else:
                        print(f"已修改: {f.name}")

            known = current
    except KeyboardInterrupt:
        print("\n监控已停止。")

def main():
    logs_dir, target_template, interval = parse_args()
    ensure_dir(logs_dir)
    try:
        monitor(logs_dir, target_template, interval)
    except Exception as e:
        print(f"发生错误: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()