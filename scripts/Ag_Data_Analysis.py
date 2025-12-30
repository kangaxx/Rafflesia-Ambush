# python
"""
修改文件: `scripts/Ag_Data_Analysis.py`

功能:
- 使用 tushare 的 trade_cal 接口判断沪期所（SHFE）是否为交易日
- 判断当前北京时间是否处于白银交易时段：
  - `09:00-11:30`
  - `13:30-15:00`
  - `21:00-02:30`（夜盘，跨日处理）
- 从项目的 `backtesting/key.json` 中优先读取 TUSHARE token，若无则使用环境变量 `TUSHARE_TOKEN`
依赖: tushare（安装: pip install tushare）
"""
import os
import json
import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


def load_tushare_token_from_project():
    """
    尝试从项目根目录下的 `backtesting/key.json` 读取 token。
    支持的字段名（优先顺序）: tushare_token, TUSHARE_TOKEN, token
    返回 token 字符串或 None
    """
    try:
        this_path = Path(__file__).resolve()
        project_root = this_path.parent.parent  # 假设脚本位于 `scripts/`
        key_path = project_root / "apps" / "backtesting" / "key.json"
        if not key_path.exists():
            return None
        content = key_path.read_text(encoding="utf-8")
        data = json.loads(content)
        # 支持多种字段名
        for key in ("tushare_token", "TUSHARE_TOKEN", "token"):
            if key in data and data[key]:
                return str(data[key])
        # 若没有上述字段，尝试查找第一个看起来像 token 的字符串值
        for v in data.values():
            if isinstance(v, str) and v.strip():
                return v
    except Exception:
        return None
    return None

def get_tushare_pro(token=None):
    try:
        import tushare as ts
    except Exception as e:
        raise RuntimeError("缺少依赖 tushare，请安装: pip install tushare") from e

    # 优先使用传入 token，其次项目文件，其次环境变量
    token = token or load_tushare_token_from_project() or os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("未提供 TUSHARE token，请设置 `backtesting/key.json` 中的 token 或环境变量 TUSHARE_TOKEN")
    ts.set_token(token)
    return ts.pro_api()

def is_shfe_open_date(pro, date_obj):
    """
    判断 date_obj (datetime.date) 是否为沪期所交易日
    返回 True/False
    """
    date_str = date_obj.strftime("%Y%m%d")
    try:
        df = pro.trade_cal(exchange="SHFE", start_date=date_str, end_date=date_str)
    except Exception:
        # 若请求失败，保守返回 False
        return False
    if df is None or df.empty:
        return False
    try:
        return int(df.iloc[0].get("is_open", 0)) == 1
    except Exception:
        return False

def is_shfe_silver_trading_now(pro=None, token=None):
    """
    判断当前北京时间是否为沪期所白银的开市时间。
    返回 (is_trading: bool, reason: str)
    """
    sh_tz = ZoneInfo("Asia/Shanghai")
    now_sh = datetime.datetime.now(sh_tz)
    t = now_sh.time()

    # 定义时间段
    def between(start_h, start_m, end_h, end_m, cur_time):
        start = datetime.time(start_h, start_m)
        end = datetime.time(end_h, end_m)
        return start <= cur_time <= end

    # 初始化 tushare pro（优先使用外部传入的 pro）
    if pro is None:
        try:
            pro = get_tushare_pro(token)
        except Exception:
            return False, "无法初始化 tushare，请设置 `backtesting/key.json` 或环境变量 TUSHARE_TOKEN，并安装 tushare"

    # 早盘 09:00-11:30 和 午盘 13:30-15:00 属于当日交易日
    if between(9, 0, 11, 30, t) or between(13, 30, 15, 0, t):
        today = now_sh.date()
        if is_shfe_open_date(pro, today):
            return True, f"北京时间 {now_sh.isoformat()}，处于日盘（当日）且当日为交易日"
        else:
            return False, f"北京时间 {now_sh.isoformat()}，处于日盘但当日非交易日"

    # 夜盘 21:00-23:59 属于当日夜盘（需当日为交易日）
    if between(21, 0, 23, 59, t):
        today = now_sh.date()
        if is_shfe_open_date(pro, today):
            return True, f"北京时间 {now_sh.isoformat()}，处于夜盘（21:00-23:59）且当日为交易日"
        else:
            return False, f"北京时间 {now_sh.isoformat()}，处于夜盘但当日非交易日"

    # 夜盘 00:00-02:30 属于前一交易日的夜盘（需检查昨天是否为交易日）
    if between(0, 0, 2, 30, t):
        yesterday = (now_sh - datetime.timedelta(days=1)).date()
        if is_shfe_open_date(pro, yesterday):
            return True, f"北京时间 {now_sh.isoformat()}，处于夜盘（00:00-02:30）且昨日期为交易日"
        else:
            return False, f"北京时间 {now_sh.isoformat()}，处于夜盘但昨日期非交易日"

    return False, f"北京时间 {now_sh.isoformat()}，不在白银交易时段"

# python
from datetime import timedelta

def get_last_shfe_trading_date(pro, lookback_days=30):
    """
    返回最近的沪期所交易日期（datetime.date），范围向前回溯 lookback_days 天。
    若无法找到则返回 None。
    """
    today = datetime.date.today()
    start = (today - timedelta(days=lookback_days)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    try:
        df = pro.trade_cal(exchange="SHFE", start_date=start, end_date=end)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    # 选择 is_open==1 的日期并返回最后一个
    try:
        open_df = df[df["is_open"] == 1]
        if open_df.empty:
            return None
        last_str = open_df["cal_date"].max()
        return datetime.datetime.strptime(last_str, "%Y%m%d").date()
    except Exception:
        return None

# python
def get_shfe_silver_main_close(pro, trade_date):
    """
    使用 pro.fut_daily 获取沪期所白银主力日线收盘价。
    trade_date: datetime.date 或 'YYYYMMDD' 字符串
    返回 float(close) 或 None。
    """
    date_str = trade_date.strftime("%Y%m%d") if isinstance(trade_date, datetime.date) else str(trade_date)
    try:
        df = pro.fut_daily(
            ts_code="AG.SHF",
            start_date=date_str,
            end_date=date_str,
            exchange="SHFE"
        )
    except Exception:
        return None

    if df is None or df.empty:
        return None

    # 常见收盘列名优先级
    for close_col in ("close", "Close", "settle", "pre_close"):
        if close_col in df.columns:
            try:
                return float(df.iloc[0][close_col])
            except Exception:
                continue

    # 回退：任意数值列的第一行
    for c in df.columns:
        try:
            if df[c].dtype.kind in "fiu":
                return float(df.iloc[0][c])
        except Exception:
            continue

    return None

if __name__ == "__main__":
    try:
        pro = get_tushare_pro(token=None)
    except Exception as e:
        print("无法初始化 tushare:", e)
        raise SystemExit(1)

    # 检查今天是否为交易日
    today = datetime.date.today()
    is_open = is_shfe_open_date(pro, today)
    if is_open:
        print(f"今天 {today.isoformat()} 为沪期所交易日。")
    else:
        last_trade = get_last_shfe_trading_date(pro, lookback_days=60)
        if last_trade is None:
            print("未能在回溯范围内找到最近交易日。")
        else:
            close_price = get_shfe_silver_main_close(pro, last_trade)
            if close_price is None:
                print(f"最近交易日: {last_trade.isoformat()}，但未能获取到白银主力合约收盘价。")
            else:
                print(f"最近交易日: {last_trade.isoformat()}，沪期所白银主力合约收盘价: {close_price}")