import argparse
import tushare as ts
import pandas as pd
import json
import os

def get_tushare_token(token_path="key.json"):
    if not os.path.exists(token_path):
        raise FileNotFoundError(f"未找到 {token_path} 文件。")
    with open(token_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("tushare_token")

def download_data(ts_code, start_date, end_date, output, token, freq):
    ts.set_token(token)
    pro = ts.pro_api()
    params = {"ts_code": ts_code}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    if freq == "month":
        df = pro.fut_weekly_monthly(freq="month", fields="ts_code,trade_date,open,high,low,close,settle,vol,amount,oi", **params)
    else:
        df = pro.fut_weekly_monthly(freq="week", fields="ts_code,trade_date,open,high,low,close,settle,vol,amount,oi", **params)
    if df.empty:
        print(f"未获取到 {ts_code} 的{ '月度' if freq == 'month' else '周度' }数据。")
    else:
        output_dir = os.path.dirname(os.path.abspath(output))
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        df.to_csv(output, index=False, encoding="utf-8-sig")
        print(f"{ts_code} {'月度' if freq == 'month' else '周度'}数据已保存到 {output}")


def main():
    parser = argparse.ArgumentParser(
        description="使用tushare下载期货月度或周度数据并保存。"
    )
    parser.add_argument("-c", "--code", required=True, help="期货代码，如 RB2310.SHF")
    parser.add_argument("-s", "--start", help="开始日期，格式YYYYMMDD，可选")
    parser.add_argument("-e", "--end", help="结束日期，格式YYYYMMDD，可选")
    parser.add_argument("-o", "--output", default="./data/out/monthly", help="输出文件路径，默认为 ./data/out/monthly")
    parser.add_argument("--token_path", default="key.json", help="Tushare token文件路径，默认为 key.json")
    parser.add_argument("-f", "--freq", choices=["month", "week"], default="month", help="数据频率，month（月度）或 week（周度），默认为 month")
    args = parser.parse_args()

    token = get_tushare_token(args.token_path)
    download_data(args.code, args.start, args.end, args.output, token, args.freq)

if __name__ == "__main__":
    main()