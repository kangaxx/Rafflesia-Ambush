import os
import pandas as pd
import numpy as np
import argparse
from qlib.data.calendar import Calendar

def get_instrument_from_path(field_dir: str) -> str:
    """从字段目录路径中提取股票代码（如 features/000001.SH → 000001.SH）"""
    return os.path.basename(field_dir)

def bin_to_csv_merged(
    qlib_data_dir: str,
    csv_output_dir: str,
    freq: str = "day",
    specific_instrument: str = None,  # 股票代码（如 000001.SH）
    specific_field_dir: str = None    # 直接指定字段目录（如 ./features/000001.SH）
) -> None:
    """
    转换 QLib .bin 到 CSV（合并字段），支持两种方式指定处理对象：
    - 股票代码（自动查找字段目录）
    - 字段目录路径（直接处理该目录下的 .bin 文件）
    """
    # 校验参数：只能指定一种方式
    if specific_instrument and specific_field_dir:
        raise ValueError("只能指定股票代码（--stock）或字段目录（--field-dir）中的一种")
    if not specific_instrument and not specific_field_dir:
        # 若都不指定，默认处理所有股票（保持批量转换能力）
        instruments_file = os.path.join(qlib_data_dir, "instruments", "all.txt")
        with open(instruments_file, "r", encoding="utf-8") as f:
            all_instruments = [line.strip().split("\t")[0] for line in f if line.strip()]
        field_dirs = [os.path.join(qlib_data_dir, "features", instr) for instr in all_instruments]
        print(f"未指定股票或目录，将转换所有 {len(field_dirs)} 个标的")
    elif specific_instrument:
        # 通过股票代码定位字段目录
        field_dir = os.path.join(qlib_data_dir, "features", specific_instrument)
        if not os.path.exists(field_dir):
            raise FileNotFoundError(f"股票 {specific_instrument} 的字段目录不存在：{field_dir}")
        field_dirs = [field_dir]
        print(f"通过股票代码定位：{field_dir}")
    else:
        # 直接使用指定的字段目录
        specific_field_dir = os.path.abspath(specific_field_dir)
        if not os.path.exists(specific_field_dir):
            raise FileNotFoundError(f"指定的字段目录不存在：{specific_field_dir}")
        field_dirs = [specific_field_dir]
        print(f"直接处理指定目录：{specific_field_dir}")

    # 初始化交易日历
    calendar = Calendar(
        provider="csv",
        root_dir=os.path.join(qlib_data_dir, "calendars"),
        freq=freq
    )
    os.makedirs(csv_output_dir, exist_ok=True)
    timestamps = calendar.get_trade_dates()
    index = pd.DatetimeIndex(timestamps)

    # 遍历所有需要处理的字段目录
    for idx, field_dir in enumerate(field_dirs, 1):
        instrument = get_instrument_from_path(field_dir)  # 提取股票代码
        try:
            # 获取该目录下所有字段的 .bin 文件
            fields = []
            for f in os.listdir(field_dir):
                if f.endswith(f".{freq}.bin"):
                    field_name = f.split(f".{freq}.bin")[0]  # 提取字段名（如 open.day.bin → open）
                    fields.append(field_name)
            if not fields:
                print(f"跳过 {instrument}：目录中无 {freq} 频率的 .bin 文件")
                continue

            # 合并字段数据到 DataFrame
            merged_df = pd.DataFrame(index=index)
            for field in fields:
                bin_file = os.path.join(field_dir, f"{field}.{freq}.bin")
                with open(bin_file, "rb") as bf:
                    data = np.fromfile(bf, dtype=np.float32)  # QLib 标准：float32 小端格式
                if len(data) != len(index):
                    print(f"警告：{instrument} 的 {field} 数据长度与日历不匹配（{len(data)} vs {len(index)}），已跳过")
                    continue
                merged_df[field] = data

            # 保存为 CSV
            csv_path = os.path.join(csv_output_dir, f"{instrument}.csv")
            merged_df.to_csv(csv_path)
            print(f"[{idx}/{len(field_dirs)}] 已保存：{csv_path}")

        except Exception as e:
            print(f"处理 {instrument} 失败：{str(e)}")

    print(f"所有任务处理完成，结果位于：{csv_output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="QLib .bin 转 CSV（支持股票代码或字段目录）")
    parser.add_argument("--qlib-dir", type=str, default=os.path.expanduser("~/.qlib/qlib_data/cn_data"),
                        help="QLib 数据根目录（默认为 ~/.qlib/qlib_data/cn_data）")
    parser.add_argument("--output-dir", type=str, default="./qlib_merged_csv",
                        help="CSV 输出目录（默认 ./qlib_merged_csv）")
    parser.add_argument("--freq", type=str, default="day",
                        help="数据频率（day 或 1min，默认 day）")
    parser.add_argument("--stock", type=str, default=None,
                        help="指定股票代码（如 000001.SH），自动查找其字段目录")
    parser.add_argument("--field-dir", type=str, default=None,
                        help="直接指定字段目录路径（如 ./features/000001.SH）")
    args = parser.parse_args()

    bin_to_csv_merged(
        qlib_data_dir=args.qlib_dir,
        csv_output_dir=args.output_dir,
        freq=args.freq,
        specific_instrument=args.stock,
        specific_field_dir=args.field_dir
    )