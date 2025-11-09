import os
import pandas as pd
import numpy as np
import argparse
from datetime import datetime

def get_instrument_from_path(field_dir: str) -> str:
    """从字段目录路径中提取股票代码"""
    return os.path.basename(field_dir)

def bin_to_csv_merged(
    qlib_data_dir: str,
    csv_output_dir: str,
    freq: str = "day",
    specific_instrument: str = None,
    specific_field_dir: str = None
) -> None:
    # 校验参数互斥
    if specific_instrument and specific_field_dir:
        raise ValueError("只能指定 --stock 或 --field-dir 中的一种")

    # 确定需要处理的字段目录列表
    if not specific_instrument and not specific_field_dir:
        instruments_file = os.path.join(qlib_data_dir, "instruments", "all.txt")
        with open(instruments_file, "r", encoding="utf-8") as f:
            all_instruments = [line.strip().split("\t")[0] for line in f if line.strip()]
        field_dirs = [os.path.join(qlib_data_dir, "features", instr) for instr in all_instruments]
        print(f"未指定股票或目录，将转换所有 {len(field_dirs)} 个标的")
    elif specific_instrument:
        field_dir = os.path.join(qlib_data_dir, "features", specific_instrument)
        if not os.path.exists(field_dir):
            raise FileNotFoundError(f"股票 {specific_instrument} 的字段目录不存在：{field_dir}")
        field_dirs = [field_dir]
        print(f"通过股票代码定位：{field_dir}")
    else:
        specific_field_dir = os.path.abspath(specific_field_dir)
        if not os.path.exists(specific_field_dir):
            raise FileNotFoundError(f"指定的字段目录不存在：{specific_field_dir}")
        field_dirs = [specific_field_dir]
        print(f"直接处理指定目录：{specific_field_dir}")

    # 读取交易日历（时间戳索引）
    calendar_file = os.path.join(qlib_data_dir, "calendars", f"{freq}.txt")
    if not os.path.exists(calendar_file):
        raise FileNotFoundError(f"未找到 {freq} 频率的日历文件：{calendar_file}")
    with open(calendar_file, "r", encoding="utf-8") as f:
        timestamps = [line.strip() for line in f if line.strip()]
    calendar_length = len(timestamps)
    index = pd.DatetimeIndex(timestamps)
    os.makedirs(csv_output_dir, exist_ok=True)

    # 初始化转换报告
    report_path = os.path.join(csv_output_dir, "convert.report")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"转换报告 - 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"数据频率：{freq}\n")
        f.write(f"日历总长度：{calendar_length} 条\n\n")

    # 处理字段目录
    for idx, field_dir in enumerate(field_dirs, 1):
        instrument = get_instrument_from_path(field_dir)
        report_content = []  # 记录当前股票的转换日志
        field_lengths = []  # 记录所有字段的数据长度（用于一致性校验）
        merged_df = None    # 存储合并后的数据

        try:
            # 获取所有字段的 .bin 文件
            fields = []
            for f in os.listdir(field_dir):
                if f.endswith(f".{freq}.bin"):
                    field_name = f.split(f".{freq}.bin")[0]
                    fields.append(field_name)
            if not fields:
                msg = f"跳过 {instrument}：目录中无 {freq} 频率的 .bin 文件"
                print(msg)
                with open(report_path, "a", encoding="utf-8") as f:
                    f.write(f"{msg}\n")
                continue

            # 读取并处理每个字段
            for field in fields:
                bin_file = os.path.join(field_dir, f"{field}.{freq}.bin")
                with open(bin_file, "rb") as bf:
                    data = np.fromfile(bf, dtype=np.float32)
                data_length = len(data)
                field_lengths.append(data_length)

                # 处理长度不匹配：截断或截取数据以匹配日历
                if data_length != calendar_length:
                    msg = f"{instrument} 的 {field} 数据长度不匹配（{data_length} vs 日历 {calendar_length}）"
                    if data_length < calendar_length:
                        # 数据少于日历：取日历尾部的 data_length 个时间戳
                        adjusted_index = index[-data_length:]
                        adjusted_data = data
                        msg += f"，使用日历尾部 {data_length} 个时间戳"
                    else:
                        # 数据多于日历：取数据头部的 calendar_length 个值
                        adjusted_index = index
                        adjusted_data = data[:calendar_length]
                        msg += f"，截断数据至 {calendar_length} 个值"
                    print(f"警告：{msg}")
                    report_content.append(f"警告：{msg}")
                else:
                    adjusted_index = index
                    adjusted_data = data

                # 初始化或合并数据
                if merged_df is None:
                    merged_df = pd.DataFrame(index=adjusted_index)
                # 确保索引一致（避免因不同字段调整导致的索引错位）
                if not merged_df.index.equals(adjusted_index):
                    raise ValueError(f"{instrument} 的 {field} 调整后索引与其他字段不一致，转换终止")
                merged_df[field] = adjusted_data

            # 校验同一股票的所有字段长度是否一致（允许与日历不一致，但字段间必须一致）
            unique_lengths = set(field_lengths)
            if len(unique_lengths) > 1:
                raise ValueError(
                    f"{instrument} 的字段数据长度不一致（{unique_lengths}），可能数据损坏"
                )

            # 保存 CSV
            csv_path = os.path.join(csv_output_dir, f"{instrument}.csv")
            merged_df.to_csv(csv_path)
            success_msg = f"[{idx}/{len(field_dirs)}] 已保存：{csv_path}"
            print(success_msg)
            report_content.append(success_msg)

        except Exception as e:
            error_msg = f"处理 {instrument} 失败：{str(e)}"
            print(error_msg)
            report_content.append(f"错误：{error_msg}")

        # 将当前股票的转换日志写入报告
        with open(report_path, "a", encoding="utf-8") as f:
            f.write(f"=== {instrument} 转换详情 ===\n")
            for line in report_content:
                f.write(f"{line}\n")
            f.write("\n")

    print(f"所有任务处理完成，结果位于：{csv_output_dir}")
    print(f"转换报告已生成：{report_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="QLib .bin 转 CSV（处理长度不匹配+生成报告）")
    parser.add_argument("--qlib-dir", type=str, default=os.path.expanduser("~/.qlib/qlib_data/cn_data"),
                        help="QLib 数据根目录")
    parser.add_argument("--output-dir", type=str, default="./qlib_merged_csv",
                        help="CSV 输出目录")
    parser.add_argument("--freq", type=str, default="day",
                        help="数据频率（day 或 1min）")
    parser.add_argument("--stock", type=str, default=None,
                        help="指定股票代码（如 000001.SH）")
    parser.add_argument("--field-dir", type=str, default=None,
                        help="直接指定字段目录路径")
    args = parser.parse_args()

    bin_to_csv_merged(
        qlib_data_dir=args.qlib_dir,
        csv_output_dir=args.output_dir,
        freq=args.freq,
        specific_instrument=args.stock,
        specific_field_dir=args.field_dir
    )