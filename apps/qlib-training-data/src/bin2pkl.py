import os
import pandas as pd
import pickle
import argparse
from datetime import datetime

def split_csv_to_pkl(
    csv_dir: str,
    output_dir: str,
    train_end: str,
    val_end: str,
    date_col: str = "date",
    process_all: bool = False  # 新增：是否处理所有 CSV，默认 False（仅处理第一个）
) -> None:
    """
    将 CSV 目录下的文件按时间分割为 train/val/test .pkl
    默认仅处理第一个 CSV 文件，可通过 process_all=True 处理所有
    """
    # 校验日期格式
    try:
        train_end_dt = datetime.strptime(train_end, "%Y-%m-%d")
        val_end_dt = datetime.strptime(val_end, "%Y-%m-%d")
        if train_end_dt >= val_end_dt:
            raise ValueError("train_end 必须早于 val_end")
    except ValueError as e:
        raise ValueError(f"日期格式错误（需为 YYYY-MM-DD）：{e}")

    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    train_path = os.path.join(output_dir, "train_data.pkl")
    val_path = os.path.join(output_dir, "val_data.pkl")
    test_path = os.path.join(output_dir, "test_data.pkl")

    # 初始化三个数据集（字典：key=股票代码，value=DataFrame）
    train_data = {}
    val_data = {}
    test_data = {}

    # 获取所有 CSV 文件并排序（确保第一个文件固定）
    csv_files = sorted([f for f in os.listdir(csv_dir) if f.endswith(".csv")])
    if not csv_files:
        raise FileNotFoundError(f"目录 {csv_dir} 中未找到 CSV 文件")

    # 决定处理的文件：默认第一个，process_all=True 则处理所有
    target_files = csv_files if process_all else [csv_files[0]]
    print(f"{'处理所有' if process_all else '仅处理第一个'} CSV 文件，共 {len(target_files)} 个")

    for idx, csv_file in enumerate(target_files, 1):
        csv_path = os.path.join(csv_dir, csv_file)
        instrument = os.path.splitext(csv_file)[0]  # 股票代码（文件名）

        try:
            # 读取 CSV（根据是否有日期表头处理索引）
            if date_col:
                df = pd.read_csv(csv_path, parse_dates=[date_col], index_col=date_col)
            else:
                df = pd.read_csv(csv_path, parse_dates=True, index_col=0)

            # 确保索引是 datetime 类型
            if not pd.api.types.is_datetime64_any_dtype(df.index):
                raise TypeError(f"{instrument} 的索引不是时间类型，请检查 CSV 格式")

            # 按时间分割数据
            train_df = df[df.index <= train_end_dt]
            val_df = df[(df.index > train_end_dt) & (df.index <= val_end_dt)]
            test_df = df[df.index > val_end_dt]

            # 过滤空数据集
            if not train_df.empty:
                train_data[instrument] = train_df
            if not val_df.empty:
                val_data[instrument] = val_df
            if not test_df.empty:
                test_data[instrument] = test_df

            print(f"[{idx}/{len(target_files)}] 处理完成：{instrument} "
                  f"(train: {len(train_df)}, val: {len(val_df)}, test: {len(test_df)})")

        except Exception as e:
            print(f"处理 {csv_file} 失败：{str(e)}")

    # 保存为 .pkl 文件
    with open(train_path, "wb") as f:
        pickle.dump(train_data, f)
    with open(val_path, "wb") as f:
        pickle.dump(val_data, f)
    with open(test_path, "wb") as f:
        pickle.dump(test_data, f)

    print(f"\n分割完成：")
    print(f"训练集（≤ {train_end}）：{len(train_data)} 只股票，保存至 {train_path}")
    print(f"验证集（{train_end} < x ≤ {val_end}）：{len(val_data)} 只股票，保存至 {val_path}")
    print(f"测试集（> {val_end}）：{len(test_data)} 只股票，保存至 {test_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CSV 按时间分割为 train/val/test .pkl（默认处理第一个文件）")
    parser.add_argument("--csv-dir", type=str, required=True,
                        help="CSV 文件所在目录（如 ./qlib_merged_csv）")
    parser.add_argument("--output-dir", type=str, default="./split_pkl",
                        help="输出 train/val/test .pkl 的目录")
    parser.add_argument("--train-end", type=str, required=True,
                        help="训练集结束日期（如 2018-12-31）")
    parser.add_argument("--val-end", type=str, required=True,
                        help="验证集结束日期（如 2020-12-31）")
    parser.add_argument("--date-col", type=str, default="date",
                        help="CSV 中时间列的表头（若未指定表头则设为 ''）")
    parser.add_argument("--process-all", action="store_true",
                        help="添加此参数则处理所有 CSV 文件（默认仅处理第一个）")
    args = parser.parse_args()

    split_csv_to_pkl(
        csv_dir=args.csv_dir,
        output_dir=args.output_dir,
        train_end=args.train_end,
        val_end=args.val_end,
        date_col=args.date_col,
        process_all=args.process_all
    )