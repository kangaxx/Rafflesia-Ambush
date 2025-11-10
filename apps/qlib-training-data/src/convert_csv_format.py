#!/usr/bin/env python3
"""
CSV格式转换工具
将CSV文件的数据字段按照指定顺序重新排序：datetime,open,high,low,close,volume
不在指定字段内的数据将被删除，如果发现指定字段数据有缺失则报错
"""

import pandas as pd
import argparse
import sys
import os
from pathlib import Path

# 必需的字段顺序
REQUIRED_FIELDS = ['datetime', 'open', 'high', 'low', 'close', 'volume']


def validate_csv_file(input_file):
    """
    验证CSV文件是否包含所有必需的字段
    
    Args:
        input_file (str): 输入CSV文件路径
        
    Returns:
        pandas.DataFrame: 读取的DataFrame
        
    Raises:
        ValueError: 如果缺少必需字段
        FileNotFoundError: 如果文件不存在
    """
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"文件不存在: {input_file}")
    
    # 读取CSV文件
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        raise ValueError(f"无法读取CSV文件: {e}")
    
    # 检查必需字段是否存在
    missing_fields = []
    for field in REQUIRED_FIELDS:
        if field not in df.columns:
            missing_fields.append(field)
    
    if missing_fields:
        raise ValueError(f"CSV文件缺少以下必需字段: {', '.join(missing_fields)}")
    
    # 检查必需字段是否有缺失值
    for field in REQUIRED_FIELDS:
        if df[field].isnull().any():
            raise ValueError(f"字段 '{field}' 包含缺失值")
    
    return df


def reorder_csv_fields(df):
    """
    重新排序DataFrame的字段，只保留必需的字段
    
    Args:
        df (pandas.DataFrame): 输入的DataFrame
        
    Returns:
        pandas.DataFrame: 重新排序后的DataFrame
    """
    # 只保留必需的字段，并按照指定顺序排序
    reordered_df = df[REQUIRED_FIELDS].copy()
    
    return reordered_df


def convert_csv_format(input_file, output_file=None):
    """
    转换CSV文件格式的主要函数
    
    Args:
        input_file (str): 输入CSV文件路径
        output_file (str, optional): 输出CSV文件路径，如果为None则自动生成
        
    Returns:
        str: 输出文件路径
    """
    # 验证输入文件
    print(f"正在验证文件: {input_file}")
    df = validate_csv_file(input_file)
    
    # 获取原始字段信息
    original_fields = list(df.columns)
    print(f"原始字段: {', '.join(original_fields)}")
    
    # 重新排序字段
    print("正在重新排序字段...")
    reordered_df = reorder_csv_fields(df)
    
    # 生成输出文件路径
    if output_file is None:
        input_path = Path(input_file)
        output_file = input_path.parent / f"{input_path.stem}_converted{input_path.suffix}"
    
    # 保存重新排序后的CSV文件
    reordered_df.to_csv(output_file, index=False)
    
    # 统计信息
    removed_fields = set(original_fields) - set(REQUIRED_FIELDS)
    if removed_fields:
        print(f"已删除的字段: {', '.join(removed_fields)}")
    
    print(f"转换完成! 输出文件: {output_file}")
    print(f"最终字段顺序: {', '.join(REQUIRED_FIELDS)}")
    
    return str(output_file)


def main():
    """主函数，处理命令行参数"""
    parser = argparse.ArgumentParser(description='CSV格式转换工具')
    parser.add_argument('input_file', help='输入CSV文件路径')
    parser.add_argument('-o', '--output', help='输出CSV文件路径（可选）')
    
    args = parser.parse_args()
    
    try:
        convert_csv_format(args.input_file, args.output)
    except (FileNotFoundError, ValueError) as e:
        print(f"错误: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"未知错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()