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


def find_datetime_column(df):
    """
    在DataFrame中查找包含日期时间数据的列
    
    Args:
        df (pandas.DataFrame): 输入的DataFrame
        
    Returns:
        tuple: (列名, 列数据) 如果找到日期时间列，否则 (None, None)
    """
    # 首先检查是否有名为datetime的列
    if 'datetime' in df.columns:
        return 'datetime', df['datetime']
    
    # 检查其他可能包含日期时间的列名
    datetime_keywords = ['date', 'time', 'timestamp', 'datetime', '交易时间', '时间', '日期']
    
    for col in df.columns:
        col_lower = col.lower()
        for keyword in datetime_keywords:
            if keyword in col_lower:
                return col, df[col]
    
    # 如果没有找到明显的日期时间列名，尝试自动识别
    for col in df.columns:
        # 跳过数值列
        if pd.api.types.is_numeric_dtype(df[col]):
            continue
            
        # 尝试将列数据转换为日期时间
        try:
            sample_data = df[col].dropna().head(10)
            if len(sample_data) == 0:
                continue
                
            # 尝试解析为日期时间
            parsed_dates = pd.to_datetime(sample_data, errors='coerce')
            valid_count = parsed_dates.notnull().sum()
            
            # 如果大部分样本都能成功解析为日期时间，则认为这是日期时间列
            if valid_count / len(sample_data) > 0.7:
                return col, df[col]
                
        except Exception:
            continue
    
    return None, None


def validate_datetime_column(datetime_series, column_name):
    """
    验证列是否包含有效的日期数据
    
    Args:
        datetime_series (pandas.Series): 日期时间列数据
        column_name (str): 列名
        
    Returns:
        dict: 包含验证结果和错误信息的字典
    """
    # 检查是否为空
    if datetime_series.isnull().any():
        return {
            'is_valid': False,
            'error_message': f'{column_name}列包含空值'
        }
    
    # 尝试将数据转换为datetime类型
    try:
        # 尝试多种常见的日期格式
        datetime_values = pd.to_datetime(datetime_series, errors='coerce')
        
        # 检查是否有无法转换的值
        invalid_count = datetime_values.isnull().sum()
        if invalid_count > 0:
            invalid_samples = datetime_series[datetime_values.isnull()].head(3).tolist()
            return {
                'is_valid': False,
                'error_message': f'{column_name}列发现{invalid_count}个无效的日期格式，示例: {invalid_samples}'
            }
        
        # 检查日期范围是否合理（假设数据是近几十年的）
        min_date = datetime_values.min()
        max_date = datetime_values.max()
        
        # 如果最早日期晚于当前日期，或者最晚日期早于1900年，可能有问题
        current_year = pd.Timestamp.now().year
        if min_date.year > current_year:
            return {
                'is_valid': False,
                'error_message': f'{column_name}列日期数据异常：最早日期{min_date}晚于当前年份'
            }
        
        if max_date.year < 1900:
            return {
                'is_valid': False,
                'error_message': f'{column_name}列日期数据异常：最晚日期{max_date}早于1900年'
            }
        
        # 检查日期是否按时间顺序排列（可选，但金融数据通常应该有序）
        if not datetime_values.is_monotonic_increasing:
            return {
                'is_valid': True,
                'warning_message': f'{column_name}列日期数据未按时间顺序排列（金融数据通常应该有序）'
            }
        
        return {
            'is_valid': True,
            'message': f'{column_name}列日期数据有效，范围: {min_date} 到 {max_date}'
        }
        
    except Exception as e:
        return {
            'is_valid': False,
            'error_message': f'{column_name}列日期解析错误: {str(e)}'
        }


def validate_csv_file(input_file):
    """
    验证CSV文件是否包含所有必需的字段
    
    Args:
        input_file (str): 输入CSV文件路径
        
    Returns:
        pandas.DataFrame: 读取的DataFrame
        
    Raises:
        ValueError: 如果缺少必需字段或字段内容无效
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
    
    # 智能查找并验证日期时间列
    datetime_col_name, datetime_col_data = find_datetime_column(df)
    
    if datetime_col_name is not None:
        print(f"找到日期时间列: {datetime_col_name}")
        
        datetime_validation_result = validate_datetime_column(datetime_col_data, datetime_col_name)
        if not datetime_validation_result['is_valid']:
            raise ValueError(f"日期时间列内容无效: {datetime_validation_result['error_message']}")
        
        # 显示datetime验证信息
        if 'message' in datetime_validation_result:
            print(f"✓ {datetime_validation_result['message']}")
        elif 'warning_message' in datetime_validation_result:
            print(f"⚠ {datetime_validation_result['warning_message']}")
    else:
        print("⚠ 未找到明显的日期时间列，将使用原始字段名")
    
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
    parser = argparse.ArgumentParser(
        description='CSV格式转换工具 - 将CSV文件字段重新排序为datetime,open,high,low,close,volume',
        epilog='''使用范例:
  python convert_csv_format.py data.csv                    # 基本用法
  python convert_csv_format.py data.csv -o result.csv      # 指定输出文件
  python convert_csv_format.py stock_data.csv              # 金融数据转换

字段要求: 输入CSV必须包含datetime,open,high,low,close,volume字段
其他字段将被删除，顺序按上述排列'''
    )
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