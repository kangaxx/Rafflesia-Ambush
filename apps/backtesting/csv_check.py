#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CSV文件时间对齐比较程序
功能：将两份CSV文件按时间对齐数据，生成合并结果，并添加比较结果列
参数：
    1. 标准数据文件来源路径（基准数据源）- 仅支持CSV格式
    2. 被检测数据来源路径（待检查数据源）- 仅支持CSV格式
    3. 开始日期（可选）- 格式：YYYYMMDD，如不指定则从最早日期开始
    4. 结束日期（可选）- 格式：YYYYMMDD，如不指定则到最晚日期结束
    5. 结果文件路径（可选）- 完整的CSV文件路径，默认是当前目录下的"merged_result_" + 时间戳 + ".csv"
"""

import os
import sys
try:
    import pandas as pd
    import numpy as np
except ImportError as e:
    print(f"错误: 无法导入必要的库: {e}")
    print("请安装所需的库: pip install pandas numpy")
    sys.exit(1)
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 要比较的字段
FIELDS_TO_COMPARE = {
    'open': ['open', '开盘价', '开盘'],
    'high': ['high', '最高价', '最高'],
    'low': ['low', '最低价', '最低'],
    'close': ['close', '收盘价', '收盘'],
    'volume': ['volume', '成交量', '量', 'vol', 'VOL', 'Vol'],
    '持仓': ['持仓', '持仓量', 'position', 'open_interest', 'oi', 'OI', 'Oi']
}

# 时间对齐比较相关配置
DEFAULT_FLOAT_THRESHOLD = 1e-6  # 默认浮点数比较阈值
MAX_DISPLAY_RECORDS = 10        # 最大显示记录数

def find_date_column(df):
    """
    在DataFrame中查找日期列
    
    Args:
        df: pandas DataFrame
    
    Returns:
        str: 日期列名
    """
    # 常见的日期列名
    date_columns = ['trade_date', 'date', 'datetime', 'time', 'trading_date', '交易日', 'trade_time', 'Trade_Time', 'TRADE_TIME']
    
    date_column = None
    for col in date_columns:
        if col in df.columns:
            date_column = col
            break
    
    # 如果没有找到常见的日期列名，尝试第一个看起来像日期的列
    if date_column is None:
        for col in df.columns:
            # 检查列中的数据是否包含日期格式
            sample_data = df[col].dropna().head(5)
            if any(isinstance(x, str) and len(str(x)) == 8 and str(x).isdigit() for x in sample_data):
                date_column = col
                break
    
    return date_column

def normalize_date(date_val):
    """
    标准化日期格式为YYYYMMDD字符串，支持各种日期格式包括trade_time
    
    Args:
        date_val: 日期值
    
    Returns:
        str: 标准化后的日期字符串，或None如果无法解析
    """
    if pd.isna(date_val):
        return None
    
    date_str = str(date_val).strip()
    # 确保是8位数字的日期格式
    if len(date_str) == 8 and date_str.isdigit():
        return date_str
    
    # 尝试各种日期格式
    formats = [
        '%Y%m%d', '%Y-%m-%d', '%Y/%m/%d',
        '%Y%m%d %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y/%m/%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%SZ',
        # 额外支持更多时间格式
        '%Y-%m-%d %H:%M', '%Y/%m/%d %H:%M',
        '%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y',
        '%Y%m%d%H%M%S'  # 紧凑的年月日时分秒格式
    ]
    
    # 尝试不同的日期格式
    for fmt in formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            return parsed_date.strftime('%Y%m%d')
        except ValueError:
            continue
    
    # 如果都失败，尝试使用pandas的to_datetime函数
    try:
        parsed_date = pd.to_datetime(date_str)
        return parsed_date.strftime('%Y%m%d')
    except Exception as e:
        logger.warning(f"无法解析日期: {date_str}, 错误: {e}")
    return None

def find_field_mapping(df):
    """
    查找DataFrame中与目标字段的映射关系
    
    Args:
        df: pandas DataFrame
    
    Returns:
        dict: 目标字段名到实际列名的映射
    """
    mapping = {}
    for field, possible_names in FIELDS_TO_COMPARE.items():
        for name in possible_names:
            if name in df.columns:
                mapping[field] = name
                logger.info(f"字段 '{field}' 映射到列 '{name}'")
                break
    return mapping

def load_data_from_file(file_path, start_date=None, end_date=None):
    """
    从文件中加载数据，返回原始DataFrame、日期列信息和字段映射
    
    Args:
        file_path: 文件路径
        start_date: 开始日期，格式YYYYMMDD
        end_date: 结束日期，格式YYYYMMDD
    
    Returns:
        tuple: (df, date_column, date_mapping, field_mapping)，其中
            df: 原始DataFrame（已处理日期和过滤）
            date_column: 日期列名
            date_mapping: 标准化日期到原始索引的映射
            field_mapping: 目标字段到实际列名的映射
    """
    if not os.path.exists(file_path):
        logger.error(f"文件不存在: {file_path}")
        return None, None, None, None
    
    try:
        # 根据文件扩展名确定读取方式
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext == '.csv':
            # 读取CSV文件
            df = pd.read_csv(file_path)
            
            # 查找日期列
            date_column = find_date_column(df)
            if date_column is None:
                logger.error(f"无法在文件中找到日期列: {file_path}")
                return None, None, None, None
            
            logger.info(f"使用列 '{date_column}' 作为日期列")
            
            # 查找字段映射
            field_mapping = find_field_mapping(df)
            missing_fields = set(FIELDS_TO_COMPARE.keys()) - set(field_mapping.keys())
            if missing_fields:
                logger.warning(f"文件 {file_path} 缺少以下必要字段: {', '.join(missing_fields)}")
            
            # 添加标准化日期列用于后续处理
            df['_standard_date'] = df[date_column].apply(normalize_date)
            
            # 根据日期范围过滤
            if start_date:
                df = df[df['_standard_date'] >= start_date]
            if end_date:
                df = df[df['_standard_date'] <= end_date]
            
            # 移除无效日期
            df = df.dropna(subset=['_standard_date'])
            
            # 创建日期映射（标准化日期到原始索引）
            date_mapping = {}
            for idx, row in df.iterrows():
                date_str = row['_standard_date']
                if date_str:
                    date_mapping[date_str] = idx
            
            logger.info(f"从 {file_path} 加载并过滤了 {len(df)} 条记录")
            return df, date_column, date_mapping, field_mapping
        
        else:
            logger.error(f"只支持CSV文件格式，不支持: {file_ext}")
            return None, None, None, None
            
    except Exception as e:
        logger.error(f"读取文件时发生错误: {e}")
        return None, None, None, None

def merge_and_compare(standard_df, check_df, standard_date_col, check_date_col,
                     standard_mapping, check_mapping):
    """
    将两个数据源按时间对齐并比较字段值
    
    Args:
        standard_df: 标准数据源的DataFrame
        check_df: 被检查数据源的DataFrame
        standard_date_col: 标准数据源的原始日期列名
        check_date_col: 被检查数据源的原始日期列名
        standard_mapping: 标准数据源的字段映射
        check_mapping: 被检查数据源的字段映射
    
    Returns:
        tuple: (merged_df, summary)
            merged_df: 合并后的DataFrame，包含时间对齐的数据和比较结果
            summary: 比较结果摘要统计信息
    """
    logger.info(f"开始按时间对齐比较两个数据源...")
    
    # 获取所有唯一日期（标准化日期）
    all_dates = sorted(set(standard_df['_standard_date']).union(set(check_df['_standard_date'])))
    logger.info(f"总共找到 {len(all_dates)} 个唯一日期")
    
    # 获取可比较的字段
    common_fields = set(standard_mapping.keys()) & set(check_mapping.keys())
    logger.info(f"找到 {len(common_fields)} 个可比较字段: {', '.join(sorted(common_fields))}")
    
    # 创建合并后的结果列表
    merged_data = []
    field_discrepancies = {field: 0 for field in common_fields}
    
    # 定义浮点数比较阈值
    float_threshold = DEFAULT_FLOAT_THRESHOLD
    
    for date_str in all_dates:
        # 查找标准数据源中的数据
        std_row = standard_df[standard_df['_standard_date'] == date_str]
        std_exists = not std_row.empty
        
        # 查找被检查数据源中的数据
        chk_row = check_df[check_df['_standard_date'] == date_str]
        chk_exists = not chk_row.empty
        
        # 创建合并行，使用统一的日期列名
        merged_row = {
            '日期': date_str,  # 使用统一的日期列名
            '_data_source': '两者都有' if std_exists and chk_exists else '仅标准数据' if std_exists else '仅检查数据',
            '比较结果': '完全一致'  # 默认值
        }
        
        # 添加标准数据源的所有字段值
        if std_exists:
            std_data = std_row.iloc[0]
            # 添加所有原始字段，不只是映射的字段
            for col in standard_df.columns:
                if col not in ['_standard_date']:  # 跳过临时列
                    merged_row[f'标准数据_{col}'] = std_data[col]
        
        # 添加被检查数据源的所有字段值
        if chk_exists:
            chk_data = chk_row.iloc[0]
            # 添加所有原始字段，不只是映射的字段
            for col in check_df.columns:
                if col not in ['_standard_date']:  # 跳过临时列
                    merged_row[f'检查数据_{col}'] = chk_data[col]
        
        # 比较共同字段值
        if std_exists and chk_exists:
            has_discrepancy = False
            discrepancy_fields = []
            
            for field in common_fields:
                std_field_col = standard_mapping[field]
                chk_field_col = check_mapping[field]
                
                std_val = std_row.iloc[0][std_field_col]
                chk_val = chk_row.iloc[0][chk_field_col]
                
                # 计算差异
                diff_result, diff_info = compare_field_values(std_val, chk_val, field, float_threshold)
                
                if not diff_result:
                    field_discrepancies[field] += 1
                    has_discrepancy = True
                    discrepancy_fields.append(field)
            
            # 设置比较结果
            if has_discrepancy:
                merged_row['比较结果'] = f'不一致: {"、".join(discrepancy_fields)}'
            else:
                merged_row['比较结果'] = '完全一致'
        elif std_exists:
            merged_row['比较结果'] = '仅标准数据存在'
        else:
            merged_row['比较结果'] = '仅检查数据存在'
        
        merged_data.append(merged_row)
    
    # 创建合并后的DataFrame
    merged_df = pd.DataFrame(merged_data)
    logger.info(f"合并完成，生成 {len(merged_df)} 条记录")
    
    # 计算统计信息
    total_standard = len(standard_df)
    total_check = len(check_df)
    total_common = len(set(standard_df['_standard_date']).intersection(set(check_df['_standard_date'])))
    coverage_rate = (total_common / total_standard) * 100 if total_standard > 0 else 0
    
    # 计算差异统计
    total_merged = len(merged_df)
    std_only_count = (merged_df['_data_source'] == '仅标准数据').sum()
    chk_only_count = (merged_df['_data_source'] == '仅检查数据').sum()
    both_count = (merged_df['_data_source'] == '两者都有').sum()
    
    summary = {
        'total_standard': total_standard,
        'total_check': total_check,
        'total_merged': total_merged,
        'total_common': total_common,
        'std_only_count': std_only_count,
        'chk_only_count': chk_only_count,
        'coverage_rate': coverage_rate,
        'common_fields': sorted(common_fields),
        'field_discrepancies': field_discrepancies,
        'field_discrepancy_rates': {}
    }
    
    # 计算每个字段的差异率
    for field in common_fields:
        if both_count > 0:
            summary['field_discrepancy_rates'][field] = (field_discrepancies[field] / both_count) * 100
        else:
            summary['field_discrepancy_rates'][field] = 0
    
    # 重命名_data_source列为更友好的名称
    merged_df = merged_df.rename(columns={'_data_source': '数据来源'})
    
    # 调整列顺序，将重要信息放在前面
    cols = merged_df.columns.tolist()
    important_cols = ['日期', '数据来源', '比较结果']
    for col in important_cols:
        if col in cols:
            cols.remove(col)
            cols.insert(0, col)
    merged_df = merged_df[cols]
    
    return merged_df, summary

def compare_field_values(std_val, chk_val, field_name, float_threshold=1e-6):
    """
    比较单个字段的值
    
    Args:
        std_val: 标准值
        chk_val: 检查值
        field_name: 字段名
        float_threshold: 浮点数比较阈值
    
    Returns:
        tuple: (是否一致, 差异信息字典)
    """
    diff_info = {
        'difference': None,
        'percentage_diff': None,
        'message': ''
    }
    
    # 检查None值情况
    if pd.isna(std_val) and pd.isna(chk_val):
        return True, diff_info
    elif pd.isna(std_val):
        diff_info['message'] = f'标准值为空，检查值为{chk_val}'
        return False, diff_info
    elif pd.isna(chk_val):
        diff_info['message'] = f'检查值为空，标准值为{std_val}'
        return False, diff_info
    
    # 数值类型比较
    if isinstance(std_val, (int, float)) and isinstance(chk_val, (int, float)):
        # 使用numpy的isclose来处理浮点精度问题
        if np.isclose(std_val, chk_val, atol=float_threshold):
            return True, diff_info
        else:
            difference = abs(std_val - chk_val)
            percentage_diff = (difference / abs(std_val) * 100) if std_val != 0 else float('inf')
            diff_info['difference'] = difference
            diff_info['percentage_diff'] = percentage_diff
            diff_info['message'] = f'{std_val}≠{chk_val}({percentage_diff:.2f}%)'
            return False, diff_info
    # 其他类型直接比较
    elif std_val == chk_val:
        return True, diff_info
    else:
        diff_info['message'] = f'{std_val}≠{chk_val}'
        return False, diff_info

def print_comparison_results(summary):
    """
    打印比较结果摘要
    
    Args:
        summary: 比较结果摘要信息
    """
    logger.info("=== CSV文件时间对齐比较结果摘要 ===")
    logger.info(f"标准数据源总记录数: {summary['total_standard']}")
    logger.info(f"被检查数据源总记录数: {summary['total_check']}")
    logger.info(f"合并后总记录数: {summary['total_merged']}")
    logger.info(f"共同日期数: {summary['total_common']}")
    logger.info(f"仅标准数据存在的记录数: {summary['std_only_count']}")
    logger.info(f"仅检查数据存在的记录数: {summary['chk_only_count']}")
    logger.info(f"覆盖率: {summary['coverage_rate']:.2f}%")
    
    # 打印字段比较结果
    logger.info(f"\n可比较的字段: {', '.join(summary['common_fields'])}")
    for field in summary['common_fields']:
        logger.info(f"\n字段 '{field}':")
        logger.info(f"  差异记录数: {summary['field_discrepancies'][field]}")
        logger.info(f"  差异率: {summary['field_discrepancy_rates'][field]:.2f}%")

def validate_date_format(date_str):
    """
    验证日期格式是否为YYYYMMDD
    
    Args:
        date_str: 日期字符串
    
    Returns:
        bool: 是否为有效格式
    """
    if len(date_str) != 8 or not date_str.isdigit():
        return False
    
    try:
        # 验证日期的有效性
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])
        datetime(year, month, day)
        return True
    except ValueError:
        return False

def main():
    """
    主函数
    """
    # 检查命令行参数
    if len(sys.argv) < 3 or len(sys.argv) > 6:
        logger.error("使用方法: python csv_check.py <标准数据文件路径> <被检测数据文件路径> [开始日期YYYYMMDD] [结束日期YYYYMMDD] [结果文件路径]")
        sys.exit(1)
    
    standard_file_path = sys.argv[1]
    check_file_path = sys.argv[2]
    
    # 处理可选参数
    start_date = None
    end_date = None
    output_file_path = None
    
    # 解析参数
    arg_index = 3
    while arg_index < len(sys.argv):
        arg = sys.argv[arg_index]
        # 判断是否为日期参数
        if validate_date_format(arg):
            if start_date is None:
                start_date = arg
            elif end_date is None:
                end_date = arg
            else:
                logger.error(f"过多的日期参数: {arg}")
                sys.exit(1)
        # 否则视为结果文件路径
        elif output_file_path is None:
            output_file_path = arg
        else:
            logger.error(f"过多的参数: {arg}")
            sys.exit(1)
        arg_index += 1
    
    # 验证开始日期不晚于结束日期
    if start_date and end_date and start_date > end_date:
        logger.error(f"开始日期 {start_date} 晚于结束日期 {end_date}")
        sys.exit(1)
    
    logger.info(f"标准数据文件: {standard_file_path}")
    logger.info(f"被检测数据文件: {check_file_path}")
    
    # 记录日期范围信息
    date_range_info = ""
    if start_date:
        date_range_info += f"开始日期: {start_date}"
    if end_date:
        date_range_info += f" 结束日期: {end_date}"
    if date_range_info:
        logger.info(date_range_info)
    
    # 如果指定了结果文件路径，记录一下
    if output_file_path:
        logger.info(f"结果文件路径: {output_file_path}")
    
    # 加载数据
    logger.info("开始加载标准数据源...")
    standard_df, standard_date_col, standard_date_map, standard_mapping = load_data_from_file(standard_file_path, start_date, end_date)
    if standard_df is None:
        logger.error("无法加载标准数据源，程序终止")
        sys.exit(1)
    
    logger.info("开始加载被检测数据源...")
    check_df, check_date_col, check_date_map, check_mapping = load_data_from_file(check_file_path, start_date, end_date)
    if check_df is None:
        logger.error("无法加载被检测数据源，程序终止")
        sys.exit(1)
    
    # 按时间对齐并比较字段
    logger.info("开始按时间对齐并比较字段值...")
    merged_df, summary = merge_and_compare(
        standard_df, check_df, 
        standard_date_col, check_date_col,
        standard_mapping, check_mapping
    )
    
    # 打印结果摘要
    print_comparison_results(summary)
    
    # 确定结果文件路径
    if not output_file_path:
        # 默认使用当前目录下的格式：merged_result_时间戳.csv
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file_path = f"merged_result_{timestamp}.csv"
    
    # 确保目录存在
    output_dir = os.path.dirname(output_file_path)
    if output_dir and not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
            logger.info(f"创建目录: {output_dir}")
        except Exception as e:
            logger.error(f"创建目录失败: {e}")
            sys.exit(1)
    
    # 保存合并结果到CSV文件
    try:
        # 调整列顺序，将数据源类型和比较结果放在前面
        cols = merged_df.columns.tolist()
        if '_data_source' in cols:
            cols.remove('_data_source')
            cols.insert(0, '_data_source')
        if '比较结果' in cols:
            cols.remove('比较结果')
            cols.insert(1, '比较结果')
        
        # 重命名_data_source列，使其更友好
        merged_df = merged_df.rename(columns={'_data_source': '数据来源'})
        cols = [col.replace('_data_source', '数据来源') for col in cols]
        
        # 重新排序列
        merged_df = merged_df[cols]
        
        # 保存到CSV
        merged_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
        
        logger.info(f"按时间对齐的比较结果已保存到: {output_file_path}")
        logger.info(f"共保存 {len(merged_df)} 条记录")
        
        # 显示部分记录作为示例
        if len(merged_df) > 0:
            logger.info("\n示例记录:")
            # 只显示前几列和前几条记录
            sample_cols = cols[:min(10, len(cols))]  # 最多显示10列
            sample_data = merged_df.head(min(5, len(merged_df)))[sample_cols]
            logger.info(f"{sample_data.to_string(index=False)}")
            if len(merged_df) > 5:
                logger.info(f"... 还有 {len(merged_df) - 5} 条记录")
    
    except Exception as e:
        logger.error(f"保存结果文件时发生错误: {e}")
        sys.exit(1)
    
    logger.info("CSV文件时间对齐比较完成")

def program_entry():
    """
    程序入口点
    """
    main()

if __name__ == "__main__":
    program_entry()