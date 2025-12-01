#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CSV文件时间对齐比较程序

功能说明：
    1. 将两份CSV文件（标准数据源和被检测数据源）按时间维度对齐数据
    2. 自动识别并规范化日期列，支持多种常见日期列名和格式
    3. 根据配置的字段映射比较相应的数据值，生成差异报告
    4. 支持时间范围筛选，可指定开始日期和结束日期
    5. 提供两种数据合并模式，满足不同的比较需求
    6. 生成详细的比较统计信息和结果文件

支持的比较字段：
    - 开盘价（open）
    - 最高价（high）
    - 最低价（low）
    - 收盘价（close）
    - 成交量（volume）
    - 持仓量（持仓）

命令行参数：
    --standard, --std, -s    标准数据文件路径（基准数据源）
    --check, -c             被检测数据文件路径（待检查数据源）
    --start-date, -sd       开始日期（格式：YYYYMMDD，可选）
    --end-date, -ed         结束日期（格式：YYYYMMDD，可选）
    --output, -o            结果文件路径（可选，默认为标准文件名+_vs_检查文件名+_result.csv）
    --full-compare, -fc     全部对比标志（0或1，默认0）
                            0: 仅比较共同日期的数据
                            1: 将全部数据按时间拼接写入结果

使用示例：
    基础比较：
    python csv_check.py --standard 标准数据.csv --check 被检测数据.csv
    
    指定时间范围：
    python csv_check.py --standard 标准数据.csv --check 被检测数据.csv --start-date 20230101 --end-date 20231231
    
    自定义输出文件并使用全部对比模式：
    python csv_check.py --standard 标准数据.csv --check 被检测数据.csv --output 结果文件.csv --full-compare 1

注意事项：
    - 文件必须为CSV格式，且包含可识别的日期列
    - 日期格式推荐使用YYYYMMDD（如20230101）
    - 若未指定输出文件，将自动生成结果文件名
"""

import os
import sys
try:
    import pandas as pd
    import numpy as np
    import argparse
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
            
            # 移除无效日期
            df = df.dropna(subset=['_standard_date'])
            
            # 根据日期范围过滤
            if start_date:
                df = df[df['_standard_date'] >= start_date]
            if end_date:
                df = df[df['_standard_date'] <= end_date]
            
            # 为标准化日期列创建索引以提高查询速度
            df = df.set_index('_standard_date')
            
            # 直接从索引创建日期映射
            date_mapping = {date_str: idx for idx, date_str in enumerate(df.index) if date_str}
            
            logger.info(f"从 {file_path} 加载并过滤了 {len(df)} 条记录")
            return df, date_column, date_mapping, field_mapping
        
        else:
            logger.error(f"只支持CSV文件格式，不支持: {file_ext}")
            return None, None, None, None
            
    except Exception as e:
        logger.error(f"读取文件时发生错误: {e}")
        return None, None, None, None

def merge_and_compare(standard_df, check_df, standard_date_col, check_date_col,
                     standard_mapping, check_mapping, full_compare=0):
    """
    将两个数据源按时间对齐并比较字段值
    
    Args:
        standard_df: 标准数据源的DataFrame
        check_df: 被检查数据源的DataFrame
        standard_date_col: 标准数据源的原始日期列名
        check_date_col: 被检查数据源的原始日期列名
        standard_mapping: 标准数据源的字段映射
        check_mapping: 被检查数据源的字段映射
        full_compare: 是否生成全部对比数据，0表示保持当前逻辑，1表示将全部数据按时间拼接
    
    Returns:
        tuple: (merged_df, summary)
            merged_df: 合并后的DataFrame，包含时间对齐的数据和比较结果
            summary: 比较结果摘要统计信息
    """
    logger.info(f"开始按时间对齐比较两个数据源，full_compare={full_compare}...")
    
    # 获取所有唯一日期（标准化日期）- 直接使用索引更高效
    all_dates = sorted(set(standard_df.index).union(set(check_df.index)))
    logger.info(f"总共找到 {len(all_dates)} 个唯一日期")
    
    # 获取可比较的字段
    common_fields = set(standard_mapping.keys()) & set(check_mapping.keys())
    logger.info(f"找到 {len(common_fields)} 个可比较字段: {', '.join(sorted(common_fields))}")
    
    # 计算统计信息 - 直接使用索引更高效
    total_standard = len(standard_df)
    total_check = len(check_df)
    total_common = len(set(standard_df.index).intersection(set(check_df.index)))
    coverage_rate = (total_common / total_standard) * 100 if total_standard > 0 else 0
    
    # 初始化差异统计字典
    field_discrepancies = {field: 0 for field in common_fields}
    
    if full_compare == 1:
        # 生成全部对比数据模式
        logger.info("使用全部对比模式：将两个数据源的所有字段按时间拼接")
        
        # 复制DataFrame以避免修改原始数据
        std_df = standard_df.copy()
        chk_df = check_df.copy()
        
        # 重命名列，添加前缀以区分不同数据源
        std_df = std_df.add_prefix('标准数据_')
        chk_df = chk_df.add_prefix('检查数据_')
        
        # 重置索引，以便可以将日期作为普通列
        std_df = std_df.reset_index()
        chk_df = chk_df.reset_index()
        
        # 重命名索引列（原_standard_date）为日期
        std_df = std_df.rename(columns={'_standard_date': '日期'})
        chk_df = chk_df.rename(columns={'_standard_date': '日期'})
        
        # 合并两个DataFrame，以日期为key
        merged_df = pd.merge(std_df, chk_df, on='日期', how='outer')
        
        # 创建比较结果和数据来源列
        merged_df['数据来源'] = '两者都有'
        merged_df['比较结果'] = '完全一致'
        
        # 更新数据来源信息
        std_only_dates = set(standard_df.index) - set(check_df.index)
        chk_only_dates = set(check_df.index) - set(standard_df.index)
        
        if std_only_dates:
            merged_df.loc[merged_df['日期'].isin(std_only_dates), '数据来源'] = '仅标准数据'
            merged_df.loc[merged_df['日期'].isin(std_only_dates), '比较结果'] = '仅标准数据存在'
        
        if chk_only_dates:
            merged_df.loc[merged_df['日期'].isin(chk_only_dates), '数据来源'] = '仅检查数据'
            merged_df.loc[merged_df['日期'].isin(chk_only_dates), '比较结果'] = '仅检查数据存在'
        
        # 对共同日期的数据进行比较 - 使用向量化操作替代逐行处理
        if common_fields and total_common > 0:
            common_dates_mask = merged_df['数据来源'] == '两者都有'
            common_df = merged_df[common_dates_mask].copy()
            
            # 定义浮点数比较阈值
            float_threshold = DEFAULT_FLOAT_THRESHOLD
            
            # 为每个字段创建比较掩码
            field_masks = {}
            for field in common_fields:
                std_field_col = standard_mapping[field]
                chk_field_col = check_mapping[field]
                
                std_col_name = f'标准数据_{std_field_col}'
                chk_col_name = f'检查数据_{chk_field_col}'
                
                if std_col_name in merged_df.columns and chk_col_name in merged_df.columns:
                    # 使用向量化操作进行比较
                    std_vals = merged_df.loc[common_dates_mask, std_col_name]
                    chk_vals = merged_df.loc[common_dates_mask, chk_col_name]
                    
                    # 处理NaN情况
                    both_notna = std_vals.notna() & chk_vals.notna()
                    
                    # 数值比较（使用numpy的isclose进行向量化比较）
                    numeric_compare = np.zeros(len(std_vals), dtype=bool)
                    numeric_mask = std_vals.apply(lambda x: isinstance(x, (int, float))) & \
                                 chk_vals.apply(lambda x: isinstance(x, (int, float))) & \
                                 both_notna
                    
                    if numeric_mask.any():
                        numeric_compare[numeric_mask] = np.isclose(
                            std_vals[numeric_mask].astype(float),
                            chk_vals[numeric_mask].astype(float),
                            atol=float_threshold
                        )
                    
                    # 非数值比较（直接相等比较）
                    non_numeric_mask = ~numeric_mask & both_notna
                    if non_numeric_mask.any():
                        numeric_compare[non_numeric_mask] = (std_vals[non_numeric_mask] == chk_vals[non_numeric_mask])
                    
                    # 所有情况的综合比较结果
                    field_mask = numeric_compare | ~both_notna
                    field_masks[field] = field_mask
                    
                    # 统计差异数量
                    field_discrepancies[field] = (~field_mask).sum()
            
            # 更新比较结果列 - 优化为向量化操作
            if field_masks:
                # 创建一个字典来存储每个索引的不一致字段列表
                index_to_discrepancies = {}
                
                # 收集所有不一致的信息
                for field, mask in field_masks.items():
                    # 获取不一致的行索引
                    discrepancy_indices = merged_df[common_dates_mask].index[~mask]
                    
                    # 更新每个索引的不一致字段列表
                    for idx in discrepancy_indices:
                        if idx not in index_to_discrepancies:
                            index_to_discrepancies[idx] = []
                        index_to_discrepancies[idx].append(field)
                
                # 一次性生成所有需要更新的行索引和对应的值
                if index_to_discrepancies:
                    # 获取所有需要更新的索引
                    update_indices = list(index_to_discrepancies.keys())
                    
                    # 为每个索引生成不一致字段的字符串
                    update_values = [f"不一致: {fields[0]}" if len(fields) == 1 else f"不一致: {fields[0]}" + "、".join(fields[1:]) 
                                    for idx, fields in index_to_discrepancies.items()]
                    
                    # 使用.loc进行批量更新，避免逐行操作
                    merged_df.loc[update_indices, '比较结果'] = update_values
    else:
        # 使用标准对比模式 - 优化版本
        logger.info("使用标准对比模式：优化版 - 先创建基础数据，再使用向量化操作进行比较")
        
        # 定义浮点数比较阈值
        float_threshold = DEFAULT_FLOAT_THRESHOLD
        
        # 为两个DataFrame添加前缀并重置索引
        std_df = standard_df.copy().add_prefix('标准数据_').reset_index().rename(columns={'_standard_date': '日期'})
        chk_df = check_df.copy().add_prefix('检查数据_').reset_index().rename(columns={'_standard_date': '日期'})
        
        # 使用merge创建基础合并数据
        merged_df = pd.merge(std_df, chk_df, on='日期', how='outer')
        
        # 添加数据来源和比较结果列
        merged_df['数据来源'] = '两者都有'
        merged_df['比较结果'] = '完全一致'
        
        # 找出只有标准数据或只有检查数据的日期
        std_only_dates = set(standard_df.index) - set(check_df.index)
        chk_only_dates = set(check_df.index) - set(standard_df.index)
        
        # 更新数据来源和比较结果
        if std_only_dates:
            merged_df.loc[merged_df['日期'].isin(std_only_dates), '数据来源'] = '仅标准数据'
            merged_df.loc[merged_df['日期'].isin(std_only_dates), '比较结果'] = '仅标准数据存在'
        
        if chk_only_dates:
            merged_df.loc[merged_df['日期'].isin(chk_only_dates), '数据来源'] = '仅检查数据'
            merged_df.loc[merged_df['日期'].isin(chk_only_dates), '比较结果'] = '仅检查数据存在'
        
        # 对共同日期的数据进行向量化比较
        if common_fields and total_common > 0:
            common_dates_mask = merged_df['数据来源'] == '两者都有'
            
            # 为每个字段创建比较掩码
            field_masks = {}
            for field in common_fields:
                std_field_col = standard_mapping[field]
                chk_field_col = check_mapping[field]
                
                std_col_name = f'标准数据_{std_field_col}'
                chk_col_name = f'检查数据_{chk_field_col}'
                
                if std_col_name in merged_df.columns and chk_col_name in merged_df.columns:
                    # 使用向量化操作进行比较
                    std_vals = merged_df.loc[common_dates_mask, std_col_name]
                    chk_vals = merged_df.loc[common_dates_mask, chk_col_name]
                    
                    # 处理NaN情况
                    both_notna = std_vals.notna() & chk_vals.notna()
                    
                    # 数值比较（使用numpy的isclose进行向量化比较）
                    numeric_compare = np.zeros(len(std_vals), dtype=bool)
                    numeric_mask = std_vals.apply(lambda x: isinstance(x, (int, float))) & \
                                 chk_vals.apply(lambda x: isinstance(x, (int, float))) & \
                                 both_notna
                    
                    if numeric_mask.any():
                        numeric_compare[numeric_mask] = np.isclose(
                            std_vals[numeric_mask].astype(float),
                            chk_vals[numeric_mask].astype(float),
                            atol=float_threshold
                        )
                    
                    # 非数值比较（直接相等比较）
                    non_numeric_mask = ~numeric_mask & both_notna
                    if non_numeric_mask.any():
                        numeric_compare[non_numeric_mask] = (std_vals[non_numeric_mask] == chk_vals[non_numeric_mask])
                    
                    # 所有情况的综合比较结果
                    field_mask = numeric_compare | ~both_notna
                    field_masks[field] = field_mask
                    
                    # 统计差异数量
                    field_discrepancies[field] = (~field_mask).sum()
            
            # 更新比较结果列
            if field_masks:
                for field, mask in field_masks.items():
                    # 获取不一致的行索引
                    discrepancy_indices = merged_df[common_dates_mask].index[~mask]
                    
                    # 更新这些行的比较结果
                    for idx in discrepancy_indices:
                        current_result = merged_df.at[idx, '比较结果']
                        if current_result == '完全一致':
                            merged_df.at[idx, '比较结果'] = f'不一致: {field}'
                        else:
                            merged_df.at[idx, '比较结果'] = f'{current_result}、{field}'
    
    logger.info(f"合并完成，生成 {len(merged_df)} 条记录")
    
    # 计算差异统计
    total_merged = len(merged_df)
    std_only_count = (merged_df['数据来源'] == '仅标准数据').sum()
    chk_only_count = (merged_df['数据来源'] == '仅检查数据').sum()
    both_count = (merged_df['数据来源'] == '两者都有').sum()
    
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
        'field_discrepancy_rates': {},
        'full_compare_mode': full_compare == 1
    }
    
    # 计算每个字段的差异率
    for field in common_fields:
        if both_count > 0:
            summary['field_discrepancy_rates'][field] = (field_discrepancies[field] / both_count) * 100
        else:
            summary['field_discrepancy_rates'][field] = 0
    
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
    # 创建参数解析器
    parser = argparse.ArgumentParser(
        description='CSV文件时间对齐比较程序 - 用于比较两份CSV格式的数据文件，按时间维度对齐并生成详细比较报告',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
功能概述:
  本程序用于金融数据质量检查，能够将标准数据源与待检测数据源进行时间对齐，
  自动识别和匹配相应字段，比较数据差异，并生成详细的比较结果。

比较逻辑:
  - 自动识别并规范化两份文件中的日期列
  - 根据配置的字段映射关系匹配对应的字段（如开盘价、收盘价等）
  - 计算共同日期的数据差异
  - 统计差异数量和详细信息

使用示例:
  # 基础比较：比较两份文件中的所有共同日期数据
  python csv_check.py --standard 标准数据.csv --check 被检测数据.csv
  
  # 指定时间范围：只比较2023年的数据
  python csv_check.py --standard 标准数据.csv --check 被检测数据.csv --start-date 20230101 --end-date 20231231
  
  # 自定义输出文件并使用全部对比模式
  python csv_check.py --standard 标准数据.csv --check 被检测数据.csv --output 比较结果.csv --full-compare 1
  
  # 使用简写参数
  python csv_check.py -s 标准数据.csv -c 被检测数据.csv -sd 20230101 -ed 20231231 -o 结果.csv
  
  # 期货数据比较示例：比较黄金期货数据
  python csv_check.py -s ~/data_server/share_path/.tushare/data/raw/futures/1min/AU2510.csv -c ~/tb_furture_data/AU9999.XSGE.csv -sd 20250630 -ed 20250918 -fc 1

详细帮助:
  运行 'python csv_check.py -h' 或 'python csv_check.py --help' 查看此帮助信息
        """)
    
    # 添加必要参数
    parser.add_argument('--standard', '--std', '-s', required=True,
                        help='标准数据文件路径（基准数据源），作为对比的参考标准')
    parser.add_argument('--check', '-c', required=True,
                        help='被检测数据文件路径（待检查数据源），需要验证的数据文件')
    
    # 添加可选参数
    parser.add_argument('--start-date', '-sd',
                        help='开始日期（格式：YYYYMMDD），仅比较此日期之后的数据')
    parser.add_argument('--end-date', '-ed',
                        help='结束日期（格式：YYYYMMDD），仅比较此日期之前的数据')
    parser.add_argument('--output', '-o',
                        help='结果文件路径，若不指定将自动生成')
    parser.add_argument('--full-compare', '-fc', type=int, choices=[0, 1], default=0,
                        help='全部对比标志（0：仅比较共同日期的数据；1：将全部数据按时间拼接；默认0）')
    
    # 解析参数
    args = parser.parse_args()
    
    # 提取参数值
    standard_file_path = args.standard
    check_file_path = args.check
    start_date = args.start_date
    end_date = args.end_date
    output_file_path = args.output
    full_compare = args.full_compare
    
    # 验证日期格式
    if start_date and not validate_date_format(start_date):
        logger.error(f"无效的开始日期格式: {start_date}，正确格式为YYYYMMDD")
        parser.print_help()
        sys.exit(1)
    
    if end_date and not validate_date_format(end_date):
        logger.error(f"无效的结束日期格式: {end_date}，正确格式为YYYYMMDD")
        parser.print_help()
        sys.exit(1)
    
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
        standard_mapping, check_mapping,
        full_compare
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