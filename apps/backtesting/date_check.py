#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
字段比较程序
功能：比较两个CSV数据源文件中的open、high、low、close、volume、持仓六个字段的值
参数：
    1. 标准数据文件来源路径（基准数据源）- 仅支持CSV格式
    2. 被检测数据来源路径（待检查数据源）- 仅支持CSV格式
    3. 开始日期（可选）- 格式：YYYYMMDD，如不指定则从最早日期开始
    4. 结束日期（可选）- 格式：YYYYMMDD，如不指定则到最晚日期结束
    5. 结果文件路径（可选）- 完整的CSV文件路径，默认是当前目录下的"data_chk_result_" + YYMMDD + ".csv"
"""

import os
import sys
import pandas as pd
import logging
from datetime import datetime
import numpy as np

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
    'volume': ['volume', '成交量', '量'],
    '持仓': ['持仓', '持仓量', 'position', 'open_interest']
}

def find_date_column(df):
    """
    在DataFrame中查找日期列
    
    Args:
        df: pandas DataFrame
    
    Returns:
        str: 日期列名
    """
    # 常见的日期列名
    date_columns = ['trade_date', 'date', 'datetime', 'time', 'trading_date', '交易日']
    
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
    标准化日期格式为YYYYMMDD字符串
    
    Args:
        date_val: 日期值
    
    Returns:
        str: 标准化后的日期字符串，或None如果无法解析
    """
    date_str = str(date_val).strip()
    # 确保是8位数字的日期格式
    if len(date_str) == 8 and date_str.isdigit():
        return date_str
    elif '-' in date_str or '/' in date_str:
        # 尝试解析标准日期格式
        try:
            # 尝试不同的日期格式
            date_formats = ['%Y-%m-%d', '%Y/%m/%d', '%Y%m%d']
            parsed_date = None
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    break
                except ValueError:
                    continue
            
            if parsed_date:
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
    从文件中加载数据，包括日期和需要比较的字段
    
    Args:
        file_path: 文件路径
        start_date: 开始日期，格式YYYYMMDD
        end_date: 结束日期，格式YYYYMMDD
    
    Returns:
        tuple: (data_dict, field_mapping)，data_dict是日期到字段值的映射，field_mapping是字段映射关系
    """
    if not os.path.exists(file_path):
        logger.error(f"文件不存在: {file_path}")
        return None, None
    
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
                return None, None
            
            logger.info(f"使用列 '{date_column}' 作为日期列")
            
            # 查找字段映射
            field_mapping = find_field_mapping(df)
            missing_fields = set(FIELDS_TO_COMPARE.keys()) - set(field_mapping.keys())
            if missing_fields:
                logger.warning(f"文件 {file_path} 缺少以下必要字段: {', '.join(missing_fields)}")
            
            # 构建数据字典
            data_dict = {}
            for idx, row in df.iterrows():
                # 标准化日期
                date_str = normalize_date(row[date_column])
                if not date_str:
                    continue
                
                # 根据日期范围过滤
                if start_date and date_str < start_date:
                    continue
                if end_date and date_str > end_date:
                    continue
                
                # 存储字段值
                fields_data = {}
                for field, actual_col in field_mapping.items():
                    val = row[actual_col]
                    # 尝试转换为数值类型
                    if pd.isna(val):
                        fields_data[field] = None
                    else:
                        try:
                            fields_data[field] = float(val)
                        except (ValueError, TypeError):
                            fields_data[field] = val
                
                data_dict[date_str] = fields_data
            
            logger.info(f"从 {file_path} 加载了 {len(data_dict)} 条记录")
            return data_dict, field_mapping
        
        else:
            logger.error(f"只支持CSV文件格式，不支持: {file_ext}")
            return None, None
            
    except Exception as e:
        logger.error(f"读取文件时发生错误: {e}")
        return None, None

def compare_fields(standard_data, check_data, standard_mapping, check_mapping):
    """
    比较两个数据源的字段值
    
    Args:
        standard_data: 标准数据源，格式为{date: {field: value}}
        check_data: 被检查数据源，格式为{date: {field: value}}
        standard_mapping: 标准数据源的字段映射
        check_mapping: 被检查数据源的字段映射
    
    Returns:
        dict: 比较结果
    """
    # 获取共同的日期
    standard_dates = set(standard_data.keys())
    check_dates = set(check_data.keys())
    common_dates = standard_dates & check_dates
    
    # 找出缺失和多出的日期
    missing_dates = sorted(standard_dates - check_dates)
    extra_dates = sorted(check_dates - standard_dates)
    
    # 获取可比较的字段
    common_fields = set(standard_mapping.keys()) & set(check_mapping.keys())
    
    # 比较字段值
    field_discrepancies = {}
    for field in common_fields:
        field_discrepancies[field] = []
    
    # 定义精度阈值
    float_threshold = 1e-6
    
    for date in common_dates:
        std_data = standard_data[date]
        chk_data = check_data[date]
        
        for field in common_fields:
            std_val = std_data.get(field)
            chk_val = chk_data.get(field)
            
            # 检查是否都有值
            if std_val is None or chk_val is None:
                if std_val != chk_val:  # 一个有值，一个无值
                    field_discrepancies[field].append({
                        'date': date,
                        'standard_value': std_val,
                        'check_value': chk_val
                    })
            # 数值类型比较
            elif isinstance(std_val, (int, float)) and isinstance(chk_val, (int, float)):
                # 使用numpy的isclose来处理浮点精度问题
                if not np.isclose(std_val, chk_val, atol=float_threshold):
                    field_discrepancies[field].append({
                        'date': date,
                        'standard_value': std_val,
                        'check_value': chk_val,
                        'difference': abs(std_val - chk_val),
                        'percentage_diff': (abs(std_val - chk_val) / abs(std_val) * 100) if std_val != 0 else float('inf')
                    })
            # 其他类型直接比较
            elif std_val != chk_val:
                field_discrepancies[field].append({
                    'date': date,
                    'standard_value': std_val,
                    'check_value': chk_val
                })
    
    # 计算统计信息
    total_standard = len(standard_dates)
    total_check = len(check_dates)
    total_common = len(common_dates)
    coverage_rate = (total_common / total_standard) * 100 if total_standard > 0 else 0
    
    # 计算每个字段的差异统计
    field_stats = {}
    for field in common_fields:
        discrepancies = field_discrepancies[field]
        field_stats[field] = {
            'total_discrepancies': len(discrepancies),
            'discrepancy_rate': (len(discrepancies) / total_common * 100) if total_common > 0 else 0
        }
    
    return {
        'missing_dates': missing_dates,
        'extra_dates': extra_dates,
        'total_standard': total_standard,
        'total_check': total_check,
        'total_common': total_common,
        'coverage_rate': coverage_rate,
        'common_fields': sorted(common_fields),
        'field_discrepancies': field_discrepancies,
        'field_stats': field_stats
    }

def print_comparison_results(results):
    """
    打印比较结果
    
    Args:
        results: 比较结果字典
    """
    logger.info("=== 字段比较结果 ===")
    logger.info(f"标准数据源总记录数: {results['total_standard']}")
    logger.info(f"被检查数据源总记录数: {results['total_check']}")
    logger.info(f"共同日期数: {results['total_common']}")
    logger.info(f"覆盖率: {results['coverage_rate']:.2f}%")
    
    if results['missing_dates']:
        logger.info(f"被检查数据源缺失的日期数: {len(results['missing_dates'])}")
        # 限制输出缺失日期的数量
        if len(results['missing_dates']) <= 20:
            logger.info(f"缺失的日期: {', '.join(results['missing_dates'][:10])}")
            if len(results['missing_dates']) > 10:
                logger.info(f"... 还有 {len(results['missing_dates']) - 10} 个缺失日期")
        else:
            logger.info(f"前10个缺失日期: {', '.join(results['missing_dates'][:10])}")
            logger.info(f"... 还有 {len(results['missing_dates']) - 10} 个缺失日期")
    else:
        logger.info("被检查数据源没有缺失日期")
    
    if results['extra_dates']:
        logger.info(f"被检查数据源多出的日期数: {len(results['extra_dates'])}")
        # 限制输出多出日期的数量
        if len(results['extra_dates']) <= 20:
            logger.info(f"多出的日期: {', '.join(results['extra_dates'][:10])}")
            if len(results['extra_dates']) > 10:
                logger.info(f"... 还有 {len(results['extra_dates']) - 10} 个多出日期")
        else:
            logger.info(f"前10个多出日期: {', '.join(results['extra_dates'][:10])}")
            logger.info(f"... 还有 {len(results['extra_dates']) - 10} 个多出日期")
    else:
        logger.info("被检查数据源没有多出日期")
    
    # 打印字段比较结果
    logger.info(f"\n可比较的字段: {', '.join(results['common_fields'])}")
    for field in results['common_fields']:
        stats = results['field_stats'][field]
        logger.info(f"\n字段 '{field}':")
        logger.info(f"  差异记录数: {stats['total_discrepancies']}")
        logger.info(f"  差异率: {stats['discrepancy_rate']:.2f}%")
        
        # 显示前几个差异记录
        discrepancies = results['field_discrepancies'][field]
        if discrepancies[:5]:  # 只显示前5个
            logger.info("  前几个差异记录:")
            for disc in discrepancies[:5]:
                if 'percentage_diff' in disc:
                    logger.info(f"    日期 {disc['date']}: 标准值={disc['standard_value']}, 检查值={disc['check_value']}, 差异={disc['difference']:.6f}, 差异百分比={disc['percentage_diff']:.2f}%")
                else:
                    logger.info(f"    日期 {disc['date']}: 标准值={disc['standard_value']}, 检查值={disc['check_value']}")
            if len(discrepancies) > 5:
                logger.info(f"    ... 还有 {len(discrepancies) - 5} 个差异记录")

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
        logger.error("使用方法: python date_check.py <标准数据文件路径> <被检测数据文件路径> [开始日期YYYYMMDD] [结束日期YYYYMMDD] [结果文件路径]")
        sys.exit(1)
    
    standard_file_path = sys.argv[1]
    check_file_path = sys.argv[2]
    
    # 处理可选的开始日期、结束日期和结果文件路径参数
    start_date = None
    end_date = None
    output_file_path = None
    
    if len(sys.argv) >= 4:
        start_date = sys.argv[3]
        if not validate_date_format(start_date):
            # 尝试判断是否为结果文件路径
            if os.path.splitext(start_date)[1].lower() == '.csv':
                output_file_path = start_date
                start_date = None
            else:
                logger.error(f"无效的开始日期格式: {start_date}，应为YYYYMMDD格式")
                sys.exit(1)
    
    if len(sys.argv) >= 5:
        if output_file_path is None:
            end_date = sys.argv[4]
            if not validate_date_format(end_date):
                # 尝试判断是否为结果文件路径
                if os.path.splitext(end_date)[1].lower() == '.csv':
                    output_file_path = end_date
                    end_date = None
                else:
                    logger.error(f"无效的结束日期格式: {end_date}，应为YYYYMMDD格式")
                    sys.exit(1)
        else:
            # 此时第4个参数是结果文件路径，第5个参数不存在
            pass
    
    if len(sys.argv) >= 6:
        output_file_path = sys.argv[5]
    
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
    standard_data, standard_mapping = load_data_from_file(standard_file_path, start_date, end_date)
    if standard_data is None:
        logger.error("无法加载标准数据源，程序终止")
        sys.exit(1)
    
    logger.info("开始加载被检测数据源...")
    check_data, check_mapping = load_data_from_file(check_file_path, start_date, end_date)
    if check_data is None:
        logger.error("无法加载被检测数据源，程序终止")
        sys.exit(1)
    
    # 比较字段
    logger.info("开始比较字段值...")
    results = compare_fields(standard_data, check_data, standard_mapping, check_mapping)
    
    # 打印结果
    print_comparison_results(results)
    
    # 确定结果文件路径
    if not output_file_path:
        # 默认使用当前目录下的格式：data_chk_result_YYMMDD.csv
        current_date = datetime.now().strftime('%y%m%d')
        output_file_path = f"data_chk_result_{current_date}.csv"
    
    # 确保目录存在
    output_dir = os.path.dirname(output_file_path)
    if output_dir and not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
            logger.info(f"创建目录: {output_dir}")
        except Exception as e:
            logger.error(f"创建目录失败: {e}")
            sys.exit(1)
    
    # 保存详细结果到CSV文件
    try:
        # 创建结果数据列表
        all_results = []
        
        # 添加汇总信息
        all_results.append({
            '类型': '汇总信息',
            '日期': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            '字段': '',
            '标准值': standard_file_path,
            '检查值': check_file_path,
            '差异': '',
            '差异百分比': '',
            '备注': '标准数据源文件路径'
        })
        all_results.append({
            '类型': '汇总信息',
            '日期': '',
            '字段': '',
            '标准值': '',
            '检查值': '',
            '差异': '',
            '差异百分比': '',
            '备注': '被检测数据源文件路径'
        })
        
        if start_date:
            all_results.append({
                '类型': '汇总信息',
                '日期': '',
                '字段': '开始日期',
                '标准值': start_date,
                '检查值': '',
                '差异': '',
                '差异百分比': '',
                '备注': ''
            })
        if end_date:
            all_results.append({
                '类型': '汇总信息',
                '日期': '',
                '字段': '结束日期',
                '标准值': end_date,
                '检查值': '',
                '差异': '',
                '差异百分比': '',
                '备注': ''
            })
        
        all_results.append({
            '类型': '汇总信息',
            '日期': '',
            '字段': '标准数据源总记录数',
            '标准值': results['total_standard'],
            '检查值': '',
            '差异': '',
            '差异百分比': '',
            '备注': ''
        })
        all_results.append({
            '类型': '汇总信息',
            '日期': '',
            '字段': '被检查数据源总记录数',
            '标准值': '',
            '检查值': results['total_check'],
            '差异': '',
            '差异百分比': '',
            '备注': ''
        })
        all_results.append({
            '类型': '汇总信息',
            '日期': '',
            '字段': '共同日期数',
            '标准值': results['total_common'],
            '检查值': results['total_common'],
            '差异': '',
            '差异百分比': '',
            '备注': ''
        })
        all_results.append({
            '类型': '汇总信息',
            '日期': '',
            '字段': '覆盖率',
            '标准值': '',
            '检查值': '',
            '差异': f"{results['coverage_rate']:.2f}%",
            '差异百分比': '',
            '备注': ''
        })
        
        # 添加缺失日期信息
        for date in results['missing_dates']:
            all_results.append({
                '类型': '缺失日期',
                '日期': date,
                '字段': '',
                '标准值': '存在',
                '检查值': '缺失',
                '差异': '',
                '差异百分比': '',
                '备注': ''
            })
        
        # 添加多出日期信息
        for date in results['extra_dates']:
            all_results.append({
                '类型': '多出日期',
                '日期': date,
                '字段': '',
                '标准值': '不存在',
                '检查值': '存在',
                '差异': '',
                '差异百分比': '',
                '备注': ''
            })
        
        # 添加字段差异详情
        for field in results['common_fields']:
            # 添加字段统计信息
            stats = results['field_stats'][field]
            all_results.append({
                '类型': '字段统计',
                '日期': '',
                '字段': field,
                '标准值': '',
                '检查值': '',
                '差异': f"{stats['total_discrepancies']}",
                '差异百分比': f"{stats['discrepancy_rate']:.2f}%",
                '备注': '差异记录数/差异率'
            })
            
            # 添加具体差异记录
            discrepancies = results['field_discrepancies'][field]
            for disc in discrepancies:
                diff_value = disc.get('difference', '')
                pct_diff = disc.get('percentage_diff', '')
                all_results.append({
                    '类型': '字段差异',
                    '日期': disc['date'],
                    '字段': field,
                    '标准值': disc['standard_value'],
                    '检查值': disc['check_value'],
                    '差异': f"{diff_value:.6f}" if isinstance(diff_value, (int, float)) else '',
                    '差异百分比': f"{pct_diff:.2f}%" if isinstance(pct_diff, (int, float)) else '',
                    '备注': ''
                })
        
        # 创建DataFrame并保存到CSV
        df = pd.DataFrame(all_results)
        df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
        
        logger.info(f"详细比较结果已保存到: {output_file_path}")
    except Exception as e:
        logger.error(f"保存结果文件时发生错误: {e}")
    
    logger.info("字段检查完成")

if __name__ == "__main__":
    main()