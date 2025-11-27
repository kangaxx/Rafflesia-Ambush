#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
期货映射代码对比工具

功能：获取期货合约映射信息并与现有数据进行对比
参数：
    1. fut_code - 期货产品代码
    2. save_path - 文件保存路径
    3. compare_source - 作为对比的文件来源
    4. result_path - 比较结果输出路径
"""

import os
import sys
import logging
import argparse
import json
import tushare as ts
from typing import Dict, List, Any, Optional

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fut_mapping_compare.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def _read_tushare_token() -> Optional[str]:
    """
    从key.json文件中读取token
    
    Returns:
        token字符串，如果文件不存在或读取失败则返回None
    """
    try:
        # 尝试读取与脚本同目录的key.json文件
        script_dir = os.path.dirname(os.path.abspath(__file__))
        key_json_path = os.path.join(script_dir, 'key.json')
        
        if not os.path.exists(key_json_path):
            logger.warning(f"key.json文件不存在: {key_json_path}")
            return None
        
        with open(key_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # 尝试从不同可能的键中获取token
        token = data.get('token')
        if not token:
            token = data.get('TUSHARE_TOKEN')
        if not token:
            token = data.get('tushare_token')
        
        if not token:
            logger.warning("key.json文件中未找到有效的token")
            return None
        
        logger.info("成功从key.json读取token")
        return token
        
    except json.JSONDecodeError as e:
        logger.error(f"解析key.json文件失败: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"读取key.json文件时发生错误: {str(e)}")
        return None

def parse_arguments():
    """解析命令行参数，支持位置参数和短选项模式"""
    parser = argparse.ArgumentParser(
        description='期货映射代码对比工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用说明:
  本工具用于获取期货合约映射数据并与现有文件进行对比。
  支持位置参数模式和短选项模式，且大部分参数都有默认值。

默认值说明:
  - save_path: './data/out'
  - compare_source: '/root/Rafflesia-Ambush/apps/backtesting/data/out/RB_main_contract_mapping.csv'
  - result_path: 'compare_futting_map.csv'

使用范例:
  # 最简模式：只提供期货代码
  python Get_And_Compare_Futting_Map.py RB.SHF
  python Get_And_Compare_Futting_Map.py -c RB.SHF
  
  # 提供期货代码和保存路径
  python Get_And_Compare_Futting_Map.py RB.SHF ./my_data
  python Get_And_Compare_Futting_Map.py -c RB.SHF -s ./my_data
  
  # 完整参数模式
  python Get_And_Compare_Futting_Map.py RB.SHF ./my_data ./compare_file.csv ./result.csv
  python Get_And_Compare_Futting_Map.py -c RB.SHF -s ./my_data -f ./compare_file.csv -r ./result.csv
  
  # 查看帮助信息
  python Get_And_Compare_Futting_Map.py -h
        """)
    
    # 移除未使用的互斥组，位置参数和短选项现在可以更灵活地使用
    
    # 短选项模式
    parser.add_argument('-c', '--fut_code', help='期货产品代码，例如：RB.SHF（必需）', required=False)
    parser.add_argument('-s', '--save_path', help=f'文件保存路径（默认: ./data/out）', required=False, default='./data/out')
    parser.add_argument('-f', '--compare_source', help=f'作为对比的文件来源（默认: /root/Rafflesia-Ambush/apps/backtesting/data/out/RB_main_contract_mapping.csv）', 
                        default='/root/Rafflesia-Ambush/apps/backtesting/data/out/RB_main_contract_mapping.csv')
    parser.add_argument('-r', '--result_path', help=f'比较结果输出路径（默认: compare_futting_map.csv）', 
                        default='compare_futting_map.csv')
    
    # 位置参数模式（通过nargs='*'和检查来实现）
    parser.add_argument('positional_args', nargs='*', help='位置参数：fut_code save_path compare_source result_path')
    
    args = parser.parse_args()
    
    # 处理位置参数模式
    if args.positional_args and len(args.positional_args) == 4:
        args.fut_code = args.positional_args[0]
        args.save_path = args.positional_args[1]
        args.compare_source = args.positional_args[2]
        args.result_path = args.positional_args[3]
    elif args.positional_args and len(args.positional_args) == 2:
        # 支持只提供前两个参数的位置参数模式
        args.fut_code = args.positional_args[0]
        args.save_path = args.positional_args[1]
    elif args.positional_args and len(args.positional_args) == 1:
        # 支持只提供fut_code的位置参数模式
        args.fut_code = args.positional_args[0]
    elif args.positional_args:
        parser.error("位置参数模式需要提供1个、2个或4个参数：fut_code [save_path [compare_source result_path]]")
    
    # 验证必需的参数都已提供
    required_args = ['fut_code']
    for arg_name in required_args:
        if not getattr(args, arg_name):
            parser.error(f"缺少必需参数：{arg_name}（请使用位置参数或短选项）")
    
    # 移除positional_args属性，保持接口一致性
    delattr(args, 'positional_args')
    
    return args

def get_future_mapping(fut_code: str) -> List[Dict[str, Any]]:
    """
    获取期货合约映射信息
    
    Args:
        fut_code: 期货产品代码
    
    Returns:
        合约映射信息列表，包含trade_date和mapping_ts_code字段
    """
    logger.info(f"获取期货产品 {fut_code} 的映射信息")
    
    # 验证期货代码格式（保持格式验证，确保代码有效）
    try:
        symbol, exchange = fut_code.split('.')
        logger.info(f"验证期货代码格式：品种={symbol}, 交易所={exchange}")
    except ValueError:
        logger.error(f"无效的期货产品代码格式：{fut_code}，应为'品种.交易所'格式")
        raise ValueError(f"无效的期货产品代码格式：{fut_code}，应为'品种.交易所'格式")
    
    # 获取tushare token
    token = _read_tushare_token()
    if not token:
        logger.error("未找到有效的tushare token，无法调用API")
        raise RuntimeError("未找到有效的tushare token，请确保key.json文件中包含有效的token")
    
    try:
        # 初始化tushare API
        ts.set_token(token)
        pro = ts.pro_api()
        logger.info("成功初始化tushare API")
        
        # 调用tushare fut_mapping接口获取期货映射数据
        logger.info(f"调用tushare fut_mapping接口，获取 {fut_code} 的映射数据")
        
        # 调用API，使用正确的参数
        df = pro.fut_mapping(
            ts_code=fut_code, # 期货品种代码
            fields='ts_code,trade_date,mapping_ts_code'  # 请求的字段
        )
        # 默认下载的数据df是时间倒序排列的，我们需要对df执行一次顺序排列
        df = df.sort_values(by='trade_date', ascending=True)

        logger.info(f"成功获取 {len(df)} 条映射数据")
        
        # 转换DataFrame为字典列表，确保包含所需字段
        mapping_data = []
        for _, row in df.iterrows():
            item = {
                'trade_date': row.get('trade_date', ''),  # 交易日期
                'mapping_ts_code': row.get('mapping_ts_code', '')  # 映射合约代码
            }
            mapping_data.append(item)
        
        # 如果没有获取到数据，提供适当的错误提示
        if not mapping_data:
            logger.warning(f"未获取到 {fut_code} 的映射数据，请检查品种代码和交易所是否正确")
        else:
            logger.info(f"成功解析映射数据，包含 {len(mapping_data)} 条记录")
        
        return mapping_data
        
    except Exception as e:
        logger.error(f"调用tushare API获取期货映射数据失败: {str(e)}")
        # 提供更详细的错误信息
        if "rate limit" in str(e).lower():
            error_msg = f"API调用频率超限，请稍后再试: {str(e)}"
        elif "invalid token" in str(e).lower():
            error_msg = f"无效的token，请检查tushare.json文件中的token是否正确: {str(e)}"
        else:
            error_msg = f"获取期货映射数据失败: {str(e)}"
        
        raise RuntimeError(error_msg)

def save_mapping_data(data: List[Dict[str, Any]], save_path: str, fut_code: str):
    """
    保存映射数据到文件
    
    Args:
        data: 映射数据列表
        save_path: 保存路径（目录）
        fut_code: 期货产品编码
    """
    # 拼接完整文件名：期货产品编码 + 'fut_mapping.csv'
    filename = f"{fut_code}_fut_mapping.csv"
    # 组合完整文件路径
    full_file_path = os.path.join(save_path, filename)
    
    logger.info(f"保存映射数据到 {full_file_path}")
    
    # 确保保存目录存在
    os.makedirs(os.path.dirname(os.path.abspath(full_file_path)), exist_ok=True)
    
    # 保存数据到文件
    try:
        with open(full_file_path, 'w', encoding='utf-8') as f:
            # 写入CSV格式，包含trade_date和mapping_ts_code字段
            f.write("trade_date,mapping_ts_code\n")  # 写入表头
            for item in data:
                f.write(f"{item.get('trade_date', '')},{item.get('mapping_ts_code', '')}\n")
        logger.info(f"映射数据已成功保存到 {full_file_path}")
    except Exception as e:
        logger.error(f"保存映射数据失败: {str(e)}")
        raise

def load_compare_source(compare_source: str) -> List[Dict[str, Any]]:
    """
    加载对比源数据
    
    Args:
        compare_source: 对比源文件路径
    
    Returns:
        对比源数据列表，包含trade_date和main_contract字段
    """
    logger.info(f"加载对比源数据: {compare_source}")
    
    # 检查文件是否存在
    if not os.path.exists(compare_source):
        logger.error(f"对比源文件不存在: {compare_source}")
        raise FileNotFoundError(f"对比源文件不存在: {compare_source}")
    
    # 读取对比源数据
    data = []
    try:
        with open(compare_source, 'r', encoding='utf-8') as f:
            # 假设文件格式为CSV，可能包含表头
            header_skipped = False
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    # 跳过表头
                    if not header_skipped and ('trade_date' in line or 'date' in line):
                        header_skipped = True
                        continue
                    
                    parts = line.split(',')
                    if len(parts) >= 2:
                        data.append({
                            'trade_date': parts[0],  # 交易日期
                            'main_contract': parts[1]  # 主合约代码
                        })
        logger.info(f"成功加载 {len(data)} 条对比源数据")
        return data
    except Exception as e:
        logger.error(f"加载对比源数据失败: {str(e)}")
        raise

def extract_contract_code(contract_str: str) -> str:
    """
    从合约代码中提取期货产品字母编号 + YY + MM 这六个字符
    
    Args:
        contract_str: 完整的合约代码
    
    Returns:
        提取的六个字符代码
    """
    if not contract_str:
        return ''
    
    # 尝试提取字母部分和年份月份
    import re
    # 匹配字母部分 + 年份(2位) + 月份(2位)的模式
    match = re.search(r'([A-Za-z]+)(\d{4})', contract_str)
    if match:
        # 提取字母部分
        symbol_part = match.group(1).upper()
        # 提取年份后两位和月份
        date_part = match.group(2)[2:]  # 取年份后两位和月份
        # 组合成6个字符
        return f"{symbol_part}{date_part}"
    
    # 如果没有匹配到，返回原始字符串的前6个字符
    return contract_str[:6].upper()

def compare_mapping_data(new_data: List[Dict[str, Any]], compare_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    比较tushare数据与文件数据，按日期对齐
    
    Args:
        new_data: tushare获取的映射数据，包含trade_date和mapping_ts_code
        compare_data: 从文件读取的对比数据，包含trade_date和main_contract
    
    Returns:
        比较结果列表，每条记录包含tushare数据、文件数据和是否相同的标志
    """
    logger.info(f"开始比较映射数据")
    
    # 创建日期到数据的映射以便快速查找
    new_date_map = {item['trade_date']: item for item in new_data if 'trade_date' in item}
    compare_date_map = {item['trade_date']: item for item in compare_data if 'trade_date' in item}
    
    # 获取所有唯一日期
    all_dates = set(new_date_map.keys()) | set(compare_date_map.keys())
    
    # 按日期排序
    sorted_dates = sorted(all_dates)
    
    # 生成比较结果
    comparison_results = []
    for date in sorted_dates:
        # 获取tushare数据和文件数据
        tushare_data = new_date_map.get(date, {'trade_date': date, 'mapping_ts_code': ''})
        file_data = compare_date_map.get(date, {'trade_date': date, 'main_contract': ''})
        
        # 提取需要比较的字段
        tushare_contract = tushare_data.get('mapping_ts_code', '')
        file_contract = file_data.get('main_contract', '')
        
        # 提取6个字符的合约代码
        tushare_code_6 = extract_contract_code(tushare_contract)
        file_code_6 = extract_contract_code(file_contract)
        
        # 比较是否相同
        is_same = tushare_code_6 == file_code_6
        
        # 构建比较结果记录
        result_record = {
            'trade_date': date,
            'tushare_mapping_ts_code': tushare_contract,
            'file_main_contract': file_contract,
            'tushare_code_6': tushare_code_6,
            'file_code_6': file_code_6,
            'is_same': is_same
        }
        
        comparison_results.append(result_record)
    
    logger.info(f"比较完成：共有 {len(comparison_results)} 条记录，其中 {(sum(1 for r in comparison_results if r['is_same']))} 条相同，{(sum(1 for r in comparison_results if not r['is_same']))} 条不同")
    
    return comparison_results

def save_comparison_result(result: List[Dict[str, Any]], result_path: str):
    """
    保存比较结果到文件
    
    Args:
        result: 比较结果列表
        result_path: 结果保存路径
    """
    logger.info(f"保存比较结果到 {result_path}")
    
    # 确保保存目录存在
    os.makedirs(os.path.dirname(os.path.abspath(result_path)), exist_ok=True)
    
    try:
        with open(result_path, 'w', encoding='utf-8') as f:
            # 写入表头
            f.write("trade_date,tushare_mapping_ts_code,file_main_contract,tushare_code_6,file_code_6,is_same\n")
            
            # 写入比较结果
            for item in result:
                is_same_str = "相同" if item['is_same'] else "不同"
                f.write(f"{item['trade_date']},{item['tushare_mapping_ts_code']},{item['file_main_contract']},{item['tushare_code_6']},{item['file_code_6']},{is_same_str}\n")
        
        # 统计相同和不同的记录数
        same_count = sum(1 for item in result if item['is_same'])
        different_count = sum(1 for item in result if not item['is_same'])
        
        logger.info(f"比较结果已成功保存到 {result_path}，共 {len(result)} 条记录，其中 {same_count} 条相同，{different_count} 条不同")
    except Exception as e:
        logger.error(f"保存比较结果失败: {str(e)}")
        raise

def main():
    """主函数"""
    try:
        # 解析命令行参数
        args = parse_arguments()
        logger.info(f"开始执行期货映射对比，参数: {args}")
        
        # 获取期货映射数据
        mapping_data = get_future_mapping(args.fut_code)
        
        # 保存映射数据
        save_mapping_data(mapping_data, args.save_path, args.fut_code)
        
        # 加载对比源数据
        compare_data = load_compare_source(args.compare_source)
        
        # 比较数据
        comparison_result = compare_mapping_data(mapping_data, compare_data)
        
        # 保存比较结果
        save_comparison_result(comparison_result, args.result_path)
        
        # 统计相同和不同的记录数
        same_count = sum(1 for item in comparison_result if item['is_same'])
        different_count = sum(1 for item in comparison_result if not item['is_same'])
        
        # 显示结果摘要
        print(f"对比完成！")
        print(f"- 总记录数: {len(comparison_result)}")
        print(f"- 相同记录: {same_count}")
        print(f"- 不同记录: {different_count}")
        print(f"详细结果已保存至: {args.result_path}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("用户中断操作")
        print("\n操作被用户中断")
        return 1
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        print(f"错误: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())