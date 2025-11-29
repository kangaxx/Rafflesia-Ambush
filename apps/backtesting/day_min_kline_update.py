#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日线分钟线K线数据更新工具

此脚本用于更新期货的日线和分钟线K线数据。
"""

import argparse
import os
import sys
import logging
import json

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('log/day_min_kline_update.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """
    解析命令行参数
    
    Returns:
        argparse.Namespace: 解析后的命令行参数
    """
    parser = argparse.ArgumentParser(description='日线分钟线K线数据更新工具')
    
    # 添加合约参数
    parser.add_argument('-c', '--contracts', type=str, required=True,
                        help='需要更新的合约代码，多个合约用逗号分隔，例如: "RB.SHF,HC.SHF,I.DCE"')
    
    # 添加数据类型参数
    parser.add_argument('-t', '--data_type', type=str, default='both', choices=['day', 'min', 'both'],
                        help='数据类型：day(日线), min(分钟线), both(两者都更新)，默认为both')
    
  
    # 添加配置文件参数
    parser.add_argument('--config', type=str, default='default_param_list.json',
                        help='配置文件路径，默认为同目录下的default_param_list.json')
    
    return parser.parse_args()

def load_config(config_path):
    """
    加载配置文件
    
    Args:
        config_path: 配置文件路径
    
    Returns:
        dict: 配置字典
    """
    # 获取脚本所在目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # 构建完整的配置文件路径
    full_config_path = os.path.join(script_dir, config_path)
    
    if os.path.exists(full_config_path):
        with open(full_config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        logger.info(f"成功加载配置文件: {full_config_path}")
        return config
    else:
        logger.warning(f"配置文件不存在: {full_config_path}，使用默认配置")
        return {}

def update_kline_data(contract, data_type, start_date, end_date, config):
    """
    更新指定合约的K线数据
    
    Args:
        contract: 合约代码
        data_type: 数据类型 ('day', 'min', 'both')
        start_date: 开始日期
        end_date: 结束日期
        config: 配置字典
    
    Returns:
        bool: 更新是否成功
    """
    try:
        logger.info(f"开始更新合约 {contract} 的数据，数据类型: {data_type}")
        
        # 在这里实现具体的K线数据更新逻辑
        # 这是一个示例框架，需要根据实际需求实现
        
        # 1. 验证合约格式
        if '.' not in contract:
            logger.error(f"合约编码格式错误: {contract}，应为'品种.交易所'格式")
            return False
        
        # 2. 根据数据类型确定需要更新的数据
        update_day = data_type in ['day', 'both']
        update_min = data_type in ['min', 'both']
        
        # 3. 从配置中获取必要的路径信息
        tushare_root = config.get('tushare_root', '~/.tushare')
        future_path = config.get('future', '/data/raw/futures')
        day_path = config.get('1d', '/1d')
        min_path = config.get('1min', '/1min')
        
        # 4. 展开路径并转换为适合当前操作系统的格式
        tushare_root = os.path.expanduser(tushare_root)
        
        # 构建日线数据路径
        if update_day:
            day_data_path = os.path.join(tushare_root, future_path.strip('/'), day_path.strip('/'))
            if sys.platform == 'win32':
                day_data_path = day_data_path.replace('/', '\\')
            logger.info(f"日线数据路径: {day_data_path}")
        
        # 构建分钟线数据路径
        if update_min:
            min_data_path = os.path.join(tushare_root, future_path.strip('/'), min_path.strip('/'))
            if sys.platform == 'win32':
                min_data_path = min_data_path.replace('/', '\\')
            logger.info(f"分钟线数据路径: {min_data_path}")
        
        # 5. 这里应该是实际的数据更新逻辑
        # 例如调用Tushare API获取数据，或从其他来源更新数据
        
        # 示例：模拟数据更新
        print(f"[更新] 合约: {contract}")
        if update_day:
            print(f"[更新] - 日线数据，时间范围: {start_date or '最早'} 到 {end_date or '最新'}")
        if update_min:
            print(f"[更新] - 分钟线数据，时间范围: {start_date or '最早'} 到 {end_date or '最新'}")
        
        logger.info(f"合约 {contract} 的数据更新完成")
        return True
        
    except Exception as e:
        logger.error(f"更新合约 {contract} 的数据时出错: {e}")
        return False

def main():
    """
    主函数
    """
    try:
        # 解析命令行参数
        args = parse_arguments()
        
        # 加载配置文件
        config = load_config(args.config)
        
        # 解析合约列表
        contracts = [c.strip() for c in args.contracts.split(',')]
        logger.info(f"需要更新的合约列表: {contracts}")
        
        # 处理结束日期为None的情况（设置为当天）
        if args.end_date is None:
            from datetime import datetime
            args.end_date = datetime.now().strftime('%Y%m%d')
            logger.info(f"未指定结束日期，使用当天日期: {args.end_date}")
        
        # 遍历合约列表并更新数据
        success_count = 0
        fail_count = 0
        
        for contract in contracts:
            if update_kline_data(contract, args.data_type, args.start_date, args.end_date, config):
                success_count += 1
            else:
                fail_count += 1
        
        # 输出汇总信息
        logger.info(f"数据更新汇总 - 成功: {success_count}, 失败: {fail_count}, 总计: {len(contracts)}")
        
        if fail_count > 0:
            logger.warning("部分合约数据更新失败")
            sys.exit(1)
        else:
            logger.info("所有合约数据更新成功")
            sys.exit(0)
            
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        sys.exit(0)
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()