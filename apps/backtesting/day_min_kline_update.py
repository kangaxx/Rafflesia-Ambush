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

# 获取脚本所在目录
script_dir = os.path.dirname(os.path.abspath(__file__))

# 创建日志目录（如果不存在）
log_dir = os.path.join(script_dir, 'log')
os.makedirs(log_dir, exist_ok=True)

# 配置日志
log_file = os.path.join(log_dir, 'day_min_kline_update.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info(f"日志文件路径: {log_file}")

def parse_arguments():
    """
    解析命令行参数
    
    Returns:
        argparse.Namespace: 解析后的命令行参数
    """
    parser = argparse.ArgumentParser(description='日线分钟线K线数据更新工具')
    
    # 添加合约参数 默认RB.SHF,AG.SHF
    parser.add_argument('-c', '--contracts', type=str, default='RB.SHF,AG.SHF',
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

def check_and_create_tushare_root(config_path='default_param_list.json'):
    """
    读取配置文件，获取并检查tushare_root路径
    若路径不存在则创建
    
    Args:
        config_path: 配置文件路径，默认为同目录下的default_param_list.json
    
    Returns:
        str: tushare_root的绝对路径
    """
    try:
        # 获取脚本所在目录
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # 构建完整的配置文件路径
        full_config_path = os.path.join(script_dir, config_path)
        
        # 读取配置文件
        if os.path.exists(full_config_path):
            with open(full_config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # 获取tushare_root配置，默认为~/.tushare
            tushare_root = config.get('tushare_root', '~/.tushare')
            logger.info(f"从配置文件读取到tushare_root: {tushare_root}")
            print(f"tushare_root值: {tushare_root}")
            
            # 展开路径（处理~符号）
            expanded_path = os.path.expanduser(tushare_root)
            
            # 确保路径格式适合当前操作系统
            if sys.platform == 'win32':
                expanded_path = expanded_path.replace('/', '\\')
            
            # 检查路径是否存在，不存在则创建
            if not os.path.exists(expanded_path):
                logger.info(f"tushare_root路径不存在，正在创建: {expanded_path}")
                os.makedirs(expanded_path, exist_ok=True)
                logger.info(f"成功创建tushare_root路径: {expanded_path}")
            else:
                logger.info(f"tushare_root路径已存在: {expanded_path}")
            
            return expanded_path
        else:
            logger.warning(f"配置文件不存在: {full_config_path}")
            # 使用默认路径
            default_path = os.path.expanduser('~/.tushare')
            if sys.platform == 'win32':
                default_path = default_path.replace('/', '\\')
            print(f"使用默认tushare_root值: {default_path}")
            
            # 检查并创建默认路径
            if not os.path.exists(default_path):
                logger.info(f"默认tushare_root路径不存在，正在创建: {default_path}")
                os.makedirs(default_path, exist_ok=True)
            
            return default_path
            
    except Exception as e:
        logger.error(f"检查并创建tushare_root路径时出错: {e}")
        raise

def update_kline_data(contract, data_type, config):
    """
    更新指定合约的K线数据
    
    Args:
        contract: 合约代码
        data_type: 数据类型 ('day', 'min', 'both')
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
            print(f"[更新] - 日线数据")
        if update_min:
            print(f"[更新] - 分钟线数据")
        
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
        
        # 检查并创建tushare_root目录
        tushare_root = check_and_create_tushare_root(args.config)
        
        # 加载配置文件
        config = load_config(args.config)
        
        # 解析合约列表
        contracts = [c.strip() for c in args.contracts.split(',')]
        logger.info(f"需要更新的合约列表: {contracts}")
        
        # 遍历合约列表并更新数据
        success_count = 0
        fail_count = 0
        
        for contract in contracts:
            if update_kline_data(contract, args.data_type, config):
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