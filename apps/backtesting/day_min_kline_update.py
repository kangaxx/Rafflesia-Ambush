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

def call_futting_map_script(fut_code, save_path):
    """
    调用Get_And_Compare_Futting_Map.py脚本获取期货映射信息
    
    Args:
        fut_code: 期货合约代码
        save_path: 保存路径
    
    Returns:
        bool: 调用是否成功
    """
    import subprocess
    
    try:
        # 获取脚本所在目录
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # 构建完整的脚本路径
        map_script_path = os.path.join(script_dir, 'Get_And_Compare_Futting_Map.py')
        
        logger.info(f"准备调用期货映射脚本，合约代码: {fut_code}，保存路径: {save_path}")
        
        # 构建命令参数
        cmd = [sys.executable, map_script_path, '-c', fut_code, '-s', save_path]
        
        # 执行命令
        logger.info(f"执行命令: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        
        # 检查执行结果
        if result.returncode == 0:
            logger.info(f"期货映射脚本调用成功: {fut_code}")
            logger.debug(f"脚本输出: {result.stdout}")
            return True
        else:
            logger.error(f"期货映射脚本调用失败: {fut_code}")
            logger.error(f"错误信息: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"调用期货映射脚本时发生异常: {str(e)}")
        return False


def check_and_create_tushare_root(config_path='default_param_list.json'):
    """
    读取配置文件，获取并检查tushare_root路径及其子路径
    若路径不存在则创建，包括future目录下的1d和1min子目录
    
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
            expanded_root = os.path.expanduser(tushare_root)
            
            # 确保路径格式适合当前操作系统
            if sys.platform == 'win32':
                expanded_root = expanded_root.replace('/', '\\')
            
            # 检查tushare_root路径是否存在，不存在则创建
            if not os.path.exists(expanded_root):
                logger.info(f"tushare_root路径不存在，正在创建: {expanded_root}")
                os.makedirs(expanded_root, exist_ok=True)
                logger.info(f"成功创建tushare_root路径: {expanded_root}")
            else:
                logger.info(f"tushare_root路径已存在: {expanded_root}")
            
            # 需要创建的子路径配置键
            subpath_keys = ['future', 'index', 'calendar', 'driver', 'calibrated']
            
            # 存储future路径以便后续创建子目录
            future_path = None
            
            # 处理每个子路径
            for key in subpath_keys:
                if key in config:
                    subpath = config[key]
                    print(f"{key}值: {subpath}")
                    
                    # 构建完整路径（去除开头的/，避免路径拼接错误）
                    full_path = os.path.join(expanded_root, subpath.lstrip('/'))
                    
                    # 确保路径格式适合当前操作系统
                    if sys.platform == 'win32':
                        full_path = full_path.replace('/', '\\')
                    
                    # 检查路径是否存在，不存在则创建
                    if not os.path.exists(full_path):
                        logger.info(f"{key}路径不存在，正在创建: {full_path}")
                        os.makedirs(full_path, exist_ok=True)
                        logger.info(f"成功创建{key}路径: {full_path}")
                    else:
                        logger.info(f"{key}路径已存在: {full_path}")
                    
                    # 保存future路径
                    if key == 'future':
                        future_path = full_path
                else:
                    logger.warning(f"配置文件中未找到{key}路径配置")
            
            # 在future目录下创建1d和1min子目录
            if future_path:
                data_subdirs = {'1d': '日线数据目录', '1min': '分钟线数据目录'}
                for subdir_key, desc in data_subdirs.items():
                    if subdir_key in config:
                        data_subpath = config[subdir_key]
                        print(f"{subdir_key}值: {data_subpath}")
                        
                        # 构建完整的数据子目录路径
                        full_data_path = os.path.join(future_path, data_subpath.lstrip('/'))
                        
                        # 确保路径格式适合当前操作系统
                        if sys.platform == 'win32':
                            full_data_path = full_data_path.replace('/', '\\')
                        
                        # 检查路径是否存在，不存在则创建
                        if not os.path.exists(full_data_path):
                            logger.info(f"future下的{desc}不存在，正在创建: {full_data_path}")
                            os.makedirs(full_data_path, exist_ok=True)
                            logger.info(f"成功创建{desc}: {full_data_path}")
                        else:
                            logger.info(f"{desc}已存在: {full_data_path}")
                    else:
                        logger.warning(f"配置文件中未找到{subdir_key}路径配置")
            
            return expanded_root
        else:
            logger.warning(f"配置文件不存在: {full_config_path}")
            # 使用默认路径
            default_root = os.path.expanduser('~/.tushare')
            if sys.platform == 'win32':
                default_root = default_root.replace('/', '\\')
            print(f"使用默认tushare_root值: {default_root}")
            
            # 检查并创建默认路径
            if not os.path.exists(default_root):
                logger.info(f"默认tushare_root路径不存在，正在创建: {default_root}")
                os.makedirs(default_root, exist_ok=True)
            
            # 创建默认子路径
            default_subpaths = {
                'future': 'data/raw/futures',
                'index': 'data/raw/index',
                'calendar': 'data/raw/calendar',
                'driver': 'data/driver',
                'calibrated': 'data/calibrated'
            }
            
            # 存储默认future路径
            default_future_path = os.path.join(default_root, default_subpaths['future'])
            if sys.platform == 'win32':
                default_future_path = default_future_path.replace('/', '\\')
            
            # 创建子路径
            for key, default_subpath in default_subpaths.items():
                full_path = os.path.join(default_root, default_subpath)
                if sys.platform == 'win32':
                    full_path = full_path.replace('/', '\\')
                print(f"使用默认{key}值: {default_subpath}")
                
                if not os.path.exists(full_path):
                    logger.info(f"默认{key}路径不存在，正在创建: {full_path}")
                    os.makedirs(full_path, exist_ok=True)
            
            # 创建默认的future下的1d和1min子目录
            default_data_subdirs = {
                '1d': '1d',
                '1min': '1min'
            }
            
            for subdir_key, default_data_subpath in default_data_subdirs.items():
                full_data_path = os.path.join(default_future_path, default_data_subpath)
                if sys.platform == 'win32':
                    full_data_path = full_data_path.replace('/', '\\')
                print(f"使用默认{subdir_key}值: {default_data_subpath}")
                
                if not os.path.exists(full_data_path):
                    desc = '日线数据目录' if subdir_key == '1d' else '分钟线数据目录'
                    logger.info(f"默认future下的{desc}不存在，正在创建: {full_data_path}")
                    os.makedirs(full_data_path, exist_ok=True)
            
            return default_root
            
    except Exception as e:
        logger.error(f"检查并创建路径时出错: {e}")
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
        
        # 构建index目录作为save_path
        index_path = config.get('index', '/data/raw/index')
        save_path = os.path.join(tushare_root, index_path.lstrip('/'))
        if sys.platform == 'win32':
            save_path = save_path.replace('/', '\\')
        logger.info(f"使用index目录作为保存路径: {save_path}")
        
        # 遍历合约列表，调用期货映射脚本
        logger.info("开始调用期货映射脚本处理合约...")
        map_success_count = 0
        map_fail_count = 0
        
        for contract in contracts:
            logger.info(f"处理合约: {contract}")
            if call_futting_map_script(contract, save_path):
                map_success_count += 1
                logger.info(f"合约 {contract} 映射处理成功")
            else:
                map_fail_count += 1
                logger.error(f"合约 {contract} 映射处理失败")
        
        # 输出映射处理汇总信息
        logger.info(f"期货映射处理汇总 - 成功: {map_success_count}, 失败: {map_fail_count}, 总计: {len(contracts)}")
        
        # 遍历合约列表并更新数据
        logger.info("开始更新K线数据...")
        success_count = 0
        fail_count = 0
        
        for contract in contracts:
            if update_kline_data(contract, args.data_type, config):
                success_count += 1
            else:
                fail_count += 1
        
        # 输出数据更新汇总信息
        logger.info(f"数据更新汇总 - 成功: {success_count}, 失败: {fail_count}, 总计: {len(contracts)}")
        
        # 综合判断退出状态
        total_fail = map_fail_count + fail_count
        if total_fail > 0:
            logger.warning(f"处理过程中存在失败项: 映射失败 {map_fail_count} 个, 数据更新失败 {fail_count} 个")
            sys.exit(1)
        else:
            logger.info("所有合约处理成功完成")
            sys.exit(0)
            
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        sys.exit(0)
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()