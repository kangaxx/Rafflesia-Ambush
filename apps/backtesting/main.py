#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import subprocess
import logging
import json
from typing import Dict, Any

# 获取当前目录
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# 设置日志
log_dir = os.path.join(CURRENT_DIR, 'log')
# 确保日志目录存在
os.makedirs(log_dir, exist_ok=True)

# 创建logger实例
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 清除可能存在的处理器
logger.handlers.clear()

# 创建文件处理器，写入到日志文件
log_file = os.path.join(log_dir, 'main_backtesting.log')
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.INFO)

# 创建控制台处理器
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# 设置日志格式
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# 添加处理器到logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

def show_welcome_page():
    """显示欢迎页面和功能说明"""
    welcome_text = """
    ====================================================
                    策略回测系统
    ====================================================
    
    本系统提供多种回测相关功能，您可以通过选择对应数字来使用各功能。
    
    功能列表：
    1. 下载期货产品全历史日线数据
    2. 获取期货合约映射关系
    3. 获取工作日信息
    4. 创建指数合约
    5. 解码数据
    6. 下载螺纹钢数据(Tushare)
    7. 交易员演示
    8. 下载月度/周度数据(Tushare)
    
    请输入功能对应的数字进行选择，或输入0退出系统。
    ====================================================
    """
    print(welcome_text)

def load_default_params():
    """
    从default_param_list.json文件加载默认参数
    
    Returns:
        dict: 默认参数字典
    """
    default_params_file = os.path.join(CURRENT_DIR, 'default_param_list.json')
    try:
        with open(default_params_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"默认参数文件不存在: {default_params_file}")
        return {}
    except json.JSONDecodeError:
        logger.warning(f"默认参数文件格式错误: {default_params_file}")
        return {}

def get_future_data_params():
    """
    获取期货数据下载参数
    
    Returns:
        tuple: (symbol, output_dir)
    """
    # 显示交易所信息
    print("支持的期货交易所：SHFE/SHF(上海), DCE(大连), CZCE(郑州), CFFEX(中金), INE(上期能源)")
    
    # 获取期货产品编码
    while True:
        symbol = input("\n请输入商品代码，范例RB.SHF: ").strip()
        if not symbol:
            print("商品代码不能为空，请重新输入")
            continue
        
        if '.' not in symbol:
            print("格式错误，请使用 '品种.交易所' 格式，例如: RB.SHF")
            continue
            
        # 验证交易所标识
        exchange = symbol.split('.')[-1].upper()
        valid_exchanges = ['SHFE', 'SHF', 'DCE', 'CZCE', 'CFFEX', 'INE']
        if exchange not in valid_exchanges:
            print(f"无效的交易所标识: {exchange}。有效的交易所为: {', '.join(valid_exchanges)}")
            continue
            
        break
            # 读取各参数值
    tushare_root = load_default_params().get('tushare_root', '')
    future_path = load_default_params().get('future', '')
    one_d_path = load_default_params().get('1d', '')
    # 获取保存路径
    output_dir = input(f"请输入文件保存路径, 如果直接回车则使用 default_param_list.json文件的 {tushare_root + future_path + one_d_path}: ").strip()
    
    # 如果用户直接回车，尝试从默认参数文件获取路径
    if not output_dir:

        

        
        if tushare_root:
            # 展开~为用户目录
            tushare_root_expanded = os.path.expanduser(tushare_root)
            # 构建默认路径: tushare_root + future + 1d
            output_dir = tushare_root_expanded + future_path + one_d_path
            
            # 显示参数值和拼接过程
            print(f"\n从default_param_list.json获取的参数值：")
            print(f"tushare_root = '{tushare_root}' (展开为: '{tushare_root_expanded}')")
            print(f"future = '{future_path}'")
            print(f"1d = '{one_d_path}'")
            print(f"\n路径拼接: {tushare_root_expanded} + {future_path} + {one_d_path}")
            print(f"使用默认保存路径: {output_dir}")
        else:
            # 如果没有默认配置，使用当前目录下的data文件夹
            output_dir = os.path.join(CURRENT_DIR, 'data')
            print(f"未找到默认配置，使用默认保存路径: {output_dir}")
    
    # 显示确认信息
    print(f"\n商品代码: {symbol}")
    print(f"保存路径: {output_dir}")
    print(f"工作模式: -m 2 (下载所有合约数据)")
    
    # 确认开始下载
    while True:
        confirm = input("\n确认开始下载？(y/n): ").strip().lower()
        if confirm == 'y':
            break
        elif confirm == 'n':
            print("取消下载操作")
            return None, None
        else:
            print("请输入 'y' 或 'n'")
    
    return symbol, output_dir

def run_script(script_name: str, args: list = None):
    """
    执行指定的脚本
    
    Args:
        script_name: 脚本名称
        args: 额外的命令行参数
    
    Returns:
        执行的返回码
    """
    # 特殊处理下载期货数据功能
    if script_name == 'rb_tushare_data_download.py':
        # 获取参数
        symbol, output_dir = get_future_data_params()
        if symbol is None or output_dir is None:
            return 0  # 用户取消操作
        
        # 构建参数列表，默认使用-m 2模式
        script_args = ['-s', symbol, '-m', '2']
        if output_dir:
            script_args.extend(['-o', output_dir])
            
        # 覆盖传入的args
        args = script_args
    
    script_path = os.path.join(CURRENT_DIR, script_name)
    
    # 检查脚本是否存在
    if not os.path.exists(script_path):
        logger.error(f"脚本不存在: {script_path}")
        return 1
    
    # 构建命令
    cmd = [sys.executable, script_path]
    if args:
        cmd.extend(args)
    
    logger.info(f"执行脚本: {script_name}, 参数: {args if args else []}")
    print(f"正在执行: {script_name}...")
    
    try:
        # 执行脚本
        result = subprocess.run(cmd, cwd=CURRENT_DIR, check=False)
        return result.returncode
    except Exception as e:
        logger.error(f"执行脚本时出错: {str(e)}")
        print(f"执行出错: {str(e)}")
        return 1

def main() -> int:
    """
    主函数
    
    Returns:
        程序执行的返回码
    """
    # 记录启动时间和相关信息
    start_time = time.strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"程序启动，启动时间: {start_time}")
    logger.info(f"Python版本: {sys.version}")
    logger.info(f"当前工作目录: {CURRENT_DIR}")
    
    # 显示欢迎页面
    show_welcome_page()
    
    # 定义功能映射
    menu_map = {
        '1': 'rb_tushare_data_download.py',
        '2': 'Get_Fut_Mapping_Code.py',
        '3': 'Get_Work_Date_Info.py',
        '4': 'create_index_contract.py',
        '5': 'decode.py',
        '6': 'create_9999_future_file.py',
        '7': 'trader_demo.py',
        '8': 'tushare_month_week_data_download.py'
    }
    
    try:
        while True:
            # 获取用户选择
            choice = input("\n请输入功能编号(0-8): ").strip()
            
            # 退出系统
            if choice == '0':
                logger.info("用户选择退出系统")
                print("\n感谢使用策略回测系统，再见！")
                break
            
            # 检查选择是否有效
            if choice not in menu_map:
                print("无效的选择，请输入0-8之间的数字")
                continue
            
            # 获取对应脚本
            script_name = menu_map[choice]
            
            # 初始化extra_args
            extra_args = []
            
            # 执行脚本
            print("-" * 60)
            exit_code = run_script(script_name, extra_args)
            
            # 显示执行结果
            if exit_code == 0:
                logger.info(f"脚本执行成功: {script_name}")
                print(f"\n脚本 {script_name} 执行成功")
            else:
                logger.warning(f"脚本执行失败: {script_name}, 退出码: {exit_code}")
                print(f"\n脚本 {script_name} 执行失败，退出码: {exit_code}")
            
            print("-" * 60)
            input("按Enter键继续...")
            
    except KeyboardInterrupt:
        logger.info("用户通过键盘中断退出")
        print("\n\n程序被用户中断，正在退出...")
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        print(f"\n程序运行出错: {str(e)}")
    finally:
        logger.info("程序退出")
    
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"系统发生未处理的错误: {str(e)}")
        sys.exit(1)
