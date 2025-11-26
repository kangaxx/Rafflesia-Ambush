#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import subprocess
import logging
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

def run_script(script_name: str, args: list = None):
    """
    执行指定的脚本
    
    Args:
        script_name: 脚本名称
        args: 额外的命令行参数
    
    Returns:
        执行的返回码
    """
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
        '1': 'create_9999_future_file.py',
        '2': 'Get_Fut_Mapping_Code.py',
        '3': 'Get_Work_Date_Info.py',
        '4': 'create_index_contract.py',
        '5': 'decode.py',
        '6': 'rb_tushare_data_download.py',
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
            
            # 询问是否需要额外参数
            extra_args_input = input("是否需要输入额外参数? (y/n): ").strip().lower()
            extra_args = []
            
            if extra_args_input == 'y':
                args_str = input("请输入额外参数(以空格分隔): ").strip()
                extra_args = args_str.split()
            
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
