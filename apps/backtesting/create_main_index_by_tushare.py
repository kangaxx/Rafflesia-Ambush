#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建主连指数工具

此脚本用于通过Tushare API创建期货主连指数数据。
"""

import argparse
import os
import sys
import logging
import re

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('log/create_main_index.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)





def create_main_index(fut_code, mapping_file, contract_path=None, output_path=None):
    """
    创建期货主连指数数据
    
    Args:
        fut_code: 合约编码，如 'RB.SHF'
        mapping_file: 映射文件路径，包含交易日期与当日主连期货的映射关系
        contract_path: 普通合约文件集所在路径，默认为None
        output_path: 主力合约数据存储路径，默认为None
    
    Returns:
        bool: 创建是否成功
    """
    try:
        # 解析期货代码获取品种和交易所
        # 格式应为 "品种.交易所"，如 "RB.SHF"
        if '.' not in fut_code:
            logger.error(f"合约编码格式错误，应为'品种.交易所'格式，如'RB.SHF'")
            raise ValueError("合约编码格式错误")
        
        logger.info(f"开始创建主连指数数据: 合约={fut_code}, 映射文件={mapping_file}")
        
        # 读取映射文件
        import pandas as pd
        if not os.path.exists(mapping_file):
            logger.error(f"映射文件不存在: {mapping_file}")
            raise FileNotFoundError(f"映射文件不存在: {mapping_file}")
        
        logger.info(f"读取映射文件: {mapping_file}")
        mapping_df = pd.read_csv(mapping_file)
        
        # 验证映射文件格式
        required_columns = ['trade_date', 'mapping_ts_code']
        for col in required_columns:
            if col not in mapping_df.columns:
                logger.error(f"映射文件缺少必需的列: {col}")
                raise ValueError(f"映射文件格式错误，缺少必需的列: {col}")
        
        logger.info(f"成功读取映射文件，共{len(mapping_df)}条映射记录")
        
        # 根据映射关系创建主连指数数据
        logger.info("开始根据映射关系创建主连指数数据")
        
        # 存储最终的主连指数数据
        main_index_data = []
        
        # 按日期顺序处理每个映射关系
        for _, row in mapping_df.iterrows():
            trade_date = row['trade_date']
            ts_code = row['mapping_ts_code']
            
            try:
                df = None
                # 仅从合约文件路径读取数据（如果指定了路径）
                if contract_path is not None and os.path.exists(contract_path):
                    # 提取基础品种代码（不含交易所）
                    base_code = fut_code.split('.')[0]
                    
                    # 从ts_code提取年份和月份
                    # 假设ts_code格式为：RB2401.SHF 或类似格式
                    try:
                        # 提取合约月份信息，如从RB2401.SHF中提取2401
                        # 先尝试匹配常见的合约代码格式
                        import re
                        match = re.search(r'\d{4}', ts_code)
                        if match:
                            year_month = match.group()
                            # 构建文件名：fut_code + YY + MM + '.csv'
                            # 例如：RB.SHF2401.csv
                            # 提取交易所代码
                            exchange = fut_code.split('.')[1]
                            
                            # 尝试基础格式: 品种+年月.csv (如RB0909.csv)
                            contract_file_name = f"{base_code}{year_month}.csv"
                            contract_file = os.path.join(contract_path, contract_file_name)
                            
                            logger.info(f"尝试读取合约文件: {contract_file}")
                            # 构建完整的合约代码（带交易所后缀）用于匹配
                            full_ts_code = f"{base_code}{year_month}.{exchange}"
                            # 使用新的get_day_kline_from_csv函数获取数据
                            # 注意：函数期望的参数顺序是fut_code, trade_date, file_name
                            row_data = get_day_kline_from_csv(full_ts_code, trade_date, contract_file)
                            
                            if row_data:
                                # 如果成功获取到数据，转换为DataFrame
                                df = pd.DataFrame([row_data])
                            else:
                                # 没有获取到数据
                                df = pd.DataFrame()
                        else:
                            logger.warning(f"无法从ts_code {ts_code} 中提取年月信息")
                    except Exception as e:
                        logger.error(f"解析合约代码时出错: {e}")
                
                # 如果从文件未获取到数据，则记录警告
                if df is None or df.empty:
                    logger.warning(f"未获取到合约 {ts_code} 在 {trade_date} 的数据")
                    continue
                
                # 如果成功读取到数据，添加到主连数据中
                if df is not None and not df.empty:
                    # 提取所需数据并添加到主连数据中
                    for _, daily_row in df.iterrows():
                        # 创建主连数据记录，保留原始数据的大部分字段
                        main_index_record = daily_row.to_dict()
                        # 添加主连合约标识
                        main_index_record['main_contract'] = fut_code
                        main_index_data.append(main_index_record)
                    
            except Exception as e:
                logger.error(f"处理合约 {ts_code} 在 {trade_date} 时出错: {e}")
                # 继续处理下一条记录
                continue
        
        if not main_index_data:
            logger.warning("未能创建任何主连指数数据")
            return False
        
        # 转换为主连指数数据框
        df = pd.DataFrame(main_index_data)
        
        # 检查是否成功创建数据
        if df.empty:
            logger.warning("未能创建主连指数数据")
            return False
        
        # 构建保存路径
        if output_path is not None:
            # 确保output_path被处理为目录路径
            # 检查是否已经是文件名（包含.csv扩展名）
            if os.path.splitext(output_path)[1] == '.csv':
                # 如果output_path已经是文件名，则直接使用
                save_path = output_path
                # 确保目录存在
                output_dir = os.path.dirname(save_path)
                if output_dir:
                    os.makedirs(output_dir, exist_ok=True)
            else:
                # 从default_param_list.json中读取main_index参数
                with open('default_param_list.json', 'r') as f:
                    params = json.load(f)
                main_index_file = params.get('main_index', '_main_index.csv')
                # 检查路径是否已存在且是一个目录
                if os.path.exists(output_path) and os.path.isdir(output_path):
                    # 如果是目录，则拼接文件名
                    save_path = os.path.join(output_path, f"{fut_code}{main_index_file}")
                else:
                    # 如果不是目录或者不存在，则将其视为目录路径
                    save_path = os.path.join(output_path, f"{fut_code}{main_index_file}")
                    # 确保输出目录存在
                    os.makedirs(output_path, exist_ok=True)
            logger.info(f"准备将主连指数数据保存至: {save_path}")
        else:
            # 使用默认路径
            save_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'main_index_data')
            os.makedirs(save_dir, exist_ok=True)
            save_path = os.path.join(save_dir, f"{fut_code}_main_index.csv")
            logger.info(f"使用默认路径，将主连指数数据保存至: {save_path}")
        df.to_csv(save_path, index=False, encoding='utf-8')
        
        logger.info(f"主连指数数据已保存至: {save_path}")
        logger.info(f"数据条数: {len(df)}")
        
        return True
    
    except Exception as e:
        logger.error(f"创建主连指数时发生错误: {e}")
        raise


import sys

def get_day_kline_from_csv(fut_code, trade_date, file_name):
    """
    从CSV文件中获取指定合约和日期的日线K线数据
    
    Args:
        fut_code: 合约代码
        trade_date: 交易日期
        file_name: CSV文件路径
    
    Returns:
        dict: 找到的一行数据（如果有且只有一行），否则终止程序
    """
    # 打印输入参数
    print(f"参数信息 - fut_code: {fut_code}, trade_date: {trade_date}, file_name: {file_name}")
    
    # 检查文件是否存在
    if not os.path.exists(file_name):
        print(f"错误：文件不存在 - {file_name}")
        logger.error(f"无法找到合约文件: {file_name}")
        sys.exit(1)
    
    # 读取文件并查找匹配的数据行
    matched_rows = []
    headers = None
    try:
        with open(file_name, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            
            for row in reader:
                # 检查fut_code和trade_date是否匹配
                if ('fut_code' in row and row['fut_code'] == fut_code and 
                    'trade_date' in row and row['trade_date'] == trade_date):
                    matched_rows.append(row)
    except Exception as e:
        print(f"读取文件时出错: {e}")
        return None
    
    # 处理查找结果
    if len(matched_rows) == 0:
        # 未找到数据，打印头三行信息并终止程序
        error_msg = f"""错误：未找到匹配的数据行 (fut_code={fut_code}, trade_date={trade_date})
无法从mapping文件中获取对应的日线数据，程序终止。"""
        print(error_msg)
        logger.error(error_msg)
        
        # 打印文件的头三行信息
        print("文件头三行信息：")
        try:
            with open(file_name, 'r', encoding='utf-8') as f:
                # 打印表头
                if headers:
                    print(f"表头: {','.join(headers)}")
                # 打印前两行数据
                line_count = 0
                for line in f:
                    if line_count <= 2:  # 包括表头在内的前三行
                        print(f"第{line_count+1}行: {line.strip()}")
                    else:
                        break
                    line_count += 1
        except Exception as e:
            print(f"读取文件头部信息时出错: {e}")
        
        sys.exit(1)
    
    elif len(matched_rows) > 1:
        # 找到多行数据，打印并报错终止程序
        error_msg = f"错误：找到多行匹配的数据 ({len(matched_rows)}行)"
        print(error_msg)
        logger.error(error_msg)
        for i, row in enumerate(matched_rows):
            print(f"第{i+1}行匹配数据: {row}")
        sys.exit(1)
    
    else:
        # 正常情况：找到且只有一行数据
        print("成功找到唯一匹配的数据行")
        return matched_rows[0]


def parse_arguments():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(
        description='创建期货主连指数数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""使用示例:
# 使用所有默认路径创建螺纹钢主连指数数据
python create_main_index_by_tushare.py -c RB.SHF

# 使用自定义映射文件路径创建主连指数数据
python create_main_index_by_tushare.py -c RB.SHF -m custom_path/RB.SHF_fut_mapping.csv

# 使用自定义映射文件路径和自定义合约文件集路径创建主连指数数据
python create_main_index_by_tushare.py -c RB.SHF -m mapping.csv -p contract_files/

# 使用所有自定义路径创建主连指数数据
# 主连数据将保存至: output_data/RB.SHF_main_index.csv
python create_main_index_by_tushare.py -c RB.SHF -m mapping.csv -p contract_files/ -o output_data/
""")
    
    # 添加带单字母模式的合约编码参数
    parser.add_argument('-c', '--fut_code', type=str, required=True, help='合约编码，如 RB.SHF')
    
    # 添加映射文件参数，设置为可选参数，将在main函数中处理默认值
    parser.add_argument('-m', '--mapping_file', type=str, default=None, 
                        help='映射文件路径，默认格式为"产品代码+交易所代码+_fut_mapping.csv"，如不指定则使用当前目录下的默认文件')
    
    # 添加第三个参数：普通合约文件集所在路径
    parser.add_argument('-p', '--contract_path', type=str, default=None, 
                        help='普通合约文件集所在路径，如不指定则使用默认路径')
    
    # 添加第四个参数：主力合约数据存储路径
    parser.add_argument('-o', '--output_path', type=str, default=None, 
                        help='主力合约数据存储路径，如不指定则使用默认路径')
    
    return parser.parse_args()


def main():
    """
    主函数
    """
    try:
        # 解析命令行参数
        args = parse_arguments()
        
        # 处理映射文件路径，如果指定了路径则在该路径下查找fut_code+'_fut_mapping.csv'
        if args.mapping_file:
            # 如果指定了映射文件路径，将其视为目录，并在其中查找特定文件名
            mapping_file = os.path.join(args.mapping_file, f"{args.fut_code}_fut_mapping.csv")
            logger.info(f"使用指定路径查找映射文件: {mapping_file}")
        else:
            # 如果未指定映射文件路径，则按照原有逻辑处理
            # 优先在output_path中查找映射文件
            if args.output_path:
                mapping_file = os.path.join(args.output_path, f"{args.fut_code}_fut_mapping.csv")
                logger.info(f"未指定映射文件路径，优先在输出路径中查找: {mapping_file}")
            else:
                # 使用当前脚本所在目录作为默认路径
                script_dir = os.path.dirname(os.path.abspath(__file__))
                mapping_file = os.path.join(script_dir, f"{args.fut_code}_fut_mapping.csv")
                logger.info(f"未指定映射文件路径和输出路径，使用默认路径: {mapping_file}")
        
        # 第一步：检查映射文件是否存在
        if not os.path.exists(mapping_file):
            logger.error(f"映射文件不存在: {mapping_file}")
            raise FileNotFoundError(f"映射文件不存在: {mapping_file}")
        
        # 处理第三个参数：普通合约文件集所在路径
        contract_path = args.contract_path
        if contract_path is not None:
            logger.info(f"使用指定的普通合约文件集路径: {contract_path}")
            # 检查合约文件路径是否存在
            if not os.path.exists(contract_path):
                logger.warning(f"指定的普通合约文件集路径不存在: {contract_path}")
        
        # 处理第四个参数：主力合约数据存储路径
        output_path = args.output_path
        if output_path is not None:
            logger.info(f"使用指定的主力合约数据存储路径: {output_path}")
        
        # 创建主连指数数据
        success = create_main_index(args.fut_code, mapping_file, contract_path, output_path)

        
        if success:
            logger.info("主连指数数据创建成功")  
            sys.exit(0)
        else:
            logger.warning("主连指数数据创建失败")
            sys.exit(1)
    
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        sys.exit(0)
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()