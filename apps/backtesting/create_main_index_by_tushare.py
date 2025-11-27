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
                            if os.path.exists(contract_file):
                                # 先完整读取数据框，避免多次过滤后数据丢失
                                original_df = pd.read_csv(contract_file)
                                # 注意：文件内的ts_code可能带有.SHF后缀
                                # 需要同时匹配ts_code和trade_date两列
                                # 构建完整的合约代码（带交易所后缀）用于匹配
                                full_ts_code = f"{base_code}{year_month}.{exchange}"
                                # 先尝试精确匹配带交易所后缀的ts_code
                                df = original_df[(original_df['ts_code'] == full_ts_code) & (original_df['trade_date'] == trade_date)]
                                
                                # 如果没有找到匹配项，可能ts_code不带后缀，尝试只根据trade_date过滤
                                if df.empty:
                                    df = original_df[original_df['trade_date'] == trade_date]
                                    logger.info(f"通过trade_date过滤获取到 {trade_date} 的数据")
                                else:
                                    logger.info(f"成功通过ts_code和trade_date匹配获取到 {trade_date} 的数据")
                            else:
                                # 注意：按照要求，合约的编号应该是RB0909这种不带.SHF的格式
                                # 只使用基础格式，不再尝试其他格式
                                logger.warning(f"合约文件不存在: {contract_file}")
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
"""
    )
    
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