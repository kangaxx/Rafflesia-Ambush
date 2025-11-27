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
from datetime import datetime
import tushare as ts

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


def _read_tushare_token():
    """从key.json文件中读取token"""
    try:
        # 获取当前脚本所在目录
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # 构建key.json文件的完整路径
        key_json_path = os.path.join(script_dir, 'key.json')
        
        # 检查文件是否存在
        if not os.path.exists(key_json_path):
            logger.error(f"配置文件 key.json 不存在，请在 {key_json_path} 创建配置文件")
            raise FileNotFoundError(f"配置文件 key.json 不存在")
        
        # 读取JSON文件
        import json
        with open(key_json_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # 尝试多种可能的键名
        token = config.get('token') or config.get('TUSHARE_TOKEN') or config.get('tushare_token')
        
        if not token:
            logger.error("key.json文件中未找到有效的token")
            raise ValueError("token未找到，请在key.json中设置正确的token")
        
        return token
    except json.JSONDecodeError as e:
        logger.error(f"解析key.json文件失败: {e}")
        raise ValueError(f"key.json文件格式错误: {e}")
    except Exception as e:
        logger.error(f"读取token时发生错误: {e}")
        raise


def create_main_index(fut_code, mapping_file, contract_path=None):
    """
    创建期货主连指数数据
    
    Args:
        fut_code: 合约编码，如 'RB.SHF'
        mapping_file: 映射文件路径，包含交易日期与当日主连期货的映射关系
        contract_path: 普通合约文件集所在路径，默认为None
    
    Returns:
        bool: 创建是否成功
    """
    try:
        # 初始化Tushare API
        token = _read_tushare_token()
        pro = ts.pro_api(token)
        
        logger.info(f"开始创建主连指数数据: 合约={fut_code}, 映射文件={mapping_file}")
        
        # 解析期货代码获取品种和交易所
        # 格式应为 "品种.交易所"，如 "RB.SHF"
        if '.' not in fut_code:
            logger.error(f"合约编码格式错误，应为'品种.交易所'格式，如'RB.SHF'")
            raise ValueError("合约编码格式错误")
        
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
                # 首先尝试从合约文件路径读取数据（如果指定了路径）
                df = None
                if contract_path is not None and os.path.exists(contract_path):
                    # 构建合约文件名，假设文件名为ts_code.csv格式
                    contract_file = os.path.join(contract_path, f"{ts_code}.csv")
                    if os.path.exists(contract_file):
                        logger.info(f"从合约文件 {contract_file} 读取数据")
                        try:
                            import pandas as pd
                            df = pd.read_csv(contract_file)
                            # 查找指定交易日的数据
                            df = df[df['trade_date'] == trade_date]
                        except Exception as e:
                            logger.error(f"读取合约文件 {contract_file} 时出错: {e}")
                            df = None
                    else:
                        logger.warning(f"合约文件不存在: {contract_file}")
                
                # 如果从文件未获取到数据，则使用Tushare API获取
                if df is None or df.empty:
                    logger.info(f"获取合约 {ts_code} 在 {trade_date} 的数据")
                    # 获取该合约的日线数据
                    df = pro.fut_daily(
                        ts_code=ts_code,
                        trade_date=trade_date
                    )
                
                if not df.empty:
                    # 提取所需数据并添加到主连数据中
                    for _, daily_row in df.iterrows():
                        # 创建主连数据记录，保留原始数据的大部分字段
                        main_index_record = daily_row.to_dict()
                        # 添加主连合约标识
                        main_index_record['main_contract'] = fut_code
                        main_index_data.append(main_index_record)
                else:
                    logger.warning(f"未获取到合约 {ts_code} 在 {trade_date} 的数据")
                    
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
        
        # 创建保存目录
        save_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'main_index_data')
        os.makedirs(save_dir, exist_ok=True)
        
        # 保存数据
        save_path = os.path.join(save_dir, f"{fut_code.replace('.', '')}_main_index.csv")
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
# 使用默认映射文件路径和默认合约文件集路径创建螺纹钢主连指数数据
python create_main_index_by_tushare.py -c RB.SHF

# 使用自定义映射文件路径和默认合约文件集路径创建主连指数数据
python create_main_index_by_tushare.py -c RB.SHF -m custom_path/RB.SHF_fut_mapping.csv

# 使用自定义映射文件路径和自定义合约文件集路径创建主连指数数据
python create_main_index_by_tushare.py -c RB.SHF -m mapping.csv -p contract_files/
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
    
    return parser.parse_args()


def main():
    """
    主函数
    """
    try:
        # 解析命令行参数
        args = parse_arguments()
        
        # 处理映射文件路径，如果未指定则使用默认路径
        mapping_file = args.mapping_file
        if mapping_file is None:
            # 使用当前脚本所在目录作为默认路径
            script_dir = os.path.dirname(os.path.abspath(__file__))
            # 使用合约编码作为默认文件名格式：产品代码+交易所代码+_fut_mapping.csv
            mapping_file = os.path.join(script_dir, f"{args.fut_code}_fut_mapping.csv")
            logger.info(f"未指定映射文件路径，使用默认路径: {mapping_file}")
        
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
        
        # 创建主连指数数据
        success = create_main_index(args.fut_code, mapping_file, contract_path)

        
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