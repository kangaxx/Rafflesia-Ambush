import os
import pandas as pd
import argparse
from datetime import datetime
import logging
from typing import Dict, List, Tuple, Optional

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_arguments() -> argparse.Namespace:
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(description='生成期货历史主力合约数据文件\n使用范例: python create_9999_future_file.py -f RB -c data/RB_1_future_contracts_info.csv')
    parser.add_argument('-v', '--volume_data_dir', required=False, default='./data',
                        help='普通合约日线数据文件集所在路径（用于获取交易量判断主力合约，默认: ./data）')
    parser.add_argument('-k', '--kline_data_dir', required=False,
                        help='期货合约K线数据文件集所在路径（原始数据，默认使用与volume_data_dir相同的路径）')
    parser.add_argument('-c', '--contract_list_file', required=False,
                        help='期货合约列表文件名称（非必须，若未提供或文件不存在/数据不准确，将跳过数据校验）')
    parser.add_argument('-o', '--output_dir', required=False, default='./data/out',
                        help='结果文件输出路径（默认: ./data/out）')
    parser.add_argument('-f', '--future_code', required=True,
                        help='期货品类代码（如 rb、cu 等）')
    parser.add_argument('-d', '--date_format', default='YYYYMMDD',
                        help='日期格式，默认为 YYYYMMDD')
    parser.add_argument('-l', '--Delivery', type=lambda x: x.lower() == 'true', default=False,
                        help='是否允许交割月合约作为主力合约，输入true或false，默认为false')
    return parser.parse_args()

def validate_directory(directory: str) -> bool:
    """
    验证目录是否存在
    """
    if not os.path.exists(directory):
        logger.error(f"目录不存在: {directory}")
        return False
    if not os.path.isdir(directory):
        logger.error(f"路径不是目录: {directory}")
        return False
    return True

def load_contract_list(contract_list_file: str) -> List[str]:
    """
    加载期货合约列表
    识别CSV文件的表头，从symbol列读取合约名称
    只校验合约文件的名称和数量，不进行日期比较
    """
    contracts = []
    try:
        with open(contract_list_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            if not lines:
                logger.error("合约列表文件为空")
                return []
            
            # 处理第一行作为表头
            header = lines[0].strip().split(',')
            symbol_index = -1
            for i, col_name in enumerate(header):
                if col_name.strip().lower() == 'symbol':
                    symbol_index = i
                    break
            
            if symbol_index == -1:
                logger.error("在表头中未找到symbol列")
                return []
            
            # 读取数据行 - 只提取合约名称，不处理日期列
            for line in lines[1:]:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split(',')
                    if len(parts) > symbol_index:
                        contract_code = parts[symbol_index].strip()
                        if contract_code:
                            contracts.append(contract_code)
        
        logger.info(f"成功加载合约列表，共 {len(contracts)} 个合约")
        
        # 打印合约列表，每行8条
        if contracts:
            logger.info("合约名称列表:")
            for i in range(0, len(contracts), 8):
                # 取当前8个合约
                batch = contracts[i:i+8]
                # 格式化并打印，确保对齐
                formatted_line = "  ".join([f"{contract:<10}" for contract in batch])
                logger.info(formatted_line)
        
        return contracts
    except Exception as e:
        logger.error(f"加载合约列表失败: {str(e)}")
        return []

def load_kline_data(file_path: str) -> Optional[pd.DataFrame]:
    """
    加载K线数据文件
    """
    try:
        df = pd.read_csv(file_path)
        # 确保日期列存在并转换为日期类型
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
        elif 'datetime' in df.columns:
            df['trade_date'] = pd.to_datetime(df['datetime'])
        elif 'date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['date'])
        else:
            logger.warning(f"文件 {file_path} 中未找到日期列")
            return None
        return df
    except Exception as e:
        logger.error(f"加载K线数据文件 {file_path} 失败: {str(e)}")
        return None

def get_all_contract_files(directory: str, future_code: str) -> Dict[str, str]:
    """
    获取目录下所有合约的文件路径
    支持文件格式：期货编号 + YY + MM + .csv
    """
    contract_files = {}
    for filename in os.listdir(directory):
        if filename.endswith('.csv') and future_code in filename:
            # 从文件名中提取合约代码（文件名格式为 "期货编号 + YY + MM.csv"）
            contract_code = filename.split('.')[0]
            contract_files[contract_code] = os.path.join(directory, filename)
    logger.info(f"找到 {len(contract_files)} 个合约文件")
    return contract_files

def collect_all_dates(contract_files: Dict[str, str]) -> pd.DatetimeIndex:
    """
    收集所有合约的日期，获取完整的日期范围
    通过从合约文件名中提取YY + MM，使用排序的方法来获取日期
    """
    all_dates = []
    
    for contract_code in contract_files.keys():
        try:
            # 从合约代码中提取年份和月份信息 (假设格式为: 期货编号 + YY + MM)
            # 提取最后4位字符作为年份和月份
            if len(contract_code) >= 4:
                yymm_part = contract_code[-4:]
                if yymm_part.isdigit():
                    # 转换为年份和月份
                    year = 2000 + int(yymm_part[:2])  # 假设是21世纪的年份
                    month = int(yymm_part[2:])
                    
                    # 验证月份是否有效
                    if 1 <= month <= 12:
                        # 创建该月份的第一天作为日期代表
                        date_str = f"{year}-{month:02d}-01"
                        date = pd.to_datetime(date_str)
                        all_dates.append(date)
        except Exception as e:
            logger.warning(f"从合约代码 {contract_code} 提取日期失败: {str(e)}")
    
    # 去重并排序
    if all_dates:
        unique_dates = list(set(all_dates))
        unique_dates.sort()
        return pd.DatetimeIndex(unique_dates)
    else:
        return pd.DatetimeIndex([])

def is_delivery_month_contract(contract_code: str, date: pd.Timestamp) -> bool:
    """
    判断合约是否处于交割月
    假设合约代码格式为：期货品种+年份+月份，如rb2310表示2023年10月交割的螺纹钢合约
    """
    try:
        # 提取合约中的年份和月份信息
        # 合约代码通常格式为：品种代码 + 年份(后两位) + 月份(两位)
        # 例如：rb2310 表示2023年10月交割
        year_month_part = contract_code[-4:]
        if len(year_month_part) == 4 and year_month_part.isdigit():
            contract_year = int('20' + year_month_part[:2])
            contract_month = int(year_month_part[2:])
            
            # 检查当前日期是否在合约交割月内
            return date.year == contract_year and date.month == contract_month
    except Exception as e:
        logger.warning(f"无法判断合约 {contract_code} 是否处于交割月: {str(e)}")
    return False

def determine_main_contract_by_volume(date: pd.Timestamp, contract_files: Dict[str, str], allow_delivery_month: bool = True) -> Tuple[str, float]:
    """
    根据交易量确定某一天的主力合约
    返回 (主力合约代码, 最大交易量)
    
    参数:
        date: 交易日期
        contract_files: 合约文件映射
        allow_delivery_month: 是否允许交割月合约作为主力合约
    """
    max_volume = 0
    main_contract = None
    
    for contract_code, file_path in contract_files.items():
        # 如果不允许交割月合约，且当前合约处于交割月，则跳过
        if not allow_delivery_month and is_delivery_month_contract(contract_code, date):
            continue
            
        df = load_kline_data(file_path)
        if df is None or 'trade_date' not in df.columns:
            continue
        
        # 查找该日期的数据
        date_data = df[df['trade_date'] == date]
        if not date_data.empty and 'volume' in date_data.columns:
            volume = date_data['volume'].iloc[0]
            if volume > max_volume:
                max_volume = volume
                main_contract = contract_code
    
    return main_contract, max_volume

def create_main_contract_series(all_dates: pd.DatetimeIndex, 
                              contract_files: Dict[str, str],
                              volume_files: Dict[str, str],
                              allow_delivery_month: bool = True) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    创建主力合约序列数据
    返回 (主力合约映射DataFrame, 切换记录列表)
    
    参数:
        all_dates: 所有交易日期
        contract_files: K线合约文件映射
        volume_files: 交易量合约文件映射
        allow_delivery_month: 是否允许交割月合约作为主力合约
    """
    main_contract_mapping = []
    switch_records = []
    previous_main_contract = None
    
    for i, date in enumerate(all_dates):
        # 使用volume_files中的交易量数据确定主力合约
        main_contract, volume = determine_main_contract_by_volume(date, volume_files, allow_delivery_month)
        
        if main_contract:
            main_contract_mapping.append({
                'trade_date': date,
                'main_contract': main_contract,
                'volume': volume
            })
            
            # 记录主力合约切换
            if previous_main_contract and previous_main_contract != main_contract:
                switch_records.append({
                    'date': date,
                    'from_contract': previous_main_contract,
                    'to_contract': main_contract,
                    'switch_index': i
                })
                logger.info(f"主力合约切换: {previous_main_contract} -> {main_contract} (日期: {date})")
            
            previous_main_contract = main_contract
        else:
            logger.warning(f"无法确定 {date} 的主力合约")
    
    return pd.DataFrame(main_contract_mapping), switch_records

def build_main_contract_kline(main_contract_mapping: pd.DataFrame, 
                            contract_files: Dict[str, str],
                            future_code: str) -> pd.DataFrame:
    """
    构建主力合约K线数据
    """
    main_contract_data = []
    
    for _, row in main_contract_mapping.iterrows():
        date = row['trade_date']
        contract_code = row['main_contract']
        
        if contract_code in contract_files:
            df = load_kline_data(contract_files[contract_code])
            if df is not None:
                # 查找该日期的K线数据
                date_data = df[df['trade_date'] == date].copy()
                if not date_data.empty:
                    # 添加主力合约标识
                    date_data['symbol'] = f"{future_code}9999"
                    date_data['original_contract'] = contract_code
                    main_contract_data.append(date_data)
    
    # 合并所有数据
    if main_contract_data:
        main_df = pd.concat(main_contract_data)
        # 按日期排序
        main_df = main_df.sort_values('trade_date')
        # 重置索引
        main_df = main_df.reset_index(drop=True)
        return main_df
    else:
        return pd.DataFrame()

def save_results(main_contract_kline: pd.DataFrame, 
                main_contract_mapping: pd.DataFrame, 
                switch_records: List[Dict],
                output_dir: str,
                future_code: str):
    """
    保存结果文件
    """
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 保存主力合约K线数据
    kline_output_file = os.path.join(output_dir, f"{future_code}9999.csv")
    main_contract_kline.to_csv(kline_output_file, index=False, encoding='utf-8')
    logger.info(f"主力合约K线数据已保存至: {kline_output_file}")
    
    # 保存每日主力合约映射记录
    mapping_output_file = os.path.join(output_dir, f"{future_code}_main_contract_mapping.csv")
    main_contract_mapping.to_csv(mapping_output_file, index=False, encoding='utf-8')
    logger.info(f"每日主力合约映射记录已保存至: {mapping_output_file}")
    
    # 保存主力合约切换记录
    switch_output_file = os.path.join(output_dir, f"{future_code}_main_contract_switches.csv")
    if switch_records:
        switch_df = pd.DataFrame(switch_records)
        switch_df.to_csv(switch_output_file, index=False, encoding='utf-8')
        logger.info(f"主力合约切换记录已保存至: {switch_output_file}")
    else:
        # 如果没有切换记录，创建空文件
        with open(switch_output_file, 'w', encoding='utf-8') as f:
            f.write("date,from_contract,to_contract,switch_index\n")
        logger.info(f"未发现主力合约切换，已创建空切换记录文件: {switch_output_file}")

def main():
    """
    主函数
    """
    try:
        # 解析参数
        args = parse_arguments()
        
        # 如果kline_data_dir未设置，则使用volume_data_dir的值
        if args.kline_data_dir is None:
            args.kline_data_dir = args.volume_data_dir
            logger.info(f"未指定K线数据目录，将使用与交易量数据相同的目录: {args.kline_data_dir}")
        
        # 验证目录
        if not validate_directory(args.volume_data_dir):
            return
        if not validate_directory(args.kline_data_dir):
            return
        
        # 确保输出目录存在
        os.makedirs(args.output_dir, exist_ok=True)
        
        # 获取所有合约文件
        volume_files = get_all_contract_files(args.volume_data_dir, args.future_code)
        kline_files = get_all_contract_files(args.kline_data_dir, args.future_code)
        
        # 处理合约列表参数
        contract_list = []
        validate_data = True
        
        if args.contract_list_file:
            contract_list = load_contract_list(args.contract_list_file)
            if not contract_list:
                logger.error(f"合约列表文件 '{args.contract_list_file}' 不存在或数据不准确，将跳过数据校验")
                validate_data = False
        else:
            logger.info("未提供合约列表文件，将跳过数据校验")
            validate_data = False
        
        # 仅当合约列表有效时进行数据校验
        if validate_data:
            logger.info("开始校验合约文件完整性（只校验合约名称和数量匹配，不进行日期比较）...")
            # 验证合约文件是否完整 - 只检查合约名称和数量是否匹配
            missing_in_volume = set(contract_list) - set(volume_files.keys())
            missing_in_kline = set(contract_list) - set(kline_files.keys())
            
            if missing_in_volume or missing_in_kline:
                if missing_in_volume:
                    logger.error("交易量数据中缺少合约:")
                    for contract in missing_in_volume:
                        logger.error(f"  {contract}")
                if missing_in_kline:
                    logger.error("K线数据中缺少合约:")
                    for contract in missing_in_kline:
                        logger.error(f"  {contract}")
                logger.error("数据文件校验失败，缺少必要的合约文件，程序中断")
                return
        else:
            logger.info("由于未提供有效的合约列表，将不对数据文件进行完整性校验")
        
        # 收集所有日期
        logger.info("收集所有日期数据...")
        all_dates = collect_all_dates(kline_files)
        logger.info(f"共收集到 {len(all_dates)} 个交易日")
        
        # 创建主力合约序列
        logger.info("开始确定主力合约...")
        logger.info(f"交割月合约处理设置: {'允许交割月合约作为主力合约' if args.Delivery else '不允许交割月合约作为主力合约'}")
        main_contract_mapping, switch_records = create_main_contract_series(
            all_dates, contract_files=kline_files, volume_files=volume_files, allow_delivery_month=args.Delivery
        )
        
        if main_contract_mapping.empty:
            logger.error("无法生成主力合约映射数据")
            return
        
        logger.info(f"成功确定 {len(main_contract_mapping)} 个交易日的主力合约")
        logger.info(f"发现 {len(switch_records)} 次主力合约切换")
        
        # 构建主力合约K线数据
        logger.info("构建主力合约K线数据...")
        main_contract_kline = build_main_contract_kline(
            main_contract_mapping, kline_files, args.future_code
        )
        
        if main_contract_kline.empty:
            logger.error("无法生成主力合约K线数据")
            return
        
        logger.info(f"成功构建主力合约K线数据，共 {len(main_contract_kline)} 条记录")
        
        # 保存结果
        logger.info("保存结果文件...")
        save_results(
            main_contract_kline, 
            main_contract_mapping, 
            switch_records,
            args.output_dir,
            args.future_code
        )
        
        logger.info("主力合约数据生成完成！")
        
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
