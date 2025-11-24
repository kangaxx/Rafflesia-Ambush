import os
import pandas as pd
import argparse
from datetime import datetime
import logging
import re
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

def _sort_files_by_yymm(contracts: List[str]) -> List[str]:
    """
    私有函数：按YYMM时间信息对合约列表进行排序
    
    Args:
        contracts: 合约名称列表，合约名称格式必须是code + YY + MM
        
    Returns:
        排序后的合约名称列表，按时间从早到晚排序
    """
    # 验证输入参数
    if not isinstance(contracts, list):
        logger.error("输入参数必须是列表类型")
        return []
    
    # 定义验证和提取YYMM的函数
    def extract_and_validate(contract):
        # 检查是否为字符串
        if not isinstance(contract, str):
            logger.warning(f"合约名称必须是字符串类型: {contract}")
            return None
        
        # 检查长度至少为6（code至少1位 + YY2位 + MM2位）
        if len(contract) < 4:
            logger.warning(f"合约名称长度不足，无法提取YYMM信息: {contract}")
            return None
        
        # 提取最后4位作为YYMM
        yymm_part = contract[-4:]
        
        # 验证YYMM部分是否为数字
        if not yymm_part.isdigit():
            logger.warning(f"合约名称的YYMM部分不是有效数字: {contract}")
            return None
        
        # 提取年份和月份
        yy = int(yymm_part[:2])
        mm = int(yymm_part[2:])
        
        # 验证月份范围
        if not (1 <= mm <= 12):
            logger.warning(f"合约名称的月份部分无效（必须在1-12之间）: {contract}")
            return None
        
        # 返回完整年份（假设21世纪）、月份和原始合约名称
        return (2000 + yy, mm, contract)
    
    # 提取有效合约并验证
    valid_contracts = []
    for contract in contracts:
        result = extract_and_validate(contract)
        if result:
            valid_contracts.append(result)
    
    # 按年份和月份排序
    valid_contracts.sort(key=lambda x: (x[0], x[1]))
    
    # 返回排序后的合约名称列表
    return [contract for _, _, contract in valid_contracts]

# 提供公共接口，兼容字典类型输入
def sort_files_by_yymm(files: List[str] or Dict[str, str]) -> List[str] or Dict[str, str]:
    """
    按YYMM时间信息对合约文件进行排序
    
    Args:
        files: 可以是合约名称列表或合约名称到文件路径的字典
        
    Returns:
        排序后的合约列表或保持键值对的排序后的字典
    """
    if isinstance(files, dict):
        # 对字典的键进行排序
        sorted_keys = _sort_files_by_yymm(list(files.keys()))
        # 创建排序后的字典
        return {key: files[key] for key in sorted_keys}
    elif isinstance(files, list):
        # 对列表进行排序
        return _sort_files_by_yymm(files)
    else:
        logger.error(f"输入参数类型不支持: {type(files)}")
        return files

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

def load_kline_data(contract_code: str, contract_files: Dict[str, str]) -> Optional[pd.DataFrame]:
    """
    加载K线数据文件
    从get_all_contract_files返回的合约文件列表中获取数据
    
    参数:
        contract_code: 合约代码
        contract_files: 合约文件字典，由get_all_contract_files函数返回
    
    返回:
        加载的K线数据DataFrame，如果加载失败则返回None
    """
    try:
        # 检查合约代码是否在合约文件列表中
        if contract_code not in contract_files:
            logger.warning(f"合约代码 {contract_code} 不在合约文件列表中")
            return None
        
        # 从合约文件列表中获取文件路径
        file_path = contract_files[contract_code]
        
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
        logger.error(f"加载K线数据文件失败: 合约代码 {contract_code}, 文件路径 {contract_files.get(contract_code, '未知')}, 错误: {str(e)}")
        return None

def get_all_contract_files(directory: str, future_code: str) -> Dict[str, str]:
    """
    获取目录下所有合约的文件路径
    K线数据文件的文件名规范是：期货代码 + YY + MM + .csv
    仅匹配符合格式规范的文件，避免将合约列表文件等非合约数据文件误判
    """
    contract_files = {}
    # 定义正则表达式模式，匹配 期货代码 + YY + MM + .csv 或 期货代码 + YY + DD + .csv 格式
    # 这里使用更精确的匹配，确保期货代码后面跟着4位数字
    pattern = re.compile(f'^{re.escape(future_code)}\d{{4}}\.csv$')
    
    for filename in os.listdir(directory):
        # 打印调试信息 filename
        logger.debug(f"get_all_contract_files 检查文件: {filename}")
        # 使用正则表达式严格匹配文件名格式
        if pattern.match(filename):
            # 从文件名中提取合约代码（文件名格式为 "期货编号 + YY + MM.csv" 或 "期货编号 + YY + DD.csv"）
            contract_code = filename.split('.')[0]
            contract_files[contract_code] = os.path.join(directory, filename)
    
    # 打印符合规范的合约文件总数和全部文件名
    logger.info(f"找到 {len(contract_files)} 个符合格式规范的合约文件")
    if contract_files:
        logger.info("符合规范的合约文件名列表:")
        # 对文件名进行排序，便于查看
        sorted_filenames = sorted(f"{code}.csv" for code in contract_files.keys())
        # 每行打印5个文件名，确保格式美观
        for i in range(0, len(sorted_filenames), 5):
            batch = sorted_filenames[i:i+5]
            logger.info("  " + "  ".join(batch))
    
    return contract_files

def collect_all_dates(contract_files: Dict[str, str]) -> pd.DatetimeIndex:
    """
    收集所有合约的日期，获取完整的日期范围
    K线数据文件的文件名规范是：期货代码 + YY + MM + .csv
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
        
def find_yydd_contract_files(directory: str, future_code: str) -> List[str]:
    """
    查找符合格式"期货编号 + YY + DD + .csv"的合约文件
    
    参数:
        directory: 要搜索的目录路径
        future_code: 期货品种代码
    
    返回:
        符合格式的合约文件名列表
    """
    yydd_contracts = []
    
    # 定义正则表达式模式，匹配 期货代码 + YY + DD + .csv 格式
    # 严格匹配以确保只有符合规范的文件被识别
    pattern = re.compile(f'^{re.escape(future_code)}\d{{4}}\.csv$')
    
    for filename in os.listdir(directory):
        # 使用正则表达式严格匹配文件名格式
        if pattern.match(filename):
            # 从文件名中提取合约代码（不包括扩展名）
            contract_code = filename.split('.')[0]
            yydd_contracts.append(contract_code)
    
    # 按YY + DD排序
    yydd_contracts.sort(key=lambda x: x[-4:])
    
    logger.info(f"找到 {len(yydd_contracts)} 个符合'期货编号 + YY + DD + .csv'格式的文件")
    return yydd_contracts

def collect_dates_from_validated_files(validated_contracts: List[str]) -> pd.DatetimeIndex:
    """
    从校验通过的合约文件名中提取日期信息
    K线数据文件的文件名规范是：期货代码 + YY + MM + .csv
    按年月排序获取日期信息
    
    参数:
        validated_contracts: 校验通过的合约文件名列表
    
    返回:
        按年月排序的日期索引
    """
    all_dates = []
    
    for contract_code in validated_contracts:
        try:
            # 从合约代码中提取年份和月份信息
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
            logger.warning(f"从校验通过的合约代码 {contract_code} 提取日期失败: {str(e)}")
    
    # 去重并按年月排序
    if all_dates:
        unique_dates = list(set(all_dates))
        # 按年月排序
        unique_dates.sort(key=lambda x: (x.year, x.month))
        logger.info(f"从校验通过的合约文件中提取并排序了 {len(unique_dates)} 个月份日期")
        return pd.DatetimeIndex(unique_dates)
    else:
        logger.warning("未能从校验通过的合约文件中提取到有效日期信息")
        return pd.DatetimeIndex([])

def is_delivery_month_contract(contract_code: str, date: pd.Timestamp) -> bool:
    """
    判断合约是否处于交割月
    K线数据文件的文件名规范是：期货代码 + YY + MM + .csv
    合约代码格式为：期货品种+年份(后两位)+月份，如rb2310表示2023年10月交割的螺纹钢合约
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
    logger.debug(f"[determine_main_contract_by_volume] 开始处理日期: {date}, 合约文件数量: {len(contract_files)}")
    
    max_volume = 0
    main_contract = None
    considered_contracts = 0
    skipped_contracts = 0
    
    for contract_code, file_path in contract_files.items():
        # 如果不允许交割月合约，且当前合约处于交割月，则跳过
        if not allow_delivery_month and is_delivery_month_contract(contract_code, date):
            logger.debug(f"[determine_main_contract_by_volume] 跳过交割月合约: {contract_code}")
            skipped_contracts += 1
            continue
            
        considered_contracts += 1
        df = load_kline_data(contract_code, contract_files)
        
        if df is None:
            logger.debug(f"[determine_main_contract_by_volume] 无法加载合约数据: {contract_code}")
            continue
            
        if 'trade_date' not in df.columns:
            logger.debug(f"[determine_main_contract_by_volume] 合约 {contract_code} 缺少 trade_date 列")
            continue
        
        # 查找该日期的数据
        date_data = df[df['trade_date'] == date]
        if date_data.empty:
            logger.debug(f"[determine_main_contract_by_volume] 合约 {contract_code} 在日期 {date} 无数据")
            continue
            
        if 'volume' not in date_data.columns:
            logger.debug(f"[determine_main_contract_by_volume] 合约 {contract_code} 缺少 volume 列")
            continue
            
        volume = date_data['volume'].iloc[0]
        logger.debug(f"[determine_main_contract_by_volume] 合约 {contract_code} 交易量: {volume}")
        
        if volume > max_volume:
            logger.debug(f"[determine_main_contract_by_volume] 更新最大交易量: {max_volume} -> {volume}, 合约: {main_contract} -> {contract_code}")
            max_volume = volume
            main_contract = contract_code
    
    logger.debug(f"[determine_main_contract_by_volume] 处理完成 - 考虑合约数: {considered_contracts}, 跳过合约数: {skipped_contracts}, 选定主力合约: {main_contract}, 最大交易量: {max_volume}")
    
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
    
    logger.debug(f"开始创建主力合约序列，共 {len(all_dates)} 个日期需要处理")
    logger.debug(f"可用交易量合约数量: {len(volume_files)}")
    logger.debug(f"交割月合约允许设置: {allow_delivery_month}")
    
    for i, date in enumerate(all_dates):
        # 添加进度信息
        if i % 10 == 0 or i == len(all_dates) - 1:
            logger.debug(f"处理进度: {i+1}/{len(all_dates)} ({(i+1)/len(all_dates)*100:.1f}%) - 正在处理日期: {date}")
        
        logger.debug(f"=== 处理日期: {date} ===")
        
        # 收集当天所有合约的交易量信息
        contract_volumes = []
        valid_contracts = 0
        skipped_delivery = 0
        
        # 打印所有可用合约及其交易量
        print(f"日期 {date} 的可用合约及交易量:")
        found_any_valid = False
        
        for contract_code, file_path in volume_files.items():
            # 检查是否为交割月合约
            is_delivery = not allow_delivery_month and is_delivery_month_contract(contract_code, date)
            if is_delivery:
                skipped_delivery += 1
                logger.debug(f"跳过交割月合约: {contract_code}")
                continue
            
            df = load_kline_data(contract_code, volume_files)
            if df is None:
                logger.debug(f"无法加载合约数据: {contract_code}")
                continue
            
            if 'trade_date' not in df.columns:
                logger.debug(f"合约 {contract_code} 数据中缺少 trade_date 列")
                continue
            
            # 查找该日期的数据
            date_data = df[df['trade_date'] == date]
            if date_data.empty:
                logger.debug(f"合约 {contract_code} 在日期 {date} 无数据")
                continue
            
            if 'volume' not in date_data.columns:
                logger.debug(f"合约 {contract_code} 数据中缺少 volume 列")
                continue
            
            volume = date_data['volume'].iloc[0]
            contract_volumes.append((contract_code, volume))
            valid_contracts += 1
            # 打印合约名称和交易量
            print(f"{contract_code}: {volume}")
            found_any_valid = True
            logger.debug(f"合约 {contract_code}: 交易量 = {volume}")
        
        # 如果没有找到任何有效合约，打印提示信息
        if not found_any_valid:
            print(f"日期 {date}: 未找到可用合约")
        
        # 记录当日合约统计信息
        logger.debug(f"日期 {date}: 有效合约数 = {valid_contracts}, 跳过交割月合约数 = {skipped_delivery}")
        
        # 确定主力合约
        main_contract = None
        max_volume = 0
        
        if contract_volumes:
            # 按交易量降序排序
            contract_volumes.sort(key=lambda x: x[1], reverse=True)
            
            # 记录排序后的合约交易量
            logger.debug(f"日期 {date} 合约交易量排序:")
            for idx, (code, vol) in enumerate(contract_volumes[:3]):  # 只显示前3名
                logger.debug(f"  第{idx+1}名: {code} = {vol}")
            
            # 选择交易量最大的合约
            main_contract, max_volume = contract_volumes[0]
            logger.debug(f"选择主力合约: {main_contract}, 交易量: {max_volume}")
        
        if main_contract:
            main_contract_mapping.append({
                'trade_date': date,
                'main_contract': main_contract,
                'volume': max_volume
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
                prev_volume = _get_contract_volume(previous_main_contract, date, volume_files)
                logger.debug(f"切换详情: 前主力合约交易量: {prev_volume}, 新主力合约交易量: {max_volume}, 交易量差额: {max_volume - prev_volume}")
            else:
                logger.debug(f"主力合约未变化: {main_contract}")
            
            previous_main_contract = main_contract
        else:
            logger.warning(f"无法确定 {date} 的主力合约")
            logger.debug(f"无法确定主力合约的原因: 有效合约数 = {valid_contracts}, 合约交易量数据 = {contract_volumes}")
    
    logger.debug(f"主力合约序列创建完成，共 {len(main_contract_mapping)} 条记录，{len(switch_records)} 次切换")
    return pd.DataFrame(main_contract_mapping), switch_records

def _get_contract_volume(contract_code: str, date: pd.Timestamp, volume_files: Dict[str, str]) -> float:
    """
    获取指定合约在指定日期的交易量
    
    Args:
        contract_code: 合约代码
        date: 交易日期
        volume_files: 交易量合约文件映射
        
    Returns:
        交易量，如果获取失败则返回0
    """
    try:
        if contract_code in volume_files:
            df = load_kline_data(contract_code, volume_files)
            if df is not None and 'trade_date' in df.columns and 'volume' in df.columns:
                date_data = df[df['trade_date'] == date]
                if not date_data.empty:
                    return date_data['volume'].iloc[0]
    except Exception as e:
        logger.debug(f"获取合约 {contract_code} 在 {date} 的交易量失败: {str(e)}")
    return 0

def build_main_contract_kline(main_contract_mapping: pd.DataFrame, 
                            contract_files: Dict[str, str],
                            future_code: str) -> pd.DataFrame:
    """
    构建主力合约K线数据
    """
    logger.debug(f"[build_main_contract_kline] 开始构建主力合约K线数据，映射记录数: {len(main_contract_mapping)}, 合约文件数: {len(contract_files)}")
    
    main_contract_data = []
    successful_records = 0  
    failed_records = 0
    missing_files = 0
    
    for idx, row in main_contract_mapping.iterrows():
        date = row['trade_date']
        contract_code = row['main_contract']
        volume = row['volume']
        
        logger.debug(f"[build_main_contract_kline] 处理记录 {idx+1}/{len(main_contract_mapping)} - 日期: {date}, 合约: {contract_code}, 交易量: {volume}")
        
        if contract_code not in contract_files:
            logger.debug(f"[build_main_contract_kline] 合约文件不存在: {contract_code}")
            missing_files += 1
            failed_records += 1
            continue
        
        df = load_kline_data(contract_code, contract_files)
        if df is None:
            logger.debug(f"[build_main_contract_kline] 无法加载合约K线数据: {contract_code}")
            failed_records += 1
            continue
        
        # 查找该日期的K线数据
        date_data = df[df['trade_date'] == date].copy()
        if date_data.empty:
            logger.debug(f"[build_main_contract_kline] 合约 {contract_code} 在日期 {date} 无K线数据")
            failed_records += 1
            continue
        
        # 添加主力合约标识
        date_data['symbol'] = f"{future_code}9999"
        date_data['original_contract'] = contract_code
        main_contract_data.append(date_data)
        successful_records += 1
        logger.debug(f"[build_main_contract_kline] 成功添加记录: {contract_code} @ {date}")
        
        # 添加进度信息
        if (idx + 1) % 10 == 0 or idx == len(main_contract_mapping) - 1:
            logger.debug(f"[build_main_contract_kline] 进度: {idx+1}/{len(main_contract_mapping)} - 成功: {successful_records}, 失败: {failed_records}")
    
    # 记录构建完成的统计信息
    logger.debug(f"[build_main_contract_kline] 构建完成 - 总记录数: {len(main_contract_mapping)}, 成功添加: {successful_records}, 失败: {failed_records}")
    logger.debug(f"[build_main_contract_kline] 失败原因统计: 缺失文件: {missing_files}, 加载失败: {failed_records - missing_files}")
    
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
                # 校验成功，记录校验通过的合约文件名
                validated_contracts = list(volume_files.keys())
                logger.info(f"数据文件校验成功！共 {len(validated_contracts)} 个合约文件通过校验")
        else:
            logger.info("由于未提供有效的合约列表，将不对数据文件进行完整性校验")
            # 未进行校验时，使用volume_files中的所有合约
            validated_contracts = list(volume_files.keys())
        
        # 收集所有日期
        logger.info("收集主力合约日期信息...")
        
        if validate_data:
            # 如果进行了数据校验，使用校验通过的合约文件名作为数据源
            all_dates = collect_dates_from_validated_files(validated_contracts)
            # 打印收集到的日期范围
            logger.info(f"收集到的日期范围: {all_dates.min().strftime('%Y-%m-%d')} 至 {all_dates.max().strftime('%Y-%m-%d')}")
            logger.info(f"共收集到 {len(all_dates)} 个月份日期")
        else:
            # 如果未进行数据校验，查找符合"期货编号 + YY + DD + .csv"格式的文件
            logger.info(f"从期货日线文件路径 {args.volume_data_dir} 查找符合'期货编号 + YY + DD + .csv'格式的文件...")
            yydd_contracts = find_yydd_contract_files(args.volume_data_dir, args.future_code)
            
            # 打印文件列表
            if yydd_contracts:
                logger.info("找到的符合格式的文件列表:")
                # 每行打印5个文件名，以便查看
                for i in range(0, len(yydd_contracts), 5):
                    batch = yydd_contracts[i:i+5]
                    formatted_line = "  ".join([f"{contract:<10}" for contract in batch])
                    logger.info(formatted_line)
                
                # 从YYDD格式的合约文件名中提取日期
                all_dates = []
                for contract_code in yydd_contracts:
                    try:
                        # 提取最后4位作为YY + DD部分
                        yydd_part = contract_code[-4:]
                        if yydd_part.isdigit():
                            # 转换为年份和日期
                            year = 2000 + int(yydd_part[:2])  # 假设是21世纪的年份
                            day = int(yydd_part[2:])
                            
                            # 验证日期是否有效
                            if 1 <= day <= 31:
                                # 创建日期，月份假设为1月（这里可能需要根据实际情况调整）
                                # 注意：由于只有年份和日期信息，这里使用固定月份可能不准确
                                # 更好的做法是将YYDD作为一个整体排序键
                                date_str = f"{year}-01-{day:02d}"
                                date = pd.to_datetime(date_str)
                                all_dates.append(date)
                    except Exception as e:
                        logger.warning(f"从合约代码 {contract_code} 提取日期失败: {str(e)}")
                
                # 去重并排序
                if all_dates:
                    unique_dates = list(set(all_dates))
                    unique_dates.sort()
                    all_dates = pd.DatetimeIndex(unique_dates)
                    logger.info(f"从YYDD格式文件中提取并排序了 {len(all_dates)} 个日期")
                else:
                    # 如果从YYDD格式文件中无法提取日期，则回退到使用原来的方法
                    logger.warning("无法从YYDD格式文件中提取有效日期，尝试使用原方法...")
                    all_dates = collect_dates_from_validated_files(validated_contracts)
                    logger.info(f"共收集到 {len(all_dates)} 个日期")
            else:
                # 如果没有找到符合YYDD格式的文件，使用原来的方法
                logger.warning("未找到符合'期货编号 + YY + DD + .csv'格式的文件，尝试使用原方法...")
                all_dates = collect_dates_from_validated_files(validated_contracts)
                logger.info(f"共收集到 {len(all_dates)} 个日期")
        
        # kline_files 还有volume_files 都需要按照YYMM日期重新排序
        kline_files = sort_files_by_yymm(kline_files)
        volume_files = sort_files_by_yymm(volume_files)
        # 创建主力合约序列前的调试信息
        logger.info("======= 主力合约创建调试信息 =======")
        logger.info(f"开始确定主力合约...")
        logger.info(f"交割月合约处理设置: {'允许交割月合约作为主力合约' if args.Delivery else '不允许交割月合约作为主力合约'}")
        logger.info(f"可用日期范围: {all_dates.min().strftime('%Y-%m-%d')} 至 {all_dates.max().strftime('%Y-%m-%d')}")
        logger.info(f"日期总数: {len(all_dates)}")
        logger.info(f"K线合约文件数量: {len(kline_files)}")
        logger.info(f"交易量合约文件数量: {len(volume_files)}")
        if kline_files:
            logger.info(f"前5个K线合约文件示例: {list(kline_files.keys())[:5]}")
        if volume_files:
            logger.info(f"前5个交易量合约文件示例: {list(volume_files.keys())[:5]}")
        logger.info("===================================")
        # 添加一个互动步骤，确认用户是否继续
        user_input = input("确认继续生成主力合约吗？(y/n): ")
        if user_input.lower() != 'y':
            logger.info("用户取消操作")
            return
        # 创建主力合约序列
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
