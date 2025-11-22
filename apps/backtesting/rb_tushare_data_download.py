import os
import json
import tushare as ts
import pandas as pd
from datetime import datetime
import argparse
import re
import time
import numpy as np

def handle_download_failure(pro, contract_code, contract_name, contract_start, contract_end, save_dir, auto_wait_mode_ref):
    """
    处理下载失败的情况
    
    参数:
    - pro: Tushare Pro接口实例
    - contract_code: 合约代码
    - contract_name: 合约名称
    - contract_start: 开始日期
    - contract_end: 结束日期
    - save_dir: 保存目录
    - auto_wait_mode_ref: 自动等待模式标志（通过列表引用传递）
    
    返回值:
    - 更新后的contract_data（如果重试成功）或None
    """
    # 如果处于自动等待模式，直接等待1分钟后继续
    if auto_wait_mode_ref[0]:
        print(f"  自动等待模式：等待1分钟后继续下载...")
        for i in range(60, 0, -1):
            print(f"  剩余等待时间: {i}秒", end='\r')
            time.sleep(1)
        print()  # 换行
        
        # 尝试重新下载
        print(f"  重新尝试下载 {contract_code} ({contract_name}) 的数据...")
        retry_data = download_future_data(pro, contract_code, contract_start, contract_end, save_dir)
        return retry_data
    
    # 非自动模式，等待用户输入
    while True:
        print(f"\n下载失败处理选项:")
        print(f"  W/w - 等待1分钟后继续下载")
        print(f"  E/e - 退出程序")
        print(f"  R/r - 立即重试")
        print(f"  C/c - 等待1分钟后自动模式（后续失败不再询问）")
        
        user_choice = input("请选择操作 [W/E/R/C]: ").strip().lower()
        
        if user_choice == 'w':
            print(f"  等待1分钟后继续下载...")
            for i in range(60, 0, -1):
                print(f"  剩余等待时间: {i}秒", end='\r')
                time.sleep(1)
            print()  # 换行
            
            # 尝试重新下载
            print(f"  重新尝试下载 {contract_code} ({contract_name}) 的数据...")
            retry_data = download_future_data(pro, contract_code, contract_start, contract_end, save_dir)
            return retry_data
        
        elif user_choice == 'e':
            print(f"\n用户选择退出程序。")
            exit(0)
        
        elif user_choice == 'r':
            print(f"  立即重试下载 {contract_code} ({contract_name}) 的数据...")
            retry_data = download_future_data(pro, contract_code, contract_start, contract_end, save_dir)
            return retry_data
        
        elif user_choice == 'c':
            print(f"  等待1分钟后进入自动模式...")
            for i in range(60, 0, -1):
                print(f"  剩余等待时间: {i}秒", end='\r')
                time.sleep(1)
            print()  # 换行
            
            # 设置自动等待模式标志
            auto_wait_mode_ref[0] = True
            print(f"  已进入自动等待模式：后续下载失败将自动等待1分钟后重试")
            
            # 尝试重新下载
            print(f"  重新尝试下载 {contract_code} ({contract_name}) 的数据...")
            retry_data = download_future_data(pro, contract_code, contract_start, contract_end, save_dir)
            return retry_data
        
        else:
            print("无效的选择，请重新输入。")

def sort_contracts_by_date(contracts_df):
    """
    对期货合约数据按照年月进行排序，将不符合产品代码+YY+MM格式的数据放在最后
    
    参数:
    contracts_df: 包含期货合约信息的DataFrame
    
    返回:
    DataFrame: 排序后的合约数据
    """
    if contracts_df is None or contracts_df.empty:
        return contracts_df
    
    # 创建临时列用于排序
    # 符合格式的合约将有正常的年月值，不符合的将有一个很大的值确保排在最后
    temp_sort_keys = []
    
    for idx, row in contracts_df.iterrows():
        ts_code = row.get('ts_code', '')
        if not isinstance(ts_code, str):
            # 非字符串类型，放在最后
            temp_sort_keys.append((9999, 13))
            continue
        
        # 尝试从代码中提取年份和月份信息 (格式: 产品代码+YY+MM)
        # 假设ts_code格式类似 RB2401.SHF
        try:
            # 提取点号前的部分，然后取最后4位
            code_part = ts_code.split('.')[0]
            if len(code_part) >= 4:
                # 尝试提取最后4位作为年月信息
                year_month_str = code_part[-4:]
                if year_month_str.isdigit():
                    # 提取年份和月份
                    year = int(year_month_str[:2])
                    month = int(year_month_str[2:])
                    # 验证月份范围
                    if 1 <= month <= 12:
                        temp_sort_keys.append((year, month))
                        continue
        except Exception:
            pass
        
        # 不符合格式，放在最后
        temp_sort_keys.append((9999, 13))
    
    # 添加排序键到DataFrame
    contracts_df['_sort_year'] = [key[0] for key in temp_sort_keys]
    contracts_df['_sort_month'] = [key[1] for key in temp_sort_keys]
    
    # 按年月排序
    sorted_df = contracts_df.sort_values(by=['_sort_year', '_sort_month'])
    
    # 删除临时排序列
    sorted_df = sorted_df.drop(['_sort_year', '_sort_month'], axis=1)
    
    print(f"合约数据已按年月排序，共{len(sorted_df)}条记录")
    return sorted_df

def get_tushare_token():
    """
    从../data/key.json文件中获取tushare的token
    """
    # 获取当前脚本所在目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # 构建key.json文件的绝对路径
    key_file_path = os.path.join(script_dir, 'key.json')
    
    try:
        with open(key_file_path, 'r', encoding='utf-8') as f:
            key_data = json.load(f)
            return key_data.get('tushare_token')
    except FileNotFoundError:
        print(f"错误：找不到文件 {key_file_path}")
        return None
    except json.JSONDecodeError:
        print(f"错误：无法解析 {key_file_path} 文件中的JSON")
        return None

def initialize_tushare(token):
    """
    初始化tushare接口
    """
    if not token:
        print("错误：未提供有效的tushare token")
        return None
    
    try:
        ts.set_token(token)
        pro = ts.pro_api()
        print("Tushare接口初始化成功")
        return pro
    except Exception as e:
        print(f"错误：Tushare接口初始化失败 - {str(e)}")
        return None

def _download_rb_future_contracts(pro, save_dir=None, fut_code='RB', fut_type='1'):
    """
    私有方法：下载指定期货品种全部合约的基本信息
    
    参数：
    pro: tushare pro_api实例
    save_dir: 保存数据的目录，默认为None（不保存）
    fut_code: 期货品种代码标识，默认: 'RB'
    fut_type: 合约类型，1:普通合约，2:主力合约和连续合约，默认: '1'
    
    返回：
    DataFrame: 期货合约信息
    """
    try:
        # 使用fut_basic接口获取期货合约信息
        df = pro.fut_basic(
            exchange='SHFE',  # 上海期货交易所
            fut_type=fut_type,    # 1: 普通合约， 2：主力合约和连续合约
            fields='ts_code,symbol,name,exchange,list_date,delist_date',
            fut_code=fut_code  # 期货品种代码
        )
        
        if df.empty:
            print(f"警告：未获取到{fut_code}期货合约信息")
            return None
        
        print(f"成功下载{fut_code}期货合约信息，共 {len(df)} 条记录")
        
        # 保存数据到CSV文件
        if save_dir:
            # 构建保存文件路径
            filename = "future_contracts_info.csv"
            filepath = os.path.join(save_dir, fut_code + "_" + fut_type + "_" + filename)
            
            # 保存数据
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"{fut_code}期货合约信息已保存至 {filepath}")
        
        return df
    except Exception as e:
        print(f"错误：下载{fut_code}期货合约信息失败 - {str(e)}")
        return None

def download_future_data(pro, symbol, start_date, end_date, save_dir=None):
    """
    下载期货数据
    
    参数：
    pro: tushare pro_api实例
    symbol: 期货代码
    start_date: 开始日期，格式如'20230101'
    end_date: 结束日期，格式如'20231231'
    save_dir: 保存数据的目录，默认为None（不保存）
    
    返回：
    DataFrame: 下载的数据
    None: 普通错误
    -1: 接口调用频率限制错误
    """
    try:
        # 下载期货日线数据
        df = pro.fut_daily(
            ts_code=symbol,
            trade_date='',
            start_date=start_date,
            end_date=end_date,
            fields='ts_code,trade_date,open,high,low,close,volume,amount,oi,oi_chg'  
        )
        
        if df.empty:
            print(f"警告：未获取到 {symbol} 的数据")
            return None
        
        # 按日期排序
        df = df.sort_values('trade_date')
        
        print(f"成功下载 {symbol} 的数据，共 {len(df)} 条记录")
        
        # 保存数据到CSV文件
        if save_dir:
            # 确保保存目录存在
            os.makedirs(save_dir, exist_ok=True)
            
            # 简化文件名，只使用合约代码+.csv格式
            filename = f"{symbol}.csv"
            filepath = os.path.join(save_dir, filename)
            
            # 保存数据
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"数据已保存至 {filepath}")
        
        return df
    except Exception as e:
        error_msg = str(e)
        print(f"错误：下载 {symbol} 数据失败 - {error_msg}")
        
        # 检查是否是接口调用频率限制错误
        # 匹配"每分钟最多访问该接口X次"的模式
        if "每分钟最多访问该接口" in error_msg and "次" in error_msg:
            # 提取限制次数（如果有）
            import re
            match = re.search(r'每分钟最多访问该接口(\d+)次', error_msg)
            if match:
                limit_count = match.group(1)
                if limit_count == '0':
                    print(f"⚠️ 警告：触发tushare接口频率限制，每分钟最多访问{limit_count}次！")
                    print(f"⚠️ 您的账户可能被禁用或权限受限，请检查您的tushare账户状态！")
                else:
                    print(f"触发tushare接口频率限制，每分钟最多访问{limit_count}次")
            else:
                print("触发tushare接口频率限制")
            return -1  # 返回特殊标记表示遇到接口频率限制错误
        
        return None

def validate_date_format(date_str):
    """验证日期格式是否为YYYYMMDD"""
    pattern = r'^\d{8}$'
    if not re.match(pattern, date_str):
        return False
    try:
        # 尝试解析日期确保有效性
        datetime.strptime(date_str, '%Y%m%d')
        return True
    except ValueError:
        return False

def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(
        description='下载期货数据工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""使用示例：
  # 使用示例：
  # 使用默认参数下载螺纹钢期货数据（模式1）
  python rb_tushare_data_download.py
  
  # 使用长选项指定参数下载螺纹钢期货数据
  python rb_tushare_data_download.py --symbol RB.SHF --start_date 20230101 --end_date 20231231
  
  # 使用短选项指定参数下载螺纹钢期货数据（更简洁）
  python rb_tushare_data_download.py -s RB.SHF -b 20230101 -e 20231231
  
  # 指定数据保存目录
  python rb_tushare_data_download.py -s RB.SHF -o D:\data\futures
  
  # 使用联动模式下载所有合约数据（模式2）
  python rb_tushare_data_download.py -s RB.SHF -m 2
  
  # 使用联动模式下载所有合约数据，并指定日期范围
  python rb_tushare_data_download.py -s RB.SHF -m 2 -b 20230101 -e 20231231
  
  # 螺纹钢模式2下载范例：下载所有螺纹钢合约2020年至2023年的数据
  python rb_tushare_data_download.py -s RB.SHF -m 2 -b 20200101 -e 20231231
  
  # 下载其他期货品种数据（例如铜期货，使用SHF作为交易所标识）
  python rb_tushare_data_download.py -s CU.SHF -b 20230601 -e 20230930
  
  # 手动指定fut_code参数（通常不需要，会自动从symbol中提取）
  python rb_tushare_data_download.py -s RB.SHF -f RB
  
  # 有效交易所标识：SHFE/SHF（上海期货交易所）、DCE（大连商品交易所）、CZCE（郑州商品交易所）
  # CFFEX（中国金融期货交易所）、INE（上海国际能源交易中心）
        """
    )
    
    # 添加参数（同时支持长选项和短选项）
    parser.add_argument('-s', '--symbol', type=str, default='RB.SHF', help='期货代码，格式为"品种.交易所"，例如: RB.SHF')
    parser.add_argument('-b', '--start_date', type=str, default='20130101', help='开始日期，必须为YYYYMMDD格式，例如: 20230101')
    parser.add_argument('-e', '--end_date', type=str, default=datetime.now().strftime("%Y%m%d"), help='结束日期，必须为YYYYMMDD格式，例如: 20231231')
    parser.add_argument('-f', '--fut_code', type=str, default=None, help='期货品种代码标识，用于查询合约信息（可选，默认会从symbol中自动提取），例如: RB')
    parser.add_argument('-o', '--output_dir', type=str, default=None, help='数据保存目录路径（可选，默认保存在脚本所在目录的data文件夹）')
    parser.add_argument('-m', '--work_mode', type=int, default=1, choices=[1, 2], help='运行模式：1-下载指定单个期货数据（默认），2-联动模式（下载所有合约数据）')
    
    # 解析参数
    args = parser.parse_args()
    
    # 参数验证
    # 自动等待模式标志（使用列表实现引用传递）
    auto_wait_mode = [False]
    if not validate_date_format(args.start_date):
        parser.error(f'开始日期格式错误: {args.start_date}。请使用YYYYMMDD格式，例如: 20230101')
    
    if not validate_date_format(args.end_date):
        parser.error(f'结束日期格式错误: {args.end_date}。请使用YYYYMMDD格式，例如: 20231231')
    
    # 检查开始日期是否早于结束日期
    start_dt = datetime.strptime(args.start_date, '%Y%m%d')
    end_dt = datetime.strptime(args.end_date, '%Y%m%d')
    
    if start_dt > end_dt:
        parser.error(f'开始日期({args.start_date})不能晚于结束日期({args.end_date})')
    
    # 检查结束日期不能超过当前日期
    current_dt = datetime.now()
    if end_dt > current_dt:
        parser.error(f'结束日期({args.end_date})不能超过当前日期')
    
    # 验证symbol格式
    if '.' not in args.symbol:
        parser.error(f'期货代码格式错误: {args.symbol}。正确格式为"品种.交易所"，例如: RB.SHF')
    
    # 验证交易所标识（常见的期货交易所）
    valid_exchanges = ['SHFE', 'SHF', 'DCE', 'CZCE', 'CFFEX', 'INE']
    exchange = args.symbol.split('.')[-1].upper()
    if exchange not in valid_exchanges:
        parser.error(f'无效的交易所标识: {exchange}。有效的交易所为: {', '.join(valid_exchanges)}')
    
    # 自动从symbol中提取fut_code（如果未指定）
    if args.fut_code is None or args.fut_code.strip() == '':
        # 从symbol中提取期货品种代码
        if '.' in args.symbol:
            args.fut_code = args.symbol.split('.')[0]
            print(f"从symbol自动提取fut_code: {args.fut_code}")
        else:
            parser.error(f'期货代码格式错误: {args.symbol}。正确格式为"品种.交易所"，例如: RB.SHF')
    
    # 确保fut_code不为空
    if not args.fut_code or args.fut_code.strip() == '':
        parser.error('期货品种代码标识(fut_code)不能为空')
    
    # 获取tushare token
    token = get_tushare_token()
    if not token:
        print("请先在../data/key.json文件中设置有效的tushare token")
        return
    
    # 初始化tushare
    pro = initialize_tushare(token)
    if not pro:
        print("Tushare接口初始化失败")
        return
    
    # 设置保存目录
    if args.output_dir:
        save_dir = args.output_dir
    else:
        save_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    
    # 确保保存目录存在
    os.makedirs(save_dir, exist_ok=True)
    print(f"数据将保存到: {save_dir}")
    
    # 模式2特殊处理：检查合约列表文件是否存在
    contracts_file_path = None
    if args.work_mode == 2:
        # 构建预期的合约信息文件名
        contracts_file_name = f"{args.fut_code}_1_future_contracts_info.csv"
        contracts_file_path = os.path.join(save_dir, contracts_file_name)
        
        # 检查文件是否存在
        if os.path.exists(contracts_file_path):
            try:
                # 尝试读取文件
                saved_contracts_data = pd.read_csv(contracts_file_path)
                # 读取数据后立即调用排序函数
                saved_contracts_data = sort_contracts_by_date(saved_contracts_data)
                total_records = len(saved_contracts_data)
                
                print(f"\n发现已存在的期货合约列表文件: {contracts_file_path}")
                print(f"文件包含 {total_records} 条合约记录")
                print("\n前10条合约数据预览:")
                print(saved_contracts_data.head(10))
                
                display_index = 10  # 已显示的记录数
                use_saved_file = False
                
                # 用户交互循环
                while True:
                    user_input = input("\n请选择操作: [Y]使用文件 [N]重新调用接口 [M]查看更多数据: ").strip().lower()
                    
                    if user_input == 'y':
                        use_saved_file = True
                        contracts_data = saved_contracts_data
                        print("\n将使用文件中的合约信息...")
                        break
                    elif user_input == 'n':
                        use_saved_file = False
                        print("\n将重新调用接口获取最新合约信息...")
                        break
                    elif user_input == 'm':
                        if display_index < total_records:
                            # 显示接下来的10条记录
                            end_index = min(display_index + 10, total_records)
                            print(f"\n接下来的10条合约数据(记录 {display_index+1}-{end_index}):")
                            print(saved_contracts_data.iloc[display_index:end_index])
                            display_index = end_index
                            
                            # 如果已经显示完所有数据，提示用户
                            if display_index >= total_records:
                                print("\n已显示所有合约数据")
                        else:
                            print("\n已显示所有合约数据")
                    else:
                        print("无效输入，请输入 Y、N 或 M")
                
                # 如果用户选择使用保存的文件，则跳过接口调用
                if use_saved_file:
                    # 跳过后续的接口调用
                    pass
                else:
                    # 用户选择重新调用接口
                    print(f"\n开始下载{args.fut_code}期货合约基本信息...")
                    contracts_data = _download_rb_future_contracts(pro, save_dir, fut_code=args.fut_code)
            except Exception as e:
                print(f"读取合约列表文件失败: {str(e)}")
                print("将重新调用接口获取合约信息...")
                print(f"\n开始下载{args.fut_code}期货合约基本信息...")
                contracts_data = _download_rb_future_contracts(pro, save_dir, fut_code=args.fut_code)
            finally:
                # 对合约数据进行排序
                if 'contracts_data' in locals() and contracts_data is not None:
                    contracts_data = sort_contracts_by_date(contracts_data)
        else:
            # 文件不存在，调用接口获取
            print(f"\n开始下载{args.fut_code}期货合约基本信息...")
            contracts_data = _download_rb_future_contracts(pro, save_dir, fut_code=args.fut_code)
            # 对合约数据进行排序
            if contracts_data is not None:
                contracts_data = sort_contracts_by_date(contracts_data)
    else:
        # 非模式2，直接调用接口
        print(f"\n开始下载{args.fut_code}期货合约基本信息...")
        contracts_data = _download_rb_future_contracts(pro, save_dir, fut_code=args.fut_code)
        # 对合约数据进行排序
        if contracts_data is not None:
            contracts_data = sort_contracts_by_date(contracts_data)
    
    # 打印合约信息样例
    if contracts_data is not None:
        print(f"\n{args.fut_code}期货合约信息样例:")
        print(contracts_data.head())
    
    # 使用命令行参数
    future_symbol = args.symbol
    start_date = args.start_date
    end_date = args.end_date
    
    # 打印使用的参数信息
    print(f"\n使用的下载参数:")
    print(f"- 运行模式: {args.work_mode} (1-单个期货数据，2-所有合约数据)")
    print(f"- 期货代码: {future_symbol}")
    print(f"- 期货品种代码标识: {args.fut_code}")
    print(f"- 开始日期: {start_date}")
    print(f"- 结束日期: {end_date}")
    print(f"- 数据保存目录: {save_dir}")
    
    # 根据工作模式执行不同的下载逻辑
    if args.work_mode == 1:
        # 模式1：下载指定单个期货数据
        print("\n开始下载期货数据...")
        future_data = download_future_data(pro, future_symbol, start_date, end_date, save_dir)
        
        # 打印数据样例
        if future_data is not None:
            print("\n期货数据样例:")
            print(future_data.head())
    else:
        # 模式2：联动模式，下载所有合约数据
        print(f"\n开始联动模式下载...")
        
        # 确保已获取合约信息
        if contracts_data is None:
            print("错误：未获取到合约信息，无法进行联动模式下载")
            return
        
        print(f"将下载{len(contracts_data)}个合约的数据")
        
        # 创建合约数据专用目录
        contracts_dir = os.path.join(save_dir, f"{args.fut_code}_contracts")
        os.makedirs(contracts_dir, exist_ok=True)
        print(f"合约数据将保存到: {contracts_dir}")
        
        # 统计信息
        total_contracts = len(contracts_data)
        successful_downloads = 0
        failed_downloads = 0  # 新增失败计数变量
        
        # 确保合约数据按年月排序
        contracts_data = sort_contracts_by_date(contracts_data)
        
        # 预览将要下载的合约及其文件状态
        print(f"\n将下载的合约预览:")
        contracts_to_download = []
        files_already_exist = 0
        
        for idx, contract in contracts_data.iterrows():
            contract_code = contract['ts_code']
            contract_name = contract['name']
            list_date = contract['list_date']
            delist_date = contract['delist_date']
            
            # 模式2特殊处理：如果用户未指定起止日期（使用默认值），则使用合约的完整历史数据
            using_default_dates = (args.start_date == '20130101' and args.end_date == datetime.now().strftime("%Y%m%d"))
            
            if using_default_dates:
                # 使用合约的完整历史数据
                contract_start = list_date
                contract_end = delist_date
            else:
                # 使用用户指定的日期范围限制
                contract_start = max(list_date, start_date)
                contract_end = min(delist_date, end_date)
            
            # 检查是否有重叠的日期范围
            if contract_start > contract_end:
                continue
            
            # 简化文件名，只使用合约代码+.csv格式
            filename = f"{contract_code}.csv"
            filepath = os.path.join(contracts_dir, filename)
            
            # 检查文件是否已存在
            file_exists = os.path.exists(filepath)
            if file_exists:
                files_already_exist += 1
            
            # 添加到待下载列表
            contracts_to_download.append({
                'index': idx,
                'contract': contract,
                'contract_start': contract_start,
                'contract_end': contract_end,
                'filepath': filepath,
                'file_exists': file_exists
            })
            
            # 打印合约信息
            status = "文件已存在" if file_exists else "将下载"
            print(f"[{idx+1}] {contract_code} ({contract_name}) - {status}")
        
        # 显示统计信息
        total_to_process = len(contracts_to_download)
        will_download = total_to_process - files_already_exist
        print(f"\n预览统计:")
        print(f"- 总共将处理合约数: {total_to_process}")
        print(f"- 文件已存在: {files_already_exist}")
        print(f"- 将要下载: {will_download}")
        
        # 询问用户是否继续
        while True:
            user_input = input("\n是否继续下载? (Y/N): ").strip().lower()
            if user_input in ['y', 'n']:
                break
            print("请输入 Y 或 N")
        
        if user_input != 'y':
            print("用户取消下载操作")
            return
        
        print(f"\n开始下载...")
        
        # 逐个下载合约数据
        for item in contracts_to_download:
            # 获取合约信息
            contract = item['contract']
            idx = item['index']
            contract_code = contract['ts_code']
            contract_name = contract['name']
            contract_start = item['contract_start']
            contract_end = item['contract_end']
            filepath = item['filepath']
            file_exists = item['file_exists']
            
            # 模式2特殊处理：如果用户未指定起止日期（使用默认值），则使用合约的完整历史数据
            # 检查是否使用了默认的起始日期和结束日期
            using_default_dates = (args.start_date == '20130101' and args.end_date == datetime.now().strftime("%Y%m%d"))
            
            if using_default_dates:
                print(f"[{idx+1}/{total_contracts}] {contract_code} ({contract_name}): 使用全历史数据")
            
            # 检查是否有重叠的日期范围
            if contract_start > contract_end:
                print(f"[{idx+1}/{total_contracts}] {contract_code} ({contract_name}): 日期范围无重叠，跳过")
                continue
            
            # 简化文件名，只使用合约代码+.csv格式
            filename = f"{contract_code}.csv"
            filepath = os.path.join(contracts_dir, filename)
            
            # 检查文件是否已存在
            if file_exists:
                print(f"[{idx+1}/{total_contracts}] {contract_code} ({contract_name}): 文件已存在，跳过")
                continue
            
            print(f"[{idx+1}/{total_contracts}] 开始下载 {contract_code} ({contract_name}) 的数据...")
            print(f"  时间范围: {contract_start} 至 {contract_end}")
            
            # 下载单个合约数据
            contract_data = download_future_data(pro, contract_code, contract_start, contract_end, contracts_dir)
            
            # 检查是否遇到接口调用频率限制错误
            if isinstance(contract_data, int) and contract_data == -1:
                # 再次检查错误信息，确认是否是限制次数为0的情况
                try:
                    # 尝试一个简单的调用以获取最新错误信息
                    test_df = pro.fut_basic(exchange='SHFE', fut_type='1', fields='ts_code', fut_code='RB', limit=1)
                    # 如果成功获取数据，说明不是限制为0的情况
                    # Properly check if DataFrame is valid
                    if test_df is not None and hasattr(test_df, 'empty') and not test_df.empty:
                        print("遇到tushare接口频率限制，等待60秒后重试...")
                        # 等待60秒
                        for i in range(60, 0, -1):
                            print(f"剩余等待时间: {i}秒", end='\r')
                            time.sleep(1)
                        print()  # 换行
                        
                        # 重新尝试下载当前合约
                        print(f"重新尝试下载 {contract_code} ({contract_name}) 的数据...")
                        contract_data = download_future_data(pro, contract_code, contract_start, contract_end, contracts_dir)
                except Exception as e_test:
                    error_test_msg = str(e_test)
                    # 检查是否包含限制为0次的信息
                    match_test = re.search(r'每分钟最多访问该接口(\d+)次', error_test_msg)
                    if match_test and match_test.group(1) == '0':
                        print(f"⚠️ 严重警告：您的tushare账户每分钟访问次数限制为0次！")
                        print(f"⚠️ 这表明您的账户可能已被禁用或权限受限！")
                        print(f"⚠️ 建议您：")
                        print(f"⚠️ 1. 检查您的tushare账户状态和会员等级")
                        print(f"⚠️ 2. 确认您的账户是否欠费")
                        print(f"⚠️ 3. 联系tushare客服了解详细情况")
                        print("\n由于账户受限，下载任务已终止。")
                        # 提前结束循环
                        break
            
            # Properly handle contract_data in various cases
            download_succeeded = False  # 跟踪下载是否成功（包括重试）
            
            if contract_data is not None and not (isinstance(contract_data, int) and contract_data == -1):
                # Ensure contract_data is a DataFrame and not empty
                if hasattr(contract_data, 'empty') and not contract_data.empty:
                    successful_downloads += 1
                    download_succeeded = True
                    print(f"  ✓ 下载成功，共{len(contract_data)}条记录")
                else:
                    print(f"  ✗ 下载失败：未获取到有效数据")
                    # 处理下载失败情况
                    retry_data = handle_download_failure(pro, contract_code, contract_name, contract_start, contract_end, contracts_dir, auto_wait_mode)
                    # 检查重试结果
                    if retry_data is not None and not (isinstance(retry_data, int) and retry_data == -1):
                        if hasattr(retry_data, 'empty') and not retry_data.empty:
                            successful_downloads += 1
                            download_succeeded = True
                            print(f"  ✓ 重试成功，共{len(retry_data)}条记录")
                        else:
                            print(f"  ✗ 重试也失败：未获取到有效数据")
                            failed_downloads += 1  # 重试失败，计入失败
                    else:
                        failed_downloads += 1  # 未重试或重试遇到限制错误，计入失败
            else:
                print(f"  ✗ 下载失败")
                # 处理下载失败情况
                retry_data = handle_download_failure(pro, contract_code, contract_name, contract_start, contract_end, contracts_dir, auto_wait_mode)
                # 检查重试结果
                if retry_data is not None and not (isinstance(retry_data, int) and retry_data == -1):
                    if hasattr(retry_data, 'empty') and not retry_data.empty:
                        successful_downloads += 1
                        download_succeeded = True
                        print(f"  ✓ 重试成功，共{len(retry_data)}条记录")
                    else:
                        print(f"  ✗ 重试也失败：未获取到有效数据")
                        failed_downloads += 1  # 重试失败，计入失败
                else:
                    failed_downloads += 1  # 未重试或重试遇到限制错误，计入失败
        
        # 打印汇总信息
        print(f"\n联动模式下载完成！")
        print(f"- 总合约数: {total_contracts}")
        print(f"- 成功下载: {successful_downloads}")
        print(f"- 失败数量: {failed_downloads}")  # 使用实际失败计数，而不是总合约数减成功数


if __name__ == "__main__":
    main()
