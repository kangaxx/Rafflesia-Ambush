import os
import json
import tushare as ts
import pandas as pd
from datetime import datetime
import argparse

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
            
            # 构建保存文件路径
            filename = f"{symbol}_{start_date}_{end_date}.csv"
            filepath = os.path.join(save_dir, filename)
            
            # 保存数据
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"数据已保存至 {filepath}")
        
        return df
    except Exception as e:
        print(f"错误：下载 {symbol} 数据失败 - {str(e)}")
        return None

def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='下载螺纹钢期货数据')
    
    # 添加参数
    parser.add_argument('--symbol', type=str, default='RB.SHF', help='期货代码，默认: RB.SHF')
    parser.add_argument('--start_date', type=str, default='20130101', help='开始日期，格式: YYYYMMDD，默认: 20130101')
    parser.add_argument('--end_date', type=str, default=datetime.now().strftime("%Y%m%d"), help='结束日期，格式: YYYYMMDD，默认: 当前日期')
    parser.add_argument('--fut_code', type=str, default='RB', help='期货品种代码标识，默认: RB')
    
    # 解析参数
    args = parser.parse_args()
    
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
    save_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    
    # 下载期货合约基本信息
    print(f"\n开始下载{args.fut_code}期货合约基本信息...")
    contracts_data = _download_rb_future_contracts(pro, save_dir, fut_code=args.fut_code)
    
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
    print(f"- 期货代码: {future_symbol}")
    print(f"- 期货品种代码标识: {args.fut_code}")
    print(f"- 开始日期: {start_date}")
    print(f"- 结束日期: {end_date}")
    
    # 下载期货数据
    print("\n开始下载期货数据...")
    future_data = download_future_data(pro, future_symbol, start_date, end_date, save_dir)
    
    # 打印数据样例
    if future_data is not None:
        print("\n期货数据样例:")
        print(future_data.head())


if __name__ == "__main__":
    main()
