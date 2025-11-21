import os
import json
import tushare as ts
import pandas as pd
from datetime import datetime

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

def _download_rb_future_contracts(pro, save_dir=None):
    """
    私有方法：下载螺纹钢期货全部合约的基本信息
    
    参数：
    pro: tushare pro_api实例
    save_dir: 保存数据的目录，默认为None（不保存）
    
    返回：
    DataFrame: 螺纹钢期货合约信息
    """
    try:
        # 使用fut_basic接口获取螺纹钢期货合约信息
        # 螺纹钢的产品代码是RB
        df = pro.fut_basic(
            exchange='SHF',  # 上海期货交易所
            fut_type='1',    # 1: 期货
            fields='ts_code,symbol,name,exchange,list_date,delist_date,status',
            name='%螺纹钢%'  # 模糊匹配螺纹钢
        )
        
        if df.empty:
            print("警告：未获取到螺纹钢期货合约信息")
            return None
        
        print(f"成功下载螺纹钢期货合约信息，共 {len(df)} 条记录")
        
        # 保存数据到CSV文件
        if save_dir:
            # 构建保存文件路径
            filename = "rb_future_contracts_info.csv"
            filepath = os.path.join(save_dir, filename)
            
            # 保存数据
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"螺纹钢期货合约信息已保存至 {filepath}")
        
        return df
    except Exception as e:
        print(f"错误：下载螺纹钢期货合约信息失败 - {str(e)}")
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
    
    # 下载螺纹钢期货合约基本信息
    print("\n开始下载螺纹钢期货合约基本信息...")
    contracts_data = _download_rb_future_contracts(pro, save_dir)
    
    # 打印合约信息样例
    if contracts_data is not None:
        print("\n螺纹钢期货合约信息样例:")
        print(contracts_data.head())
    
    # 设置下载参数
    # 示例：下载螺纹钢期货数据
    future_symbol = "RB.SHF"  # 螺纹钢期货
    start_date = "20230101"
    end_date = datetime.now().strftime("%Y%m%d")  # 使用当前日期作为结束日期
    
    # 下载期货数据
    print("\n开始下载期货数据...")
    future_data = download_future_data(pro, future_symbol, start_date, end_date, save_dir)
    
    # 打印数据样例
    if future_data is not None:
        print("\n期货数据样例:")
        print(future_data.head())


if __name__ == "__main__":
    main()
