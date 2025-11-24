import json
import tushare as ts
import logging
import datetime
import os
import argparse

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_tushare_token(token_file="key.json"):
    """
    从本地JSON文件中读取Tushare token
    
    Args:
        token_file: token文件路径
    
    Returns:
        token字符串
    """
    try:
        with open(token_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config.get('tushare_token')
    except FileNotFoundError:
        logger.error(f"token文件 {token_file} 未找到")
        raise
    except json.JSONDecodeError:
        logger.error(f"token文件 {token_file} 格式错误")
        raise
    except Exception as e:
        logger.error(f"读取token时出错: {e}")
        raise

def init_tushare(token):
    """
    初始化Tushare接口
    
    Args:
        token: tushare token
    """
    try:
        ts.set_token(token)
        pro = ts.pro_api()
        logger.info("Tushare接口初始化成功")
        return pro
    except Exception as e:
        logger.error(f"Tushare接口初始化失败: {e}")
        raise

def get_exchange_start_date(pro, exchange='SHFE', start_date='19990504', end_date=None):
    """
    获取交易所开始日期
    
    Args:
        pro: tushare pro_api实例
        exchange: 交易所代码，默认上海期货交易所(SHFE)
                 可选值: SSE(上交所), SZSE(深交所), CFFEX(中金所), SHFE(上期所), 
                        CZCE(郑商所), DCE(大商所), INE(能源中心)
        start_date: 开始日期，默认1999年5月4日
        end_date: 结束日期，默认为系统当前日期
    
    Returns:
        交易所开始日期字符串 (YYYYMMDD格式)
    """
    try:
        # 如果未提供结束日期，使用当前系统日期
        if end_date is None:
            end_date = datetime.datetime.now().strftime('%Y%m%d')
            logger.info(f"未提供结束日期，使用系统当前日期: {end_date}")
        
        # 获取交易日历
        cal = pro.trade_cal(exchange=exchange, start_date=start_date, end_date=end_date)
        
        # 筛选交易日
        trade_days = cal[cal.is_open == 1]
        
        if trade_days.empty:
            logger.error(f"未找到交易所 {exchange} 的交易日期数据")
            return None
        
        # 获取最早的交易日
        start_date = trade_days['cal_date'].min()
        logger.info(f"交易所 {exchange} 的开始日期: {start_date}")
        
        return trade_days
    except Exception as e:
        logger.error(f"获取交易所 {exchange} 开始日期失败: {e}")
        raise

def write_exchange_dates_to_file(date_list, file_path):
    """
    将期货交易所日期列表写入文件
    
    Args:
        date_list: 期货交易所日期列表
        file_path: 写入文件的完整文件名
    
    Returns:
        bool: 写入是否成功
    """
    try:
        # 确保date_list是列表类型
        if not isinstance(date_list, list):
            logger.error(f"date_list参数必须是列表类型，当前类型: {type(date_list).__name__}")
            return False
        
        # 写入文件
        with open(file_path, 'w', encoding='utf-8') as f:
            for date in date_list:
                f.write(f"{date}\n")
        
        logger.info(f"成功将{len(date_list)}个日期写入文件: {file_path}")
        return True
    except FileNotFoundError:
        # 尝试创建目录
        directory = os.path.dirname(file_path)
        if directory and not os.path.exists(directory):
            try:
                os.makedirs(directory)
                logger.info(f"创建目录: {directory}")
                # 重新尝试写入
                return write_exchange_dates_to_file(date_list, file_path)
            except Exception as e:
                logger.error(f"创建目录失败: {e}")
        logger.error(f"文件路径不存在: {file_path}")
        return False
    except Exception as e:
        logger.error(f"写入日期列表到文件时出错: {e}")
        return False

def main():
    """
    主函数
    """
    # 创建参数解析器
    parser = argparse.ArgumentParser(
        description='获取交易所日期信息并支持保存到文件',
        epilog='示例:\n  python Get_Work_Date_Info.py -x SHFE -f ./shfe_open_day.csv  # 将上海期货交易所交易日历保存到shfe_open_day.csv'
    )
    
    # 添加命令行参数
    parser.add_argument('--start_date', '-s', type=str, default='19990504', 
                        help='开始日期，格式: YYYYMMDD，默认: 19990504')
    parser.add_argument('--end_date', '-e', type=str, default=None, 
                        help='结束日期，格式: YYYYMMDD，默认: 当前系统日期')
    parser.add_argument('--save_file', '-f', type=str, default=None, 
                        help='保存交易日历的文件路径')
    parser.add_argument('--exchange', '-x', type=str, default='SHFE', 
                        help='交易所代码，默认: SHFE (上海期货交易所)')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 如果未提供结束日期，使用当前系统日期
    if args.end_date is None:
        args.end_date = datetime.datetime.now().strftime('%Y%m%d')
        logger.info(f"未提供结束日期，使用系统当前日期: {args.end_date}")
    
    logger.info(f"命令行参数: 开始日期={args.start_date}, 结束日期={args.end_date}, ")
    logger.info(f"          交易所={args.exchange}, 保存文件={args.save_file}")
    
    try:
        # 获取token
        token = get_tushare_token()
        
        # 初始化tushare
        pro = init_tushare(token)
        
        # 交易所名称映射
        exchange_names = {
            'SSE': '上海证券交易所',
            'SZSE': '深圳证券交易所',
            'CFFEX': '中国金融期货交易所',
            'SHFE': '上海期货交易所',
            'CZCE': '郑州商品交易所',
            'DCE': '大连商品交易所',
            'INE': '上海国际能源交易中心'
        }
        
        # 如果指定了单个交易所
        if args.exchange:
            exchange_code = args.exchange.upper()
            try:
                # 获取交易日历
                cal = pro.trade_cal(exchange=exchange_code, start_date=args.start_date, end_date=args.end_date)
                trade_days = cal[cal.is_open == 1]
                date_list = trade_days['cal_date'].tolist()
                
                # 显示信息
                exchange_name = exchange_names.get(exchange_code, exchange_code)
                print(f"\n{exchange_name}({exchange_code}) 交易日历信息:")
                print(f"- 日期范围: {args.start_date} 至 {args.end_date}")
                print(f"- 交易日数量: {len(date_list)}")
                if date_list:
                    print(f"- 最早交易日: {date_list[0]}")
                    print(f"- 最晚交易日: {date_list[-1]}")
                
                # 如果指定了保存文件
                if args.save_file:
                    success = write_exchange_dates_to_file(date_list, args.save_file)
                    if success:
                        print(f"\n成功将交易日历保存至: {args.save_file}")
            except Exception as e:
                logger.error(f"获取 {exchange_code} 数据失败: {e}")
        else:
            # 默认显示所有交易所信息
            for exchange_code, exchange_name in exchange_names.items():
                try:
                    start_date = get_exchange_start_date(pro, exchange_code)
                    if start_date:
                        print(f"{exchange_name}({exchange_code}) 开始日期: {start_date}")
                except Exception as e:
                    logger.warning(f"获取 {exchange_name} 数据失败: {e}")
                    continue
                
    except Exception as e:
        logger.error(f"程序运行出错: {e}")

if __name__ == "__main__":
    main()
