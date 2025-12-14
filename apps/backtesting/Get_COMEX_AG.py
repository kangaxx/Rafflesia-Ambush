import akshare as ak
import argparse
from datetime import datetime, timedelta
import sys

def get_comex_silver_data(symbol: str, start_date: str, end_date: str) -> None:
    """
    使用akshare接口获取COMEX白银期货数据并打印
    
    Args:
        symbol: 期货产品代码
        start_date: 开始时间，格式为'YYYYMMDD'
        end_date: 结束时间，格式为'YYYYMMDD'
    """
    try:
        # 验证日期格式
        datetime.strptime(start_date, '%Y%m%d')
        datetime.strptime(end_date, '%Y%m%d')
        
        print(f"正在获取COMEX白银期货数据...")
        print(f"产品代码: {symbol}")
        print(f"开始时间: {start_date}")
        print(f"结束时间: {end_date}")
        
        # 使用akshare获取期货数据
        # 注意：akshare的接口可能会有变化，请根据最新文档调整
        # 这里使用futures_international_hist接口，这是一个常见的国际期货数据接口
        df = ak.futures_international_hist(symbol=symbol, start_date=start_date, end_date=end_date)
        
        print("\n获取数据成功！")
        print(f"数据总行数: {len(df)}")
        print("\n数据预览:")
        print(df.head())
        print("\n完整数据:")
        print(df)
        
    except ValueError as e:
        print(f"日期格式错误: {e}", file=sys.stderr)
        print("请使用'YYYYMMDD'格式的日期，例如'20230101'", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"获取数据失败: {e}", file=sys.stderr)
        print("请检查产品代码是否正确，或者akshare接口是否需要更新", file=sys.stderr)
        sys.exit(1)

def main():
    # 获取默认日期值
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    default_start_date = yesterday.strftime('%Y%m%d')
    default_end_date = today.strftime('%Y%m%d')
    
    parser = argparse.ArgumentParser(
        description="使用akshare接口获取COMEX白银期货数据"
    )
    parser.add_argument(
        "symbol",
        help="期货产品代码，例如'SI'代表白银"
    )
    parser.add_argument(
        "start_date",
        nargs='?',
        default=default_start_date,
        help=f"开始时间，格式为'YYYYMMDD'，默认为前一天 ({default_start_date})"
    )
    parser.add_argument(
        "end_date",
        nargs='?',
        default=default_end_date,
        help=f"结束时间，格式为'YYYYMMDD'，默认为今天 ({default_end_date})"
    )
    
    args = parser.parse_args()
    get_comex_silver_data(args.symbol, args.start_date, args.end_date)

if __name__ == "__main__":
    main()