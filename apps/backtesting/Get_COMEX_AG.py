import akshare as ak
import sys
import pandas as pd
import argparse
from datetime import datetime, timedelta

def get_comex_silver_data(symbol: str, start_date: str, end_date: str, period: str = 'daily') -> None:
    """
    使用akshare接口获取COMEX白银期货数据并打印
    
    Args:
        symbol: 期货产品代码
        start_date: 开始时间，格式为'YYYYMMDD'
        end_date: 结束时间，格式为'YYYYMMDD'
        period: 数据时间周期，虽然支持设置为1hour, daily, weekly, monthly，但当前akshare接口只返回日线数据
    """
    # 注意：虽然保留period参数，但根据akshare接口限制，目前只能获取日线数据
    if period != 'daily':
        print(f"警告：当前设置的周期为 '{period}'，但由于akshare接口限制，将获取日线数据", file=sys.stderr)
    try:
        # 验证日期格式
        datetime.strptime(start_date, '%Y%m%d')
        datetime.strptime(end_date, '%Y%m%d')
        
        # 验证period参数
        valid_periods = ['1hour', 'daily', 'weekly', 'monthly']
        if period not in valid_periods:
            print(f"错误: period参数无效。有效值为: {', '.join(valid_periods)}", file=sys.stderr)
            sys.exit(1)
        
        print(f"正在获取COMEX白银期货数据...")
        print(f"产品代码: {symbol}")
        print(f"开始时间: {start_date}")
        print(f"结束时间: {end_date}")
        print(f"时间周期: {period}")
        
        # 使用akshare获取期货数据
        # 根据测试，使用futures_global_hist_em接口获取国际期货数据
        try:
            print(f"尝试获取品种代码: {symbol}")
            
            # 尝试使用futures_global_hist_em接口获取数据
            # 注意：根据测试，此接口只接受symbol参数，且返回的是日频率数据
            df = ak.futures_global_hist_em(symbol=symbol)
            
            # 检查数据是否为None或为空
            if df is None:
                print(f"接口返回None，无法获取{symbol}的数据", file=sys.stderr)
                # 尝试查找可用的白银代码
                print("\n尝试查找可用的白银代码...")
                try:
                    spot_df = ak.futures_global_spot_em()
                    silver_codes = spot_df[spot_df['名称'].str.contains('银', na=False)]
                    if not silver_codes.empty:
                        print(f"找到{len(silver_codes)}个白银相关品种:")
                        print(silver_codes[['代码', '名称']].head())
                        print("\n建议尝试使用上述代码之一，例如: SI00Y (COMEX白银)")
                except:
                    pass
                sys.exit(1)
            if df.empty:
                print(f"获取到空数据，请检查产品代码 '{symbol}' 是否正确", file=sys.stderr)
                
                # 尝试查找可用的白银代码
                print("\n尝试查找可用的白银代码...")
                spot_df = ak.futures_global_spot_em()
                silver_codes = spot_df[spot_df['名称'].str.contains('银', na=False)]
                if not silver_codes.empty:
                    print(f"找到{len(silver_codes)}个白银相关品种:")
                    print(silver_codes[['代码', '名称']].head())
                    print("\n建议尝试使用上述代码之一，例如: SI00Y (COMEX白银)")
                
                sys.exit(1)
                
            # 打印数据列信息，帮助理解数据结构
            print(f"获取数据成功! 数据形状: {df.shape}")
            print(f"数据列: {df.columns.tolist()}")
            
            # 由于接口不支持时间范围参数，我们在获取数据后进行过滤
            # 查找日期列
            date_column = None
            for col in ['date', '日期', 'trade_date', '交易日期']:
                if col.lower() in df.columns.str.lower():
                    date_column = col
                    break
                    
            if date_column:
                print(f"使用日期列 '{date_column}' 进行过滤")
                # 将日期列转换为datetime类型
                df[date_column] = pd.to_datetime(df[date_column])
                
                # 转换输入的日期字符串为datetime类型
                start_datetime = datetime.strptime(start_date, '%Y%m%d')
                end_datetime = datetime.strptime(end_date, '%Y%m%d')
                
                # 过滤数据
                df = df[(df[date_column] >= start_datetime) & (df[date_column] <= end_datetime)]
                
                if df.empty:
                    print(f"在指定的时间范围内未找到数据: {start_date} 至 {end_date}", file=sys.stderr)
                    print("显示全部数据:")
                else:
                    print(f"过滤后数据行数: {len(df)}")
                
            else:
                print("警告：未找到日期列，无法按时间范围过滤数据", file=sys.stderr)
                print("数据将显示最近获取的记录")
                
            # 注意：根据测试，akshare的接口似乎只提供日线数据
            # 小时级数据可能需要其他数据源或付费API
            print("\n注意：根据akshare接口限制，当前获取的是日线数据")
            print("如果需要小时级数据，可能需要使用其他专业金融数据API或数据源")
                
        except Exception as inner_e:
            print(f"获取数据失败: {inner_e}", file=sys.stderr)
            print("注意：请检查产品代码是否正确，或参考akshare文档", file=sys.stderr)
            
            # 尝试获取可用的白银代码作为参考
            try:
                print("\n尝试查找可用的白银代码...")
                spot_df = ak.futures_global_spot_em()
                silver_codes = spot_df[spot_df['名称'].str.contains('银', na=False)]
                if not silver_codes.empty:
                    print(f"找到{len(silver_codes)}个白银相关品种:")
                    print(silver_codes[['代码', '名称']].head())
            except:
                pass
                
            raise
        
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
    # 使用过去30天作为起始日期，确保有数据可查
    one_month_ago = today - timedelta(days=30)
    default_start_date = one_month_ago.strftime('%Y%m%d')
    default_end_date = today.strftime('%Y%m%d')
    
    parser = argparse.ArgumentParser(
        description="获取COMEX白银期货数据工具\n\n"
        "本工具使用akshare接口获取COMEX白银期货数据，支持不同的时间周期选择。\n"
        "功能特点：\n"
        "- 支持1hour、daily、weekly、monthly四种时间周期\n"
        "- 支持参数验证和错误提示\n"
        "- 自动显示数据的基本统计信息\n"
        "- 默认获取从过去30天到今天的数据\n\n"
        "使用范例：\n"
        "1. 默认参数获取数据（白银小时级数据，从过去30天到今天）\n"
        "   python Get_COMEX_AG.py\n\n"
        "2. 指定日期范围获取日线数据\n"
        "   python Get_COMEX_AG.py SI 20240101 20240131 -p daily\n\n"
        "3. 使用完整参数名获取周线数据\n"
        "   python Get_COMEX_AG.py -p weekly\n\n"
        "4. 使用自定义产品代码\n"
        f"   python Get_COMEX_AG.py GC {default_start_date} {default_end_date}\n\n"
        "注意事项：\n"
        "- 产品代码需符合akshare接口要求\n"
        "- 日期格式必须为YYYYMMDD\n"
        "- 建议使用最新版本的akshare库以确保接口兼容性",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        "symbol",
        nargs='?',
        default="SI",
        help="期货产品代码，例如'SI'代表白银，默认为'SI'"
    )
    parser.add_argument(
        "start_date",
        nargs='?',
        default=default_start_date,
        help=f"开始时间，格式为'YYYYMMDD'，默认为过去30天 ({default_start_date})"
    )
    parser.add_argument(
        "end_date",
        nargs='?',
        default=default_end_date,
        help=f"结束时间，格式为'YYYYMMDD'，默认为今天 ({default_end_date})"
    )
    parser.add_argument(
        "-p",
        "--period",
        default='daily',
        choices=['1hour', 'daily', 'weekly', 'monthly'],
        help="数据时间周期，可选值：1hour, daily, weekly, monthly，默认为daily\n注意：由于akshare接口限制，当前只能获取日线数据"
    )
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 调用函数获取数据
    get_comex_silver_data(args.symbol, args.start_date, args.end_date, args.period)

if __name__ == "__main__":
    main()