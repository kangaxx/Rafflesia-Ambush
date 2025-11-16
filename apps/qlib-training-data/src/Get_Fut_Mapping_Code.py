import tushare as ts
import pandas as pd
import argparse
import os
from datetime import datetime

# 设置 token（替换为你的实际 token）
TOKEN = "80c4b7da4069bc6ef309b653dc5c6e421c8618b763a2772eb55fd33f"
# 打印帮助信息的函数
def print_help():
    help_text = """
    用法: python Get_Fut_Mapping_Code.py --ts_code <期货合约代码> [--output_dir <输出目录>] [--token <Tushare Token>]
    
    参数:
    --ts_code: 期货合约代码，例如 'CU.SHF' (必需)
    --output_dir: 输出文件保存路径，默认为 '~/.tushare'
    --token: Tushare Pro接口的token，如果不提供则使用环境变量或配置文件中的token
    
    示例:
    python Get_Fut_Mapping_Code.py --ts_code CU.SHF
    python Get_Fut_Mapping_Code.py --ts_code CU.SHF --output_dir ~/data/futures --token your_token_here
    """
    print(help_text)

def remove_suffix(text, suffix):
    """
    移除字符串末尾的后缀
    """
    if text.endswith(suffix):
        return text[:-len(suffix)]
    return text

def get_future_mapping(ts_code):
    """
    使用pro.fut_mapping函数获取指定期货代码的映射信息
    
    参数:
    ts_code: 期货合约代码，例如 'CU2303.SHF'
    
    返回:
    pandas DataFrame: 包含期货代码映射信息的数据框
    """
    # 初始化tushare
    ts.set_token(TOKEN)
    pro = ts.pro_api()
    
    try:
        # 打印调试信息：API调用参数
        print(f"[调试] 调用 fut_mapping API 参数: ts_code={ts_code}")
        
        # 使用fut_mapping接口获取期货映射信息
        df = pro.fut_mapping(
            ts_code=ts_code
        )
        
        # 打印调试信息：获取到的数据
        print(f"[调试] fut_mapping API 返回数据形状: {df.shape}")
        if not df.empty:
            print("[调试] 返回数据的列名:", df.columns.tolist())
            print("[调试] 返回数据前5行:")
            print(df.head())
            
            # 移除数据中的.SHF后缀
            print("[调试] 开始移除数据中的.SHF后缀...")
            # 遍历所有字符串类型的列
            for col in df.columns:
                if df[col].dtype == 'object':
                    # 移除该列中所有字符串末尾的.SHF
                    df[col] = df[col].apply(lambda x: remove_suffix(x, '.SHF') if isinstance(x, str) else x)
            print("[调试] 移除.SHF后缀完成")
            print("[调试] 处理后前5行数据:")
            print(df.head())
        else:
            print(f"[调试] 未获取到 {ts_code} 的映射数据")
        
        return df
    
    except Exception as e:
        print(f"获取期货映射信息时出错: {e}")
        return pd.DataFrame()

def expand_user_path(path):
    """
    处理路径中的~符号，扩展为用户主目录
    """
    if path.startswith('~'):
        home_dir = os.path.expanduser('~')
        return os.path.join(home_dir, path[2:]) if path.startswith('~/') else home_dir
    return path

def save_to_csv(df, ts_code, output_dir):
    """
    保存期货代码映射表到CSV文件
    
    参数:
    df: 期货代码映射数据
    ts_code: 期货合约代码，用于文件名
    output_dir: 输出目录路径
    """
    if df.empty:
        print(f"没有 {ts_code} 的数据可保存")
        return
    
    # 打印调试信息：保存前的数据统计
    print("[调试] 保存前的数据统计:")
    print(f"[调试] 数据形状: {df.shape}")
    print(f"[调试] 数据类型:")
    print(df.dtypes)
    
    # 检查是否有缺失值
    print("[调试] 缺失值统计:")
    print(df.isnull().sum())
    
    # 处理路径中的~符号
    output_dir = expand_user_path(output_dir)
    print(f"[调试] 扩展后的输出目录: {output_dir}")
    
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    print(f"[调试] 已确保输出目录存在: {output_dir}")
    
    # 移除ts_code中的.SHF后缀用于文件名
    ts_code_no_suffix = remove_suffix(ts_code, '.SHF')
    print(f"[调试] 原始ts_code: {ts_code}, 移除.SHF后的ts_code: {ts_code_no_suffix}")
    
    # 生成文件名：mapping_ + ts_code_no_suffix.csv
    file_name = f"mapping_{ts_code_no_suffix}.csv"
    
    # 组合完整的文件路径
    file_path = os.path.join(output_dir, file_name)
    
    # 打印调试信息：保存参数
    print(f"[调试] 保存文件参数: file_path={file_path}, encoding=utf-8-sig")

    # 倒置df,默认情况下数据是从最后一天往第一天写的
    df_reversed = df[::-1]
    # 保存为CSV，使用UTF-8-SIG编码确保跨平台兼容性
    df_reversed.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"期货代码映射数据已保存到 {file_path}，共 {len(df)} 条记录")
    
    return file_path

def main():
    """
    主函数，处理命令行参数并执行获取期货代码映射表的操作
    """
    # 如果没有提供任何参数，或者输入了参数-h 或者 --h ,显示帮助信息 并退出
    import sys
    if len(sys.argv) == 1 or '-h' in sys.argv or '--h' in sys.argv:
        print_help()
        sys.exit(0)
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description="获取期货代码号映射表")
    # 添加命令行参数
    parser.add_argument('--ts_code', type=str, required=True, help='期货合约代码，例如：CU.SHF')
    parser.add_argument('--output_dir', type=str, default='~/.tushare', help='输出文件保存路径，默认为~/.tushare')
    parser.add_argument('--token', type=str, help='Tushare Pro接口的token')
    
    # 解析命令行参数
    args = parser.parse_args()
    # 打印调试信息：命令行参数
    print("[调试] 命令行参数:")
    print(f"[调试] ts_code: {args.ts_code}")
    print(f"[调试] output_dir: {args.output_dir}")
    print(f"[调试] token: {'已提供' if args.token else '未提供'}")
    
    # 初始化Tushare Pro
    if args.token:
        ts.set_token(args.token)
        print("[调试] Tushare Pro已使用提供的token初始化")
    else:
        print("[调试] 未提供token，使用环境变量或配置文件中的token")
    
    # 调用函数获取期货代码映射表
    print("\n[调试] 开始获取期货代码映射表...")
    
    # 获取指定合约代码的数据
    df = get_future_mapping(args.ts_code)
    
    # 打印结果统计信息
    print("\n[调试] 获取数据完成，结果统计:")
    print(f"[调试] 数据形状: {df.shape}")
    
    if not df.empty:
        # 打印列名信息
        print(f"[调试] 数据列名: {list(df.columns)}")
        
        # 打印前5行数据示例
        print("\n[调试] 前5行数据示例:")
        print(df.head())
    
    # 保存结果到CSV文件
    save_to_csv(df, args.ts_code, args.output_dir)

if __name__ == "__main__":
    main()
