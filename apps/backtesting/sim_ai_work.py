import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

"""
K线数据预测模块

该模块提供了基于历史K线数据预测未来三天K线数据的功能，主要用于回测系统。

主要接口:
- predict: 核心外部接口，返回预测结果
- predict_next_three_days: 包含文件保存功能的预测函数（兼容性保留）

使用示例:
    # 在回测程序中导入并使用
    from sim_ai_work import predict
    
    # 传入历史K线数据（DataFrame格式）
    historical_data = pd.read_csv('historical_kline.csv')
    
    # 获取预测结果（默认返回DataFrame格式）
    predicted_df = predict(historical_data)
    
    # 或者获取字典列表格式的结果
    predicted_dict_list = predict(historical_data, as_dict=True)
"""

def _predict_core(historical_data):
    """
    内部核心预测函数，执行实际的预测逻辑
    
    Args:
        historical_data (dict or pd.DataFrame): 包含日期、开盘价、最高价、最低价、收盘价、交易量、持仓量字段的K线数据
    
    Returns:
        pd.DataFrame: 预测的未来三天K线数据
    """
    # 转换为DataFrame格式
    if isinstance(historical_data, dict):
        df = pd.DataFrame([historical_data])
    elif isinstance(historical_data, list):
        df = pd.DataFrame(historical_data)
    else:
        df = historical_data.copy()
    
    # 定义字段映射，支持常见的字段名变体
    field_mapping = {
        'date': ['date', 'trade_date', 'datetime', 'time'],
        'open': ['open', 'open_price'],
        'high': ['high', 'high_price'],
        'low': ['low', 'low_price'],
        'close': ['close', 'close_price', 'price'],
        'vol': ['vol', 'volume', 'amount'],
        'oi': ['oi', 'open_interest', 'position']
    }
    
    # 执行字段映射
    mapped_columns = {}
    for standard_col, possible_cols in field_mapping.items():
        found = False
        for possible_col in possible_cols:
            if possible_col in df.columns:
                mapped_columns[standard_col] = possible_col
                found = True
                break
        if not found:
            # 尝试不区分大小写的匹配
            for possible_col in possible_cols:
                for actual_col in df.columns:
                    if actual_col.lower() == possible_col.lower():
                        mapped_columns[standard_col] = actual_col
                        found = True
                        break
                if found:
                    break
        if not found:
            raise ValueError(f"找不到与 '{standard_col}' 对应的字段。可用字段: {list(df.columns)}")
    
    # 创建标准化的DataFrame
    df_standard = pd.DataFrame()
    for standard_col, actual_col in mapped_columns.items():
        df_standard[standard_col] = df[actual_col]
    
    df = df_standard
    
    # 转换日期格式
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
    
    # 计算基本统计数据作为预测基础
    last_close = df['close'].iloc[-1]
    last_open = df['open'].iloc[-1]
    last_high = df['high'].iloc[-1]
    last_low = df['low'].iloc[-1]
    last_vol = df['vol'].iloc[-1]
    last_oi = df['oi'].iloc[-1]
    
    # 计算价格波动率
    if len(df) > 1:
        df['returns'] = df['close'].pct_change()
        volatility = df['returns'].std() * np.sqrt(252)  # 年化波动率
        price_std = volatility / np.sqrt(252)  # 日波动率
    else:
        price_std = 0.01  # 默认1%的日波动率
    
    # 预测未来三天
    predictions = []
    last_date = df['date'].iloc[-1]
    
    for i in range(1, 4):
        # 生成日期
        next_date = last_date + timedelta(days=i)
        
        # 基于前一天的收盘价预测当天的开盘价（小幅随机变动）
        if i == 1:
            prev_close = last_close
        else:
            prev_close = predictions[i-2]['close']
        
        # 开盘价在收盘价基础上有小幅变动
        open_change = random.uniform(-price_std/2, price_std/2)
        predicted_open = prev_close * (1 + open_change)
        
        # 收盘价在开盘价基础上有随机变动，但保持适度趋势
        close_change = random.uniform(-price_std, price_std)
        predicted_close = predicted_open * (1 + close_change)
        
        # 计算最高价和最低价（基于开盘价和收盘价）
        if predicted_open > predicted_close:
            predicted_high = max(predicted_open, predicted_close) * (1 + random.uniform(0, 0.02))
            predicted_low = min(predicted_open, predicted_close) * (1 - random.uniform(0, 0.02))
        else:
            predicted_high = max(predicted_open, predicted_close) * (1 + random.uniform(0, 0.02))
            predicted_low = min(predicted_open, predicted_close) * (1 - random.uniform(0, 0.02))
        
        # 交易量在之前交易量基础上随机变动
        vol_change = random.uniform(-0.1, 0.1)
        predicted_vol = max(0, last_vol * (1 + vol_change))
        
        # 持仓量在之前持仓量基础上随机变动
        oi_change = random.uniform(-0.05, 0.05)
        predicted_oi = max(0, last_oi * (1 + oi_change))
        
        # 添加预测结果
        predictions.append({
            'date': next_date,
            'open': predicted_open,
            'high': predicted_high,
            'low': predicted_low,
            'close': predicted_close,
            'vol': predicted_vol,
            'oi': predicted_oi
        })
    
    # 转换为DataFrame
    pred_df = pd.DataFrame(predictions)
    
    # 保留原始数据的精度
    for col in ['open', 'high', 'low', 'close']:
        # 获取原始数据的精度
        if col in df.columns:
            # 检查是否有小数位
            sample_data = df[col].dropna()
            if len(sample_data) > 0:
                # 尝试检测小数位数
                sample_values = sample_data.astype(float)
                decimal_places = max(0, int(-np.log10(min(abs(sample_values - sample_values.astype(int)))))) if len(sample_values) > 0 and (sample_values - sample_values.astype(int)).any() else 0
                pred_df[col] = pred_df[col].round(decimal_places)
    
    # 交易量和持仓量保持整数
    pred_df['vol'] = pred_df['vol'].round().astype(int)
    pred_df['oi'] = pred_df['oi'].round().astype(int)
    
    return pred_df

def predict(historical_data, as_dict=False):
    """
    外部接口函数，供回测程序直接调用
    
    Args:
        historical_data (dict or pd.DataFrame or list): 包含日期、开盘价、最高价、最低价、收盘价、交易量、持仓量字段的K线数据
        as_dict (bool): 是否以字典列表形式返回结果，默认为False（返回DataFrame）
    
    Returns:
        pd.DataFrame or list[dict]: 预测的未来三天K线数据
    """
    # 调用核心预测函数
    pred_df = _predict_core(historical_data)
    
    # 根据参数决定返回格式
    if as_dict:
        return pred_df.to_dict('records')
    return pred_df

def predict_next_three_days(historical_data, output_file='predictor.csv'):
    """
    基于历史K线数据预测未来三天的K线数据
    
    Args:
        historical_data (dict or pd.DataFrame): 包含日期、开盘价、最高价、最低价、收盘价、交易量、持仓量字段的K线数据
        output_file (str): 保存预测结果的CSV文件名，默认为'predictor.csv'
    
    Returns:
        pd.DataFrame: 预测的未来三天K线数据
    """
    # 转换为DataFrame格式
    if isinstance(historical_data, dict):
        df = pd.DataFrame([historical_data])
    elif isinstance(historical_data, list):
        df = pd.DataFrame(historical_data)
    else:
        df = historical_data.copy()
    
    # 定义字段映射，支持常见的字段名变体
    field_mapping = {
        'date': ['date', 'trade_date', 'datetime', 'time'],
        'open': ['open', 'open_price'],
        'high': ['high', 'high_price'],
        'low': ['low', 'low_price'],
        'close': ['close', 'close_price', 'price'],
        'vol': ['vol', 'volume', 'amount'],
        'oi': ['oi', 'open_interest', 'position']
    }
    
    # 执行字段映射
    mapped_columns = {}
    for standard_col, possible_cols in field_mapping.items():
        found = False
        for possible_col in possible_cols:
            if possible_col in df.columns:
                mapped_columns[standard_col] = possible_col
                found = True
                break
        if not found:
            # 尝试不区分大小写的匹配
            for possible_col in possible_cols:
                for actual_col in df.columns:
                    if actual_col.lower() == possible_col.lower():
                        mapped_columns[standard_col] = actual_col
                        found = True
                        break
                if found:
                    break
        if not found:
            raise ValueError(f"找不到与 '{standard_col}' 对应的字段。可用字段: {list(df.columns)}")
    
    # 创建标准化的DataFrame
    df_standard = pd.DataFrame()
    for standard_col, actual_col in mapped_columns.items():
        df_standard[standard_col] = df[actual_col]
    
    df = df_standard
    
    # 转换日期格式
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
    
    # 计算基本统计数据作为预测基础
    last_close = df['close'].iloc[-1]
    last_open = df['open'].iloc[-1]
    last_high = df['high'].iloc[-1]
    last_low = df['low'].iloc[-1]
    last_vol = df['vol'].iloc[-1]
    last_oi = df['oi'].iloc[-1]
    
    # 计算价格波动率
    if len(df) > 1:
        df['returns'] = df['close'].pct_change()
        volatility = df['returns'].std() * np.sqrt(252)  # 年化波动率
        price_std = volatility / np.sqrt(252)  # 日波动率
    else:
        price_std = 0.01  # 默认1%的日波动率
    
    # 预测未来三天
    predictions = []
    last_date = df['date'].iloc[-1]
    
    for i in range(1, 4):
        # 生成日期
        next_date = last_date + timedelta(days=i)
        
        # 基于前一天的收盘价预测当天的开盘价（小幅随机变动）
        if i == 1:
            prev_close = last_close
        else:
            prev_close = predictions[i-2]['close']
        
        # 开盘价在收盘价基础上有小幅变动
        open_change = random.uniform(-price_std/2, price_std/2)
        predicted_open = prev_close * (1 + open_change)
        
        # 收盘价在开盘价基础上有随机变动，但保持适度趋势
        close_change = random.uniform(-price_std, price_std)
        predicted_close = predicted_open * (1 + close_change)
        
        # 计算最高价和最低价（基于开盘价和收盘价）
        if predicted_open > predicted_close:
            predicted_high = max(predicted_open, predicted_close) * (1 + random.uniform(0, 0.02))
            predicted_low = min(predicted_open, predicted_close) * (1 - random.uniform(0, 0.02))
        else:
            predicted_high = max(predicted_open, predicted_close) * (1 + random.uniform(0, 0.02))
            predicted_low = min(predicted_open, predicted_close) * (1 - random.uniform(0, 0.02))
        
        # 交易量在之前交易量基础上随机变动
        vol_change = random.uniform(-0.1, 0.1)
        predicted_vol = max(0, last_vol * (1 + vol_change))
        
        # 持仓量在之前持仓量基础上随机变动
        oi_change = random.uniform(-0.05, 0.05)
        predicted_oi = max(0, last_oi * (1 + oi_change))
        
        # 添加预测结果
        predictions.append({
            'date': next_date,
            'open': predicted_open,
            'high': predicted_high,
            'low': predicted_low,
            'close': predicted_close,
            'vol': predicted_vol,
            'oi': predicted_oi
        })
    
    # 转换为DataFrame
    pred_df = pd.DataFrame(predictions)
    
    # 保留原始数据的精度
    for col in ['open', 'high', 'low', 'close']:
        # 获取原始数据的精度
        if col in df.columns:
            # 检查是否有小数位
            sample_data = df[col].dropna()
            if len(sample_data) > 0:
                # 尝试检测小数位数
                sample_values = sample_data.astype(float)
                decimal_places = max(0, int(-np.log10(min(abs(sample_values - sample_values.astype(int)))))) if len(sample_values) > 0 and (sample_values - sample_values.astype(int)).any() else 0
                pred_df[col] = pred_df[col].round(decimal_places)
    
    # 交易量和持仓量保持整数
    pred_df['vol'] = pred_df['vol'].round().astype(int)
    pred_df['oi'] = pred_df['oi'].round().astype(int)
    
    # 保存结果到CSV文件
    pred_df.to_csv(output_file, index=False)
    print(f"预测结果已保存到: {output_file}")
    
    return pred_df

def main():
    """
    主函数，提供示例用法
    """
    # 示例1：单条K线数据输入
    sample_kline = {
        'date': '2024-01-15',
        'open': 5200.0,
        'high': 5250.0,
        'low': 5180.0,
        'close': 5220.0,
        'vol': 12500,
        'oi': 38000
    }
    
    print("示例1：基于单条K线数据的预测")
    print("输入数据:")
    print(pd.DataFrame([sample_kline]))
    
    # 示例：使用新的predict接口
    pred_result = predict(sample_kline)
    print("\n预测结果:")
    print(pred_result)
    
    # 示例2：从CSV文件读取数据进行预测
    print("\n" + "="*60 + "\n")
    print("示例2：从CSV文件读取数据进行预测")
    
    # 尝试读取数据目录下的文件
    csv_file = "Data/RB9999.csv"  # 使用相对路径
    
    try:
        # 尝试读取文件的最后5行作为历史数据
        historical_df = pd.read_csv(csv_file)
        print(f"成功读取文件: {csv_file}")
        print(f"文件总行数: {len(historical_df)}")
        
        # 显示最近的几条数据
        recent_data = historical_df.tail(min(5, len(historical_df)))
        print("\n最近的K线数据:")
        print(recent_data)
        
        # 进行预测
        file_pred_result = predict(historical_df)
        print("\n预测结果:")
        print(file_pred_result)
        
    except FileNotFoundError:
        print(f"文件未找到: {csv_file}")
    except Exception as e:
        print(f"读取文件时出错: {e}")

if __name__ == "__main__":
    # 设置随机种子以确保结果可复现
    random.seed(42)
    np.random.seed(42)
    main()
