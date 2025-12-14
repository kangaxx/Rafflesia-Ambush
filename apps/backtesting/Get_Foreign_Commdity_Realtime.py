import time
import akshare as ak
import pandas as pd

print("开始接收实时行情, 每 3 秒刷新一次")

# 添加异常处理获取订阅列表
try:
    subscribe_list = ak.futures_foreign_commodity_subscribe_exchange_symbol()  
    print(f"订阅列表: {subscribe_list}")
    print(f"订阅品种数量: {len(subscribe_list)}")
except Exception as e:
    print(f"获取订阅列表失败: {e}")
    # 使用默认的关键品种列表作为备用
    subscribe_list = ['SI', 'GC', 'CL', 'NG', 'HG', 'W', 'C', 'S', 'BO']
    print(f"使用备用品种列表: {subscribe_list}")

# 主循环添加异常处理
while True:
    try:
        time.sleep(3)
        print(f"\n[{time.strftime('%H:%M:%S')}] 获取实时行情数据...")
        
        # 分批获取数据，避免一次性请求过多品种导致的问题
        batch_size = 5
        all_data = None
        
        for i in range(0, len(subscribe_list), batch_size):
            batch = subscribe_list[i:i+batch_size]
            try:
                batch_data = ak.futures_foreign_commodity_realtime(symbol=batch)
                if all_data is None:
                    all_data = batch_data
                else:
                    # 安全地合并数据
                    try:
                        all_data = pd.concat([all_data, batch_data], ignore_index=True)
                    except:
                        # 如果合并失败，就单独处理这一批数据
                        print(f"\n批次 {i//batch_size + 1} 数据:")
                        print(batch_data)
            except Exception as batch_error:
                print(f"获取批次 {batch} 数据失败: {batch_error}")
                
        # 如果成功合并了所有数据，则打印完整数据
        if all_data is not None:
            print(f"\n成功获取 {len(all_data)} 条实时行情数据")
            print(all_data)
            
    except Exception as e:
        print(f"获取实时行情失败: {e}")
        # 继续下一次循环，不会因为单次错误而终止程序
        continue
