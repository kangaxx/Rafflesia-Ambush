import time
import akshare as ak

print("开始接收实时行情, 每 3 秒刷新一次")
subscribe_list = ak.futures_foreign_commodity_subscribe_exchange_symbol()  
print(f"订阅列表: {subscribe_list}")
# 其中 subscribe_list 为列表
while True:
    time.sleep(3)
    futures_foreign_commodity_realtime_df = ak.futures_foreign_commodity_realtime(symbol=subscribe_list)
    print(futures_foreign_commodity_realtime_df)
