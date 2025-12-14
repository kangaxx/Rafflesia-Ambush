import time
import akshare as ak

print("获取商品期货行情, 3秒钟一次")
subscribe = ak.futures_foreign_subscribe()
print(subscribe)
while True:
    time.sleep(3)
    df = ak.futures_foreign_realtime(subscribe)
    print(df)
