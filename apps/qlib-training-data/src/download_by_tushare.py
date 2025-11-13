import tushare as ts
import pandas as pd

# 设置 token（替换为你的实际 token）
ts.set_token("80c4b7da4069bc6ef309b653dc5c6e421c8618b763a2772eb55fd33f")
pro = ts.pro_api()

# 螺纹钢主力合约代码为 "RB9999.SHF"（上期所）
df = pro.fut_daily(
    ts_code="RB9999.SHF",
    start_date="20130101",
    end_date="20251112"
)

# 转换日期格式并排序
df["trade_date"] = pd.to_datetime(df["trade_date"])
df = df.sort_values("trade_date")

# 保存为 CSV，使用 \n 作为行结束符以确保跨平台兼容性
df.to_csv("rb_main_daily_tushare.csv", index=False, encoding="utf-8-sig", line_terminator='\n')
print(f"数据已保存，共 {len(df)} 条记录")