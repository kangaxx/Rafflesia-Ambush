from vnpy.data.manager import DataManager
from vnpy.trader.constant import Exchange, Interval

# 初始化数据管理器时指定数据接口为 CTP
manager = DataManager(datafeed_name="CTP")

# 下载螺纹钢主力合约数据
manager.download_bar_data(
    symbol="RB",
    exchange=Exchange.SHFE,
    interval=Interval.DAY,
    start="2013-01-01",
    end="2025-11-12"
)