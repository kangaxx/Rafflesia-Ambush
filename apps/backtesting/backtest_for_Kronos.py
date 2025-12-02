"""
基于AI预测的交易回测系统

该程序使用backtrader框架实现了一个基于AI预测的交易策略回测系统。
主要功能包括：
- 读取历史K线数据
- 使用AI预测模块预测未来价格走势
- 基于预测结果执行买入操作
- 实现止损和动态止盈机制
- 输出详细的交易日志和回测统计结果

使用方法：
    python backtest_for_Kronos.py

默认使用的数据文件：./Data/RB9999.csv

策略特点：
- 预测上涨则买入
- 下跌3%止损
- 初始5%止盈，后续使用2%跟踪止损动态调整止盈点
- 单次交易固定10手
- 有持仓时不进行新的买入
- 支持当天平仓和非当天平仓的不同手续费计算

重要参数（可在AI_Prediction_Strategy类的params中调整）：
- stop_loss_pct: 止损百分比，默认0.03（3%）
- take_profit_pct: 初始止盈百分比，默认0.05（5%）
- trailing_stop_pct: 跟踪止损百分比，默认0.02（2%）
- trade_size: 单次交易手数，默认10手
- buy_fee: 买入手续费，默认6.2元
- same_day_sell_fee: 当天卖出手续费，默认6.2元
- margin_rate: 保证金率，默认0.17（17%）
- multiplier: 合约乘数，默认10

回测结果分析：
程序会输出初始资金、最终资金、总收益率、夏普比率、最大回撤等关键指标，并尝试绘制回测结果图表。

依赖模块：
- backtrader: 回测框架
- pandas: 数据处理
- numpy: 数值计算
- sim_ai_work: 自定义的AI预测模块

注意事项：
1. 请确保数据文件格式正确，包含必要的字段：trade_date, open, high, low, close, vol
2. 绘制图表需要tkinter模块支持
3. 回测结果仅供参考，实际交易可能因滑点、流动性等因素而有所不同
"""

import backtrader as bt
import pandas as pd
import numpy as np
from datetime import datetime
import os

# 导入AI预测模块
from sim_ai_work import predict
# 导入文件路径查找函数
from find_file_path import find_file_path

# 设置随机种子以确保结果可复现
import random
random.seed(42)
np.random.seed(42)

class AI_Prediction_Strategy(bt.Strategy):
    """
    基于AI预测的交易策略
    - 预测上涨则买入
    - 下跌3%止损
    - 动态止盈
    - 单次交易10手
    - 有持仓则不进行交易
    """
    params = (
        ('stop_loss_pct', 0.03),  # 3%止损
        ('take_profit_pct', 0.05),  # 初始5%止盈
        ('trailing_stop_pct', 0.02),  # 2%跟踪止损
        ('trade_size', 10),  # 单次交易10手
        ('buy_fee', 6.2),  # 买入手续费6.2元
        ('same_day_sell_fee', 6.2),  # 当天卖出手续费6.2元
        ('margin_rate', 0.17),  # 保证金率17%
        ('multiplier', 10),  # 乘数10
    )
    
    def __init__(self):
        # 基本指标
        self.dataclose = self.datas[0].close
        self.order = None  # 跟踪订单
        self.buyprice = None  # 买入价格
        self.buycomm = None  # 买入佣金
        self.take_profit = None  # 止盈价格
        self.stop_loss = None  # 止损价格
        self.max_price = None  # 持仓期间最高价
        self.buy_date = None  # 买入日期
        self.current_date = None  # 当前日期
    
    def notify_order(self, order):
        # 订单状态通知
        if order.status in [order.Submitted, order.Accepted]:
            # 订单已提交或已接受，不需要操作
            return
        
        # 获取当前日期
        self.current_date = self.datas[0].datetime.date(0)
        
        # 检查订单是否已完成
        if order.status in [order.Completed]:
            if order.isbuy():
                # 买入订单完成
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
                self.max_price = self.buyprice
                self.stop_loss = self.buyprice * (1 - self.params.stop_loss_pct)
                self.take_profit = self.buyprice * (1 + self.params.take_profit_pct)
                self.buy_date = self.current_date  # 记录买入日期
                # 计算实际使用的保证金
                margin_used = self.buyprice * self.params.multiplier * self.params.margin_rate * self.params.trade_size
                
                # 获取当前K线数据
                current_open = self.datas[0].open[0]
                current_high = self.datas[0].high[0]
                current_low = self.datas[0].low[0]
                current_close = self.datas[0].close[0]
                current_volume = self.datas[0].volume[0]
                
                # 打印详细的块状买入信息
                print("\n" + "="*80)
                print(f"{'买入操作信息':^80}")
                print("="*80)
                print(f"交易日期: {self.current_date}")
                print(f"当前K线数据:")
                print(f"  开盘价: {current_open:.2f}")
                print(f"  最高价: {current_high:.2f}")
                print(f"  最低价: {current_low:.2f}")
                print(f"  收盘价: {current_close:.2f}")
                print(f"  成交量: {current_volume:.0f}")
                print(f"交易数据:")
                print(f"  买入价格: {order.executed.price:.2f}")
                print(f"  买入数量: {order.executed.size}手")
                print(f"  成交价格: {self.buyprice:.2f}")
                print(f"  买入手续费: {self.params.buy_fee:.2f}元")
                print(f"  使用保证金: {margin_used:.2f}元")
                print(f"风控设置:")
                print(f"  止损价格: {self.stop_loss:.2f}")
                print(f"  初始止盈: {self.take_profit:.2f}")
                print("="*80 + "\n")
            
            elif order.issell():
                # 卖出订单完成
                # 计算手续费（当天卖出6.2元，非当天卖出0元）
                is_same_day = (self.current_date == self.buy_date)
                sell_fee = self.params.same_day_sell_fee if is_same_day else 0
                # 计算利润（考虑手续费）
                gross_profit = (order.executed.price - self.buyprice) * order.executed.size * self.params.multiplier
                net_profit = gross_profit - self.params.buy_fee - sell_fee
                
                # 获取当前K线数据
                current_open = self.datas[0].open[0]
                current_high = self.datas[0].high[0]
                current_low = self.datas[0].low[0]
                current_close = self.datas[0].close[0]
                current_volume = self.datas[0].volume[0]
                
                # 打印详细的块状卖出信息
                print("\n" + "="*80)
                print(f"{'平仓操作信息':^80}")
                print("="*80)
                print(f"交易日期: {self.current_date}")
                print(f"买入日期: {self.buy_date}")
                print(f"当前K线数据:")
                print(f"  开盘价: {current_open:.2f}")
                print(f"  最高价: {current_high:.2f}")
                print(f"  最低价: {current_low:.2f}")
                print(f"  收盘价: {current_close:.2f}")
                print(f"  成交量: {current_volume:.0f}")
                print(f"交易数据:")
                print(f"  买入价格: {self.buyprice:.2f}")
                print(f"  卖出价格: {order.executed.price:.2f}")
                print(f"  交易数量: {order.executed.size}手")
                print(f"  买入手续费: {self.params.buy_fee:.2f}元")
                print(f"  卖出手续费: {sell_fee:.2f}元")
                print(f"  毛利润: {gross_profit:.2f}元")
                print(f"  净利润: {net_profit:.2f}元")
                print(f"交易类型: {'当天平仓' if is_same_day else '非当天平仓'}")
                print("="*80 + "\n")
            
            # 记录订单完成时间
            self.bar_executed = len(self)
        
        # 订单被取消、拒绝或保证金不足
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            print('\n' + '='*80)
            print(f"{'订单状态信息':^80}")
            print('='*80)
            print(f"交易日期: {self.current_date}")
            print(f"订单状态: 订单被取消/拒绝/保证金不足")
            print('='*80 + '\n')
        
        # 重置订单状态
        self.order = None
    
    def notify_trade(self, trade):
        # 交易状态通知
        if not trade.isclosed:
            return
        
        print(f'交易结果: 毛利润={trade.pnl:.2f}, 净利润={trade.pnlcomm:.2f}')
    
    def next(self):
        # 检查是否有未完成的订单
        if self.order:
            return
        
        # 检查是否已经持仓
        if not self.position:
            # 没有持仓，检查是否买入
            # 使用最近的历史数据进行预测
            lookback_period = 20  # 回看20根K线
            if len(self) >= lookback_period:
                try:
                    # 收集历史数据（使用backtrader的索引方式，-i表示i根前的K线）
                    historical_data = []
                    for i in range(lookback_period):
                        # 获取当前日期
                        dt = self.datas[0].datetime.date(-(lookback_period - i - 1))
                        trade_date = dt.strftime('%Y%m%d')
                        
                        historical_data.append({
                            'trade_date': trade_date,
                            'open': self.datas[0].open[-(lookback_period - i - 1)],
                            'high': self.datas[0].high[-(lookback_period - i - 1)],
                            'low': self.datas[0].low[-(lookback_period - i - 1)],
                            'close': self.datas[0].close[-(lookback_period - i - 1)],
                            'vol': self.datas[0].volume[-(lookback_period - i - 1)],
                            'oi': 0  # 简化处理，不依赖openinterest
                        })
                    
                    # 转换为DataFrame
                    hist_df = pd.DataFrame(historical_data)
                    
                    # 调用AI预测接口
                    prediction = predict(hist_df, as_dict=True)
                    
                    # 检查预测是否上涨（未来三天第一天收盘价高于当前收盘价）
                    current_close = self.dataclose[0]
                    predicted_close = prediction[0]['close']  # 预测的第一天收盘价
                    
                    # 获取当前日期
                    current_date = self.datas[0].datetime.date(0)
                    
                    # 获取当前K线数据
                    current_open = self.datas[0].open[0]
                    current_high = self.datas[0].high[0]
                    current_low = self.datas[0].low[0]
                    current_volume = self.datas[0].volume[0]
                    
                    if predicted_close > current_close:
                        # 预测上涨，买入
                        self.order = self.buy(size=self.params.trade_size)
                        
                        # 打印详细的AI预测信息
                        print("\n" + "="*80)
                        print(f"{'AI预测信号信息':^80}")
                        print("="*80)
                        print(f"交易日期: {current_date}")
                        print(f"当前K线数据:")
                        print(f"  开盘价: {current_open:.2f}")
                        print(f"  最高价: {current_high:.2f}")
                        print(f"  最低价: {current_low:.2f}")
                        print(f"  收盘价: {current_close:.2f}")
                        print(f"  成交量: {current_volume:.0f}")
                        print(f"AI预测数据:")
                        for i, pred in enumerate(prediction):
                            print(f"  预测第{i+1}天: 收盘价={pred['close']:.2f}")
                        print(f"预测结果: AI预测上涨")
                        print(f"操作: 买入信号触发")
                        print(f"买入数量: {self.params.trade_size}手")
                        print("="*80 + "\n")
                except Exception as e:
                    print(f"预测出错: {e}")
        else:
            # 有持仓，检查止损和止盈
            current_price = self.dataclose[0]
            
            # 更新最高价用于动态止盈
            if current_price > self.max_price:
                self.max_price = current_price
                # 动态调整止盈价格
                self.take_profit = self.max_price * (1 + self.params.trailing_stop_pct)
            
            # 检查止损条件
            if current_price < self.stop_loss:
                # 获取当前日期
                current_date = self.datas[0].datetime.date(0)
                
                # 打印详细的止损触发信息
                print("\n" + "="*80)
                print(f"{'止损触发信息':^80}")
                print("="*80)
                print(f"交易日期: {current_date}")
                print(f"买入日期: {self.buy_date}")
                print(f"当前价格: {current_price:.2f}")
                print(f"止损价格: {self.stop_loss:.2f}")
                print(f"最大价格: {self.max_price:.2f}")
                print(f"操作: 触发止损")
                print(f"卖出数量: {self.position.size}手")
                print("="*80 + "\n")
                
                self.order = self.sell(size=self.position.size)
            # 检查止盈条件
            elif current_price > self.take_profit:
                # 获取当前日期
                current_date = self.datas[0].datetime.date(0)
                
                # 打印详细的止盈触发信息
                print("\n" + "="*80)
                print(f"{'止盈触发信息':^80}")
                print("="*80)
                print(f"交易日期: {current_date}")
                print(f"买入日期: {self.buy_date}")
                print(f"当前价格: {current_price:.2f}")
                print(f"止盈价格: {self.take_profit:.2f}")
                print(f"最大价格: {self.max_price:.2f}")
                print(f"操作: 触发止盈")
                print(f"卖出数量: {self.position.size}手")
                print("="*80 + "\n")
                
                self.order = self.sell(size=self.position.size)

def prepare_data(data_file):
    """
    准备回测数据
    
    Args:
        data_file (str): 数据文件路径
    
    Returns:
        pd.DataFrame: 格式化后的DataFrame
    """
    # 读取CSV文件
    df = pd.read_csv(data_file)
    
    # 检查并处理日期格式
    if 'trade_date' in df.columns:
        # 将YYYYMMDD格式转换为datetime
        df['datetime'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    else:
        raise ValueError("数据文件中未找到'trade_date'字段")
    
    # 重命名列以适应backtrader的要求
    df = df.rename(columns={
        'datetime': 'datetime',
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
        'vol': 'volume'
    })
    
    # 确保所有必要的列都存在
    required_columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"数据文件中缺少必要的列: {col}")
    
    # 设置索引
    df.set_index('datetime', inplace=True)
    
    return df

def run_backtest(data_file):
    """
    运行回测
    
    Args:
        data_file (str): 数据文件路径
    """
    # 创建Cerebro引擎
    cerebro = bt.Cerebro()
    
    # 添加策略
    cerebro.addstrategy(AI_Prediction_Strategy)
    
    # 准备数据
    df = prepare_data(data_file)
    
    # 创建数据源
    data = bt.feeds.PandasData(
        dataname=df,
        open='open',
        high='high',
        low='low',
        close='close',
        volume='volume',
        openinterest=-1,  # 如果没有openinterest数据
        timeframe=bt.TimeFrame.Days
    )
    
    # 添加数据到Cerebro
    cerebro.adddata(data)
    
    # 设置初始资金
    initial_cash = 200000  # 20万
    cerebro.broker.setcash(initial_cash)
    
    # 设置自定义的佣金和保证金计算
    # 由于backtrader默认的佣金系统不支持固定费用和T+0差异化费用，我们在notify_order中手动处理
    cerebro.broker.setcommission(commission=0.0)  # 设置为0，在策略中手动计算
    # 设置正确的乘数
    cerebro.broker.setcommission(mult=10.0)
    # 设置保证金率为17%
    cerebro.broker.setcommission(margin=0.17)
    
    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    
    # 打印初始资金
    print(f'初始资金: {initial_cash:.2f}')
    
    # 运行回测
    print("开始回测...")
    results = cerebro.run()
    
    # 获取策略实例
    strategy = results[0]
    
    # 打印最终资金
    final_cash = cerebro.broker.getvalue()
    print(f'最终资金: {final_cash:.2f}')
    print(f'总收益率: {(final_cash - initial_cash) / initial_cash * 100:.2f}%')
    
    # 打印分析结果
    try:
        sharpe = strategy.analyzers.sharpe.get_analysis().get('sharperatio', 'N/A')
        print(f"夏普比率: {sharpe:.2f}" if sharpe != 'N/A' else "夏普比率: N/A")
    except:
        print("夏普比率: 无法计算")
    
    try:
        max_dd = strategy.analyzers.drawdown.get_analysis().get('max', {}).get('drawdown', 'N/A')
        print(f"最大回撤: {max_dd:.2f}%" if max_dd != 'N/A' else "最大回撤: N/A")
    except:
        print("最大回撤: 无法计算")
    
    try:
        rtot = strategy.analyzers.returns.get_analysis().get('rtot', 'N/A')
        print(f"总回报率: {rtot * 100:.2f}%" if rtot != 'N/A' else "总回报率: N/A")
    except:
        print("总回报率: 无法计算")
    
    # 尝试绘制回测结果，如果缺少tkinter则跳过
    try:
        print("正在绘制回测结果...")
        cerebro.plot(style='candlestick')
    except ImportError:
        print("警告: 无法绘制图表，缺少tkinter模块。可以安装tkinter或在GUI环境中运行此程序。")
    except Exception as e:
        print(f"警告: 绘制图表时出错: {e}")

if __name__ == '__main__':
    # 使用find_file_path函数查找数据文件
    data_file = find_file_path('RB9999.csv')
    
    # 检查文件是否存在
    if not data_file:
        print(f"错误: 找不到数据文件 RB9999.csv")
        print("请确保文件存在于以下任一位置:")
        print("1. 当前目录下的 ./Data 文件夹")
        print("2. 当前目录下的 ./data 文件夹")
        print("3. default_param_list.json 中配置的 tushare_root + index 路径下")
        exit(1)
    
    print(f"使用数据文件: {data_file}")
    
    # 运行回测
    run_backtest(data_file)