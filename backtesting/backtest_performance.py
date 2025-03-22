'''
1. implement Momentum & Mean Reversion strategies.
2. Simulate trades based on historical data.
3. Calculate profitability and risk metrics.
'''

import pymysql
import pandas as pd
import numpy as np
import backtrader as bt
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text


# db conn
def connect_db():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="test123",
        database="hedgefund_db"
    )


# function to fetch historical stock data:
def fetch_stock_data(ticker, start_date, end_date):
    engine = create_engine("mysql+pymysql://root:test123@localhost/hedgefund_db")

    # Debug: Print available date range
    with engine.connect() as conn:
        result = conn.execute(text(
            f"SELECT MIN(date), MAX(date) FROM stock_prices WHERE ticker = '{ticker}'")).fetchall()
        print(f"📊 Available data for {ticker}: {result}")

    query = f"""
        SELECT date, open, high, low, close, volume
        FROM stock_prices
        WHERE ticker = '{ticker}' AND date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY date ASC;
    """
    df = pd.read_sql(query, con=engine)

    if df.empty:
        print(f"⚠️ No data found for {ticker} between {start_date} and {end_date}")
        return None

    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    return df


# Backtrader Strategy: Momentum Trading (SMA Crossover)
class MomentumStrategy(bt.Strategy):
    params = (
        ("short_period", 50), ("long_period", 200), ("stop_loss", 0.02), ("take_profit", 0.05))

    def __init__(self):
        self.short_sma = bt.indicators.SimpleMovingAverage(self.data.close,
                                                           period=self.params.short_period)
        self.long_sma = bt.indicators.SimpleMovingAverage(self.data.close,
                                                          period=self.params.long_period)
        self.order = None  # Track open orders

    def log(self, txt, dt=None):
        """ Logging function for trade execution debugging """
        dt = dt or self.datas[0].datetime.date(0)
        print(f"{dt} - {txt}")  # 🛠️ Fixed: Log method properly defined

    def next(self):
        if self.order:  # Prevent overlapping trades
            return

        # Stop-Loss & Take-Profit Logic (Exit before new entries)
        if self.position:
            if self.data.close[0] <= self.stop_price:
                self.log(f"STOP LOSS HIT, SELLING at {self.data.close[0]}")
                self.order = self.close()
                return
            elif self.data.close[0] >= self.take_profit_price:
                self.log(f"TAKE PROFIT HIT, SELLING at {self.data.close[0]}")
                self.order = self.close()
                return

        # Entry Logic (Buy)
        if not self.position and self.short_sma[0] > self.long_sma[0] and self.short_sma[-1] <= \
                self.long_sma[-1]:
            self.log(f"BUY EXECUTED, Price: {self.data.close[0]}")
            self.order = self.buy()
            self.stop_price = self.data.close[0] * (1 - self.params.stop_loss)
            self.take_profit_price = self.data.close[0] * (1 + self.params.take_profit)

        # Entry Logic (Sell)
        elif not self.position and self.short_sma[0] < self.long_sma[0] and self.short_sma[-1] >= \
                self.long_sma[-1]:
            self.log(f"SELL EXECUTED, Price: {self.data.close[0]}")
            self.order = self.sell()


# Backtrader Strategy: Mean Reversion (Bollinger Bands)
class MeanReversionStrategy(bt.Strategy):
    params = (("bollinger_period", 20), ("std_dev", 2), ("atr_period", 14), ("atr_filter", 1.5))

    def __init__(self):
        self.bb = bt.indicators.BollingerBands(period=self.params.bollinger_period,
                                               devfactor=self.params.std_dev)
        self.atr = bt.indicators.ATR(period=self.params.atr_period)  # ATR Indicator
        self.order = None  # Track open orders

    def next(self):
        if self.order:  # Prevent overlapping trades
            return

        # ATR-based filter to reduce unnecessary trades
        if self.atr[0] < self.params.atr_filter:
            return  # Skip trade if volatility is too low

        if not self.position and self.data.close[0] < self.bb.lines.bot[0]:  # Buy Signal
            self.order = self.buy()
        elif not self.position and self.data.close[0] > self.bb.lines.top[0]:  # Sell Signal
            self.order = self.sell()


# def next(self):
#     if self.order:  # Skip if there's an open order
#         return
#
#     # Check Stop Loss
#     if self.position:
#         if self.data.close[0] <= self.stop_price:
#             self.log(f"STOP LOSS HIT, SELLING at {self.data.close[0]}")
#             self.order = self.close()
#             return
#         elif self.data.close[0] >= self.take_profit_price:
#             self.log(f"TAKE PROFIT HIT, SELLING at {self.data.close[0]}")
#             self.order = self.close()
#             return
#
#     # Entry Logic (Buy)
#     if self.short_sma[0] > self.long_sma[0] and self.short_sma[-1] <= self.long_sma[-1]:
#         self.log(f"BUY EXECUTED, Price: {self.data.close[0]}")
#         self.order = self.buy()
#         self.stop_price = self.data.close[0] * (1 - self.params.stop_loss)
#         self.take_profit_price = self.data.close[0] * (1 + self.params.take_profit)
#
#     # Entry Logic (Sell)
#     elif self.short_sma[0] < self.long_sma[0] and self.short_sma[-1] >= self.long_sma[-1]:
#         self.log(f"SELL EXECUTED, Price: {self.data.close[0]}")
#         self.order = self.sell()
#

# Performance Metrics Calculation
def calculate_performance(cerebro, strategy):
    # Extract portfolio value history
    portfolio_values = np.array([cerebro.broker.getvalue() for _ in cerebro.run()])

    # ✅ **Sharpe Ratio Calculation**
    try:
        sharpe_ratio = strategy.analyzers.sharpe.get_analysis().get('sharperatio', np.nan)
    except KeyError:
        sharpe_ratio = np.nan  # Handle missing data

    # ✅ **Max Drawdown Calculation**
    drawdown_analysis = strategy.analyzers.drawdown.get_analysis()
    max_drawdown = drawdown_analysis.get('max', {}).get('drawdown', np.nan)

    # ✅ **Win Rate Calculation**
    trade_analysis = strategy.analyzers.trades.get_analysis()

    total_trades = trade_analysis.get("total", {}).get("total", 0)
    profitable_trades = trade_analysis.get("won", {}).get("total", 0)
    win_rate = (profitable_trades / total_trades) if total_trades > 0 else 0

    # ✅ Print Results
    print("\n📊 **Performance Metrics**")
    print(f"✅ **Sharpe Ratio:** {sharpe_ratio:.2f}")
    print(f"📉 **Max Drawdown:** {max_drawdown:.2%}")
    print(f"🏆 **Win Rate:** {win_rate:.2%}")


# Backtest Runner with Analyzers
def run_backtest(strategy_class, ticker, start_date="2010-03-23", end_date="2025-03-21"):
    df = fetch_stock_data(ticker, start_date, end_date)
    if df is None:
        return

    cerebro = bt.Cerebro()
    data_feed = bt.feeds.PandasData(dataname=df)
    cerebro.adddata(data_feed)
    cerebro.addstrategy(strategy_class)

    # Add Analyzers
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="trades")
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe")
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")

    print(f"🚀 Running backtest for {ticker} using {strategy_class.__name__}...")

    # Run Cerebro and extract the strategy instance
    results = cerebro.run()
    strat = results[0]  # Extract the first strategy instance ✅

    # ✅ **Pass both `cerebro` and `strat` to `calculate_performance()`**
    calculate_performance(cerebro, strat)  # 🛠 FIXED

    # Generate plot
    fig = cerebro.plot(style='candlestick')[0][0]
    fig.savefig(f"backtest_results/{ticker}_{strategy_class.__name__}.png", dpi=300,
                bbox_inches="tight")
    print(f"📊 Backtest plot saved as backtest_results/{ticker}_{strategy_class.__name__}.png")

    plt.show()


# Run backtests for both strategies
if __name__ == "__main__":
    ticker = "AAPL"

    # Run Momentum Strategy
    run_backtest(MomentumStrategy, ticker)

    # Run Mean Reversion Strategy
    run_backtest(MeanReversionStrategy, ticker)
