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

#db conn
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
    params = (("short_period", 50), ("long_period", 200),)

    def __init__(self):
        self.short_sma = bt.indicators.SimpleMovingAverage(self.data.close,
                                                           period=self.params.short_period)
        self.long_sma = bt.indicators.SimpleMovingAverage(self.data.close,
                                                          period=self.params.long_period)

    def next(self):
        if self.short_sma[0] > self.long_sma[0] and self.short_sma[-1] <= self.long_sma[-1]:
            self.buy()  # Buy when short SMA crosses above long SMA
        elif self.short_sma[0] < self.long_sma[0] and self.short_sma[-1] >= self.long_sma[-1]:
            self.sell()  # Sell when short SMA crosses below long SMA


# Backtrader Strategy: Mean Reversion (Bollinger Bands)
class MeanReversionStrategy(bt.Strategy):
    params = (("bollinger_period", 20), ("std_dev", 2),)

    def __init__(self):
        self.bb = bt.indicators.BollingerBands(period=self.params.bollinger_period,
                                               devfactor=self.params.std_dev)

    def next(self):
        if self.data.close[0] < self.bb.lines.bot[0]:  # Price below lower Bollinger Band
            self.buy()
        elif self.data.close[0] > self.bb.lines.top[0]:  # Price above upper Bollinger Band
            self.sell()


# Backtest Runner
def run_backtest(strategy, ticker, start_date="2020-01-01", end_date="2025-01-01"):
    df = fetch_stock_data(ticker, start_date, end_date)
    if df is None:
        return

    cerebro = bt.Cerebro()
    data_feed = bt.feeds.PandasData(dataname=df)
    cerebro.adddata(data_feed)
    cerebro.addstrategy(strategy)

    print(f"🚀 Running backtest for {ticker} using {strategy.__name__}...")
    results = cerebro.run()

    # # Plot equity curve
    # cerebro.plot(style='candlestick')

    # Generate plot
    fig = cerebro.plot(style='candlestick')[0][0]  # Extract figure

    # Save plot as PNG
    save_path = f"backtest_results/{ticker}_{strategy.__name__}.png"
    fig.savefig(save_path, dpi=300, bbox_inches="tight")
    print(f"📊 Backtest plot saved as {save_path}")

    # Display the plot
    plt.show()


# Run backtests for both strategies
if __name__ == "__main__":
    ticker = "AAPL"

    # Run Momentum Strategy
    run_backtest(MomentumStrategy, ticker)

    # Run Mean Reversion Strategy
    run_backtest(MeanReversionStrategy, ticker)
