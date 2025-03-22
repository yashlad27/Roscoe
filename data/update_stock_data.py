import pymysql
import pandas as pd
import yfinance as yf
import schedule
import time
from datetime import datetime, timedelta

# List of S&P 500 tickers (Can be expanded)
sp500_tickers = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "JPM", "V", "NFLX", "META", "NVDA"
]

from datetime import datetime, timedelta


def get_last_market_day():
    today = datetime.today()

    if today.weekday() == 5:  # Saturday → Use Friday’s data (T-1)
        last_market_day = today - timedelta(days=1)
    elif today.weekday() == 6:  # Sunday → Use Friday’s data (T-2)
        last_market_day = today - timedelta(days=2)
    else:  # Weekday → Use today’s data
        last_market_day = today

    return last_market_day.strftime('%Y-%m-%d')


print("Last Market Day:", get_last_market_day())


# Database connection function
def connect_db():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="test123",
        database="hedgefund_db"
    )


# Function to update stock data
def update_stock_data():
    conn = connect_db()
    cursor = conn.cursor()

    last_market_day = get_last_market_day()
    print(f"Fetching stock data for market date: {last_market_day}")

    for ticker in sp500_tickers:
        print(f"Fetching stock data for {ticker} at {datetime.now()}...")

        # Fetch stock data (fixing weekend errors)
        start_date = (datetime.strptime(last_market_day, "%Y-%m-%d") - timedelta(days=1)).strftime(
            '%Y-%m-%d')
        end_date = (datetime.strptime(last_market_day, "%Y-%m-%d") + timedelta(days=1)).strftime(
            '%Y-%m-%d')

        data = yf.download(ticker, start=start_date, end=end_date)

        if data.empty:
            print(f"⚠️ No data for {ticker} on {last_market_day}, skipping...")
            continue

        data.reset_index(inplace=True)
        data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        data.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        data['date'] = pd.to_datetime(data['date']).dt.date  # Convert to MySQL DATE format

        # Insert data into MySQL
        for _, row in data.iterrows():
            cursor.execute("""
                INSERT INTO stock_prices (ticker, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high),
                low=VALUES(low), close=VALUES(close), volume=VALUES(volume);
            """, (
            ticker, row['date'], row['open'], row['high'], row['low'], row['close'], row['volume']))

        print(f"✅ Data for {ticker} updated successfully!")

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ All S&P 500 stocks updated successfully!")


# Schedule updates
schedule.every().monday.at("18:00").do(update_stock_data)
schedule.every().tuesday.at("18:00").do(update_stock_data)
schedule.every().wednesday.at("18:00").do(update_stock_data)
schedule.every().thursday.at("18:00").do(update_stock_data)
schedule.every().friday.at("18:00").do(update_stock_data)

# On weekends, fetch last Friday's data
schedule.every().saturday.at("10:00").do(update_stock_data)
schedule.every().sunday.at("10:00").do(update_stock_data)

print("⏳ Stock data update script running. Waiting for scheduled time...")

# Run update_stock_data immediately for debugging
if __name__ == "__main__":
    update_stock_data()  # <-- Add this line to force execution

while True:
    schedule.run_pending()
    time.sleep(60)  # Wait 1 min before checking again
