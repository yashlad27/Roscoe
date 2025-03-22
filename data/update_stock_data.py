import pymysql
import pandas as pd
import yfinance as yf
import schedule
import time
from datetime import datetime

# Database connection
def connect_db():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="test123",
        database="hedgefund_db"
    )

# Function to fetch and update stock data
def update_stock_data():
    ticker = "AAPL"  # You can extend this to multiple tickers
    print(f"Fetching new stock data for {ticker} at {datetime.now()}...")

    # Fetch latest stock data (1 day)
    data = yf.download(ticker, period="1d")

    if data.empty:
        print("⚠️ No new data available. Skipping update.")
        return

    data.reset_index(inplace=True)
    data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
    data.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
    data['date'] = pd.to_datetime(data['date']).dt.date  # Convert to MySQL DATE format

    # Insert new data into MySQL
    conn = connect_db()
    cursor = conn.cursor()

    for _, row in data.iterrows():
        cursor.execute("""
            INSERT INTO stock_prices (ticker, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high),
            low=VALUES(low), close=VALUES(close), volume=VALUES(volume);
        """, (ticker, row['date'], row['open'], row['high'], row['low'], row['close'], row['volume']))

    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Stock data updated successfully!")

# Schedule the function to run daily at 6:00 PM (market close)
schedule.every().day.at("18:00").do(update_stock_data)

print("⏳ Stock data update script running. Waiting for scheduled time...")

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(60)  # Wait for 1 minute before checking again