import pymysql
import pandas as pd
import yfinance as yf

# Database connection
conn = pymysql.connect(
    host="localhost",
    user="root",
    password="test123",
    database="hedgefund_db"
)

cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_prices (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ticker VARCHAR(10),
        date DATE,
        open DECIMAL(10,2),
        high DECIMAL(10,2),
        low DECIMAL(10,2),
        close DECIMAL(10,2),
        volume BIGINT
    );
""")

conn.commit()

# Fetch stock data
ticker = "AAPL"
data = yf.download(ticker, period="5y")

# Ensure 'Date' is properly formatted
data.reset_index(inplace=True)  # Convert date index to a column

# Rename columns explicitly to avoid misalignment
data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
data.columns = ['date', 'open', 'high', 'low', 'close', 'volume']

# Convert 'date' column to MySQL-compatible format
data['date'] = pd.to_datetime(data['date']).dt.date

# Print to verify structure before inserting
print("Sample Data Before Inserting:\n", data.head())

# Insert data into MySQL
for _, row in data.iterrows():
    cursor.execute("""
        INSERT INTO stock_prices (ticker, date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (ticker, row['date'], row['open'], row['high'], row['low'], row['close'], row['volume']))

conn.commit()
cursor.close()
conn.close()

print("✅ Stock data successfully inserted into MySQL!")