import yfinance as yf

# Define stock ticker
ticker = "AAPL"

# Fetch historical stock data (last 5 years)
data = yf.download(ticker, period="5y")

# Print sample data
print(data.head())