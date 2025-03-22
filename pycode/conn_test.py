import pymysql

# Connect to MySQL
conn = pymysql.connect(
    host="localhost",
    user="root",
    password="test123",
    database="hedgefund_db",
    cursorclass=pymysql.cursors.DictCursor  # Optional: Returns results as dictionaries
)

print("✅ Connected to MySQL successfully!")

conn.close()