import sys
sys.path.insert(0, "/opt/anaconda3/lib/python3.12/site-packages")
import schedule

print(schedule.__file__)  # Verify correct import

# schedule.every().day.at("18:00").do(update_stock_data)  # Should work now