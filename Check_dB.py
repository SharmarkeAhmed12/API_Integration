# check_weather_db.py
import sqlite3
import pandas as pd

# Connect to your SQLite database
conn = sqlite3.connect("weather_data.db")

# Fetch the last 10 rows ordered by timestamp
query = """
SELECT * 
FROM weather_data
ORDER BY timestamp DESC
LIMIT 10;
"""

df = pd.read_sql_query(query, conn)
conn.close()

print(df)
