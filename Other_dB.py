import sqlite3
import logging

def migrate_database(db_path="weather_data.db"):
    required_columns = {
        "country": "TEXT",
        "latitude": "REAL",
        "longitude": "REAL",
        "temperature": "REAL",
        "feels_like": "REAL",
        "temp_min": "REAL",
        "temp_max": "REAL",
        "pressure": "REAL",
        "humidity": "REAL",
        "visibility": "INTEGER",
        "wind_speed": "REAL",
        "wind_deg": "REAL",
        "wind_gust": "REAL",
        "clouds": "INTEGER",
        "weather_id": "INTEGER",
        "weather_main": "TEXT",
        "weather_description": "TEXT",
        "weather_icon": "TEXT",
        "timestamp": "DATETIME DEFAULT CURRENT_TIMESTAMP"
    }

    try:
        conn = sqlite3.connect(db_path = "weather_data.db")
        cursor = conn.cursor()

        logging.info("Checking and migrating database schema...")

        # Get existing columns
        cursor.execute("PRAGMA table_info(weather_data);")
        existing_cols = {row[1] for row in cursor.fetchall()}

        # Add any missing columns
        for col, col_type in required_columns.items():
            if col not in existing_cols:
                alter_sql = f"ALTER TABLE weather_data ADD COLUMN {col} {col_type};"
                logging.info(f"Adding missing column: {col} ({col_type})")
                cursor.execute(alter_sql)

        conn.commit()
        logging.info("Database schema migration complete ✅")

    except Exception as e:
        logging.error(f"Database migration failed: {e}")
    finally:
        conn.close()
