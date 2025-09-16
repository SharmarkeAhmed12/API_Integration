import requests
import sqlite3
import os
import logging
import json
import time
from datetime import datetime
from typing import List, Dict, Any
import argparse


# Configuration
API_KEY = os.getenv("OPENWEATHER_API_KEY", "eea5f0d577d8539c9f2732e5d1f735d5")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
DB_PATH = "weather_data.db"
DATA_LAKE_DIR = "datalake/raw"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("weather_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("weather_pipeline")

def setup_database() -> sqlite3.Connection:
    """Set up database with enhanced schema"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Enhanced schema with more weather metrics
    cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT NOT NULL,
        country TEXT,
        latitude REAL,
        longitude REAL,
        temperature REAL,
        feels_like REAL,
        temp_min REAL,
        temp_max REAL,
        pressure INTEGER,
        humidity INTEGER,
        visibility INTEGER,
        wind_speed REAL,
        wind_deg INTEGER,
        wind_gust REAL,
        clouds INTEGER,
        weather_id INTEGER,
        weather_main TEXT,
        weather_description TEXT,
        weather_icon TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(city, timestamp)
    )
    """)
    
    # Create index for faster queries
    cur.execute("CREATE INDEX IF NOT EXISTS idx_city ON weather_data(city)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON weather_data(timestamp)")
    
    conn.commit()
    return conn

def save_to_datalake(data: Dict[str, Any], city: str) -> None:
    """Save raw API response to data lake"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        date_dir = datetime.now().strftime("%Y/%m/%d")
        dir_path = f"{DATA_LAKE_DIR}/{date_dir}"
        
        os.makedirs(dir_path, exist_ok=True)
        
        filename = f"{dir_path}/{city}_{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
            
        logger.info(f"Saved raw data for {city} to data lake: {filename}")
    except Exception as e:
        logger.error(f"Failed to save data to data lake for {city}: {e}")

def fetch_weather_data(city: str) -> Dict[str, Any]:
    """Fetch weather data from API with error handling and retries"""
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric"
    }
    
    for attempt in range(3):  # Retry up to 3 times
        try:
            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt == 2:  # Final attempt
                logger.error(f"Failed to fetch data for {city} after 3 attempts: {e}")
                raise
            logger.warning(f"Attempt {attempt + 1} failed for {city}, retrying...")
            time.sleep(2)  # Wait before retrying
    
    return {}

def insert_weather_data(conn: sqlite3.Connection, data: Dict[str, Any], city: str) -> None:
    """Insert weather data into database"""
    try:
        cur = conn.cursor()
        
        # Extract data with error handling for missing fields
        main = data.get("main", {})
        coord = data.get("coord", {})
        weather = data.get("weather", [{}])[0]
        wind = data.get("wind", {})
        clouds = data.get("clouds", {})
        sys = data.get("sys", {})
        
        cur.execute("""
        INSERT OR IGNORE INTO weather_data 
        (city, temperature, pressure, humidity, wind_speed, wind_direction, 
         weather_id, weather_main, weather_description, weather_icon)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            city,
            coord.get("lat"),
            coord.get("lon"),
            main.get("temp"),
            main.get("feels_like"),
            main.get("temp_min"),
            main.get("temp_max"),
            main.get("pressure"),
            main.get("humidity"),
            data.get("visibility"),
            wind.get("speed"),
            wind.get("deg"),
            wind.get("gust"),
            clouds.get("all"),
            weather.get("id"),
            weather.get("main"),
            weather.get("description"),
            weather.get("icon")
        ))
        
        conn.commit()
        logger.info(f"Weather data inserted for {city}")
        
    except Exception as e:
        logger.error(f"Failed to insert data for {city}: {e}")
        conn.rollback()

def process_cities(cities: List[str]) -> None:
    """Process list of cities to fetch and store weather data"""
    conn = setup_database()
    
    success_count = 0
    failure_count = 0
    
    for city in cities:
        try:
            logger.info(f"Processing city: {city}")
            
            # Fetch data from API
            data = fetch_weather_data(city)
            if not data:
                failure_count += 1
                continue
                
            # Save raw data to data lake
            save_to_datalake(data, city)
            
            # Insert processed data into database
            insert_weather_data(conn, data, city)
            
            success_count += 1
            
            # Be nice to the API - add a small delay between requests
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Failed to process city {city}: {e}")
            failure_count += 1
    
    conn.close()
    
    logger.info(f"Processing complete. Success: {success_count}, Failures: {failure_count}")

def get_cities_from_user() -> List[str]:
    """Get cities from user input with validation"""
    while True:
        cities_input = input("Enter city names separated by commas (or type 'exit' to quit): ").strip()
        
        if cities_input.lower() == 'exit':
            print("Exiting...")
            exit(0)
            
        cities = [c.strip() for c in cities_input.split(",") if c.strip()]
        
        if cities:
            return cities
        else:
            print("No valid cities entered. Please try again.")

def get_cities_from_file(filename: str) -> List[str]:
    """Get cities from a text file"""
    try:
        with open(filename, 'r') as f:
            cities = [line.strip() for line in f if line.strip()]
        return cities
    except FileNotFoundError:
        logger.error(f"File {filename} not found")
        return []

def main():
    """Main function with command line argument support"""
    parser = argparse.ArgumentParser(description="Weather Data Ingestion Pipeline")
    parser.add_argument("--cities", nargs="+", help="List of cities to process")
    parser.add_argument("--file", help="File containing list of cities")
    
    args = parser.parse_args()
    
    cities = []
    
    # Get cities from command line arguments
    if args.cities:
        cities.extend(args.cities)
    
    # Get cities from file
    if args.file:
        cities_from_file = get_cities_from_file(args.file)
        cities.extend(cities_from_file)
    
    # If no cities provided via arguments, ask user
    if not cities:
        cities = get_cities_from_user()
    
    if not cities:
        logger.error("No cities to process")
        return
    
    logger.info(f"Starting processing for {len(cities)} cities: {', '.join(cities)}")
    process_cities(cities)

if __name__ == "__main__":
    main()