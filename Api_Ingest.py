#!/usr/bin/env python3
"""
Weather ingestion pipeline
- Interactive city input
- Continuous 24-hour batch updates
- Latest-state + historical tracking
"""

import argparse
import json
import logging
import os
import shutil
import signal
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

# ===========================
# Environment & Configuration
# ===========================
# Explicit path to your .env file
dotenv_path = r"C:\API_Integration\Secrets.env"
load_dotenv(dotenv_path)

API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise RuntimeError("OPENWEATHER_API_KEY not found in .env file")

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "weather_data.db")
DATA_LAKE_DIR = os.path.join(BASE_DIR, "datalake", "raw")

UPDATE_INTERVAL_SECONDS = 24 * 3600  # 24 hours

# ===========================
# Logging
# ===========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("weather_pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("weather_pipeline")

# ===========================
# Graceful Shutdown
# ===========================
shutdown_requested = False

def _signal_handler(signum, frame):
    global shutdown_requested
    logger.info("Received signal %s — shutting down gracefully", signum)
    shutdown_requested = True

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ===========================
# Database Setup
# ===========================
def setup_database(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            city TEXT PRIMARY KEY,
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
            timestamp TEXT,
            country TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
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
            timestamp TEXT,
            country TEXT
        );
    """)

    conn.commit()
    return conn

# ===========================
# Data Lake Utilities
# ===========================
def atomic_write_json(path: str, data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".json")
    os.close(fd)
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        shutil.move(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)

def save_to_datalake(data: Dict[str, Any], city: str) -> None:
    now = datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    ts = now.strftime("%Y%m%d_%H%M%S_%f")[:-3]

    filename = os.path.join(
        DATA_LAKE_DIR,
        date_path,
        f"{city.replace(' ', '_')}_{ts}.json",
    )

    atomic_write_json(filename, data)
    logger.info("Raw data saved: %s", filename)

# ===========================
# API Fetch
# ===========================
def fetch_weather(city: str) -> Optional[Dict[str, Any]]:
    try:
        resp = requests.get(
            BASE_URL,
            params={"q": city, "appid": API_KEY, "units": "metric"},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.error("API error for %s: %s", city, exc)
        return None

# ===========================
# Insert Logic
# ===========================
def insert_weather(conn: sqlite3.Connection, data: Dict[str, Any]) -> None:
    cur = conn.cursor()

    main = data.get("main", {})
    coord = data.get("coord", {})
    weather = data.get("weather", [{}])[0]
    wind = data.get("wind", {})
    clouds = data.get("clouds", {})
    sys_info = data.get("sys", {})

    ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    values = (
        data.get("name"),
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
        weather.get("icon"),
        ts,
        sys_info.get("country"),
    )

    # Insert or update latest data
    cur.execute("""
        INSERT INTO weather_data
        (city, latitude, longitude, temperature, feels_like, temp_min, temp_max,
         pressure, humidity, visibility, wind_speed, wind_deg, wind_gust,
         clouds, weather_id, weather_main, weather_description, weather_icon,
         timestamp, country)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(city) DO UPDATE SET
            latitude=excluded.latitude,
            longitude=excluded.longitude,
            temperature=excluded.temperature,
            feels_like=excluded.feels_like,
            temp_min=excluded.temp_min,
            temp_max=excluded.temp_max,
            pressure=excluded.pressure,
            humidity=excluded.humidity,
            visibility=excluded.visibility,
            wind_speed=excluded.wind_speed,
            wind_deg=excluded.wind_deg,
            wind_gust=excluded.wind_gust,
            clouds=excluded.clouds,
            weather_id=excluded.weather_id,
            weather_main=excluded.weather_main,
            weather_description=excluded.weather_description,
            weather_icon=excluded.weather_icon,
            timestamp=excluded.timestamp,
            country=excluded.country;
    """, values)

    # Insert snapshot into history
    cur.execute("""
        INSERT INTO weather_history
        (city, latitude, longitude, temperature, feels_like, temp_min, temp_max,
         pressure, humidity, visibility, wind_speed, wind_deg, wind_gust,
         clouds, weather_id, weather_main, weather_description, weather_icon,
         timestamp, country)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, values)

    conn.commit()
    logger.info("Weather updated for %s", data.get("name"))

# ===========================
# Processing Loop
# ===========================
def process_cycle(conn: sqlite3.Connection, cities: List[str]) -> None:
    for city in cities:
        if shutdown_requested:
            break
        logger.info("Processing city: %s", city)
        data = fetch_weather(city)
        if not data:
            continue
        save_to_datalake(data, city)
        insert_weather(conn, data)
        time.sleep(1)

# ===========================
# Main Loop
# ===========================
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cities", nargs="+")
    parser.add_argument("--file")
    args = parser.parse_args()

    cities: List[str] = []

    if args.cities:
        cities.extend(args.cities)

    if args.file:
        with open(args.file) as f:
            cities.extend(line.strip() for line in f if line.strip())

    if not cities:
        cities = [c.strip() for c in input("Enter cities (comma-separated): ").split(",")]

    conn = setup_database(DB_PATH)
    logger.info("Pipeline started for cities: %s", ", ".join(cities))

    try:
        while not shutdown_requested:
            logger.info("=== New ingestion cycle at %s ===",
                        datetime.now(timezone.utc).isoformat())
            start = time.time()
            process_cycle(conn, cities)
            elapsed = time.time() - start
            sleep_time = max(0, UPDATE_INTERVAL_SECONDS - elapsed)
            logger.info("Cycle complete. Sleeping for %.1f seconds", sleep_time)

            slept = 0
            while slept < sleep_time and not shutdown_requested:
                time.sleep(1)
                slept += 1

    finally:
        conn.close()
        logger.info("Pipeline stopped cleanly")

if __name__ == "__main__":
    main()
