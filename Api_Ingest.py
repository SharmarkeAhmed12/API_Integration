#!/usr/bin/env python3
"""
Weather ingestion pipeline - interactive + daily updates with history tracking.
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

# ---------------------------
# Configuration
# ---------------------------
API_KEY = "eea5f0d577d8539c9f2732e5d1f735d5"
if not API_KEY:
    print("ERROR: OPENWEATHER_API_KEY must be set.", file=sys.stderr)
    sys.exit(1)

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
DB_PATH = "weather_data.db"
DATA_LAKE_DIR = "datalake/raw"
UPDATE_INTERVAL_SECONDS = 24 * 3600  # 24 hours

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("weather_pipeline.log"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("weather_pipeline")

# ---------------------------
# Graceful shutdown
# ---------------------------
shutdown_requested = False
def _signal_handler(signum, frame):
    global shutdown_requested
    logger.info(f"Received signal {signum}. Shutdown requested.")
    shutdown_requested = True

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ---------------------------
# Database utilities
# ---------------------------
def setup_database(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    cur = conn.cursor()

    # Latest weather table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS weather_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL UNIQUE,
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
        """
    )

    # Historical weather table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS weather_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL,
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
        """
    )

    conn.commit()
    return conn

# ---------------------------
# Data lake utilities
# ---------------------------
def atomic_write_json(filepath: str, data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(filepath), prefix=".tmp_", suffix=".json")
    os.close(fd)
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        shutil.move(tmp, filepath)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass

def save_to_datalake(data: Dict[str, Any], city: str) -> None:
    try:
        now = datetime.utcnow()
        timestamp = now.strftime("%Y%m%d_%H%M%S_%f")[:-3]
        date_dir = now.strftime("%Y/%m/%d")
        filename = f"{DATA_LAKE_DIR}/{date_dir}/{city.replace(' ', '_')}_{timestamp}.json"
        atomic_write_json(filename, data)
        logger.info(f"Saved raw data for {city} to data lake: {filename}")
    except Exception as e:
        logger.exception(f"Failed to save data to data lake for {city}: {e}")

# ---------------------------
# Fetch and insert
# ---------------------------
def fetch_weather_data(city: str, retries: int = 3, backoff: float = 2.0, timeout: int = 10) -> Optional[Dict[str, Any]]:
    params = {"q": city, "appid": API_KEY, "units": "metric"}
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as exc:
            if attempt == retries:
                logger.error("Final attempt failed for %s: %s", city, exc)
                return None
            logger.warning("Attempt %d/%d failed for %s: %s. Retrying in %.1fs", attempt, retries, city, exc, backoff)
            time.sleep(backoff)
            backoff *= 2
    return None

def insert_weather_data(conn: sqlite3.Connection, data: Dict[str, Any]) -> None:
    try:
        cur = conn.cursor()
        main = data.get("main", {})
        coord = data.get("coord", {})
        weather = data.get("weather", [{}])[0] if data.get("weather") else {}
        wind = data.get("wind", {})
        clouds = data.get("clouds", {})
        sys_info = data.get("sys", {})

        ts = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(timespec="milliseconds")

        values_tuple = (
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

        # Fetch previous data to log changes
        cur.execute("SELECT temperature, humidity, weather_description FROM weather_data WHERE city=?", (data.get("name"),))
        prev = cur.fetchone()
        if prev:
            temp_change = (main.get("temp") - prev[0]) if prev[0] is not None else 0
            humidity_change = (main.get("humidity") - prev[1]) if prev[1] is not None else 0
            logger.info(
                "Changes for %s: temp %+0.1f°C, humidity %+d, description %s -> %s",
                data.get("name"),
                temp_change,
                humidity_change,
                prev[2],
                weather.get("description"),
            )

        # Insert or update latest table
        cur.execute(
            """
            INSERT INTO weather_data
            (city, latitude, longitude, temperature, feels_like, temp_min, temp_max,
             pressure, humidity, visibility, wind_speed, wind_deg, wind_gust,
             clouds, weather_id, weather_main, weather_description, weather_icon, timestamp, country)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            """,
            values_tuple,
        )

        # Insert snapshot into history
        cur.execute(
            """
            INSERT INTO weather_history
            (city, latitude, longitude, temperature, feels_like, temp_min, temp_max,
             pressure, humidity, visibility, wind_speed, wind_deg, wind_gust,
             clouds, weather_id, weather_main, weather_description, weather_icon, timestamp, country)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            values_tuple,
        )

        conn.commit()
        logger.info("Weather data inserted/updated for %s", data.get("name"))
    except Exception:
        logger.exception("Failed to insert/update data for %s", data.get("name"))
        try:
            conn.rollback()
        except Exception:
            pass

# ---------------------------
# Processing loop
# ---------------------------
def process_cities_once(conn: sqlite3.Connection, cities: List[str]) -> Dict[str, int]:
    success_count = 0
    failure_count = 0
    for city in cities:
        if shutdown_requested:
            logger.info("Shutdown requested; stopping early.")
            break
        logger.info("Processing city: %s", city)
        data = fetch_weather_data(city)
        if not data:
            logger.warning("No data for %s", city)
            failure_count += 1
            continue
        try:
            save_to_datalake(data, city)
        except Exception:
            logger.exception("Failed saving raw data for %s", city)
        insert_weather_data(conn, data)
        success_count += 1
        time.sleep(1)
    return {"success": success_count, "failure": failure_count}

# ---------------------------
# CLI helpers
# ---------------------------
def get_cities_from_file(filename: str) -> List[str]:
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    except Exception:
        logger.exception("Failed to read cities from file: %s", filename)
        return []

# ---------------------------
# Main
# ---------------------------
def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Weather Data Ingestion Pipeline")
    parser.add_argument("--cities", nargs="+", help="List of cities to process")
    parser.add_argument("--file", help="File containing list of cities")
    args = parser.parse_args(argv)

    cities: List[str] = []
    if args.cities:
        cities.extend(args.cities)
    if args.file:
        cities.extend(get_cities_from_file(args.file))

    if not cities:
        cities_input = input("Enter city names separated by commas: ").strip()
        if not cities_input:
            logger.error("No cities entered. Exiting.")
            return 2
        cities = [c.strip() for c in cities_input.split(",") if c.strip()]

    conn = setup_database(DB_PATH)
    logger.info("Database initialized at %s", DB_PATH)
    logger.info("Starting daily weather update for cities: %s", ", ".join(cities))

    try:
        while not shutdown_requested:
            start_time = time.time()
            logger.info("--- Starting daily update at %s ---", datetime.utcnow().isoformat())
            counts = process_cities_once(conn, cities)
            logger.info("Daily update results: success=%d, failures=%d", counts["success"], counts["failure"])
            elapsed = time.time() - start_time
            sleep_time = UPDATE_INTERVAL_SECONDS - elapsed
            if shutdown_requested:
                break
            if sleep_time > 0:
                slept = 0.0
                while slept < sleep_time and not shutdown_requested:
                    to_sleep = min(1.0, sleep_time - slept)
                    time.sleep(to_sleep)
                    slept += to_sleep
    except Exception:
        logger.exception("Fatal error in daily update loop")
    finally:
        try:
            conn.close()
            logger.info("Database connection closed.")
        except Exception:
            pass
        logger.info("Shutdown complete.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
