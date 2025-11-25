#!/usr/bin/env python3
"""
Weather ingestion pipeline (polling) - production-ready rewrite.

Usage examples:
    python weather_pipeline.py --cities Nairobi London
    python weather_pipeline.py --file cities.txt
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
# Configuration (edit as needed)
# ---------------------------
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    print("ERROR: OPENWEATHER_API_KEY environment variable must be set.", file=sys.stderr)
    sys.exit(1)

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
DB_PATH = "weather_data.db"
DATA_LAKE_DIR = "datalake/raw"
POLLING_INTERVAL_SECONDS = 900  # default 15 minutes

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("weather_pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("weather_pipeline")

# ---------------------------
# Graceful shutdown handling
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
    """
    Create or open the database with a robust schema. Place 'country' at the end.
    Timestamp has millisecond precision and is part of UNIQUE constraint.
    """
    conn = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    conn.execute("PRAGMA journal_mode=WAL;")  # Better concurrency
    conn.execute("PRAGMA foreign_keys=ON;")
    cur = conn.cursor()

    # Schema: country deliberately placed at the end (Option 2)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS weather_data (
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
            timestamp TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            country TEXT,
            UNIQUE(city, timestamp)
        );
    """
    )

    cur.execute("CREATE INDEX IF NOT EXISTS idx_city ON weather_data(city);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON weather_data(timestamp);")

    conn.commit()
    return conn


# ---------------------------
# Data lake utilities
# ---------------------------
def atomic_write_json(filepath: str, data: Dict[str, Any]) -> None:
    """
    Write JSON to a temp file then atomically rename.
    """
    dirpath = os.path.dirname(filepath)
    os.makedirs(dirpath, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=dirpath, prefix=".tmp_", suffix=".json")
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
    """
    Save raw API response under DATA_LAKE_DIR/YYYY/MM/DD/<city>_<ts>.json
    """
    try:
        now = datetime.utcnow()
        timestamp = now.strftime("%Y%m%d_%H%M%S_%f")[:-3]  # ms precision
        date_dir = now.strftime("%Y/%m/%d")
        sanitized_city = city.replace(" ", "_")
        filename = f"{DATA_LAKE_DIR}/{date_dir}/{sanitized_city}_{timestamp}.json"
        atomic_write_json(filename, data)
        logger.info(f"Saved raw data for {city} to data lake: {filename}")
    except Exception as e:
        logger.exception(f"Failed to save data to data lake for {city}: {e}")


# ---------------------------
# Fetching and insertion
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
    """
    Insert processed data into DB. Column order matches schema with 'country' last.
    Uses INSERT OR IGNORE to avoid duplicate on same city+timestamp.
    """
    try:
        cur = conn.cursor()
        main = data.get("main", {})
        coord = data.get("coord", {})
        weather = data.get("weather", [{}])[0] if data.get("weather") else {}
        wind = data.get("wind", {})
        clouds = data.get("clouds", {})
        sys = data.get("sys", {})

        # We will explicitly prepare the timestamp in ISO format with ms to guarantee uniqueness precision
        ts = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(timespec="milliseconds")

        cur.execute(
            """
            INSERT OR IGNORE INTO weather_data
                (city, latitude, longitude, temperature, feels_like, temp_min, temp_max,
                 pressure, humidity, visibility, wind_speed, wind_deg, wind_gust,
                 clouds, weather_id, weather_main, weather_description, weather_icon, timestamp, country)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
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
                sys.get("country"),
            ),
        )
        conn.commit()
        logger.info("Weather data inserted for %s (ts=%s)", data.get("name"), ts)
    except Exception:
        logger.exception("Failed to insert data for %s", data.get("name"))
        try:
            conn.rollback()
        except Exception:
            pass


# ---------------------------
# Processing loop
# ---------------------------
def process_cities_once(conn: sqlite3.Connection, cities: List[str]) -> Dict[str, int]:
    """
    Process list of cities once. Returns dict with counts.
    """
    success_count = 0
    failure_count = 0

    for city in cities:
        if shutdown_requested:
            logger.info("Shutdown requested; stopping current processing loop early.")
            break

        logger.info("Processing city: %s", city)
        try:
            data = fetch_weather_data(city)
            if not data:
                logger.warning("No data returned for %s", city)
                failure_count += 1
                continue

            # Save raw response
            try:
                save_to_datalake(data, city)
            except Exception:
                logger.exception("Failed saving raw data for %s", city)

            # Insert processed data
            insert_weather_data(conn, data)
            success_count += 1

            # be polite to API
            time.sleep(1)
        except Exception:
            logger.exception("Unexpected error processing %s", city)
            failure_count += 1

    return {"success": success_count, "failure": failure_count}


# ---------------------------
# CLI helpers
# ---------------------------
def get_cities_from_file(filename: str) -> List[str]:
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logger.error("Cities file not found: %s", filename)
        return []
    except Exception:
        logger.exception("Failed to read cities from file: %s", filename)
        return []


# ---------------------------
# Main
# ---------------------------
def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Weather Data Ingestion Pipeline (Polling)")
    parser.add_argument("--cities", nargs="+", help="List of cities to process (e.g. Nairobi London)")
    parser.add_argument("--file", help="File containing list of cities (one per line)")
    parser.add_argument("--interval", type=int, default=POLLING_INTERVAL_SECONDS, help="Polling interval in seconds")
    args = parser.parse_args(argv)

    cities: List[str] = []
    if args.cities:
        cities.extend(args.cities)
    if args.file:
        cities_from_file = get_cities_from_file(args.file)
        cities.extend(cities_from_file)

    # If still empty, abort (avoid blocking input())
    if not cities:
        logger.error("No cities provided. Use --cities or --file. Exiting.")
        return 2

    interval = max(1, args.interval)

    # Setup DB once
    conn = setup_database(DB_PATH)
    logger.info("Database initialized at %s", DB_PATH)
    logger.info("Starting continuous polling for %d cities every %d seconds.", len(cities), interval)
    logger.info("Cities: %s", ", ".join(cities))

    try:
        while not shutdown_requested:
            cycle_start = time.time()
            logger.info("--- Starting new polling cycle at %s ---", datetime.utcnow().isoformat())

            counts = process_cities_once(conn, cities)
            logger.info("Cycle results: success=%d, failures=%d", counts["success"], counts["failure"])

            cycle_end = time.time()
            duration = cycle_end - cycle_start
            sleep_time = interval - duration

            if shutdown_requested:
                logger.info("Shutdown requested; breaking main loop before sleep.")
                break

            if sleep_time > 0:
                logger.info("Cycle finished in %.2f seconds. Sleeping for %.2f seconds.", duration, sleep_time)
                # Sleep in chunks so we can respond reasonably quickly to signals
                slept = 0.0
                while slept < sleep_time and not shutdown_requested:
                    to_sleep = min(1.0, sleep_time - slept)
                    time.sleep(to_sleep)
                    slept += to_sleep
            else:
                logger.warning(
                    "Processing cycle took longer than interval (%.2fs > %ds). Starting next cycle immediately.",
                    duration,
                    interval,
                )
    except Exception:
        logger.exception("Fatal error in main loop")
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
