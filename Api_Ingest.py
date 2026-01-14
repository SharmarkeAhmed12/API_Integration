#!/usr/bin/env python3
"""
Kafka-based weather ingestion service
- Runs once every 24 hours
- Cities loaded dynamically from file
- Produces to Kafka topic `weather_raw`
- Appends historical data to SQLite
- Writes raw JSON to datalake
"""

import json
import logging
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# ===========================
# Configuration
# ===========================
INTERVAL_SECONDS = 24 * 60 * 60  # 24 hours

BASE_DIR = Path(__file__).parent

# Load .env from same folder as script
dotenv_path = BASE_DIR / "Secrets.env"
load_dotenv(dotenv_path)

API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise RuntimeError("OPENWEATHER_API_KEY not found")

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

DB_PATH = BASE_DIR / "weather_data.db"
DATA_LAKE_DIR = BASE_DIR / "datalake/raw"

CITIES_FILE = BASE_DIR / "cities.txt"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather_raw")

# ===========================
# Logging
# ===========================
LOG_FILE = BASE_DIR / "weather_pipeline.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("weather-stream")

# ===========================
# Kafka Producer
# ===========================
def create_producer():
    """Create Kafka producer with retry logic"""
    for attempt in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=3,
            )
            logger.info("Kafka producer connected successfully")
            return producer
        except Exception as e:
            logger.warning(f"Connection attempt {attempt + 1}/5 failed: {e}")
            if attempt < 4:
                time.sleep(2 ** attempt)  # exponential backoff
            else:
                raise

# NOTE: Do NOT create a producer at import time. Creating network clients
# during module import causes side effects when Airflow imports this file.
# We'll create a producer inside each run cycle.

# ===========================
# Utilities
# ===========================
def load_cities(path: Path) -> List[str]:
    if not path.exists():
        raise RuntimeError(f"Cities file not found: {path}")
    with open(path) as f:
        cities = [line.strip() for line in f if line.strip()]
    if not cities:
        raise RuntimeError("Cities file is empty")
    return cities

# ===========================
# Database
# ===========================
def setup_database(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
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

def save_to_db(conn: sqlite3.Connection, data: Dict[str, Any]) -> None:
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO weather_data (
            city, latitude, longitude, temperature, feels_like,
            temp_min, temp_max, pressure, humidity, visibility,
            wind_speed, wind_deg, wind_gust, clouds,
            weather_id, weather_main, weather_description, weather_icon,
            timestamp, country
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["name"],
        data["coord"]["lat"],
        data["coord"]["lon"],
        data["main"]["temp"],
        data["main"].get("feels_like"),
        data["main"].get("temp_min"),
        data["main"].get("temp_max"),
        data["main"].get("pressure"),
        data["main"].get("humidity"),
        data.get("visibility"),
        data.get("wind", {}).get("speed"),
        data.get("wind", {}).get("deg"),
        data.get("wind", {}).get("gust"),
        data.get("clouds", {}).get("all"),
        data["weather"][0]["id"],
        data["weather"][0]["main"],
        data["weather"][0]["description"],
        data["weather"][0]["icon"],
        datetime.fromtimestamp(data["dt"], tz=timezone.utc).isoformat(),
        data.get("sys", {}).get("country"),
    ))
    conn.commit()

# ===========================
# Datalake
# ===========================
def atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    os.makedirs(path.parent, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".json")
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
    filename = DATA_LAKE_DIR / date_path / f"{city.replace(' ', '_')}_{ts}.json"
    atomic_write_json(filename, data)

# ===========================
# API + Kafka
# ===========================
def fetch_weather(city: str) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(
            BASE_URL,
            params={"q": city, "appid": API_KEY, "units": "metric"},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error("API error for %s: %s", city, e)
        return None

def ingest_cycle(conn: sqlite3.Connection) -> None:
    cities = load_cities(CITIES_FILE)
    logger.info("Starting ingestion cycle for cities: %s", ", ".join(cities))

    # create a producer for this cycle (so importing this module is side-effect free)
    producer = create_producer()

    for city in cities:
        data = fetch_weather(city)
        if not data:
            continue

        # Retry sending to Kafka if topic not ready
        for attempt in range(5):
            try:
                producer.send(TOPIC, value=data)
                break
            except Exception as e:
                logger.warning(f"Send attempt {attempt + 1}/5 failed: {e}")
                if attempt < 4:
                    time.sleep(1)
                else:
                    logger.error(f"Failed to send {city} to Kafka after 5 attempts")
                    raise
        
        save_to_db(conn, data)
        save_to_datalake(data, city)

        logger.info("Ingested weather for %s", city)

    # flush and close producer for this cycle
    try:
        producer.flush()
        producer.close()
    except Exception:
        logger.warning("Error flushing/closing Kafka producer")

# ===========================
# Main Loop
# ===========================
def main() -> None:
    logger.info("Starting weather ingestion service (24h interval)")
    conn = setup_database(DB_PATH)

    try:
        while True:
            start = time.time()
            ingest_cycle(conn)

            elapsed = time.time() - start
            sleep_for = max(0, INTERVAL_SECONDS - elapsed)

            logger.info("Cycle complete. Sleeping for %.1f seconds", sleep_for)
            time.sleep(sleep_for)

    except KeyboardInterrupt:
        logger.info("Shutdown signal received")

    finally:
        conn.close()
        logger.info("Service stopped cleanly")


def run_once() -> None:
    """Run a single ingestion cycle and exit (useful for testing or Airflow)."""
    conn = setup_database(DB_PATH)
    try:
        ingest_cycle(conn)
    finally:
        conn.close()

if __name__ == "__main__":
    # Allow running a single cycle with `--once` for testing / Airflow integration
    import argparse

    parser = argparse.ArgumentParser(description="Run weather ingestion")
    parser.add_argument("--once", action="store_true", help="Run a single ingestion cycle and exit")
    args = parser.parse_args()

    if args.once:
        run_once()
    else:
        main()
