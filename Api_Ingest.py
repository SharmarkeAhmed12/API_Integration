#!/usr/bin/env python3
"""
Weather ingestion pipeline as Kafka producer
- Sends data to Kafka topic `weather_raw`
- Saves data to SQLite database and datalake
- Supports multiple runs (historical data)
"""

import argparse
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
# Environment & Configuration
# ===========================
dotenv_path = Path(r"C:\API_Integration\Secrets.env")
load_dotenv(dotenv_path=dotenv_path)

API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise RuntimeError("OPENWEATHER_API_KEY not found in .env file")

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "weather_data.db")
DATA_LAKE_DIR = os.path.join(BASE_DIR, "datalake", "raw")

# Kafka configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather_raw")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# ===========================
# Logging
# ===========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("weather_pipeline.log"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("weather_pipeline")

# ===========================
# Database Setup
# ===========================
def setup_database(path: str) -> sqlite3.Connection:
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
        data.get("sys", {}).get("country")
    ))
    conn.commit()

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

    filename = os.path.join(DATA_LAKE_DIR, date_path, f"{city.replace(' ', '_')}_{ts}.json")
    atomic_write_json(filename, data)
    logger.info("Raw data saved: %s", filename)

# ===========================
# API Fetch
# ===========================
def fetch_weather(city: str) -> Optional[Dict[str, Any]]:
    try:
        resp = requests.get(BASE_URL, params={"q": city, "appid": API_KEY, "units": "metric"}, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.error("API error for %s: %s", city, exc)
        return None

# ===========================
# Kafka Producer
# ===========================
def send_to_kafka(city: str, conn: sqlite3.Connection) -> None:
    data = fetch_weather(city)
    if not data:
        return
    # Send to Kafka
    producer.send(TOPIC, value=data)
    logger.info("Sent weather for %s to Kafka", city)
    # Save to datalake
    save_to_datalake(data, city)
    # Save to DB (append new record)
    save_to_db(conn, data)

# ===========================
# Main
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

    logger.info("Kafka Producer started for cities: %s", ", ".join(cities))

    conn = setup_database(DB_PATH)

    try:
        for city in cities:
            send_to_kafka(city, conn)
            time.sleep(1)  # avoid overwhelming API
    finally:
        conn.close()
        producer.flush()
        producer.close()
        logger.info("Producer stopped cleanly")

if __name__ == "__main__":
    main()
