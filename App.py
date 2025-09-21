# App.py
import streamlit as st
import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

# Try to import Plotly with fallback
try:
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except Exception:
    PLOTLY_AVAILABLE = False

# Config
API_KEY = "eea5f0d577d8539c9f2732e5d1f735d5"  # replace if needed
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
FORECAST_URL = "http://api.openweathermap.org/data/2.5/forecast"
DB_PATH = "weather_data.db"

st.set_page_config(page_title="Weather Dashboard", page_icon="🌍", layout="wide")
st.title("🌍 Weather Dashboard")

# ---------- Helpers ----------
def get_live_weather(city: str, api_key: str = API_KEY) -> dict | None:
    """Fetch live weather; returns dict or None on failure."""
    params = {"q": city, "appid": api_key, "units": "metric"}
    try:
        r = requests.get(BASE_URL, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        # compute timezone-aware local time
        utc_time = datetime.fromtimestamp(data.get("dt", datetime.utcnow().timestamp()), tz=timezone.utc)
        tz_offset = int(data.get("timezone", 0))
        local_time = utc_time + timedelta(seconds=tz_offset)
        return {
            "ok": True,
            "data": data,
            "local_time": local_time,
            "lat": data.get("coord", {}).get("lat"),
            "lon": data.get("coord", {}).get("lon")
        }
    except requests.RequestException as e:
        return {"ok": False, "error": str(e)}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@st.cache_data(ttl=300)
def load_historical(city: str, limit: int = 200) -> pd.DataFrame:
    """Load historical records for a city from SQLite safely."""
    try:
        conn = sqlite3.connect(DB_PATH)
        query = "SELECT * FROM weather_data WHERE city = ? ORDER BY timestamp DESC LIMIT ?"
        df = pd.read_sql_query(query, conn, params=(city, limit))
        conn.close()
    except Exception:
        # return empty df with no crash
        return pd.DataFrame()

    if df.empty:
        return df

    # normalize common column name variants
    # ensure timestamp column exists and is datetime
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    elif "date" in df.columns:
        df["timestamp"] = pd.to_datetime(df["date"], errors="coerce")
    else:
        # try to find any datetime-like column
        for c in df.columns:
            if "time" in c.lower() or "date" in c.lower():
                df["timestamp"] = pd.to_datetime(df[c], errors="coerce")
                break

    # Accept either 'temperature' or 'temp', etc.
    # create canonical names
    if "temperature" not in df.columns and "temp" in df.columns:
        df = df.rename(columns={"temp": "temperature"})
    if "condition" not in df.columns and "weather_description" in df.columns:
        df = df.rename(columns={"weather_description": "condition"})
    if "condition" not in df.columns and "weather_main" in df.columns:
        df = df.rename(columns={"weather_main": "condition"})
    if "humidity" not in df.columns and "hum" in df.columns:
        df = df.rename(columns={"hum": "humidity"})

    # Ensure timestamp sorted ascending for plotting
    if "timestamp" in df.columns:
        df = df.sort_values("timestamp")
    return df

def safe_get_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None

# ---------- UI ----------
city = st.text_input("Enter a city (e.g. Nairobi, London):").strip()

if city:
    col1, col2 = st.columns([1, 1])

    # LIVE WEATHER
    with col1:
        st.subheader("📡 Live Weather")
        live = get_live_weather(city)
        if not live:
            st.error("Unexpected error getting live weather.")
        elif not live.get("ok"):
            st.error(f"Could not fetch live data: {live.get('error')}")
        else:
            data = live["data"]
            lt = live["local_time"]
            main = data.get("main", {})
            weather = data.get("weather", [{}])[0]
            wind = data.get("wind", {})
            clouds = data.get("clouds", {})

            temp = main.get("temp")
            humidity = main.get("humidity")
            feels_like = main.get("feels_like")
            desc = weather.get("description", "").title()

            st.metric("Temperature", f"{temp} °C", delta=f"Feels {feels_like} °C" if feels_like is not None else "")
            st.metric("Humidity", f"{humidity} %")
            st.write(f"**Conditions:** {desc}")
            st.write(f"**Local Time:** {lt.strftime('%Y-%m-%d %H:%M:%S')}")

            # Map if coordinates exist
            lat, lon = live.get("lat"), live.get("lon")
            if lat is not None and lon is not None:
                map_df = pd.DataFrame({"lat": [lat], "lon": [lon]})
                st.map(map_df, zoom=8)
            else:
                st.write("Location coordinates not available.")

    # HISTORICAL DATA
    with col2:
        st.subheader("📜 Historical Data (from DB)")
        df_hist = load_historical(city)

        if df_hist.empty:
            st.info("No historical records found for this city in the database.")
        else:
            # pick a temperature column candidate
            temp_col = safe_get_col(df_hist, ["temperature", "temp", "temp_c"])
            hum_col = safe_get_col(df_hist, ["humidity", "hum"])
            ts_col = "timestamp" if "timestamp" in df_hist.columns else None

            # show recent table
            st.write("Latest records:")
            st.dataframe(df_hist.tail(20).sort_values(ts_col if ts_col else df_hist.columns[0], ascending=False))

            # Plotting
            if temp_col and ts_col:
                if PLOTLY_AVAILABLE:
                    fig = px.line(df_hist, x=ts_col, y=temp_col, title=f"Temperature trend - {city}")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.line_chart(df_hist.set_index(ts_col)[temp_col])

            if hum_col and ts_col:
                if PLOTLY_AVAILABLE:
                    fig2 = px.line(df_hist, x=ts_col, y=hum_col, title=f"Humidity trend - {city}")
                    st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.line_chart(df_hist.set_index(ts_col)[hum_col])

            # Option to show raw JSON of last saved raw file preview (if present)
            if "weather_description" in df_hist.columns or "condition" in df_hist.columns:
                st.write("Sample condition values:")
                cond_col = safe_get_col(df_hist, ["condition", "weather_description", "weather_main"])
                st.write(df_hist[cond_col].value_counts().head(10))

    # Refresh / rerun button
    if st.button("🔄 Refresh"):
        # NOTE: use st.rerun() — experimental_rerun is deprecated
        st.rerun()
else:
    st.info("Please enter a city to view live and historical weather data.")
