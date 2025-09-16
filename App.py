import streamlit as st
import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

# Try to import Plotly with fallback
try:
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    st.warning("Plotly not available. Using basic charts.")

# OpenWeather API key
API_KEY = "eea5f0d577d8539c9f2732e5d1f735d5"
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

# SQLite database
DB_PATH = "weather_data.db"

st.set_page_config(page_title="Weather Dashboard", page_icon="🌍", layout="wide")
st.title("🌍 Weather Dashboard")

# User input city
city = st.text_input("Enter a city:")

if city:
    col1, col2 = st.columns(2)
    
    with col1:
        # --- Fetch live weather data ---
        st.subheader("📡 Live Weather")
        try:
            params = {"q": city, "appid": API_KEY, "units": "metric"}
            response = requests.get(BASE_URL, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()

                # Convert UTC timestamp to datetime with proper timezone
                utc_time = datetime.fromtimestamp(data["dt"], tz=timezone.utc)
                local_time = utc_time + timedelta(seconds=data["timezone"])

                # Extract details
                temp = data["main"]["temp"]
                humidity = data["main"]["humidity"]
                conditions = data["weather"][0]["description"].title()

                st.metric("Temperature", f"{temp} °C")
                st.metric("Humidity", f"{humidity} %")
                st.metric("Conditions", conditions)
                st.write(f"**Local Time:** {local_time.strftime('%Y-%m-%d %H:%M:%S')}")

            else:
                st.error(f"City not found. Status code: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            st.error(f"Error fetching weather data: {str(e)}")
    
    with col2:
        # --- Fetch historical data from SQLite ---
        st.subheader("📜 Historical Data")
        try:
            conn = sqlite3.connect(DB_PATH)
            query = "SELECT city, temperature, humidity, timestamp FROM weather_data WHERE city = ? ORDER BY timestamp DESC LIMIT 50"
            df = pd.read_sql_query(query, conn, params=(city,))
            conn.close()

            if not df.empty:
                # Convert timestamp to datetime for better plotting
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                if PLOTLY_AVAILABLE:
                    # Use Plotly for better charts
                    fig = px.line(df, x='timestamp', y='temperature', title='Temperature Trend')
                    st.plotly_chart(fig)
                    
                    fig2 = px.line(df, x='timestamp', y='humidity', title='Humidity Trend')
                    st.plotly_chart(fig2)
                else:
                    # Fallback to Streamlit's native charts
                    st.line_chart(df.set_index('timestamp')['temperature'])
                    st.line_chart(df.set_index('timestamp')['humidity'])
                
                # Show data table
                if st.checkbox("Show raw data"):
                    st.dataframe(df)
            else:
                st.info("No historical data found for this city.")
                
        except sqlite3.Error as e:
            st.error(f"Database error: {str(e)}")

else:
    st.info("Please enter a city name to get started.")

# Add refresh button
if st.button("🔄 Refresh"):
    st.experimental_rerun()