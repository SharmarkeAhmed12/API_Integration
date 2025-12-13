import streamlit as st
import sqlite3
import requests
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), "Secrets.env"))
API_KEY = os.getenv("OPENWEATHER_API_KEY")

BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
FORECAST_URL = "http://api.openweathermap.org/data/2.5/forecast"
DB_PATH = "weather_data.db"

st.set_page_config(
    page_title="Advanced Weather Dashboard",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom styling
st.markdown("""
<style>
    .main-header {font-size:3rem;color:#1f77b4;text-align:center;}
    .metric-card {background-color:#f0f2f6;padding:15px;border-radius:10px;margin-bottom:10px;}
    .weather-icon {font-size:2rem;text-align:center;}
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="main-header">🌍 Advanced Weather Dashboard</h1>', unsafe_allow_html=True)

# Sidebar
st.sidebar.header("🌤️ Dashboard Settings")
city = st.sidebar.text_input("Enter a city name:", placeholder="e.g., London, Tokyo, New York")

st.sidebar.subheader("📅 Date Range")
days_back = st.sidebar.slider("Show data from last (days):", 1, 30, 7)

st.sidebar.subheader("📊 Options")
show_forecast = st.sidebar.checkbox("Show 5-day forecast", value=True)
show_comparison = st.sidebar.checkbox("Show city comparison", value=False)

if city:
    tab1, tab2, tab3 = st.tabs(["Current Weather", "Historical Trends", "Forecast & Analysis"])
    
    # ================= Current Weather =================
    with tab1:
        col1, col2 = st.columns([1, 2])
        with col1:
            st.subheader("📡 Live Weather Data")
            try:
                resp = requests.get(BASE_URL, params={"q": city, "appid": API_KEY, "units":"metric"}, timeout=10)
                resp.raise_for_status()
                data = resp.json()

                utc_time = datetime.fromtimestamp(data["dt"], tz=timezone.utc)
                local_time = utc_time + timedelta(seconds=data["timezone"])
                main = data["main"]
                weather = data["weather"][0]
                wind = data.get("wind", {})
                clouds = data.get("clouds", {})
                sys_info = data.get("sys", {})

                # Weather icon
                icon = "🌤️"
                desc = weather.get("description", "").lower()
                if "cloud" in desc: icon="☁️"
                elif "rain" in desc: icon="🌧️"
                elif "sun" in desc: icon="☀️"
                elif "snow" in desc: icon="❄️"

                st.markdown(f'<div class="weather-icon">{icon}</div>', unsafe_allow_html=True)
                
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.metric("Temperature", f"{main['temp']} °C", f"Feels like: {main.get('feels_like','N/A')}°C")
                st.markdown('</div>', unsafe_allow_html=True)

                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.metric("Humidity", f"{main['humidity']}%")
                st.metric("Pressure", f"{main['pressure']} hPa")
                st.markdown('</div>', unsafe_allow_html=True)

                if wind:
                    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                    st.metric("Wind Speed", f"{wind.get('speed','N/A')} m/s")
                    if 'deg' in wind: st.metric("Wind Direction", f"{wind['deg']}°")
                    st.markdown('</div>', unsafe_allow_html=True)

                if clouds:
                    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                    st.metric("Cloudiness", f"{clouds.get('all','N/A')}%")
                    st.markdown('</div>', unsafe_allow_html=True)

                st.write(f"**Conditions:** {weather['description'].title()}")
                st.write(f"**Visibility:** {data.get('visibility','N/A')} meters")
                st.write(f"**Local Time:** {local_time.strftime('%Y-%m-%d %H:%M:%S')}")

                if sys_info:
                    sunrise = datetime.fromtimestamp(sys_info.get('sunrise',0), tz=timezone.utc)+timedelta(seconds=data["timezone"])
                    sunset = datetime.fromtimestamp(sys_info.get('sunset',0), tz=timezone.utc)+timedelta(seconds=data["timezone"])
                    st.write(f"**Sunrise:** {sunrise.strftime('%H:%M:%S')}")
                    st.write(f"**Sunset:** {sunset.strftime('%H:%M:%S')}")
            except Exception as e:
                st.error(f"Error fetching current weather: {str(e)}")

        with col2:
            st.subheader("📍 Location Map")
            try:
                resp = requests.get(BASE_URL, params={"q": city, "appid": API_KEY}, timeout=10)
                resp.raise_for_status()
                coords = resp.json().get("coord", {})
                if coords:
                    st.map(pd.DataFrame({'lat':[coords["lat"]],'lon':[coords["lon"]]}), zoom=10)
                    st.write(f"**Latitude:** {coords['lat']}, **Longitude:** {coords['lon']}")
            except Exception as e:
                st.warning(f"Map error: {str(e)}")

    # ================= Historical Trends =================
    with tab2:
        st.subheader("📈 Historical Trends")
        try:
            conn = sqlite3.connect(DB_PATH)
            threshold_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d %H:%M:%S')
            query = "SELECT datetime(timestamp) as date, temperature, humidity, weather_description FROM weather_data WHERE city=? AND timestamp>=? ORDER BY timestamp ASC"
            df = pd.read_sql_query(query, conn, params=(city, threshold_date))
            conn.close()

            if not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                fig = make_subplots(rows=2, cols=1, subplot_titles=("Temperature Trend","Humidity Trend"), vertical_spacing=0.1)
                fig.add_trace(go.Scatter(x=df['date'], y=df['temperature'], name="Temperature", line=dict(color='red')), row=1, col=1)
                fig.add_trace(go.Scatter(x=df['date'], y=df['humidity'], name="Humidity", line=dict(color='blue')), row=2, col=1)
                fig.update_yaxes(title_text="Temperature (°C)", row=1, col=1)
                fig.update_yaxes(title_text="Humidity (%)", row=2, col=1)
                fig.update_layout(height=600, title_text=f"Weather Trends for {city} (Last {days_back} days)")
                st.plotly_chart(fig, use_container_width=True)

                col1, col2, col3 = st.columns(3)
                col1.metric("Avg Temp", f"{df['temperature'].mean():.1f} °C")
                col2.metric("Max Temp", f"{df['temperature'].max():.1f} °C")
                col3.metric("Min Temp", f"{df['temperature'].min():.1f} °C")

                if st.checkbox("Show raw historical data"):
                    st.dataframe(df)
            else:
                st.info("No historical data for selected city in the given range.")
        except Exception as e:
            st.error(f"DB error: {str(e)}")

    # ================= Forecast =================
    with tab3:
        if show_forecast:
            st.subheader("🔮 5-Day Weather Forecast (Live)")
            try:
                resp = requests.get(FORECAST_URL, params={"q":city,"appid":API_KEY,"units":"metric"}, timeout=10)
                resp.raise_for_status()
                forecast = resp.json()
                forecast_df = pd.DataFrame([{
                    "date": f["dt_txt"],
                    "temperature": f["main"]["temp"],
                    "humidity": f["main"]["humidity"],
                    "weather_description": f["weather"][0]["description"]
                } for f in forecast['list']])
                forecast_df['date'] = pd.to_datetime(forecast_df['date'])

                fig = make_subplots(rows=2, cols=1, subplot_titles=("Temperature Forecast","Humidity Forecast"), vertical_spacing=0.1)
                fig.add_trace(go.Scatter(x=forecast_df['date'], y=forecast_df['temperature'], name="Temperature", line=dict(color='red')), row=1, col=1)
                fig.add_trace(go.Scatter(x=forecast_df['date'], y=forecast_df['humidity'], name="Humidity", line=dict(color='blue')), row=2, col=1)
                fig.update_yaxes(title_text="Temperature (°C)", row=1, col=1)
                fig.update_yaxes(title_text="Humidity (%)", row=2, col=1)
                fig.update_layout(height=600, title_text=f"5-Day Forecast for {city}")
                st.plotly_chart(fig, use_container_width=True)

                # Show forecast table
                df_display = forecast_df.copy()
                df_display['date'] = df_display['date'].dt.strftime('%Y-%m-%d %H:%M')
                st.dataframe(df_display[['date','temperature','humidity','weather_description']])
            except Exception as e:
                st.error(f"Forecast error: {str(e)}")
else:
    st.info("👈 Please enter a city name in the sidebar to get started!")
