import streamlit as st
import sqlite3
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
from plotly.subplots import make_subplots

# Try to import Plotly with fallback
try:
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except Exception:
    PLOTLY_AVAILABLE = False

# OpenWeather API key
API_KEY = "eea5f0d577d8539c9f2732e5d1f735d5"
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
FORECAST_URL = "http://api.openweathermap.org/data/2.5/forecast"

# SQLite database
DB_PATH = "weather_data.db"

# Page configuration
st.set_page_config(
    page_title="Advanced Weather Dashboard", 
    page_icon="🌍", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        margin-bottom: 10px;
    }
    .weather-icon {
        font-size: 2rem;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

# Title with custom styling
st.markdown('<h1 class="main-header">🌍 Advanced Weather Dashboard</h1>', unsafe_allow_html=True)

# Sidebar for city selection and options
st.sidebar.header("🌤️ Dashboard Settings")
city = st.sidebar.text_input("Enter a city name:", placeholder="e.g., London, Tokyo, New York")

# Add date range selector for historical data
st.sidebar.subheader("📅 Date Range")
days_back = st.sidebar.slider("Show data from last (days):", 1, 30, 7)

# Add visualization options
st.sidebar.subheader("📊 Chart Options")
show_forecast = st.sidebar.checkbox("Show 5-day forecast", value=True)
show_comparison = st.sidebar.checkbox("Show city comparison", value=False)

if city:
    # Create tabs for better organization
    tab1, tab2, tab3 = st.tabs(["Current Weather", "Historical Trends", "Forecast & Analysis"])
    
    with tab1:
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("📡 Live Weather Data")
            try:
                params = {"q": city, "appid": API_KEY, "units": "metric"}
                response = requests.get(BASE_URL, params=params, timeout=10)

                if response.status_code == 200:
                    data = response.json()

                    # Convert UTC timestamp to datetime with proper timezone
                    utc_time = datetime.fromtimestamp(data["dt"], tz=timezone.utc)
                    local_time = utc_time + timedelta(seconds=data["timezone"])

                    # Extract details
                    main = data["main"]
                    weather = data["weather"][0]
                    wind = data.get("wind", {})
                    clouds = data.get("clouds", {})
                    sys = data.get("sys", {})
                    
                    # Display weather icon based on condition
                    weather_icon = "🌤️"  # default
                    if "cloud" in weather["description"].lower():
                        weather_icon = "☁️"
                    elif "rain" in weather["description"].lower():
                        weather_icon = "🌧️"
                    elif "sun" in weather["description"].lower():
                        weather_icon = "☀️"
                    elif "snow" in weather["description"].lower():
                        weather_icon = "❄️"
                    
                    st.markdown(f'<div class="weather-icon">{weather_icon}</div>', unsafe_allow_html=True)
                    
                    # Create metrics with better styling
                    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                    st.metric("Temperature", f"{main['temp']} °C", f"Feels like: {main.get('feels_like', 'N/A')}°C")
                    st.markdown('</div>', unsafe_allow_html=True)
                    
                    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                    st.metric("Humidity", f"{main['humidity']}%")
                    st.metric("Pressure", f"{main['pressure']} hPa")
                    st.markdown('</div>', unsafe_allow_html=True)
                    
                    if wind:
                        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                        st.metric("Wind Speed", f"{wind.get('speed', 'N/A')} m/s")
                        if 'deg' in wind:
                            st.metric("Wind Direction", f"{wind['deg']}°")
                        st.markdown('</div>', unsafe_allow_html=True)
                    
                    if clouds:
                        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                        st.metric("Cloudiness", f"{clouds.get('all', 'N/A')}%")
                        st.markdown('</div>', unsafe_allow_html=True)
                    
                    st.write(f"**Conditions:** {weather['description'].title()}")
                    st.write(f"**Visibility:** {data.get('visibility', 'N/A')} meters")
                    st.write(f"**Local Time:** {local_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    if sys:
                        sunrise = datetime.fromtimestamp(sys.get('sunrise', 0), tz=timezone.utc) + timedelta(seconds=data["timezone"])
                        sunset = datetime.fromtimestamp(sys.get('sunset', 0), tz=timezone.utc) + timedelta(seconds=data["timezone"])
                        st.write(f"**Sunrise:** {sunrise.strftime('%H:%M:%S')}")
                        st.write(f"**Sunset:** {sunset.strftime('%H:%M:%S')}")

                else:
                    st.error(f"City not found in OpenWeather API. Status code: {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                st.error(f"Error fetching weather data: {str(e)}")
        
        with col2:
            st.subheader("📍 Location Map")
            try:
                # Get coordinates for map
                params = {"q": city, "appid": API_KEY}
                response = requests.get(BASE_URL, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    lat = data["coord"]["lat"]
                    lon = data["coord"]["lon"]
                    
                    # Create a map
                    map_data = pd.DataFrame({
                        'lat': [lat],
                        'lon': [lon],
                        'city': [city]
                    })
                    
                    st.map(map_data, zoom=10)
                    
                    # Display coordinates
                    st.write(f"**Latitude:** {lat}, **Longitude:** {lon}")
                    
            except Exception as e:
                st.warning(f"Could not display map: {str(e)}")
    
    with tab2:
        st.subheader("📈 Historical Trends")
        try:
            conn = sqlite3.connect(DB_PATH)
            
            # Calculate date threshold
            threshold_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d %H:%M:%S')
            
            # Get historical data
            query = """
            SELECT datetime(timestamp) as date, temperature, humidity, condition 
            FROM weather_data 
            WHERE city = ? AND timestamp >= ?
            ORDER BY timestamp ASC
            """
            df = pd.read_sql_query(query, conn, params=(city, threshold_date))
            conn.close()

            if not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                
                # Create subplots with secondary y-axis
                fig = make_subplots(
                    rows=2, cols=1,
                    subplot_titles=('Temperature Trend', 'Humidity Trend'),
                    vertical_spacing=0.1
                )
                
                # Add temperature trace
                fig.add_trace(
                    go.Scatter(x=df['date'], y=df['temperature'], name="Temperature", line=dict(color='red')),
                    row=1, col=1
                )
                
                # Add humidity trace
                fig.add_trace(
                    go.Scatter(x=df['date'], y=df['humidity'], name="Humidity", line=dict(color='blue')),
                    row=2, col=1
                )
                
                # Update layout
                fig.update_layout(
                    height=600,
                    showlegend=True,
                    title_text=f"Weather Trends for {city} (Last {days_back} days)"
                )
                
                # Update y-axes titles
                fig.update_yaxes(title_text="Temperature (°C)", row=1, col=1)
                fig.update_yaxes(title_text="Humidity (%)", row=2, col=1)
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Show statistics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Avg Temperature", f"{df['temperature'].mean():.1f} °C")
                with col2:
                    st.metric("Max Temperature", f"{df['temperature'].max():.1f} °C")
                with col3:
                    st.metric("Min Temperature", f"{df['temperature'].min():.1f} °C")
                
                # Show data table
                if st.checkbox("Show raw historical data"):
                    st.dataframe(df)
            else:
                st.info("No historical data found for this city in the selected time range.")
                
        except sqlite3.Error as e:
            st.error(f"Database error: {str(e)}")
    
    with tab3:
        if show_forecast:
            st.subheader("🔮 5-Day Weather Forecast")
            try:
                params = {"q": city, "appid": API_KEY, "units": "metric"}
                response = requests.get(FORECAST_URL, params=params, timeout=10)
                
                if response.status_code == 200:
                    forecast_data = response.json()
                    
                    # Process forecast data
                    forecast_list = forecast_data['list']
                    forecast_dates = []
                    forecast_temps = []
                    forecast_humidity = []
                    forecast_descriptions = []
                    
                    for forecast in forecast_list:
                        forecast_dates.append(forecast['dt_txt'])
                        forecast_temps.append(forecast['main']['temp'])
                        forecast_humidity.append(forecast['main']['humidity'])
                        forecast_descriptions.append(forecast['weather'][0]['description'])
                    
                    # Create forecast DataFrame
                    forecast_df = pd.DataFrame({
                        'date': forecast_dates,
                        'temperature': forecast_temps,
                        'humidity': forecast_humidity,
                        'description': forecast_descriptions
                    })
                    forecast_df['date'] = pd.to_datetime(forecast_df['date'])
                    
                    # Display forecast chart
                    fig = px.line(forecast_df, x='date', y='temperature',
                                title=f'5-Day Temperature Forecast for {city}',
                                labels={'temperature': 'Temperature (°C)', 'date': 'Date'})
                    fig.update_traces(line=dict(color='red'))
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Show forecast table
                    st.write("Detailed Forecast:")
                    forecast_df_display = forecast_df.copy()
                    forecast_df_display['date'] = forecast_df_display['date'].dt.strftime('%Y-%m-%d %H:%M')
                    st.dataframe(forecast_df_display[['date', 'temperature', 'humidity', 'description']])
                    
                else:
                    st.error("Could not fetch forecast data.")
            except Exception as e:
                st.error(f"Error fetching forecast: {str(e)}")
        
        if show_comparison:
            st.subheader("🏙️ City Comparison")
            try:
                conn = sqlite3.connect(DB_PATH)
                
                # Get all available cities
                cities_query = "SELECT DISTINCT city FROM weather_data WHERE city != ? LIMIT 5"
                other_cities = pd.read_sql_query(cities_query, conn, params=(city,))['city'].tolist()
                
                if other_cities:
                    comparison_cities = st.multiselect("Select cities to compare:", other_cities, default=other_cities[:2])
                    
                    if comparison_cities:
                        # Get comparison data
                        placeholders = ','.join('?' * len(comparison_cities))
                        comp_query = f"""
                        SELECT city, datetime(timestamp) as date, temperature 
                        FROM weather_data 
                        WHERE city IN ({placeholders}) 
                        AND date(timestamp) = date('now')
                        ORDER BY timestamp DESC
                        """
                        comp_df = pd.read_sql_query(comp_query, conn, params=comparison_cities)
                        
                        if not comp_df.empty:
                            comp_df['date'] = pd.to_datetime(comp_df['date'])
                            
                            # Pivot for comparison
                            pivot_df = comp_df.pivot(index='date', columns='city', values='temperature')
                            
                            fig = px.line(pivot_df, title=f"Temperature Comparison with {city}",
                                        labels={'value': 'Temperature (°C)', 'date': 'Time'})
                            st.plotly_chart(fig, use_container_width=True)
                
                conn.close()
                
            except Exception as e:
                st.error(f"Error in city comparison: {str(e)}")

else:
    st.info("👈 Please enter a city name in the sidebar to get started!")
    
    # Show available cities in database
    try:
        conn = sqlite3.connect(DB_PATH)
        available_cities = pd.read_sql_query("SELECT DISTINCT city FROM weather_data ORDER BY city", conn)
        conn.close()
        
        if not available_cities.empty:
            st.subheader("🌆 Cities in Database")
            st.write("You can view historical data for these cities:")
            cols = st.columns(3)
            for i, city in enumerate(available_cities['city']):
                cols[i % 3].button(city, on_click=lambda c=city: st.session_state.update({"selected_city": c}))
                
            if "selected_city" in st.session_state:
                st.query_params(city=st.session_state["selected_city"])
                st.rerun()

                
    except:
        pass

# Add footer with last update time
st.sidebar.markdown("---")
st.sidebar.info(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Add refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.rerun()
