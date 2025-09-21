# 🌦️ Weather Data Pipeline & Dashboard

This project fetches real-time weather data from the **OpenWeather API**, stores it in a local SQLite database, saves raw API responses to a local "data lake", and provides a **Streamlit dashboard** to explore and visualize the data.

---

## 📂 Project Structure

.
├── datalake/ # Raw API JSON responses stored by date
├── weather_data.db # SQLite database
├── weather_pipeline.py # Main ingestion pipeline script
├── visualize.py # Streamlit dashboard
├── .gitignore # Git ignore file
├── README.md # Project documentation
└── requirements.txt # Dependencies

