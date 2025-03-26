import requests
import pandas as pd 
import streamlit as st 
import pyodbc 
import datetime

st.title("ETL Weather Pipeline")

API_KEY = "bbac60ecf85dd87030a8a9ec2f42989f"
CITY = "Windsor"
API_URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

DB_Connection = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=DESKTOP-4CMUAR0\\SQLEXPRESS;DATABASE=Weather_Database;TrustServerCertificate=yes;Trusted_Connection=yes;"

def run_etl():
    try:
        response = requests.get(API_URL)
        data = response.json()

        weather_data = {
            "city": CITY,
            "temperature_C": data["main"]["temp"],
            "temperature_F": data["main"]["temp"] * 9/5 + 32,
            "humidity": data["main"]["humidity"],
            "pressure": data["main"]["pressure"],
            "weather_description": data["weather"][0]["description"],
            "wind_speed": data["wind"]["speed"],
            "dew_point": data["main"].get("dew_point", None),
            "Feels_Like_C": data["main"]["feels_like"],
            "Rain_mm": data.get("rain", {}).get("1h", 0),
            "Snow_mm": data.get("snow", {}).get("1h", 0),
            "Cloud_Cover_Percent": data["clouds"]["all"],
            "Wind_Deg": data["wind"]["deg"],
            "Wind_Gust": data["wind"].get("gust", None),
            "Min_Temp_C": data["main"]["temp_min"],
            "Max_Temp_C": data["main"]["temp_max"],
            "date_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        conn = pyodbc.connect(DB_Connection)
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO WeatherData (city, temperature_C, temperature_F, humidity, pressure, weather_description, wind_speed, 
                                 dew_point, Feels_Like_C, Rain_mm, Snow_mm, Cloud_Cover_Percent, Wind_Deg, Wind_Gust, 
                                 Min_Temp_C, Max_Temp_C, date_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        cursor.execute(insert_query, weather_data["city"], weather_data["temperature_C"], weather_data["temperature_F"], 
                       weather_data["humidity"], weather_data["pressure"], weather_data["weather_description"], 
                       weather_data["wind_speed"], weather_data["dew_point"], weather_data["Feels_Like_C"], 
                       weather_data["Rain_mm"], weather_data["Snow_mm"], weather_data["Cloud_Cover_Percent"], 
                       weather_data["Wind_Deg"], weather_data["Wind_Gust"], weather_data["Min_Temp_C"], 
                       weather_data["Max_Temp_C"], weather_data["date_time"])

        conn.commit()
        cursor.close()
        conn.close()

        return "Success"

    except Exception as e:
        return f"Failed - {str(e)}"

if st.button("Run ETL Pipeline"):
    result = run_etl()
    st.write("ETL Status: ", result)

st.write("Click the button above to trigger the ETL pipeline")
