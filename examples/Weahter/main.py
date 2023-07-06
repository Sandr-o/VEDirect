import requests
import json
import mysql.connector
from datetime import datetime
import time

def get_weather_from_api():
    url = "https://api.brightsky.dev/weather?lat=48.68182629342932&lon=10.155106811493186&date=2023-07-06"
    response = requests.get(url)
    json_response = response.json()
    return json_response["weather"]

# Verbindung zur MySQL-Datenbank herstellen
conn = mysql.connector.connect(
    host='127.0.0.1',
    user='dhbw2022',
    password='secret',
    port=32001,
    database='container'
)

def insert_weather_data(conn, timestamp_execution, timestamp_dataset, sunshine, temperature, cloud_cover, visibility, condition_status, solar, icon):
    try:
        # Cursor-Objekt erstellen
        cursor = conn.cursor()

        # SQL-Statement zum Einfügen des Datensatzes
        sql = """INSERT INTO weather_data
                 (timestamp_execution, timestamp_dataset, sunshine, temperature, cloud_cover, visibility, condition_status, solar, icon)
                 VALUES
                 (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        # Werte für den Datensatz
        values = (timestamp_execution, timestamp_dataset, sunshine, temperature, cloud_cover, visibility, condition_status, solar, icon)

        # Datensatz in die Tabelle einfügen
        cursor.execute(sql, values)

        # Änderungen in der Datenbank bestätigen
        conn.commit()

        print("Datensatz erfolgreich eingefügt.")

    except mysql.connector.Error as error:
        print(f"Fehler beim Einfügen des Datensatzes: {error}")

    finally:
        # Cursor schließen
        cursor.close()

# Endlosschleife
while True:
    weather_data = get_weather_from_api()
    current_timestamp = datetime.now()
    for i in range(24):
        data_timestamp = datetime.strptime(weather_data[i]["timestamp"], "%Y-%m-%dT%H:%M:%S%z")
        insert_weather_data(conn, current_timestamp, data_timestamp, weather_data[i]["sunshine"], weather_data[i]["temperature"], weather_data[i]["cloud_cover"], weather_data[i]["visibility"], weather_data[i]["condition"], weather_data[i]["solar"], weather_data[i]["icon"])
    
    # Wartezeit von einer Stunde
    time.sleep(3600)

# Verbindung zur Datenbank schließen
if conn.is_connected():
    conn.close()