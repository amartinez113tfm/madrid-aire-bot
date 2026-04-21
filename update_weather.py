import pandas as pd
import requests
from pymongo import MongoClient
from datetime import datetime
import os

def procesar_open_meteo():
    # Conexión (Usa secretos en GitHub Actions)
    client = MongoClient(os.getenv('MONGO_URI'))
    db = client['madrid_aire']
    coleccion = db['meteorologia']

    # Cargar tus estaciones con coordenadas
    estaciones = pd.read_csv('estaciones_coordenadas.csv')

    for _, est in estaciones.iterrows():
        url = (f"https://api.open-meteo.com/v1/forecast?latitude={est['lat']}&longitude={est['lon']}"
               f"&hourly=temperature_2m,relative_humidity_2m,rain,wind_speed_10m,surface_preasure,wind_direction_10m,direct_radiation_instant"
               f"&past_days=0&forecast_days=1")
        
        response = requests.get(url).json()
        
        if 'hourly' not in response:
            print(f"Error en estación {est['id']}: No se recibieron datos horarios.")
            continue

        h = response['hourly']
        
        # Iteramos sobre la lista de 'time' (que tiene 24 elementos según tu ejemplo)
        for i in range(len(h['time'])):
            # Convertimos el string '2026-04-21T00:00' a objeto datetime
            ts = datetime.fromisoformat(h['time'][i])
            
            doc = {
                "timestamp": ts,
                "estacion_id": str(est['id']),
                "variables": {
                    "viento": {
                        "velocidad": h['wind_speed_10m'][i],
                        "direccion": h['wind_direction_10m'][i]
                    },
                    "temperatura": h['temperature_2m'][i],
                    "humedad": h['relative_humidity_2m'][i],
                    "presion": h['surface_preasure'], # No incluido en tu URL actual
                    "radiacion_solar": h['direct_radiation_instant'][i],
                    "precipitacion": h['rain'][i]
                }
            }

            # Upsert para no duplicar datos si el Action se ejecuta varias veces
            coleccion.update_one(
                {
                    "timestamp": doc['timestamp'],
                    "estacion_id": doc['estacion_id']
                },
                {"$set": doc},
                upsert=True
            )

    print("Proceso de actualización finalizado correctamente.")

if __name__ == "__main__":
    procesar_open_meteo()
