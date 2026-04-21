import pandas as pd
import pandas as pd
import requests
from pymongo import MongoClient
from datetime import datetime
import os
import sys

def run():
    print("--- INICIANDO PROCESO DE ACTUALIZACIÓN ---", flush=True)
    
    # 1. Configuración y Conexión a MongoDB
    mongo_uri = os.getenv('MONGO_URI')
    if not mongo_uri:
        print("ERROR: La variable MONGO_URI no está configurada.", flush=True)
        sys.exit(1)

    try:
        # Timeout de 5 segundos para la conexión inicial
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        db = client['madrid_aire']
        coleccion = db['meteorologia']
        # Forzar un ping para verificar conexión
        client.admin.command('ping')
        print("Conexión a MongoDB Atlas: EXITOSA", flush=True)
    except Exception as e:
        print(f"ERROR al conectar a MongoDB: {e}", flush=True)
        sys.exit(1)

    # 2. Lectura del archivo de estaciones
    try:
        # Usamos el separador ';' según tu archivo
        estaciones = pd.read_csv('estaciones_coordenadas.csv', sep=';')
        estaciones.columns = estaciones.columns.str.strip()
        print(f"Archivo de coordenadas cargado. {len(estaciones)} estaciones encontradas.", flush=True)
    except Exception as e:
        print(f"ERROR al leer el CSV: {e}", flush=True)
        sys.exit(1)

    # 3. Bucle principal de actualización
    for _, est in estaciones.iterrows():
        est_id = str(int(est['id'])) # Forzamos a string para consistencia
        lat = float(est['lat'])
        lon = float(est['lon'])
        
        print(f"Procesando Estación {est_id} (Lat: {lat}, Lon: {lon})...", end="", flush=True)
        
        url = (f"https://api.open-meteo.com/v1/forecast?"
               f"latitude={lat:.6f}&longitude={lon:.6f}"
               f"&hourly=temperature_2m,relative_humidity_2m,rain,wind_speed_10m,wind_direction_10m,surface_pressure,direct_radiation_instant"
               f"&past_days=0&forecast_days=1")
        
        try:
            # Timeout de 10 segundos para la petición a la API
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'hourly' not in data:
                print(" [SIN DATOS HOURLY]", flush=True)
                continue

            h = data['hourly']
            registros_estacion = 0

            # Iteramos por las 24-48 horas que devuelve la API
            for i in range(len(h['time'])):
                ts = datetime.fromisoformat(h['time'][i])
                
                doc = {
                    "timestamp": ts,
                    "estacion_id": est_id,
                    "variables": {
                        "viento": {
                            "velocidad": h['wind_speed_10m'][i],
                            "direccion": h['wind_direction_10m'][i]
                        },
                        "temperatura": h['temperature_2m'][i],
                        "humedad": h['relative_humidity_2m'][i],
                        "presion": h['surface_pressure'][i], # No incluido en esta URL
                        "radiacion_solar": h['direct_radiation_instant'][i],
                        "precipitacion": h['rain'][i]
                    }
                }

                # Upsert: Si existe la combinación estación/hora, actualiza. Si no, crea.
                coleccion.update_one(
                    {"timestamp": ts, "estacion_id": est_id},
                    {"$set": doc},
                    upsert=True
                )
                registros_estacion += 1
            
            print(f" OK ({registros_estacion} horas)", flush=True)

        except Exception as e:
            print(f" ERROR: {e}", flush=True)

    print("--- PROCESO FINALIZADO ---", flush=True)

if __name__ == "__main__":
    run()
