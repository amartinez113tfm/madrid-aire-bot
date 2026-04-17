import requests
import pymongo
import os
from datetime import datetime

# Configuración (Las claves se cargan desde los secretos de GitHub)
MONGO_URI = os.getenv("MONGO_URI")
API_URL = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal_ult.json?pageSize=5000"

def run():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        registros = data.get("@graph", [])
        if not registros:
            print("No hay datos nuevos.")
            return

        # Conectar a MongoDB
        client = pymongo.MongoClient(MONGO_URI)
        db = client["madrid_aire"]
        coleccion = db["contaminantes"]

        ahora = datetime.utcnow()
        documentos = []

        for item in registros:
            documentos.append({
                "timestamp": ahora,
                "estacion": item.get("title"),
                "contaminantes": {
                    "o3": float(item.get("O3", 0)),
                    "no2": float(item.get("NO2", 0)),
                    "pm2_5": float(item.get("PM2_5", 0)),
                    "pm10": float(item.get("PM10", 0))
                }
            })

        if documentos:
            coleccion.insert_many(documentos)
            print(f"Éxito: {len(documentos)} registros guardados en MongoDB.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    run()
