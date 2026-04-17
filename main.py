import requests
import pymongo
import os
from datetime import datetime

# Configuración (Las claves se cargan desde los secretos de GitHub)
MONGO_URI = os.getenv("MONGO_URI")
API_URL = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal_ult.json?pageSize=5000"

def run():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json"
    }
    
    try:
        print(f"Consultando API: {API_URL}")
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # Madrid a veces usa '@graph', otras veces 'items', otras 'lista'
        registros = data.get("@graph") or data.get("items") or data.get("lista")
        
        # Si sigue sin encontrar, y data es una lista directamente:
        if registros is None and isinstance(data, list):
            registros = data

        if not registros:
            print("Estructura del JSON recibida:")
            print(str(data)[:500]) # Imprime los primeros 500 caracteres para depurar
            print("No se encontraron registros en las claves conocidas.")
            return

        print(f"Detectados {len(registros)} registros. Conectando a MongoDB...")
        
        client = pymongo.MongoClient(MONGO_URI)
        db = client["madrid_aire"]
        coleccion = db["historico_contaminantes"]

        ahora = datetime.utcnow()
        documentos = []

        for item in registros:
            # Extraemos los valores con nombres de clave comunes en el API de Madrid
            # Algunos vienen como 'O3', otros como 'valor' dentro de una lista.
            documentos.append({
                "timestamp": ahora,
                "estacion": item.get("title") or item.get("ESTACION"),
                "contaminantes": {
                    "o3": float(item.get("O3", 0)),
                    "no2": float(item.get("NO2", 0)),
                    "pm2_5": float(item.get("PM2_5", 0)),
                    "pm10": float(item.get("PM10", 0))
                }
            })

        if documentos:
            coleccion.insert_many(documentos)
            print(f"✅ Éxito: {len(documentos)} registros guardados en la base de datos.")

    except Exception as e:
        print(f"❌ Error crítico: {e}")

if __name__ == "__main__":
    run()
