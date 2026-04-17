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
        print(f"Conectando a: {API_URL}")
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # Según tu log, los datos están en 'lista'
        registros = data.get("lista", [])
        
        if not registros:
            print("La clave 'lista' está vacía o no existe.")
            return

        print(f"Procesando {len(registros)} filas de datos...")
        
        client = pymongo.MongoClient(MONGO_URI)
        db = client["madrid_aire"]
        coleccion = db["historico_contaminantes"]

        # Agruparemos por estación para que no tengas miles de documentos sueltos
        # Madrid envía una fila por cada contaminante/hora
        estaciones_map = {}
        ahora = datetime.utcnow()

        for item in registros:
            nombre_estacion = item.get("ESTACION", "Desconocida")
            if nombre_estacion not in estaciones_map:
                estaciones_map[nombre_estacion] = {
                    "timestamp": ahora,
                    "estacion": nombre_estacion,
                    "o3": 0, "no2": 0, "pm2_5": 0, "pm10": 0
                }
            
            # Mapeo de contaminantes según los campos del JSON de Madrid
            if "O3" in item: estaciones_map[nombre_estacion]["o3"] = float(item["O3"] or 0)
            if "NO2" in item: estaciones_map[nombre_estacion]["no2"] = float(item["NO2"] or 0)
            if "PM2_5" in item: estaciones_map[nombre_estacion]["pm2_5"] = float(item["PM2_5"] or 0)
            if "PM10" in item: estaciones_map[nombre_estacion]["pm10"] = float(item["PM10"] or 0)

        documentos = list(estaciones_map.values())

        if documentos:
            coleccion.insert_many(documentos)
            print(f"✅ ¡Éxito total! Se han guardado {len(documentos)} estaciones en MongoDB.")
        else:
            print("No se pudieron generar documentos válidos.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run()
