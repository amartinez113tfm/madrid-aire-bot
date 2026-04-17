import requests
import pymongo
import os
from datetime import datetime

# Configuración (Las claves se cargan desde los secretos de GitHub)
MONGO_URI = os.getenv("MONGO_URI")
API_URL = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal_ult.json?pageSize=5000"
# Diccionario de magnitudes según el Ayuntamiento de Madrid
MAGNITUDES = {
    "1": "no2",
    "8": "no2",  # A veces viene como 8 o 1
    "14": "o3",
    "9": "pm2_5",
    "10": "pm10"
}

def run():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # La clave correcta es 'records'
        registros = data.get("records", [])
        
        if not registros:
            print("No se encontraron datos en la clave 'records'.")
            return

        client = pymongo.MongoClient(MONGO_URI)
        db = client["madrid_aire"]
        coleccion = db["historico_contaminantes"]

        ahora = datetime.utcnow()
        # La última hora reportada suele ser la hora actual menos 1
        hora_actual = ahora.hour if ahora.hour > 0 else 24
        clave_hora = f"H{hora_actual:02d}" 

        estaciones_map = {}

        for item in registros:
            estacion_id = item.get("ESTACION")
            magnitud_id = item.get("MAGNITUD")
            
            if magnitud_id in MAGNITUDES:
                nombre_contaminante = MAGNITUDES[magnitud_id]
                
                if estacion_id not in estaciones_map:
                    estaciones_map[estacion_id] = {
                        "timestamp": ahora,
                        "estacion_id": estacion_id,
                        "o3": 0, "no2": 0, "pm2_5": 0, "pm10": 0
                    }
                
                # Extraemos el valor de la hora actual
                valor = float(item.get(clave_hora, 0))
                estaciones_map[estacion_id][nombre_contaminante] = valor

        documentos = list(estaciones_map.values())

        if documentos:
            coleccion.insert_many(documentos)
            print(f"✅ ¡Conseguido! Guardadas {len(documentos)} estaciones con datos de {clave_hora}.")
        else:
            print("No se encontraron las magnitudes deseadas (O3, NO2, PMs).")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run()
