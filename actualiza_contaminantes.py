import requests
import pymongo
import os
from datetime import datetime
from pymongo import UpdateOne  # Necesario para el procesamiento masivo

MONGO_URI = os.getenv("MONGO_URI")
API_URL = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal_ult.json?pageSize=5000"

MAGNITUDES = {
    "1": "no2",
    "8": "no2",
    "14": "o3",
    "9": "pm2_5",
    "10": "pm10"
}

def run():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        data = response.json()
        registros = data.get("records", [])
        
        if not registros:
            print("No se encontraron datos en la clave 'records'.")
            return

        client = pymongo.MongoClient(MONGO_URI)
        db = client["madrid_aire"]
        coleccion = db["historico_contaminantes"]

        documentos_map = {}

        # 1. Agrupamos todo en memoria en un diccionario (esto es instantaneo)
        for item in registros:
            estacion_id = str(item.get("ESTACION")).lstrip("0")
            magnitud_id = str(item.get("MAGNITUD"))
            
            if magnitud_id in MAGNITUDES:
                nombre_contaminante = MAGNITUDES[magnitud_id]
                
                try:
                    año = int(item.get("ANO"))
                    mes = int(item.get("MES"))
                    dia = int(item.get("DIA"))
                except (ValueError, TypeError):
                    continue

                for h in range(1, 25):
                    clave_hora = f"H{h:02d}"
                    clave_val = f"V{h:02d}"

                    if clave_hora in item and item.get(clave_val) == "V":
                        try:
                            valor_medido = float(item[clave_hora])
                            hora_ajustada = h - 1
                            fecha_real = datetime(año, mes, dia, hora_ajustada)
                            
                            clave_unica = (estacion_id, fecha_real)
                            
                            if clave_unica not in documentos_map:
                                documentos_map[clave_unica] = {
                                    "timestamp": fecha_real,
                                    "estacion_id": estacion_id,
                                    "o3": 0.0, "no2": 0.0, "pm2_5": 0.0, "pm10": 0.0
                                }
                            
                            documentos_map[clave_unica][nombre_contaminante] = valor_medido
                        except (ValueError, TypeError):
                            continue

        # 2. Convertimos el mapa en una lista de operaciones Bulk de PyMongo
        operaciones_bulk = []
        for doc in documentos_map.values():
            operaciones_bulk.append(
                UpdateOne(
                    {"timestamp": doc["timestamp"], "estacion_id": doc["estacion_id"]},
                    {"$set": doc},
                    upsert=True
                )
            )

        # 3. Lanzamos todo a la base de datos en UNA SOLA llamada de red
        if operaciones_bulk:
            resultado = coleccion.bulk_write(operaciones_bulk, ordered=False)
            print(f"✅ ¡Proceso completado con Bulk Write!")
            print(f"- Total registros procesados: {len(operaciones_bulk)}")
            print(f"- Nuevos insertados (Upserted): {resultado.upserted_count}")
            print(f"- Actualizados/Sincronizados: {resultado.modified_count}")
        else:
            print("No se encontraron magnitudes validas para procesar.")

    except Exception as e:
        print(f"❌ Error en la ejecucion del script: {e}")

if __name__ == "__main__":
    run()
