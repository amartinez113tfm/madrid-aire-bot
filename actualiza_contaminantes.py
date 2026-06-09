import requests
import pymongo
import os
from datetime import datetime

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

        # Creamos un diccionario indexado por (estacion, fecha_hora) para agrupar magnitudes
        documentos_map = {}

        for item in registros:
            estacion_id = str(item.get("ESTACION")).lstrip("0")
            magnitud_id = str(item.get("MAGNITUD"))
            
            if magnitud_id in MAGNITUDES:
                nombre_contaminante = MAGNITUDES[magnitud_id]
                
                # Extraemos la fecha real del documento de la API
                try:
                    año = int(item.get("ANO"))
                    mes = int(item.get("MES"))
                    dia = int(item.get("DIA"))
                except (ValueError, TypeError):
                    continue # Si la fecha viene corrupta salta al siguiente

                # Recorremos las 24 horas posibles del registro vertical
                for h in range(1, 25):
                    clave_hora = f"H{h:02d}"
                    clave_val = f"V{h:02d}"

                    # 'V' significa que la medicion es valida y oficial
                    if clave_hora in item and item.get(clave_val) == "V":
                        try:
                            valor_medido = float(item[clave_hora])
                            hora_ajustada = h - 1 # H01 corresponde a las 00:00, H24 a las 23:00
                            
                            # Construimos el datetime real del dato histórico
                            fecha_real = datetime(año, mes, dia, hora_ajustada)
                            
                            # Clave unica para agrupar (Estacion + Hora exacta)
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

        # Convertimos el mapa a una lista de documentos para hacer el insert
        documentos_a_guardar = list(documentos_map.values())

        if documentos_a_guardar:
            conteo_nuevos = 0
            for doc in documentos_a_guardar:
                # Evitamos duplicados usando update_one con upsert=True 
                # (Sincroniza si existe, inserta si es nuevo)
                resultado = coleccion.update_one(
                    {"timestamp": doc["timestamp"], "estacion_id": doc["estacion_id"]},
                    {"$set": doc},
                    upsert=True
                )
                if resultado.upserted_id:
                    conteo_nuevos += 1
            
            print(f"✅ ¡Proceso completado! Se han procesado {len(documentos_a_guardar)} registros horarias. Nuevos insertados: {conteo_nuevos}")
        else:
            print("No se encontraron magnitudes validas para procesar.")

    except Exception as e:
        print(f"❌ Error en la ejecucion del script: {e}")

if __name__ == "__main__":
    run()
