import requests
import pymongo
import os
from datetime import datetime

# Configuracion (Las claves se cargan desde los secretos de GitHub)
MONGO_URI = os.getenv("MONGO_URI")
API_URL = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal_ult.json?pageSize=5000"

# Diccionario de magnitudes segun el Ayuntamiento de Madrid
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
        # 1. Peticion a la API de Datos Abiertos
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        registros = data.get("records", [])
        
        if not registros:
            print("No se encontraron datos en la clave 'records'.")
            return

        # 2. Conexion a MongoDB Atlas
        client = pymongo.MongoClient(MONGO_URI)
        db = client["madrid_aire"]
        coleccion = db["historico_contaminantes"]

        # Creamos un diccionario indexado por (estacion, fecha_hora) para agrupar magnitudes en memoria
        documentos_map = {}

        # 3. Procesamiento y desanidado de las 24 horas del JSON
        for item in registros:
            estacion_id = str(item.get("ESTACION")).lstrip("0")
            magnitud_id = str(item.get("MAGNITUD"))
            
            if magnitud_id in MAGNITUDES:
                nombre_contaminante = MAGNITUDES[magnitud_id]
                
                # Extraemos la fecha real que reporta el Ayuntamiento para este registro
                try:
                    año = int(item.get("ANO"))
                    mes = int(item.get("MES"))
                    dia = int(item.get("DIA"))
                except (ValueError, TypeError):
                    continue  # Si la fecha viene corrupta saltamos la linea

                # Recorremos las 24 columnas horarias potenciales (H01 a H24)
                for h in range(1, 25):
                    clave_hora = f"H{h:02d}"
                    clave_val = f"V{h:02d}"

                    # Si la columna existe y su flag de validacion es 'V' (Valido)
                    if clave_hora in item and item.get(clave_val) == "V":
                        try:
                            valor_medido = float(item[clave_hora])
                            hora_ajustada = h - 1  # H01 mapea a las 00:00, H24 mapea a las 23:00
                            
                            # Construimos el datetime real del dato historico
                            fecha_real = datetime(año, mes, dia, hora_ajustada)
                            
                            # Tupla unica para agrupar las distintas magnitudes de una misma estacion/hora
                            clave_unica = (estacion_id, fecha_real)
                            
                            if clave_unica not in documentos_map:
                                documentos_map[clave_unica] = {
                                    "timestamp": fecha_real,
                                    "estacion_id": estacion_id,
                                    "o3": 0.0, "no2": 0.0, "pm2_5": 0.0, "pm10": 0.0
                                }
                            
                            # Asignamos el valor en su columna correspondiente
                            documentos_map[clave_unica][nombre_contaminante] = valor_medido
                        except (ValueError, TypeError):
                            continue

        # 4. Transformamos el mapa de memoria en una lista plana de documentos para insertar
        documentos_a_guardar = list(documentos_map.values())

        if documentos_a_guardar:
            # Determinamos la fecha minima del lote actual para evitar solapamientos temporales
            fecha_minima = min(doc["timestamp"] for doc in documentos_a_guardar)
            
            # Extraemos la lista unica de estaciones presentes en la respuesta de la API
            lista_estaciones = list(set(doc["estacion_id"] for doc in documentos_a_guardar))
            
            print(f"Limpiando solapamientos en Atlas desde: {fecha_minima}...")
            
            # Borrado rapido: Eliminamos los registros previos de estas estaciones a partir de esa fecha
            coleccion.delete_many({
                "estacion_id": {"$in": lista_estaciones},
                "timestamp": {"$gte": fecha_minima}
            })
            
            # 5. Insercion masiva limpia sin sobrecargar las llamadas por red a Atlas
            resultado = coleccion.insert_many(documentos_a_guardar)
            
            print(f"✅ ¡Conseguido! Base de datos de contaminantes actualizada con exito.")
            print(f"- Total registros horarios guardados: {len(resultado.inserted_ids)}")
        else:
            print("No se encontraron magnitudes validas listas para procesar.")

    except Exception as e:
        print(f"❌ Error critico en la ejecucion del script: {e}")

if __name__ == "__main__":
    run()
