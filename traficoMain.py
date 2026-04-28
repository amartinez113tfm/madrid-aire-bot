import os
import pandas as pd
from prophet.serialize import model_from_json
from pymongo import MongoClient, UpdateOne
from huggingface_hub import hf_hub_download
from datetime import datetime, timedelta

# Configuración
REPO_ID = "amartinez113/trafico-madrid-prophet" # Tu repo en HF
STATIONS = [4,8,16,18,24,35,36,38,39,54,56,58,59] # Lista de tus estaciones

def run_pipeline():
    # 1. Conexión a Mongo (usando la variable de entorno de GitHub Secrets)
    mongo_uri = os.getenv("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client['trafico_madrid']
    collection = db['predicciones_horarias']

    for cod in STATIONS:
        # 2. Descargar el modelo desde Hugging Face
        model_path = hf_hub_download(repo_id=REPO_ID, filename=f"prophet_estacion_{cod}.json")
        
        with open(model_path, 'r') as f:
            m = model_from_json(f.read())
        
        # 3. Crear fechas futuras (próximas 24h)
        ahora = datetime.now().replace(minute=0, second=0, microsecond=0)
        future = pd.DataFrame({'ds': [ahora + timedelta(hours=x) for x in range(1, 25)]})
        
        # 4. Añadir tus regresores (importante que coincidan con el entrenamiento)
        future['IS_LABORABLE'] = future['ds'].apply(lambda x: 1 if x.weekday() < 5 else 0)
        future['IS_SABADO'] = future['ds'].apply(lambda x: 1 if x.weekday() == 5 else 0)
        future['IS_FEST_DOM'] = future['ds'].apply(lambda x: 1 if x.weekday() == 6 else 0)
        
        # 5. Predecir
        forecast = m.predict(future)
        
        # 6. Preparar carga masiva a Mongo (Upsert)
        ops = []
        for _, row in forecast.iterrows():
            ops.append(UpdateOne(
                {"estacion": cod, "timestamp": row['ds']},
                {"$set": {
                    "valor": round(row['yhat'], 2),
                    "limites": [round(row['yhat_lower'], 2), round(row['yhat_upper'], 2)],
                    "actualizado": datetime.now()
                }},
                upsert=True
            ))
        
        if ops:
            collection.bulk_write(ops)
            print(f"Estación {cod} actualizada.")

if __name__ == "__main__":
    run_pipeline()
