
from kafka import KafkaProducer
import csv
import time
import json

# Inicializamos el productor Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Ruta del dataset CSV
file_path = '/home/vboxuser/proyecto_spark/datos_terremoto_tsunami.csv'

# Leemos el archivo y enviamos cada fila al topic
with open(file_path, 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        data = {
            "id": row.get("id"),
            "magnitude": row.get("magnitude"),
            "depth": row.get("depth"),
            "location": row.get("location"),
            "tsunami": row.get("tsunami")
        }
        producer.send('terremotos_topic', value=data)
        print(f"Enviado: {data}")
        time.sleep(1)  # Enviar un mensaje por segundo

producer.flush()
producer.close()
