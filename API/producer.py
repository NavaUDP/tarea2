from confluent_kafka import Producer
import json
import sys

# Configuración del productor
conf = {'bootstrap.servers': 'localhost:9092'}

# Crear el productor con la configuración dada
producer = Producer(conf)

# Función de callback para saber si un mensaje fue entregado o no
def delivery_report(err, msg):
    if err is not None:
        print(f'Mensaje falló la entrega: {err}')
    else:
        print(f'Mensaje entregado a {msg.topic()} [{msg.partition()}]')

with open('/home/nava/Documentos/Universidad/Distribuidos/tarea2/data/datos_con_ids.json', 'r') as file:
    data = json.load(file)

for item in data:
    # Convertir el producto a una cadena JSON
    item_str = json.dumps(item)

    # Enviar el producto al topic
    producer.produce('test', key='key', value=item_str, callback=delivery_report)

    # Asegurar que el mensaje se envíe antes de continuar con el siguiente
    producer.flush()