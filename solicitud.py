from confluent_kafka import Producer
import json
import os
import uuid

# Configuración del producer
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

# Función de callback para saber si un mensaje fue entregado o no
def delivery_report(err, msg):
    if err is not None:
        print(f'Mensaje falló la entrega: {err}')
    else:
        print(f'Mensaje entregado a {msg.topic()} [{msg.partition()}]')

# Función para verificar y asignar una id a los productos
def verify_and_assign_id(envio):
    if 'id' not in envio:
        envio['id'] = str(uuid.uuid4())
    return envio

# Abrir el archivo deliverytime.json
with open('data/deliverytime.json', 'r') as file:
    envios = json.load(file)

# Iterar sobre cada envío y enviarlo al topic de Kafka
for envio in envios:
    # Verificar y asignar una id si no tiene
    envio_con_id = verify_and_assign_id(envio)

    # Convertir el envío a bytes
    envio_bytes = json.dumps(envio_con_id).encode('utf-8')

    # Enviar el mensaje al topic 'envios'
    producer.produce('envios', value=envio_bytes, callback=delivery_report)

# Esperar a que todos los mensajes sean enviados
producer.flush()