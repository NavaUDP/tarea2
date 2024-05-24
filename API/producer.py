from confluent_kafka import Producer
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

with open('/home/nava/Documentos/Universidad/Distribuidos/tarea2/data/datos.txt', 'r') as file:
    data = file.read()

#enviar los datos al topic test
producer.produce('test', key='key', value=data, callback=delivery_report)

producer.flush()