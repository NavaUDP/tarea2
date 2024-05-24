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

try:
    while True:
        message = input('Introduce un mensaje para enviar (o "exit" para terminar): ')

        if message.lower() == 'exit':
            break

        # Enviar el mensaje al topic 'test'
        producer.produce('test', key='key', value=message, callback=delivery_report)

        # Esperar a que los mensajes en cola sean entregados
        producer.flush()
except KeyboardInterrupt:
    pass
finally:
    # Cerrar el productor
    producer.flush()