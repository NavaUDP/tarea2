from confluent_kafka import Consumer, KafkaException
import json
import sys

# Configuración del consumer
conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'notification_group', 'auto.offset.reset': 'earliest'}

# Crear el consumer con la configuración dada
consumer = Consumer(conf)

#Entrar al topic
consumer.subscribe(['test'])
print("Entrando al topic test")

try:
    while True:
        #lee el mensaje
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            #Decodifica el mensaje
            msg_dict = json.loads(msg.value().decode('utf-8'))

            #Si el estado es cualquiera excepto 'finalizado', se ignora el mensaje
            if 'estado' not in msg_dict or msg_dict['estado'] != 'finalizado':
                continue

            print(f"Mensaje recibido: {msg_dict}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
