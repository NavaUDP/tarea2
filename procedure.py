from confluent_kafka import Consumer, Producer, KafkaException
import json

#Configuraciones Consumer y Producer
conf_consumer = {'bootstrap.servers': 'localhost:9092, localhost:9093, localhost:9094', 'group.id': 'procedure_group', 'auto.offset.reset': 'earliest'}
conf_producer = {'bootstrap.servers': 'localhost:9092, localhost:9093, localhost:9094'}

#crear al consumer y producer
consumer = Consumer(conf_consumer)
producer = Producer(conf_producer)

#Entramos al topic
consumer.subscribe(['test'])
print("Entrando al topic test")

try:
    while True:
        #lee el mensaje
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            raise KafkaException(message.error())
        else:
            #Decodifica el mensaje
            msg_dict = json.loads(message.value().decode('utf-8'))

            #Si el estado es finalizado, se ignora el mensaje
            if 'estado' in msg_dict and msg_dict['estado'] == 'finalizado':
                continue

            print(f"Mensaje recibido: {msg_dict}")

            #Verificamos que el mensaje tenga un estado
            if 'estado' not in msg_dict:
                #Asigna uno
                msg_dict['estado'] = 'recibido'
            else:
                #cambia al siguiente estado
                if msg_dict['estado'] == 'recibido':
                    msg_dict['estado'] = 'preparando'
                elif msg_dict['estado'] == 'preparando':
                    msg_dict['estado'] = 'entregado'
                elif msg_dict['estado'] == 'entregado':
                    msg_dict['estado'] = 'finalizado'

            #Envia el mensaje de vuelta al topic
            producer.produce('test', json.dumps(msg_dict).encode('utf-8'))

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    producer.flush()
