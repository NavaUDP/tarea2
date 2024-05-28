from confluent_kafka import Consumer, KafkaException
import json

# Configuración del consumidor
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest',
}

# Crear el consumidor con la configuración dada
consumer = Consumer(conf)

# Suscribirse al topic 'test'
consumer.subscribe(['test'])

try:
    while True:
        # Leer mensaje del topic
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            # Decodificar mensaje y cargarlo como un diccionario
            msg_dict = json.loads(msg.value().decode("utf-8"))

            # Verificar y cambiar el estado
            if 'estado' not in msg_dict:
                msg_dict['estado'] = 'recibido'
            elif msg_dict['estado'] == 'recibido':
                msg_dict['estado'] = 'preparando'
            elif msg_dict['estado'] == 'preparando':
                msg_dict['estado'] = 'entregando'
            elif msg_dict['estado'] == 'entregando':
                msg_dict['estado'] = 'finalizado'

            # Guardar el diccionario actualizado en el archivo 'data_con_parametros.json'
            with open('data/datos_con_estado.json', 'a') as file:
                json.dump(msg_dict, file)
                file.write('\n')

            # Imprimir mensaje
            print(f'Mensaje recibido: {json.dumps(msg_dict)}')
except KeyboardInterrupt:
    pass
finally:
    # Cerrar el consumidor
    consumer.close()