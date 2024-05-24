from confluent_kafka import Consumer, KafkaException

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
            # Imprimir mensaje
            print(f'Mensaje recibido: {msg.value().decode("utf-8")}')
except KeyboardInterrupt:
    pass
finally:
    # Cerrar el consumidor
    consumer.close()