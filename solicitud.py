from confluent_kafka import Producer
import json
import os

#Configuracion producer
conf = {'bootstrap.servers': 'localhost:9092'}

#creamos al productor
producer = Producer(conf)

# Función de callback para saber si un mensaje fue entregado o no
def delivery_report(err, msg):
    if err is not None:
        print(f'Mensaje falló la entrega: {err}')
    else:
        print(f'Mensaje entregado a {msg.topic()} [{msg.partition()}]')

fin = 0
while fin == 0:
    #Funcion para saber si se agregaron las id a los productos
    if os.path.isfile('data/datos_con_ids.json'):
        #en caso de existir el archivo entonces se envía el mensaje a kafka
        with open('data/datos_con_ids.json', 'r') as file:
            data = json.load(file)
    
        for item in data:
            #Convertir el producto a una cadena JSON
            item_str = json.dumps(item)

            #Enviar el producto al topic
            producer.produce('test', key = 'key', value=item_str, callback=delivery_report)

            producer.flush()
        print("Productos enviados a Kafka.")
        fin = 1
    else:
        with open('data/datos.json', 'r') as file:
            data = json.load(file)

        productos = data['productos']
        for i, producto in enumerate(productos):
            producto['id'] = i + 1

            # Escribir los productos con sus identificadores en un nuevo archivo JSON
            with open('data/datos_con_ids.json', 'w') as file:
                json.dump(productos, file)
    
        print("Productos guardados con id.")