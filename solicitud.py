from confluent_kafka import Producer
from flask import Flask, request, jsonify
import json
import uuid
import subprocess

# Configuración del producer
conf = {'bootstrap.servers': 'localhost:9092, localhost:9093, localhost:9094'}
producer = Producer(conf)

# Función de callback para saber si un mensaje fue entregado o no
def delivery_report(err, msg):
    if err is not None:
        print(f'Mensaje falló la entrega: {err}')
    else:
        print(f'Mensaje entregado a {msg.topic()} [{msg.partition()}]')

# Cargar el dataset
with open('data/deliverytime.json', 'r') as file:
    envios = json.load(file)

# Indexar los datos por Delivery_person_ID para búsquedas rápidas
data_by_id = {item['id-envio']: item for item in envios}

# Inicializar el contador de ID global
id_counter = max([int(envio['ID']) for envio in envios if 'id' in envio] + [0]) + 1

# Función para verificar y asignar una id a los productos
def verify_and_assign_id(envio):
    global id_counter
    if 'ID' not in envio:
        envio['ID'] = id_counter
        id_counter += 1
    return envio

# Inicializar la aplicación Flask
app = Flask(__name__)

@app.route('/solicitar', methods=['POST'])
def solicitar_pedido():
    data = request.get_json()
    delivery_id = data['id-envio']
    
    # Buscar el pedido por ID
    envio = data_by_id.get(delivery_id)
    if not envio:
        return jsonify({"error": f"No se encontró el pedido con ID: {delivery_id}"}), 404

    # Verificar y asignar una id si no tiene
    envio_con_id = verify_and_assign_id(envio)

    # Convertir el envío a bytes
    envio_bytes = json.dumps(envio_con_id).encode('utf-8')
    
    # Enviar el mensaje al topic 'envios'
    producer.produce('test', value=envio_bytes, callback=delivery_report)
    
    # Esperar a que el mensaje sea enviado
    producer.flush()
    
    return jsonify({"mensaje": f"Pedido de {delivery_id} recibido y enviado a Kafka", "pedido": envio_con_id})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
