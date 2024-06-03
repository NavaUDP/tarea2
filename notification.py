import logging
from confluent_kafka import Consumer, KafkaException
import json
import os
from flask import Flask, jsonify
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Configuración del consumer
conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'notification_group', 'auto.offset.reset': 'earliest'}

# Crear el consumer con la configuración dada
consumer = Consumer(conf)

# Entrar al topic
consumer.subscribe(['test'])
print("Entrando al topic test")

# Diccionario para almacenar el último estado de cada pedido
last_status = {}

# Configuración de Flask
app = Flask(__name__)

@app.route('/status/<int:order_id>', methods=['GET'])
def get_status(order_id):
    if order_id in last_status:
        return jsonify(last_status[order_id])
    else:
        return jsonify({"error": "Pedido no encontrado"}), 404

 #Configuracion para realizar la peticion smtp


HOST = "smtp-mail.outlook.com"
PORT = 587
smtp_username = os.environ.get('API envíos', 'dnavarretev26@gmail.com')
smtp_password = os.environ.get('API envíos', 'fcpd tevo mbmf ilrj') 


def process_message(msg_dict):
    order_id = msg_dict['id-envio']
    if order_id is None:
        logging.error(f"No se encontró el ID en el mensaje: {msg_dict}")
        return
        
    last_status[order_id] = msg_dict

    # Verificar si el mensaje tiene un correo electrónico
    if 'correo' in msg_dict:
        to_email = msg_dict['correo']
        subject = f"Actualización de tu pedido {order_id}"
        body = create_email_body(msg_dict)
        send_email(to_email, subject, body)
    else:
        logging.warning(f"No se encontró campo 'correo' en el mensaje: {msg_dict}")

    print(f"Mensaje recibido y procesado: {msg_dict}")

def send_email(to_email, subject, body):
    msg = MIMEMultipart()
    msg['From'] = smtp_username
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP(HOST, PORT) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
            logging.info(f"Correo enviado a {to_email}")
    except smtplib.SMTPAuthenticationError:
        logging.error("Error de autenticación SMTP. Verifica tus credenciales.")
    except smtplib.SMTPException as e:
        logging.error(f"Error SMTP: {e}")
    except Exception as e:
        logging.error(f"Error inesperado al enviar el correo a {to_email}: {e}")

def create_email_body(msg_dict):
    order_id = msg_dict.get('id-envio')
    estado = msg_dict.get('estado', 'En proceso')

    return f"""
    ¡Hola! Gracias por usar nuestro servicio de delivery.

    (ID: {order_id}) ha sido actualizado.

    - Estado actual: {estado}


    Te mantendremos informado sobre cualquier cambio en tu pedido.
    ¡Que tengas un buen día!

    El equipo de Delivery UDM
    """

try:
    # Iniciar Flask en un hilo separado
    from threading import Thread
    Thread(target=app.run, kwargs={'host': '0.0.0.0', 'port': 5001}).start()

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
