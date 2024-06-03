from confluent_kafka import Consumer, KafkaException
import json
import sys
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

# Configuración de SMTP para Outlook.es
smtp_server = "smtp-mail.outlook.com"  # El mismo servidor que outlook.com
smtp_port = 587
smtp_username = "diegopro2.0@outlook.es" 
smtp_password = "Chipotle2024" 

def send_email(to_email, subject, body):
    msg = MIMEMultipart()
    msg['From'] = smtp_username
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
            print(f"Correo enviado a {to_email}")
    except smtplib.SMTPAuthenticationError:
        print("Error de autenticación. Verifica tu correo y contraseña.")
    except smtplib.SMTPException as e:
        print(f"Error SMTP: {e}")
    except Exception as e:
        print(f"Error inesperado al enviar el correo: {e}")

def process_message(msg_dict):
    order_id = msg_dict['id']
    last_status[order_id] = msg_dict

    if 'correo' in msg_dict:
        subject = f"Actualización de tu pedido {order_id}"
        body = f"""
        ¡Hola! Gracias por usar nuestro servicio de delivery.

        Tu pedido {msg_dict['nombre']} (ID: {order_id}) ha sido actualizado.

        - Estado actual: {msg_dict['estado']}
        - Precio: {msg_dict.get('precio', 'No disponible')} €

        Te mantendremos informado sobre cualquier cambio en tu pedido.
        ¡Que tengas un buen día!

        El equipo de Delivery UDM
        """
        send_email(msg_dict['correo'], subject, body)

    print(f"Mensaje recibido y procesado: {msg_dict}")

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
