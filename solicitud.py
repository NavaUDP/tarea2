import sys
import subprocess
import os

#para no tener archivos .txt de más durante las pruebas, se borra el archivo de datos antes de crear otro
if os.path.exists('/home/nava/Documentos/Universidad/Distribuidos/tarea2/data/datos.txt'):
    os.remove('/home/nava/Documentos/Universidad/Distribuidos/tarea2/data/datos.txt')

#ingresar una solicitud 
nombre_producto = input("Ingrese el nombre del producto: ")
precio = float(input("Ingrese el precio del producto: "))
correo = input("Ingrese su correo: ")

# Guardar los datos en un archivo
with open('/home/nava/Documentos/Universidad/Distribuidos/tarea2/data/datos.txt', 'w') as file:
    file.write(f"Nombre del producto: {nombre_producto}\n")
    file.write(f"Precio del producto: {precio}\n")
    file.write(f"Correo: {correo}\n")

#ejecutar producer.py
subprocess.run(["python3", "API/producer.py"])
