import subprocess
import json
import os

#obtener el producto
with open('data/datos.json', 'r') as file:
    data = json.load(file)

# si ya existe el archivo datos_con_ids.json existe, se ejecuta el producer y se termina el script
if os.path.exists('data/datos_con_ids.json'):
    print("El archivo datos_con_ids.json ya existe.")
    subprocess.run(["python3", "API/producer.py"])
    exit()
else:
    productos = data['productos']
    for i, producto in enumerate(productos):
        producto['id'] = i + 1

    # Escribir los productos con sus identificadores en un nuevo archivo JSON
    with open('data/datos_con_ids.json', 'w') as file:
        json.dump(productos, file)
    print("Productos guardados con id.")

    #ejecutar producer.py
    subprocess.run(["python3", "API/producer.py"])
