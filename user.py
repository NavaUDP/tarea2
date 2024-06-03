import requests

id_input = input("Ingrese el ID del pedido: ")
ids = [id.strip() for id in id_input.split(',')]

for delivery_id in ids:
    data = {
    "id-envio": delivery_id
    }

    response = requests.post("http://localhost:5000/solicitar", json=data)

    if response.status_code == 200:
        print(response.json())
    else:
        print(f"Error: {response.status_code} - {response.text}")

