# hacer_pedido.py
import requests

delivery_id = input("Ingrese el id-envio (por ejemplo, SURRES17DEL03): ")

data = {
    "id-envio": delivery_id
}

response = requests.post("http://localhost:5000/solicitar", json=data)

if response.status_code == 200:
    print(response.json())
else:
    print(f"Error: {response.status_code} - {response.text}")