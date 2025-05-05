import requests

def get_agente_cliente(token):
    url = "https://rest.megaerp.online/api/globalagente/AgenteCliente/codigocliente/"
    headers = {
        'Accept': 'text/plain',
        'Authorization': f'Bearer {token}'
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            raise ValueError("Erro ao decodificar JSON")
    else:
        raise Exception(f"Erro {response.status_code}: {response.text}")
    
    