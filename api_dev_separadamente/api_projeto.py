import requests
import json
import mysql.connector
import utils

def api_projeto(token):
    DB_CONFIG = utils.Config_BD()

    url = "https://rest.megaerp.online/api/global/Projeto?expand=<string>"

    payload = {}
    headers = {
    'Authorization': f'Bearer {token}'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    if response.status_code == 200:
        try: 
            data = response.json()

            db_connection = mysql.connector.connect(**DB_CONFIG)
            cursor = db_connection.cursor()

            if isinstance(data, list):
                for item in data:
                    idprojeto = item.get("id")
                    padrao = item.get("padrao")
                    identificador = item.get("identificador")
                    reduzido = item.get("reduzido")
                    extenso = item.get("extenso")
                    descricao = item.get("descricao")
                    global_v = item.get("global")

                    if idprojeto:
                        cursor.execute("SELECT COUNT(*) FROM projeto WHERE idprojeto = %s", (idprojeto,))
                        result = cursor.fetchone()

                        if result[0] == 0:
                            cursor.execute("""
                            INSERT INTO projeto (idprojeto, padrao, identificador, reduzido, extenso, descricao, global, usuarios)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                            (idprojeto, padrao, identificador, reduzido, extenso, descricao, global_v, "null")
                            )
                            db_connection.commit()
                            print(f"Projeto {idprojeto} salvo no banco.")
                        else:
                            print(f"Projeto {idprojeto} já existe no banco")

            else:
                print("Erro: A resposta da API não é uma lista de dados")
            cursor.close()
            db_connection.close()
        
        except json.JSONDecodeError:
            print("Erro ao decodificar a resposta JSON:", response.text)

    else: 
        print(f"Erro na aquisição. Código: {response.status_code}")
        print("Resposta do servidor:", response.text)