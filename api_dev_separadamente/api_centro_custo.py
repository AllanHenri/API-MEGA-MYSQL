import requests
import json
import mysql.connector
import utils

def api_centro_custo(token):
    # Configuração do banco de dados MySQL
    DB_CONFIG = utils.Config_BD()


    # URL da API
    url = "https://rest.megaerp.online/api/contabilidadecadastros/CentroCusto?expand=<string>"

    # Cabeçalhos da requisição
    headers = {
        'Accept': 'text/plain',
        'Authorization': f'Bearer {token}'
    }

    # Fazendo a requisição HTTP
    response = requests.get(url, headers=headers)

    # Verificando se a resposta foi bem-sucedida
    if response.status_code == 200:
        try:
            data = response.json()  # Converte a resposta JSON

            # Conectando ao banco MySQL
            db_connection = mysql.connector.connect(**DB_CONFIG)
            cursor = db_connection.cursor()

            # Verificando se a resposta é uma lista de objetos
            if isinstance(data, list):
                for item in data:
                    # Extraindo os dados necessários
                    idcentroCusto = item.get("Id")  # Ajuste conforme o JSON da API
                    padrao = item.get("Padrao", 0)  # Caso não venha, assume 0
                    identificador = item.get("Identificador")
                    reduzido = item.get("Reduzido", 0)
                    extenso = item.get("Extenso")
                    descricao = item.get("Descricao")
                    global_valor = item.get("Global")
                    usuarios = item.get("Usuarios")

                    if idcentroCusto:
                        # Verificar se o `idcentroCusto` já existe no banco
                        cursor.execute("SELECT COUNT(*) FROM centroCusto WHERE idcentroCusto = %s", (idcentroCusto,))
                        result = cursor.fetchone()

                        if result[0] == 0:  # Se ainda não existir
                            cursor.execute(
                                """INSERT INTO centroCusto 
                                (idcentroCusto, padrao, identificador, reduzido, extenso, descricao, global, usuarios)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                                (idcentroCusto, padrao, identificador, reduzido, extenso, descricao, global_valor, usuarios)
                            )
                            db_connection.commit()
                            print(f"Centro de Custo {idcentroCusto} salvo no banco.")
                        else:
                            print(f"Centro de Custo {idcentroCusto} já existe no banco.")

            else:
                print("Erro: A resposta da API não é uma lista de dados.")

            # Fechando conexão
            cursor.close()
            db_connection.close()

        except json.JSONDecodeError:
            print("Erro ao decodificar a resposta JSON:", response.text)
    else:
        print(f"Erro na requisição. Código: {response.status_code}")
        print("Resposta do servidor:", response.text)
