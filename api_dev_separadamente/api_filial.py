import requests 
import json
import mysql.connector
import utils

def api_filial(token):
    DB_CONFIG = utils.Config_BD()

    url = "https://rest.megaerp.online/api/globalagente/Organizacao/Filiais"

    payload = {}
    headers = {
    'Accept': 'text/plain',
    'Authorization': f'bearer {token}'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    if response.status_code == 200:
        try: 
            data = response.json()
            db_connection = mysql.connector.connect(**DB_CONFIG)
            cursor = db_connection.cursor()

            if isinstance(data, list):
                for item in data:
                    id_filial = utils.tratar_none(item.get("id"))
                    agente = utils.tratar_none(item.get("agente"))
                    if agente:
                        expand = utils.tratar_none(agente.get("expand"))
                        id = utils.tratar_none(agente.get("id"))
                        padrao = utils.tratar_none(agente.get("padrao"))
                        codigo = utils.tratar_none(agente.get("codigo"))
                        nome = utils.tratar_none(agente.get("nome"))
                        nomeFantasia = utils.tratar_none(agente.get("nomeFantasia"))
                        cnpj = utils.tratar_none(agente.get("cnpj"))
                        consolidador = utils.tratar_none(agente.get("consolidador"))
                    cursor.execute("SELECT COUNT(*) FROM Filial WHERE idFilial = %s", (id_filial,))
                    result = cursor.fetchone()

                    if result[0] == 0:
                        colunas = [
                            "idFilial", "expand", "id", "padrao", "codigo" ,"nome", "nomeFantasia",
                            "cnpj", "consolidador"
                        ]

                        valores = (
                            id_filial, expand, id, padrao,codigo ,nome, nomeFantasia, cnpj, consolidador
                        )

                        placeholders = ", ".join(['%s'] * len(colunas))
                        colunas_str = ', '.join(colunas)
                        query = f"INSERT INTO Filial ({colunas_str}) VALUES ({placeholders})"

                        print("🔹 Número de colunas na tabela:", colunas_str.count(",") + 1)
                        print("🔹 Número de valores passados:", len(valores))

                        print("🔢 Colunas:", colunas_str.count(",") + 1)
                        print("🔢 Valores:", len(valores))
                        if colunas_str.count(", ") + 1 != len(valores):
                            print("❌ ERRO: O número de colunas não corresponde ao número de valores fornecidos!")
                        else:
                            cursor.execute(query, valores)
                            db_connection.commit()
                            print(f"✅ Filial {id_filial} salvo no banco.")
                    else:
                        print(f"⚠️ Filial {id_filial} já existe no banco.")

                else:
                    print("❌ Erro: A resposta da API não é uma lista de dados.")


        except json.JSONDecodeError:
            print("❌ Erro ao decodificar a resposta JSON:", response.text)
    else:
        print(f"❌ Erro na requisição. Código: {response.status_code}")
        print("Resposta do servidor:", response.text)

    cursor.close()
    db_connection.close()
