import requests
import json
import mysql.connector
import utils
import time


def api_agentes(token, min, max):
    DB_CONFIG = utils.Config_BD()
    db_connection = mysql.connector.connect(**DB_CONFIG)
    cursor = db_connection.cursor()

    for i in range(min,max):
        url = f"https://rest.megaerp.online/api/globalagente/Agente/1-{i}"

        payload = {}
        headers = {
            'Accept': 'text/plain',
            'Authorization': f'Bearer {token}'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        if response.status_code == 200:
            try: 
                agente = response.json()
                if isinstance( agente, dict):
                    expand = agente.get("expand")
                    id = agente.get("id")
                    padrao = agente.get("padrao")
                    codigo = agente.get("codigo")
                    nome = agente.get("nome")
                    nomeFantasia = agente.get("nomeFantasia")
                    cnpj = agente.get("cnpj")
                    consolidador = agente.get("consolidador")

                    if id:
                        cursor.execute("SELECT COUNT(*) FROM Agente WHERE  idAgente = %s", (id,))
                        result = cursor.fetchone()

                        if result[0] == 0:
                            colunas = [
                                "idAgente", "expand", "padrao", "codigo", "nome","nomeFantasia", "cnpj", "consolidador"
                            ]

                            valores = (
                                id, expand, padrao, codigo, nome, nomeFantasia, cnpj, consolidador
                            )

                            placeholders = ', '.join(['%s'] * len(colunas))
                            colunas_str = ', '.join(colunas)
                            query = f"INSERT INTO Agente ({colunas_str}) VALUES ({placeholders})"

                            
                            print("🔹 Número de colunas na tabela:", colunas_str.count(",") + 1)
                            print("🔹 Número de valores passados:", len(valores))

                            print("🔢 Colunas:", colunas_str.count(",") + 1)
                            print("🔢 Valores:", len(valores))
                            if colunas_str.count(", ") + 1 != len(valores):
                                print("❌ ERRO: O número de colunas não corresponde ao número de valores fornecidos!")
                            else:
                                cursor.execute(query, valores)
                                db_connection.commit()
                                print(f"✅ Agente {id} salvo no banco.")
                        else:
                            print(f"⚠️ Agente {id} já existe no banco.")

                else:
                    print("❌ Erro: A resposta da API não é uma lista de dados.")


            except json.JSONDecodeError:
                print("❌ Erro ao decodificar a resposta JSON:", response.text)
        else:
            print(f"❌ Erro na requisição. Código: {response.status_code}")
            print("Resposta do servidor:", response.text)
            time.sleep(1)
    cursor.close()
    db_connection.close()