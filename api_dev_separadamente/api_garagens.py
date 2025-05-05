import requests
import json 
import mysql.connector
import utils

def api_garagens(token):
    DB_CONFIG = utils.Config_BD()
    db_connection = mysql.connector.connect(**DB_CONFIG)
    cursor = db_connection.cursor()

    cursor.execute("SELECT codigo,codigoFilial FROM Empreendimentos")
    for row in cursor.fetchall():
        dict_adicionar = {'codigo': row[0], 'filial': row[1]}
        cursor.execute("SELECT idBlocos FROM Blocos WHERE codigoFilial = %s", (dict_adicionar['filial'],))
        codigo_filial_lista = [row[0] for row in cursor.fetchall()]
        for idbloco in codigo_filial_lista:
            
            url = f"https://rest.megaerp.online/api/globalestruturas/Empreendimentos/{dict_adicionar['codigo']}/Blocos/{idbloco}/Garagens"

            payload = {}
            headers = {
            'Accept': 'text/plain',
            'Authorization': f'bearer {token}'
            }

            response = requests.request("GET", url, headers=headers, data=payload)

            if response.status_code == 200:
                try:
                    garagens = response.json()
                    if isinstance(garagens,list):
                        for garagem in garagens:
                            id_garagem = utils.tratar_none(garagem.get("id"))
                            codigo = utils.tratar_none(garagem.get("codigo"))
                            codigoExterno = utils.tratar_none(garagem.get("codigoExterno"))
                            nome = utils.tratar_none(garagem.get("nome"))
                            status = utils.tratar_none(garagem.get("status"))
                            cadastro = utils.tratar_none(garagem.get("cadastro"))
                            lancamento = utils.tratar_none(garagem.get("lancamento"))
                            area = utils.tratar_none(garagem.get("area"))
                            quota = utils.tratar_none(garagem.get("quota"))
                            peso = utils.tratar_none(garagem.get("peso"))
                            localizacao = utils.tratar_none(garagem.get("localizacao"))
                            tipologiaDescricao = utils.tratar_none(garagem.get("tipologiaDescricao"))
                            tipologiaEstendida = utils.tratar_none(garagem.get("tipologiaEstendida"))
                            andar = utils.tratar_none(garagem.get("andar"))
                            dataCriacao = utils.tratar_none(garagem.get("dataCriacao"))
                            dataAlteracao = utils.tratar_none(garagem.get("dataAlteracao"))
                            codigoFilial = utils.tratar_none(garagem.get("codigoFilial"))
                            cursor.execute("SELECT COUNT(*) FROM Garagens WHERE idGaragens = %s", (id_garagem,))
                            result = cursor.fetchone()

                            if result[0] == 0:
                                colunas = [
                                    "idGaragens", "codigo", "codigoExterno", "nome", "status", "cadastro", "lancamento",
                                    "area", "quota", "peso", "localizacao", "tipologiaDescricao", "tipologiaEstendida",
                                    "andar", "dataCriacao", "dataAlteracao", "codigoFilial", "id_bloco"
                                ]

                                valores =(id_garagem, codigo, codigoExterno, nome, status, cadastro, lancamento,
                                        area, quota, peso, localizacao, tipologiaDescricao, tipologiaEstendida,
                                        andar, dataCriacao, dataAlteracao, codigoFilial, idbloco)
                                
                                placeholders = ', '.join(['%s'] * len(colunas))
                                colunas_str = ', '.join(colunas)
                                query = f"INSERT INTO Garagens ({colunas_str}) VALUES ({placeholders})"
                                print("🔹 Número de colunas na tabela:", colunas_str.count(",") + 1)
                                print("🔹 Número de valores passados:", len(valores))

                                print("🔢 Colunas:", colunas_str.count(",") + 1)
                                print("🔢 Valores:", len(valores))
                                if colunas_str.count(", ") + 1 != len(valores):
                                    print("❌ ERRO: O número de colunas não corresponde ao número de valores fornecidos!")
                                else:
                                    cursor.execute(query, valores)
                                    db_connection.commit()
                                    print(f"✅ Garagem {id_garagem} salvo no banco.")
                            else:
                                print(f"⚠️ Garagem {id_garagem} já existe no banco.")

                    else:
                        print("❌ Erro: A resposta da API não é uma lista de dados.")


                except json.JSONDecodeError:
                    print("❌ Erro ao decodificar a resposta JSON:", response.text)
            else:
                print(f"❌ Erro na requisição. Código: {response.status_code}")
                print("Resposta do servidor:", response.text)

    cursor.close()
    db_connection.close()
