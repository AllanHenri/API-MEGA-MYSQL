import requests
import json
import mysql.connector
import utils


def api_unidades(token):
    DB_CONFIG = utils.Config_BD()
    db_connection = mysql.connector.connect(**DB_CONFIG)
    cursor = utils.cursor()


    cursor.execute("SELECT id,codigoFilial FROM Empreendimentos")
    for row in cursor.fetchall():
        dict_adicionar = {'id_empreendimento': row[0], 'filial': row[1] }
        cursor.execute("SELECT idBlocos FROM Blocos WHERE codigoFilial = %s", (dict_adicionar['filial'],))
        codigo_filial_lista = [row[0] for row in cursor.fetchall()]
        for idbloco in codigo_filial_lista:
            url = f"https://rest.megaerp.online/api/globalestruturas/Empreendimentos/{dict_adicionar['id_empreendimento']}/Blocos/{idbloco}/Unidades"
            payload = {}
            headers = {
            'Accept': 'text/plain',
            'Authorization': f'Bearer {token}'
            }

            response = requests.request("GET", url, headers=headers, data=payload)

            if response.status_code == 200:
                try:
                    unidades = response.json()
                    if isinstance(unidades,list):
                        for unidade in unidades:
                            id_unidade = utils.tratar_none(unidade.get("id"))
                            codigo = utils.tratar_none(unidade.get("codigo"))
                            codigoExterno = utils.tratar_none(unidade.get("codigoExterno"))
                            nome = utils.tratar_none(unidade.get("nome"))
                            status = utils.tratar_none(unidade.get("status"))
                            areaPrivativa = utils.tratar_none(unidade.get("areaPrivativa"))
                            areaComum = utils.tratar_none(unidade.get("areaComum"))
                            areaTotal = utils.tratar_none(unidade.get("areaTotal"))
                            areaGaragem = utils.tratar_none(unidade.get("areaGaragem"))
                            areaOutros = utils.tratar_none(unidade.get("areaOutros"))
                            areaTerraco = utils.tratar_none(unidade.get("areaTerraco"))
                            areaDeposito = utils.tratar_none(unidade.get("areaDeposito"))
                            quota = utils.tratar_none(unidade.get("quota"))
                            andar = utils.tratar_none(unidade.get("andar"))
                            peso = utils.tratar_none(unidade.get("peso"))
                            dependencias = utils.tratar_none(unidade.get("dependencias"))
                            quartos = utils.tratar_none(unidade.get("quartos"))
                            dataMatricula = utils.tratar_none(unidade.get("dataMatricula"))
                            dataAlvara = utils.tratar_none(unidade.get("dataAlvara"))
                            cadastro = utils.tratar_none(unidade.get("cadastro"))
                            lancamento = utils.tratar_none(unidade.get("lancamento"))
                            matricula = utils.tratar_none(unidade.get("matricula"))
                            localMatricula = utils.tratar_none(unidade.get("localMatricula"))
                            registroMatricula = utils.tratar_none(unidade.get("registroMatricula"))
                            alvara = utils.tratar_none(unidade.get("alvara"))
                            processoAlvara = utils.tratar_none(unidade.get("processoAlvara"))
                            prefeituraAlvara = utils.tratar_none(unidade.get("prefeituraAlvara"))
                            inscricaoMunicipal = utils.tratar_none(unidade.get("inscricaoMunicipal"))
                            indicacaoFiscal = utils.tratar_none(unidade.get("indicacaoFiscal"))
                            localDeposito = utils.tratar_none(unidade.get("localDeposito"))
                            enderecoProprio = utils.tratar_none(unidade.get("enderecoProprio"))
                            box = utils.tratar_none(unidade.get("box"))
                            tipologiaDescricao = utils.tratar_none(unidade.get("tipologiaDescricao"))
                            tipologiaEstendida = utils.tratar_none(unidade.get("tipologiaEstendida"))
                            dataCriacao = utils.tratar_none(unidade.get("dataCriacao"))
                            dataAlteracao = utils.tratar_none(unidade.get("dataAlteracao"))
                            codigoFilial = utils.tratar_none(unidade.get("codigoFilial"))
                            agente = utils.tratar_none(unidade.get("agente"))
                            if agente:
                                id_agente = agente[0].get("codigo")
                            cursor.execute("SELECT COUNT(*) FROM Unidades WHERE idUnidades = %s", (id_unidade,))
                            result = cursor.fetchone()

                            if result[0] == 0:
                                colunas = [
                                    "idUnidades", "codigo", "codigoExterno", "nome", "status", "areaPrivativa",
                                    "areaComum", "areaTotal", "areaGaragem", "areaOutros", "areaTerraco", "areaDeposito",
                                    "quota", "andar", "peso", "dependencias", "quartos", "dataMatricula", "dataAlvara", 
                                    "cadastro", "lancamento", "matricula", "localMatricula", "registroMatricula", "alvara",
                                    "processoAlvara", "prefeituraAlvara", "inscricaoMunicipal", "indicacaoFiscal", "localDeposito",
                                    "enderecoProprio", "box", "tipologiaDescricao", "tipologiaEstendida", "dataCriacao", "dataAlteracao",
                                    "codigoFilial", "idAgente", "idBloco"
                                ]

                                valores =(
                                    id_unidade, codigo, codigoExterno, nome, status, areaPrivativa, areaComum, 
                                    areaTotal, areaGaragem, areaOutros, areaTerraco, areaDeposito, quota, andar,
                                    peso, dependencias, quartos, dataMatricula, dataAlvara,cadastro,lancamento,
                                    matricula, localMatricula, registroMatricula, alvara, processoAlvara, prefeituraAlvara,
                                    inscricaoMunicipal, indicacaoFiscal, localDeposito, enderecoProprio, box, tipologiaDescricao,
                                    tipologiaEstendida, dataCriacao, dataAlteracao, codigoFilial, id_agente, idbloco
                                )
                                placeholders = ', '.join(['%s'] * len(colunas))
                                colunas_str = ', '.join(colunas)
                                query = f"INSERT INTO Unidades ({colunas_str}) VALUES ({placeholders})"


                                print("🔹 Número de colunas na tabela:", colunas_str.count(",") + 1)
                                print("🔹 Número de valores passados:", len(valores))

                                print("🔢 Colunas:", colunas_str.count(",") + 1)
                                print("🔢 Valores:", len(valores))
                                if colunas_str.count(", ") + 1 != len(valores):
                                    print("❌ ERRO: O número de colunas não corresponde ao número de valores fornecidos!")
                                else:
                                    cursor.execute(query, valores)
                                    db_connection.commit()
                                    print(f"✅ Unidade {id_unidade} salvo no banco.")
                            else:
                                print(f"⚠️ Unidade {id_unidade} já existe no banco.")
                                cursor.execute("SELECT status, idAgente FROM Unidades WHERE idUnidades = %s", (id_unidade,))
                                resultado = cursor.fetchall()
                                if resultado[0][0] != status:
                                    cursor.execute("UPDATE Unidades SET status = %s WHERE idUnidades = %s",(status, id_unidade))
                                    print("ℹ️Status Atualizado")
                                    db_connection.commit()  
                                id_agente_db = resultado[0][1]  # do banco

                                # Só atualiza se forem diferentes, considerando também o caso de None
                                if id_agente_db != id_agente:
                                    print(f"ℹ️id_agente Atualizado {id_agente_db} -> {id_agente}")
                                    cursor.execute(
                                        "UPDATE Unidades SET idAgente = %s WHERE idUnidades = %s",
                                        (id_agente, id_unidade)
                                    )
                                    db_connection.commit()  
                    else:
                        print("❌ Erro: A resposta da API não é uma lista de dados.")


                except json.JSONDecodeError:
                    print("❌ Erro ao decodificar a resposta JSON:", response.text)
            else:
                print(f"❌ Erro na requisição. Código: {response.status_code}")
                print("Resposta do servidor:", response.text)

    cursor.close()
    db_connection.close()
