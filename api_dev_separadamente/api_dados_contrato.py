import requests
import json
import mysql.connector
import utils
def api_dados_contrato(token):
    DB_CONFIG = utils.Config_BD()
    db_connection = mysql.connector.connect(**DB_CONFIG)
    cursor = db_connection.cursor()

    def tratar_none(valor, tipo="string"):
        if valor is None:
            return None if tipo == "int" else ""
        return valor 

    from datetime import datetime

    def tratar_data(data_str):
        if data_str is None or data_str == "":
            return None
        try:
            return datetime.strptime(data_str, "%d/%m/%Y").date()
        except ValueError:
            return None

    cursor.execute("SELECT codigo FROM Empreendimentos")
    codigos_empreendimentos = [row[0] for row in cursor.fetchall()]

    for codigo_emp in codigos_empreendimentos:

        url = f"https://rest.megaerp.online/api/Carteira/DadosContrato/IdEmpreendimento={codigo_emp}"

        payload = {}
        headers = {
        'Accept': 'text/plain',
        'Authorization': f'Bearer {token}'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        if response.status_code == 200:
            try:
                contratos = response.json()
                if isinstance(contratos,list):
                    for contrato in contratos:
                        cod_contrato = tratar_none(contrato.get("cod_contrato"))
                        cod_filial = tratar_none(contrato.get("cod_filial"))
                        nome_filial = tratar_none(contrato.get("nome_filial"))
                        cod_proposta = tratar_none(contrato.get("cod_proposta"))
                        cod_cliente = tratar_none(contrato.get("cod_cliente"))
                        valor_contrato = tratar_none(contrato.get("valor_contrato"))
                        tipo_contrato = tratar_none(contrato.get("tipo_contrato"))
                        data_cadastro = tratar_none(tratar_data(contrato.get("data_cadastro")))
                        status_contrato = tratar_none(contrato.get("status_contrato"))
                        data_status = tratar_none(tratar_data(contrato.get("data_status")))
                        classificacaocto = tratar_none(contrato.get("classificacaocto"))
                        data_classificacaocto = tratar_none(tratar_data(contrato.get("data_classificacaocto")))
                        perc_multa = tratar_none(contrato.get("perc_multa"))
                        perc_mora = tratar_none(contrato.get("perc_mora"))
                        data_entrega = tratar_none(tratar_data(contrato.get("data_entrega")))
                        data_assinatura = tratar_none(tratar_data(contrato.get("data_assinatura")))                   
                        tipo_estrutura = tratar_none(contrato.get("tipo_estrutura"))
                        status_estrutura = tratar_none(contrato.get("status_estrutura"))
                        cod_empreendimento = tratar_none(contrato.get("cod_empreendimento"))
                        cod_etapa = tratar_none(contrato.get("cod_etapa"))
                        cod_st_etapa = tratar_none(contrato.get("cod_st_etapa"))
                        nome_etapa = tratar_none(contrato.get("nome_etapa"))
                        cod_bloco = tratar_none(contrato.get("cod_bloco"))
                        cod_unidade = tratar_none(contrato.get("cod_unidade"))
                        desc_classificacao = tratar_none(contrato.get("desc_classificacao"))
                        data_EntregaChaves = tratar_none(tratar_data(contrato.get("data_EntregaChaves")))
                        cursor.execute("SELECT 1 FROM Unidades WHERE codigo = %s", (cod_unidade,))
                        unidade_existe = cursor.fetchone()

                        if not unidade_existe:
                            print(f"❌ Unidade {cod_unidade} não encontrada na tabela Unidades. Contrato {cod_contrato} ignorado.")
                            continue


                        cursor.execute("SELECT COUNT(*) FROM DadosContrato WHERE cod_contrato = %s", (cod_contrato,))
                        result = cursor.fetchone()

                        if result[0] == 0:
                            colunas = [
                                "cod_contrato", "cod_filial", "nome_filial", "cod_proposta", "cod_cliente", "valor_contrato",
                                "tipo_contrato", "data_cadastro", "status_contrato", "data_status", "classificacaocto", "data_classificacaocto",
                                "perc_multa", "perc_mora", "data_entrega", "data_assinatura", "tipo_estrutura", "status_estrutura",
                                "cod_empreendimento", "cod_etapa", "cod_st_etapa", "nome_etapa", "cod_bloco", "cod_unidade", "desc_classificacao", "data_EntregaChaves"
                            ]

                            valores =(
                                cod_contrato, cod_filial, nome_filial, cod_proposta, cod_cliente, valor_contrato, tipo_contrato, data_cadastro, status_contrato,
                                data_status, classificacaocto, data_classificacaocto, perc_multa, perc_mora, data_entrega, data_assinatura, tipo_estrutura, status_estrutura,
                                cod_empreendimento, cod_etapa, cod_st_etapa, nome_etapa, cod_bloco, cod_unidade, desc_classificacao, data_EntregaChaves
                            )
                            placeholder = ', '.join(['%s'] * len(colunas))
                            colunas_str = ', '.join(colunas)
                            query = f"INSERT INTO DadosContrato ({colunas_str}) Values ({placeholder})"
                            print("🔹 Número de colunas na tabela:", colunas_str.count(",") + 1)
                            print("🔹 Número de valores passados:", len(valores))

                            print("🔢 Colunas:", colunas_str.count(",") + 1)
                            print("🔢 Valores:", len(valores))
                            if colunas_str.count(", ") + 1 != len(valores):
                                print("❌ ERRO: O número de colunas não corresponde ao número de valores fornecidos!")
                            else:
                                cursor.execute(query, valores)
                                db_connection.commit()
                                print(f"✅ Contrato {cod_contrato} salvo no banco.")
                        else:
                            print(f"⚠️ Contrato {cod_contrato} já existe no banco.")

                    else:
                        print("❌ Erro: A resposta da API não é uma lista de dados.")


            except json.JSONDecodeError:
                print("❌ Erro ao decodificar a resposta JSON:", response.text)
        else:
            print(f"❌ Erro na requisição. Código: {response.status_code}")
            print("Resposta do servidor:", response.text)

    cursor.close()
    db_connection.close()
 