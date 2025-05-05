import requests
import json
import mysql.connector
import utils
def api_faturaPagar(token, data_inicial, data_final):
    DB_CONFIG = utils.Config_BD()
    db_connection = mysql.connector.connect(**DB_CONFIG)
    cursor = db_connection.cursor()

    url = f"https://rest.megaerp.online/api/FinanceiroMovimentacao/FaturaPagar/Saldo/{data_inicial}/{data_final}"

    payload = {}
    headers = {
    'Authorization': f'bearer {token}'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    if response.status_code == 200:
        try:
            movimentacoes = response.json()
            if isinstance(movimentacoes, list):
                for movimentacao in movimentacoes:
                    NumeroAP = movimentacao.get("NumeroAP")
                    TipoDocumento = movimentacao.get("TipoDocumento")
                    NumeroDocumento = movimentacao.get("NumeroDocumento")
                    NumeroParcela = movimentacao.get("NumeroParcela")
                    DataVencimento = utils.tratar_data(movimentacao.get("DataVencimento"))
                    DataProrrogado = utils.tratar_data(movimentacao.get("DataProrrogado"))
                    ValorParcela = movimentacao.get("ValorParcela")
                    SaldoAtual = movimentacao.get("SaldoAtual")
                    agente = movimentacao.get("Agente")
                    idAgente = agente.get("Id")
                    filial = movimentacao.get("Filial")
                    idFilial= filial.get("Id")
                    print(idAgente)

                    cursor.execute("SELECT COUNT(*) FROM FaturaPagar WHERE NumeroAP = %s AND idFilial = %s AND TipoDocumento = %s", (NumeroAP, idFilial, TipoDocumento))
                    result = cursor.fetchone()
                    if result[0] == 0:
                        colunas = [
                            "NumeroAP", "TipoDocumento", "NumeroDocumento", "NumeroParcela", "DataVencimento", "DataProrrogado",
                            "ValorParcela", "SaldoAtual", "idFilial", "idAgente"
                        ]

                        valores = (
                            NumeroAP, TipoDocumento, NumeroDocumento, NumeroParcela, DataVencimento, DataProrrogado, ValorParcela,
                            SaldoAtual, idFilial, idAgente
                        )
                        placeholders = ', '.join(['%s'] * len(colunas))
                        colunas_str = ', '.join(colunas)
                        query = f"INSERT INTO FaturaPagar ({colunas_str}) VALUES ({placeholders})"

                        print("🔹 Número de colunas na tabela:", colunas_str.count(",") + 1)
                        print("🔹 Número de valores passados:", len(valores))

                        print("🔢 Colunas:", colunas_str.count(",") + 1)
                        print("🔢 Valores:", len(valores))
                        if colunas_str.count(", ") + 1 != len(valores):
                            print("❌ ERRO: O número de colunas não corresponde ao número de valores fornecidos!")
                        else:
                            cursor.execute(query, valores)
                            db_connection.commit()
                            print(f"✅ movimentacao {NumeroAP} salvo no banco.")
                    else:
                        print(f"⚠️ movimentacao {NumeroAP} já existe no banco.")
                        cursor.execute("SELECT idFaturaPagar, SaldoAtual FROM FaturaPagar WHERE NumeroAP = %s AND idFilial = %s AND TipoDocumento = %s", (NumeroAP, idFilial, TipoDocumento))
                        valor = cursor.fetchall()
                        if valor [0][1] != SaldoAtual:
                            cursor.execute("UPDATE FaturaPagar SET SaldoAtual = %s WHERE idFaturaPagar = %s", (SaldoAtual,valor[0][0]))
                            print("💮Valor de saldo atualizado")
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