import requests
import json
import mysql.connector
import utils

# Configuração do banco de dados MySQL
DB_CONFIG = utils.Config_BD()

url = "https://rest.megaerp.online/api/globalestruturas/Empreendimentos"

payload = {}
headers = {
  'Authorization': 'Bearer eyJhbGciOiJodHRwOi8vd3d3LnczLm9yZy8yMDAxLzA0L3htbGRzaWctbW9yZSNobWFjLXNoYTI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1lIjoiYXBpLnRlc3RlIiwiaHR0cDovL3NjaGVtYXMueG1sc29hcC5vcmcvd3MvMjAwNS8wNS9pZGVudGl0eS9jbGFpbXMvbmFtZWlkZW50aWZpZXIiOiIyMDciLCJ0ZW5hbnRJZCI6IjZhYzAzYzBjLWEwZDgtNGYwYi1hMDNhLThkZDUwNWEwMjgxMSIsImN1c3RvbV9jbGFpbXMiOiIiLCJuYmYiOjE3NDI4MzgwNTgsImV4cCI6MTc0Mjg0NTI1OCwiYXVkIjoibWVnYS1hcGlzIn0.Gf84cg61dvJ1nmw83970C0SWCIIBxBK3YSXXyS9Hi0U'
}

response = requests.request("GET", url, headers=headers, data=payload)

# Função para tratar valores None
def tratar_none(valor, tipo="string"):
    if valor is None:
        return None if tipo == "int" else ""
    return valor

# Função para converter booleanos e strings ('S', 'N', 'TRUE', 'FALSE') para 1 ou 0

if response.status_code == 200:
    try:
        data = response.json()
        db_connection = mysql.connector.connect(**DB_CONFIG)
        cursor = db_connection.cursor()

        if isinstance(data, list):
            for item in data:
                id_empreendimento = utils.tratar_none(item.get("id"), "string")
                if id_empreendimento=="NTPDuDHDuDk5w7g3NTU1":
                    continue
                codigo = utils.tratar_none(item.get("codigo"), "int")
                codigoOrganizacao = utils.tratar_none(item.get("codigoOrganizacao"), "int")
                codigoFilial = utils.tratar_none(item.get("codigoFilial"), "int")
                nomeFilial = utils.tratar_none(item.get("nomeFilial"))
                fantasiaFilial = utils.tratar_none(item.get("fantasiaFilial"))
                cnpjFilial = utils.tratar_none(item.get("cnpjFilial"))
                codigo_externo = utils.tratar_none(item.get("codigo_externo"))
                nome = utils.tratar_none(item.get("nome"))
                nomeReal = utils.tratar_none(item.get("nomeReal"))
                tipoImovel = utils.tratar_none(item.get("tipoImovel"))
                
                endereco = item.get("endereco", {})
                cep = utils.tratar_none(endereco.get("cep"))
                pais = utils.tratar_none(endereco.get("pais"))
                estado = utils.tratar_none(endereco.get("estado"))
                municipio = utils.tratar_none(endereco.get("municipio"))
                bairro = utils.tratar_none(endereco.get("bairro"))
                tipoLogradouro = utils.tratar_none(endereco.get("tipoLogradouro"))
                logradouro = utils.tratar_none(endereco.get("logradouro"))
                numero = utils.tratar_none(endereco.get("numero"))
                complemento = utils.tratar_none(endereco.get("complemento"))
                referencia = utils.tratar_none(endereco.get("referencia"))
                idEndereco = None
                if cep and logradouro and numero:
                    cursor.execute(
                        "SELECT idEndereco FROM endereco WHERE cep = %s AND logradouro = %s AND numero = %s",
                        (cep, logradouro, numero)
                    )
                    result = cursor.fetchone()

                    if result:
                        idEndereco = result[0]
                        print(f"Endereço já existe no banco com id {idEndereco}.")
                    else: 
                        cursor.execute(
                            """INSERT INTO endereco (cep, pais, estado, municipio, bairro, tipoLogradouro, logradouro, numero, complemento, referencia)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            (cep, pais, estado, municipio, bairro, tipoLogradouro, logradouro, numero, complemento, referencia)
                        )
                        db_connection.commit()
                        idEndereco = cursor.lastrowid
                        print(f"Novo endereço inserido com id {idEndereco}.")

                # ✅ Converte corretamente os valores booleanos para INT (0 ou 1)
                disponivelPortalCliente = item.get("disponivelPortalCliente")
                disponivelHomepay = item.get("disponivelHomepay")
                
                inicioObra = utils.tratar_none(item.get("inicioObra"))
                fimObra = utils.tratar_none(item.get("fimObra"))
                habite = utils.tratar_none(item.get("habite"))
                previsaoHabite = utils.tratar_none(item.get("previsaoHabite"))
                averbacao = utils.tratar_none(item.get("averbacao"))
                instalacaoCondominio = utils.tratar_none(item.get("instalacaoCondominio"))
                cadastro = utils.tratar_none(item.get("cadastro"))
                lancamento = utils.tratar_none(item.get("lancamento"))
                vencimentoDivida = utils.tratar_none(item.get("vencimentoDivida"))
                conclusaoPastaMae = utils.tratar_none(item.get("conclusaoPastaMae"))
                entregaAreaComum = utils.tratar_none(item.get("entregaAreaComum"))
                areaTerreno = utils.tratar_none(item.get("areaTerreno"), "int")
                areaEquivalente = utils.tratar_none(item.get("areaEquivalente"), "int")
                areaPrivada = utils.tratar_none(item.get("areaPrivada"), "int")
                areaTotal = utils.tratar_none(item.get("areaTotal"), "int")
                areaConstrucaoPrefeitura = utils.tratar_none(item.get("areaConstrucaoPrefeitura"), "int")
                areaConstrucaoEquivalente = utils.tratar_none(item.get("areaConstrucaoEquivalente"), "int")
                areaConstrucaoTotal = utils.tratar_none(item.get("areaConstrucaoTotal"), "int")
                codigoCentroCustoComercial = utils.tratar_none(item.get("codigoCentroCustoComercial"), "int")
                codigoCentroCustoCliente = utils.tratar_none(item.get("codigoCentroCustoCliente"), "int")
                dataCriacao = utils.tratar_none(item.get("dataCriacao"))
                dataAlteracao = utils.tratar_none(item.get("dataAlteracao"))

                # Pegando os dados do centro de custo e projeto
                centroCusto = item.get("centroCusto", {})
                idcentroCusto = utils.tratar_none(centroCusto.get("id"), "string")
                projeto = item.get("projeto", {})
                idprojeto = utils.tratar_none(projeto.get("id"), "string")
                tabOrganizacao = utils.tratar_none(item.get("tabOrganizacao"), "int")
                padraoOrganizacao = utils.tratar_none(item.get("padraoOrganizacao"), "int")
                expand = utils.tratar_none(item.get("expand"), "string")

                # **Verifica se o empreendimento já existe no banco**
                cursor.execute("SELECT COUNT(*) FROM Empreendimentos WHERE id = %s" , (id_empreendimento,))
                result = cursor.fetchone()

                if result[0] == 0:
                    colunas = """
                        id, codigo, codigoOrganizacao, codigoFilial, nomeFilial, fantasiaFilial, 
                        cnpjFilial, codigo_externo, nome, nomeReal, tipoImovel, disponivelPortalCliente, 
                        disponivelHomepay, inicioObra, fimObra, habite, previsaoHabite, averbacao, 
                        instalacaoCondominio, cadastro, lancamento, vencimentoDivida, conclusaoPastaMae, 
                        entregaAreaComum, areaTerreno, areaEquivalente, areaPrivada, areaTotal, 
                        areaConstrucaoPrefeitura, areaConstrucaoEquivalente, areaConstrucaoTotal, 
                        codigoCentroCustoComercial, codigoCentroCustoCliente, dataCriacao, dataAlteracao,idendereco, 
                        idcentroCusto, idprojeto, tabOrganizacao, padraoOrganizacao, expand
                    """

                    valores = (
                        id_empreendimento, codigo, codigoOrganizacao, codigoFilial, nomeFilial, fantasiaFilial, cnpjFilial,
                        codigo_externo, nome, nomeReal, tipoImovel, disponivelPortalCliente, disponivelHomepay, inicioObra,
                        fimObra, habite, previsaoHabite, averbacao, instalacaoCondominio, cadastro, lancamento, vencimentoDivida,
                        conclusaoPastaMae, entregaAreaComum, areaTerreno, areaEquivalente, areaPrivada, areaTotal,
                        areaConstrucaoPrefeitura, areaConstrucaoEquivalente, areaConstrucaoTotal, codigoCentroCustoComercial,
                        codigoCentroCustoCliente, dataCriacao, dataAlteracao,idEndereco
                        , idcentroCusto, idprojeto, tabOrganizacao, 
                        padraoOrganizacao, expand
                    )

                    # **Verificar quantidade de valores e colunas**
                    placeholders = ", ".join(["%s"] * len(valores))
                    query = f"INSERT INTO Empreendimentos ({colunas}) VALUES ({placeholders})"

                    print("🔹 Número de colunas na tabela:", colunas.count(",") + 1)
                    print("🔹 Número de valores passados:", len(valores))

                    if colunas.count(",") + 1 != len(valores):
                        print("❌ ERRO: O número de colunas não corresponde ao número de valores fornecidos!")
                    else:
                        cursor.execute(query, valores)
                        db_connection.commit()
                        print(f"✅ Empreendimento {id_empreendimento} salvo no banco.")
                else:
                    print(f"⚠️ Empreendimento {id_empreendimento} já existe no banco.")

        else:
            print("❌ Erro: A resposta da API não é uma lista de dados.")

        cursor.close()
        db_connection.close()

    except json.JSONDecodeError:
        print("❌ Erro ao decodificar a resposta JSON:", response.text)
else:
    print(f"❌ Erro na requisição. Código: {response.status_code}")
    print("Resposta do servidor:", response.text)
