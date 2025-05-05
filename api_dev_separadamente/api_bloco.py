import requests
import json
import mysql.connector
import utils

def api_bloco(token):
    DB_CONFIG = utils.Config_BD()
    db_connection = mysql.connector.connect(**DB_CONFIG)
    cursor = db_connection.cursor()

    cursor.execute("SELECT codigoFilial from Empreendimentos")

    codigo_filial_lista = [row[0] for row in cursor.fetchall()]


    dados_api = []

    print (codigo_filial_lista)
    for filial in codigo_filial_lista:
        url = f"https://rest.megaerp.online/api/globalestruturas/Empreendimentos/Blocos/Filial/{filial}"

        payload = {}
        headers = {
        'Accept': 'text/plain',
        'Authorization': f'Bearer {token}'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        if response.status_code == 200:
            try:
                dados_json = response.json()
                dados_api.append(dados_json)
            except json.JSONDecodeError:
                print(f"❌ Erro ao decodificar JSON para filial {filial}. Resposta:", response.text)
        else:
            print(f"❌ Erro na requisição para filial {filial}. Código {response.status_code}.")

    if isinstance(dados_api, list):
        for lista in dados_api:
            for item in lista:
                id_bloco = item.get("id")
                codigo = item.get("codigo")
                nome = item.get("nome")
                numeroUnidades = item.get("numeroUnidades")
                numeroAndares = item.get("numeroAndares")
                areaTerreno = item.get("areaTerreno")
                areaConstruida = item.get("areaConstruida")
                areaMedia = item.get("areaMedia")
                valorMetroQuadrado = item.get("valorMetroQuadrado")
                cei = item.get("cei")
                averbacao = item.get("averbacao")
                natureza = item.get("natureza")
                vagaAutonoma = item.get("vagaAutonoma")
                codigoExterno = item.get("codigoExterno")
                cadastro = item.get("cadastro")
                lancamento = item.get("lancamento")
                enderecoProprio = item.get("enderecoProprio")
                dataCriacao = item.get("dataCriacao")
                dataAlteracao = item.get("dataAlteracao")
                codigoFilial = int(item.get("codigoFilial"))
                if enderecoProprio == "N":
                    cursor.execute(f"SELECT idendereco FROM Empreendimentos WHERE codigoFilial = {codigoFilial};")
                    id_endereco = [row[0] for row in cursor.fetchall()][0]
                else:
                    endereco = item.get("endereco", {})
                    cep = endereco.get("cep")
                    pais = endereco.get("pais")
                    estado = endereco.get("estado")
                    municipio = endereco.get("municipio")
                    bairro = endereco.get("bairro")
                    tipoLogradouro = endereco.get("tipoLogradouro")
                    logradouro = endereco.get("logradouro")
                    numero = endereco.get("numero")
                    complemento = endereco.get("complemento")
                    referencia = endereco.get("referencia")
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

                centroCusto = item.get("centroCusto")
                id_centroCusto = centroCusto.get("id")
                projeto = item.get("projeto")
                id_projeto = projeto.get("id")
                tabOrganizacao = item.get("tabOrganizacao")
                padraoOrganizacao = item.get("padraoOrganizacao")
                codigoOrganizacao = item.get("codigoOrganizacao")
                expand = item.get("expand")

                cursor.execute("SELECT COUNT(*) FROM Blocos WHERE idBlocos = %s", (id_bloco,))
                result = cursor.fetchone()

                if result[0] == 0:
                    colunas = """idBlocos, codigo, nome, numeroUnidades, numeroAndares, areaTerreno, areaConstruida,
                        areaMedia, valorMetroQuadrado, cei, averbacao, natureza, vagaAutonoma, codigoExterno, cadastro, lancamento,
                        enderecoProprio, dataCriacao, dataAlteracao, codigoFilial, idendereco, idcentroCusto, idprojeto, tabOrganizacao,
                        padraoOrganizacao, codigoOrganizacao, expand
                    """

                    valores =(
                        id_bloco,codigo , nome ,numeroUnidades, numeroAndares, areaTerreno, areaConstruida, areaMedia, valorMetroQuadrado, cei, 
                        averbacao, natureza, vagaAutonoma, codigoExterno, cadastro, lancamento, enderecoProprio, dataCriacao, dataAlteracao, codigoFilial, 
                        id_endereco, id_centroCusto, id_projeto, tabOrganizacao, padraoOrganizacao, codigoOrganizacao, expand
                    )
                
                    placeholders = ", ".join(["%s"]*len(valores))
                    query = f"INSERT INTO Blocos ({colunas}) VALUES ({placeholders})"

                    print ("Numero de colunas na tabela: ", colunas.count(",") + 1)
                    print("Numero de valores passados: ", len(valores))

                    if colunas.count(",") + 1 != len(valores):
                        print("ERRO: o número de colunas não corresponde ao número de  fornecidos!")
                    else:
                        cursor.execute("SELECT 1 FROM Empreendimentos WHERE codigo = %s", (codigoFilial,))
                        existe = cursor.fetchone()

                        #if existe:
                        cursor.execute(query, valores)
                        db_connection.commit()
                        print(f"Bloco {id_bloco} salvo no banco.")
                        #else:
                        #print(f"❌ Bloco {id_bloco} IGNORADO: códigoFilial {codigoFilial} não existe na tabela Empreendimentos.")

                else:
                    print(f"Bloco {id_bloco} já existe no banco.")
        else:
            print("❌ Erro: A resposta da API não é uma lista de dados.")
        cursor.close()
        db_connection.close()
