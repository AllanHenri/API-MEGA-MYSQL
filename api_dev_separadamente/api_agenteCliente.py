import requests
import json
import mysql.connector
import utils

def api_agenteCliente(token):
    DB_CONFIG = utils.Config_BD()

    url = "https://rest.megaerp.online/api/globalagente/AgenteCliente/codigocliente/"

    payload = {}
    headers = {
    'Accept': 'text/plain',
    'Authorization': f'Bearer {token}'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    # Função para tratar valores None
    def tratar_none(valor, tipo="string"):
        if valor is None:
            return None if tipo == "int" else ""
        return valor

    def tratar_tiny(valor):
        if valor:
            return 1
        return 0
    if response.status_code == 200:
        try:
            dados = response.json()
            db_connection = mysql.connector.connect(**DB_CONFIG)
            cursor = db_connection.cursor()

            if isinstance(dados, list):
                for item in dados:
                    codigoSistema = tratar_none(item.get("codigoSistema"))
                    tipoOrganizacao = tratar_none(item.get("tipoOrganizacao"))
                    isEnquadraIPI = tratar_none(tratar_tiny(item.get("isEnquadraIPI")))
                    isEnquadraICMS = tratar_none(tratar_tiny(item.get("isEnquadraICMS")))
                    isEnquadraISS = tratar_none(tratar_tiny(item.get("isEnquadraISS")))
                    statusCNPJ = tratar_none(item.get("statusCNPJ"))
                    isIPISimples = tratar_none(tratar_tiny(item.get("isIPISimples")))
                    isICMSSimples = tratar_none(tratar_tiny(item.get("isICMSSimples")))
                    isISSSimples = tratar_none(tratar_tiny(item.get("isISSSimples")))
                    isSimplesNacional = tratar_none(tratar_tiny(item.get("isSimplesNacional")))
                    formaPagamento = tratar_none(item.get("formaPagamento"), tipo="int")
                    ultimaAtualizacao = tratar_none(item.get("ultimaAtualizacao"))
                    validadeCadastro = tratar_none(item.get("validadeCadastro"), tipo="int")
                    tipoInscricao = tratar_none(item.get("tipoInscricao"))
                    isRetemIR = tratar_none(item.get("isRetemIR"))
                    isRetemINSS = tratar_none(item.get("isRetemINSS"))
                    codigoIbge = tratar_none(item.get("codigoIbge"))
                    telefones = item.get("telefone", [])

                    numeroTelefone = ""
                    tipoTelefone = ""
                    idTelefone = None

                    if isinstance(telefones, list) and telefones:
                        primeiro_telefone = telefones[0]
                        numeroTelefone = tratar_none(primeiro_telefone.get("numero"))
                        tipoTelefone = tratar_none(primeiro_telefone.get("tipo"))

                        if numeroTelefone:
                            cursor.execute(
                                "SELECT idTelefone FROM Telefone WHERE numero = %s", (numeroTelefone,)
                            )
                            result = cursor.fetchone()
                            if result:
                                idTelefone = result[0]
                                print(f"Telefone já existe com o id {idTelefone}.")
                            else:
                                cursor.execute(
                                    """INSERT INTO Telefone (numero, tipo) 
                                    VALUES (%s, %s)""",
                                    (numeroTelefone, tipoTelefone)
                                )
                                db_connection.commit()
                                idTelefone = cursor.lastrowid
                                print(f"Novo telefone inserido com id {idTelefone}.")

                    nomeFantasia = tratar_none(item.get("nomeFantasia"))
                    nomeAgente = tratar_none(item.get("nomeAgente"))
                    tipoInscricaoESocial = tratar_none(item.get("tipoInscricaoESocial"))
                    tipoPessoa = tratar_none(item.get("tipoPessoa"))
                    tipoPessoaRural = tratar_none(item.get("tipoPessoaRural"))
                    isFluxoCaixa = tratar_none(tratar_tiny(item.get("isFluxoCaixa")))
                    naturezaJuridica = tratar_none(item.get("naturezaJuridica"))
                    codigoCNAE = tratar_none(item.get("codigoCNAE"))
                    email = tratar_none(item.get("email"))
                    paisSigla = tratar_none(item.get("paisSigla"))
                    ufSigla = tratar_none(item.get("ufSigla"))
                    enquadramentoEmpresa = tratar_none(item.get("enquadramentoEmpresa"))
                    inscricaoEstadual = tratar_none(tratar_tiny(item.get("inscricaoEstadual")))
                    inscricaoMunicipal = tratar_none(tratar_tiny(item.get("inscricaoMunicipal")))
                    codigoCEI = tratar_none(item.get("codigoCEI"))
                    cnpj = tratar_none(item.get("cnpj"))
                    codigoMunicipio = tratar_none(item.get("codigoMunicipio"), tipo="int")
                    tipoLogradouro = tratar_none(item.get("tipoLogradouro"))
                    nomeLogradouro = tratar_none(item.get("nomeLogradouro"))
                    numeroEndereco = tratar_none(item.get("numeroEndereco"))
                    bairro = tratar_none(item.get("bairro"))
                    cep = tratar_none(item.get("cep"))
                    complementoEndereco = tratar_none(item.get("complementoEndereco"))
                    referenciaEndereco = tratar_none(item.get("referenciaEndereco"))
                    idEndereco = None
                    if cep and nomeLogradouro and numeroEndereco:
                        cursor.execute(
                            "SELECT idEndereco FROM endereco WHERE cep = %s AND logradouro = %s AND numero = %s",
                            (cep, nomeLogradouro, numeroEndereco)
                        )
                        result = cursor.fetchone()

                        if result:
                            idEndereco = result[0]
                            print(f"Endereço já existe no banco com id {idEndereco}.")
                        else: 
                            cursor.execute(
                                """INSERT INTO endereco (cep, pais, estado, municipio, bairro, tipoLogradouro, logradouro, numero, complemento, referencia)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                                (cep, paisSigla, ufSigla, codigoMunicipio, bairro, tipoLogradouro, nomeLogradouro, numeroEndereco, complementoEndereco, referenciaEndereco)
                            )
                            db_connection.commit()
                            idEndereco = cursor.lastrowid
                            print(f"Novo endereço inserido com id {idEndereco}.")
                    isOrgaoPublico = tratar_none(tratar_tiny(item.get("isOrgaoPublico")))
                    tipoDespAduaneira = tratar_none(item.get("tipoDespAduaneira"))
                    fretePesoValor = tratar_none(item.get("fretePesoValor"))
                    isDispensadoNIF = tratar_none(item.get("isDispensadoNIF"))
                    cursor.execute("SELECT COUNT(*) FROM AgenteCliente WHERE codigoSistema = %s", (codigoSistema,))
                    result = cursor.fetchone()

                    if result[0] == 0:
                        colunas = [
                        "CodigoSistema", "tipoOrganizacao", "isEnquadraIPI", "isEnquadraICMS", "isEnquadraISS",
                        "statusCNPJ", "isIPISimples", "isICMSSimples", "isISSSimples", "isSimplesNacional",
                        "formaPagamento", "ultimaAtualizacao", "validadeCadastro", "tipoInscricao", "isRetemIR",
                        "isRetemINSS", "codigoIbge", "idTelefone", "nomeFantasia", "nomeAgente",
                        "tipoInscricaoESocial", "tipoPessoa", "tipoPessoaRural", "isFluxoCaixa", "naturezaJuridica",
                        "codigoCNAE", "email", "paisSigla", "ufSigla", "enquadramentoEmpresa",
                        "inscricaoEstadual", "inscricaoMunicipal", "codigoCEI", "cnpj", "codigoMunicipio",
                        "id_endereco", "isOrgaoPublico", "tipoDespAduaneira", "fretePesoValor", "isDispensadoNIF"
                        ]

                        valores =(
                        codigoSistema, tipoOrganizacao, isEnquadraIPI, isEnquadraICMS, isEnquadraISS, statusCNPJ, isIPISimples,
                        isICMSSimples, isISSSimples, isSimplesNacional, formaPagamento, ultimaAtualizacao, validadeCadastro, tipoInscricao,
                        isRetemIR, isRetemINSS, codigoIbge, idTelefone, nomeFantasia, nomeAgente, tipoInscricaoESocial, tipoPessoa,
                        tipoPessoaRural, isFluxoCaixa, naturezaJuridica, codigoCNAE, email, paisSigla, ufSigla, enquadramentoEmpresa,
                        inscricaoEstadual, inscricaoMunicipal, codigoCEI, cnpj, codigoMunicipio, idEndereco, isOrgaoPublico, tipoDespAduaneira,
                        fretePesoValor, isDispensadoNIF
                        )
                        placeholders = ', '.join(['%s'] * len(colunas))
                        colunas_str = ', '.join(colunas)
                        query = f"INSERT INTO AgenteCliente ({colunas_str}) VALUES ({placeholders})"


                        print("🔹 Número de colunas na tabela:", colunas_str.count(",") + 1)
                        print("🔹 Número de valores passados:", len(valores))
                        
                        print("🔢 Colunas:", colunas_str.count(",") + 1)
                        print("🔢 Valores:", len(valores))


                        if colunas_str.count(", ") + 1 != len(valores):
                            print("❌ ERRO: O número de colunas não corresponde ao número de valores fornecidos!")
                        else:
                            cursor.execute(query, valores)
                            db_connection.commit()
                            print(f"✅ Agente {codigoSistema} salvo no banco.")
                    else:
                        print(f"⚠️ Agente {codigoSistema} já existe no banco.")

            else:
                print("❌ Erro: A resposta da API não é uma lista de dados.")

            cursor.close()
            db_connection.close()

        except json.JSONDecodeError:
            print("❌ Erro ao decodificar a resposta JSON:", response.text)
    else:
        print(f"❌ Erro na requisição. Código: {response.status_code}")
        print("Resposta do servidor:", response.text)