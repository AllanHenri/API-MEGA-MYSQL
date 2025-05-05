import requests
import json
import mysql.connector
import utils
import time

def api(password, username, tenantId):
  url = "https://rest.megaerp.online/api/Auth/SignIn"

  payload = json.dumps({
    "password": str(password),
    "userName": str(username)
  })
  headers = {
    'tenantId': str(tenantId),
    'grantType': 'Api',
    'Content-Type': 'application/json',
    'Accept': 'text/plain'
  }

  response = requests.request("POST", url, headers=headers, data=payload)


  token_de_acesso = response.json()

  return token_de_acesso["accessToken"]



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
 
def api_estrutura(token):
    # Configuração do banco de dados MySQL
    DB_CONFIG = utils.Config_BD()

    url = "https://rest.megaerp.online/api/globalestruturas/Empreendimentos"

    payload = {}
    headers = {
    'Authorization': f'Bearer {token}'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

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



def api_projeto(token):
    DB_CONFIG = utils.Config_BD()

    url = "https://rest.megaerp.online/api/global/Projeto?expand=<string>"

    payload = {}
    headers = {
    'Authorization': f'Bearer {token}'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    if response.status_code == 200:
        try: 
            data = response.json()

            db_connection = mysql.connector.connect(**DB_CONFIG)
            cursor = db_connection.cursor()

            if isinstance(data, list):
                for item in data:
                    idprojeto = item.get("id")
                    padrao = item.get("padrao")
                    identificador = item.get("identificador")
                    reduzido = item.get("reduzido")
                    extenso = item.get("extenso")
                    descricao = item.get("descricao")
                    global_v = item.get("global")

                    if idprojeto:
                        cursor.execute("SELECT COUNT(*) FROM projeto WHERE idprojeto = %s", (idprojeto,))
                        result = cursor.fetchone()

                        if result[0] == 0:
                            cursor.execute("""
                            INSERT INTO projeto (idprojeto, padrao, identificador, reduzido, extenso, descricao, global, usuarios)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                            (idprojeto, padrao, identificador, reduzido, extenso, descricao, global_v, "null")
                            )
                            db_connection.commit()
                            print(f"Projeto {idprojeto} salvo no banco.")
                        else:
                            print(f"Projeto {idprojeto} já existe no banco")

            else:
                print("Erro: A resposta da API não é uma lista de dados")
            cursor.close()
            db_connection.close()
        
        except json.JSONDecodeError:
            print("Erro ao decodificar a resposta JSON:", response.text)

    else: 
        print(f"Erro na aquisição. Código: {response.status_code}")
        print("Resposta do servidor:", response.text)




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
