import requests
import json
import mysql.connector
import utils

DB_CONFIG = utils.Config_BD()
db_connection = mysql.connector.connect(**DB_CONFIG)
cursor = db_connection.cursor()

cursor.execute("SELECT codigoFilial FROM Empreendimentos")
codigo_filial_lista = [row[0] for row in cursor.fetchall()]

for filial in codigo_filial_lista:
    url = f"https://rest.megaerp.online/api/globalestruturas/Empreendimentos/Blocos/Filial/{filial}"
    headers = {
        'Accept': 'text/plain',
        'Authorization': 'Bearer eyJhbGciOiJodHRwOi8vd3d3LnczLm9yZy8yMDAxLzA0L3htbGRzaWctbW9yZSNobWFjLXNoYTI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1lIjoiYXBpLnRlc3RlIiwiaHR0cDovL3NjaGVtYXMueG1sc29hcC5vcmcvd3MvMjAwNS8wNS9pZGVudGl0eS9jbGFpbXMvbmFtZWlkZW50aWZpZXIiOiIyMDciLCJ0ZW5hbnRJZCI6IjZhYzAzYzBjLWEwZDgtNGYwYi1hMDNhLThkZDUwNWEwMjgxMSIsImN1c3RvbV9jbGFpbXMiOiIiLCJuYmYiOjE3NDI4MzE0NDgsImV4cCI6MTc0MjgzODY0OCwiYXVkIjoibWVnYS1hcGlzIn0.MzVev3a_tVQgoaref27dGhKLN6HBsCYkLqL3GcdP3Pk'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        try:
            blocos = response.json()
            if isinstance(blocos, list):
                for bloco in blocos:
                    id_bloco = bloco.get("id")
                    cursor.execute("SELECT COUNT(*) FROM Blocos WHERE idBlocos = %s", (id_bloco,))
                    result = cursor.fetchone()

                    if result[0] == 0:
                        colunas = [
                            "idBlocos", "codigo", "nome", "numeroUnidades", "numeroAndares", "areaTerreno", "areaConstruida",
                            "areaMedia", "valorMetroQuadrado", "cei", "averbacao", "natureza", "vagaAutonoma", "codigoExterno",
                            "cadastro", "lancamento", "enderecoProprio", "dataCriacao", "dataAlteracao", "codigoFilial",
                            "idendereco", "idcentroCusto", "idprojeto", "tabOrganizacao", "padraoOrganizacao", "codigoOrganizacao", "expand"
                        ]

                        valores = (
                            bloco.get("id"), bloco.get("codigo"), bloco.get("nome"), bloco.get("numeroUnidades"),
                            bloco.get("numeroAndares"), bloco.get("areaTerreno"), bloco.get("areaConstruida"), bloco.get("areaMedia"),
                            bloco.get("valorMetroQuadrado"), bloco.get("cei"), bloco.get("averbacao"), bloco.get("natureza"),
                            bloco.get("vagaAutonoma"), bloco.get("codigoExterno"), bloco.get("cadastro"), bloco.get("lancamento"),
                            bloco.get("enderecoProprio"), bloco.get("dataCriacao"), bloco.get("dataAlteracao"), bloco.get("codigoFilial"),
                            bloco.get("idendereco"), bloco.get("idcentroCusto"), bloco.get("idprojeto"), bloco.get("tabOrganizacao"),
                            bloco.get("padraoOrganizacao"), bloco.get("codigoOrganizacao"), bloco.get("expand")
                        )

                        if len(colunas) != len(valores):
                            print(f"❌ ERRO: número de colunas ({len(colunas)}) não bate com número de valores ({len(valores)})")
                        else:
                            colunas_sql = ", ".join(colunas)
                            placeholders = ", ".join(["%s"] * len(valores))
                            query = f"INSERT INTO Blocos ({colunas_sql}) VALUES ({placeholders})"

                            cursor.execute(query, valores)
                            db_connection.commit()
                            print(f"✅ Bloco {id_bloco} salvo no banco.")
                    else:
                        print(f"ℹ️ Bloco {id_bloco} já existe no banco.")
        except json.JSONDecodeError:
            print(f"❌ Erro ao decodificar JSON da filial {filial}")
    else:
        print(f"❌ Erro na requisição para filial {filial}. Código {response.status_code}")

cursor.close()
db_connection.close()
