from datetime import datetime
def cursor():
    BD_CONFIG = {
    "host": "",
    "port": ,
    "user": "",
    "password": "",
    "database": "",
    "auth_plugin": ""  # Adiciona suporte ao método correto
    }
    return BD_CONFIG

def tratar_none(valor, tipo="string"):
    if valor is None:
        return None if tipo == "int" else ""
    return valor

def tratar_data(data_str):
        if data_str is None or data_str == "":
            return None
        try:
            return datetime.strptime(data_str, "%d/%m/%Y").date()
        except ValueError:
            return None