# 🔄 Integração MegaERP → MySQL

Este projeto tem como objetivo integrar automaticamente os dados do sistema de gestão **MegaERP** com um banco de dados **MySQL**, consolidando informações críticas de agentes, empreendimentos, contratos, blocos, unidades, centros de custo, entre outros.

---

## 📌 Visão Geral

Este sistema consome APIs REST públicas do MegaERP, trata os dados retornados (validação, conversão, normalização) e persiste-os em tabelas relacionais no banco MySQL, evitando duplicidades e mantendo os dados sincronizados com atualizações inteligentes.

---

## ⚙️ Funcionalidades

- 🔐 Autenticação via API com token JWT
- 🔍 Coleta automatizada das seguintes rotas:
  - `Auth/SignIn`
  - `globalagente/AgenteCliente`
  - `globalagente/Agente`
  - `globalestruturas/Empreendimentos`
  - `Carteira/DadosContrato`
  - `globalestruturas/Blocos`, `Garagens`, `Unidades`
  - `contabilidadecadastros/CentroCusto`
  - `global/Projeto`
  - `FinanceiroMovimentacao/FaturaPagar`
  - `globalagente/Organizacao/Filiais`
- 🧠 Tratamento de dados nulos, booleanos e datas
- 🏷️ Verificação de duplicidade antes de inserção
- 🔁 Atualizações de campos sensíveis como status e saldo
- 🧩 Relacionamento entre entidades via chaves estrangeiras

---

## 🧱 Estrutura do Projeto (Refatorado)

```bash
project/
├── api_client.py          # Módulo de chamadas HTTP (GET, POST)
├── data_transformer.py    # Padronização e tratamento dos dados brutos
├── db_repository.py       # Inserção e atualização no MySQL
├── utils.py               # Funções utilitárias reutilizáveis
├── main.py                # Execução principal das importações
