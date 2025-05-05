import requests
import json
def api(password, username, tenantId)
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

  print(token_de_acesso["accessToken"])