import requests

# URL da API
url = "https://hermes-app.vocalcom.com.br/RelatoriosWAP"

# Fazendo a requisição GET
try:
    response = requests.get(url, timeout=10)

    # Verifica o status da resposta
    if response.status_code == 200:
        print("Resposta recebida com sucesso!")
        print(response.text)  # Exibe o conteúdo da resposta
    else:
        print(f"Erro ao acessar a API. Código: {response.status_code}")

except requests.exceptions.RequestException as e:
    print(f"Erro ao acessar a API: {e}")
