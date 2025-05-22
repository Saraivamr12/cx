import requests
import pandas as pd
from datetime import datetime

# Parâmetros
start_date = "20250401"
end_date = '20250430'
api_key = "I/IhnbTAavD3x9axxeb9OQ=="

# URL da API
url = f"https://hermes-app.vocalcom.com.br/RelatoriosWAP/API/DataExtraction/GetActionsReport?startDate={start_date}&endDate={end_date}&apiKey={api_key}"

# Requisição GET
response = requests.get(url)

# Verificando resposta
if response.status_code == 200:
    try:
        data = response.json()  # tenta converter para JSON
    except:
        from io import StringIO
        data = pd.read_csv(StringIO(response.text))  # caso seja CSV
    else:
        data = pd.DataFrame(data)  # se for JSON
        
    # Salvando no Excel
    data.to_excel("Relatorio_Acoes_2025.xlsx", index=False)
    print("Arquivo Excel salvo com sucesso!")
else:
    print(f"Erro ao acessar API: {response.status_code}")

