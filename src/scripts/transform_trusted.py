import pandas as pd
import requests
from datetime import datetime
import os

def get_cotation():
    # Obter cotação do dólar em 22/06/2022
    url = "https://economia.awesomeapi.com.br/json/daily/USD-BRL/1?start_date=20220622&end_date=20220622"
    response = requests.get(url)
    dolar_cotacao = float(response.json()[0]['high'])

    return dolar_cotacao

def transformacao_trusted():
    # Define o diretório da camada raw
    raw_dir = os.path.join("datalake", "raw")
    latest_raw_dir = sorted(os.listdir(raw_dir))[-1]
    raw_dir = os.path.join(raw_dir, latest_raw_dir)

    despesas = pd.read_csv(os.path.join(raw_dir, "despesas.csv"))
    receitas = pd.read_csv(os.path.join(raw_dir, "receitas.csv"))

    dolar_cotacao = get_cotation()

    # Aplica as transformações para a camada trusted
    despesas['liquidado_brl'] = despesas['Liquidado'].str.replace('.', '').str.replace(',', '.').astype(float) * dolar_cotacao
    receitas['arrecadado_brl'] = receitas['Arrecadado'].str.replace('.', '').str.replace(',', '.').astype(float) * dolar_cotacao

    # Remove a coluna "Fonte de Recursos" já que id_fonte e nome_fonte já foram extraídos
    despesas = despesas[['id_fonte', 'nome_fonte', 'Despesa', 'Liquidado', 'liquidado_brl']]
    receitas = receitas[['id_fonte', 'nome_fonte', 'Receita', 'Arrecadado', 'arrecadado_brl']]

    # Define o diretório para salvar os arquivos trusted
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    trusted_dir = os.path.join("datalake", "trusted", timestamp)
    os.makedirs(trusted_dir, exist_ok=True)

    # Salva os arquivos CSV na camada trusted
    despesas.to_csv(os.path.join(trusted_dir, "trusted_despesas.csv"), index=False)
    receitas.to_csv(os.path.join(trusted_dir, "trusted_receitas.csv"), index=False)



