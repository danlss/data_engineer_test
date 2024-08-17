import pandas as pd
import requests
from datetime import datetime
import os

def get_exchange_rate():
    # Get USD to BRL exchange rate on 22/06/2022
    url = "https://economia.awesomeapi.com.br/json/daily/USD-BRL/1?start_date=20220622&end_date=20220622"
    response = requests.get(url)
    usd_brl_rate = float(response.json()[0]['high'])

    return usd_brl_rate

def transform_trusted():
    # Define the directory for the raw layer
    raw_dir = os.path.join("datalake", "raw")
    latest_raw_dir = sorted(os.listdir(raw_dir))[-1]
    raw_dir = os.path.join(raw_dir, latest_raw_dir)

    expenses = pd.read_csv(os.path.join(raw_dir, "expenses.csv"))
    revenues = pd.read_csv(os.path.join(raw_dir, "revenues.csv"))

    usd_brl_rate = get_exchange_rate()

    # Apply transformations for the trusted layer
    expenses['liquidated_brl'] = expenses['Liquidado'].str.replace('.', '').str.replace(',', '.').astype(float) * usd_brl_rate
    revenues['collected_brl'] = revenues['Arrecadado'].str.replace('.', '').str.replace(',', '.').astype(float) * usd_brl_rate

    # Remove the "Fonte de Recursos" column since fund_id and fund_name have already been extracted
    expenses = expenses[['fund_id', 'fund_name', 'Despesa', 'Liquidado', 'liquidated_brl']]
    revenues = revenues[['fund_id', 'fund_name', 'Receita', 'Arrecadado', 'collected_brl']]

    # Define the directory to save the trusted files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    trusted_dir = os.path.join("datalake", "trusted", timestamp)
    os.makedirs(trusted_dir, exist_ok=True)

    # Save the CSV files in the trusted layer
    expenses.to_csv(os.path.join(trusted_dir, "trusted_expenses.csv"), index=False)
    revenues.to_csv(os.path.join(trusted_dir, "trusted_revenues.csv"), index=False)
