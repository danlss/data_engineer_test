import pandas as pd
import os
from datetime import datetime
import re
from src.scripts.preparation import download_files

def extract_id_and_name(fund_source):
    if pd.isna(fund_source):  # Check if the value is NaN
        return None, None
    match = re.match(r'(\d+)\s-\s(.+)', str(fund_source))
    if match:
        return match.group(1), match.group(2)
    else:
        return None, None

def preparation():
    directory = download_files()
    return directory
    
# Task 1: Data Ingestion (ETL) into Raw Layer
def ingest_raw():
    directory = preparation()

    # Define the directory to save the raw files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_dir = os.path.join("datalake", "raw", timestamp)
    os.makedirs(raw_dir, exist_ok=True)

    # Load CSV files with Pandas
    expenses = pd.read_csv(os.path.join(directory, "gdvDespesasExcel.csv"), encoding='ISO-8859-1')
    revenues = pd.read_csv(os.path.join(directory, "gdvReceitasExcel.csv"), encoding='ISO-8859-1')

    # Extract the ID and Name of the Fund Source in the raw layer
    expenses[['fund_id', 'fund_name']] = expenses['Fonte de Recursos'].apply(lambda x: pd.Series(extract_id_and_name(x)))
    revenues[['fund_id', 'fund_name']] = revenues['Fonte de Recursos'].apply(lambda x: pd.Series(extract_id_and_name(x)))

    # Remove the 'Fonte de Recursos' column since fund_id and fund_name have already been extracted
    expenses = expenses.drop(columns=['Fonte de Recursos'])
    revenues = revenues.drop(columns=['Fonte de Recursos'])

    # Save the CSV files in the raw layer
    expenses.to_csv(os.path.join(raw_dir, "expenses.csv"), index=False)
    revenues.to_csv(os.path.join(raw_dir, "revenues.csv"), index=False)
