import pandas as pd
import os
from datetime import datetime
import re
from src.scripts.preparation import download_files

def extract_id_and_name(fonte_recurso):
    if pd.isna(fonte_recurso):  # Verifica se o valor é NaN
        return None, None
    match = re.match(r'(\d+)\s-\s(.+)', str(fonte_recurso))
    if match:
        return match.group(1), match.group(2)
    else:
        return None, None

def preparation():
    directory = download_files()

    return directory
    
# Task 1: Ingestão de Dados (ETL) na Raw
def ingestao_raw():
    directory = preparation()

    # Define o diretório para salvar os arquivos raw
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_dir = os.path.join("datalake", "raw", timestamp)
    os.makedirs(raw_dir, exist_ok=True)

    # Carrega os arquivos CSV com Pandas
    despesas = pd.read_csv(os.path.join(directory, "gdvDespesasExcel.csv"), encoding='ISO-8859-1')
    receitas = pd.read_csv(os.path.join(directory, "gdvReceitasExcel.csv"), encoding='ISO-8859-1')

    # Extrai o ID e o Nome da Fonte de Recurso na camada raw
    despesas[['id_fonte', 'nome_fonte']] = despesas['Fonte de Recursos'].apply(lambda x: pd.Series(extract_id_and_name(x)))
    receitas[['id_fonte', 'nome_fonte']] = receitas['Fonte de Recursos'].apply(lambda x: pd.Series(extract_id_and_name(x)))

    # Remove a coluna 'Fonte de Recursos' já que id_fonte e nome_fonte já foram extraídos
    despesas = despesas.drop(columns=['Fonte de Recursos'])
    receitas = receitas.drop(columns=['Fonte de Recursos'])

    # Salva os arquivos CSV na camada raw
    despesas.to_csv(os.path.join(raw_dir, "despesas.csv"), index=False)
    receitas.to_csv(os.path.join(raw_dir, "receitas.csv"), index=False)
