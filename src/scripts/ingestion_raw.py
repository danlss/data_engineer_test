import pandas as pd
import os
from datetime import datetime
import re
from src.scripts.preparation import download_files, criar_tabelas, get_postgres_connection

def extract_id_and_name(fonte_recurso):
    if pd.isna(fonte_recurso):  # Verifica se o valor é NaN
        return None, None
    match = re.match(r'(\d+)\s-\s(.+)', str(fonte_recurso))
    if match:
        return match.group(1), match.group(2)
    else:
        return None, None
    
def ingestao_raw():
    criar_tabelas()
    directory = download_files()

    conn, cur = get_postgres_connection()

    # Leitura dos arquivos CSV com especificação da codificação
    despesas = pd.read_csv(os.path.join(directory, "gdvDespesasExcel.csv"), encoding='ISO-8859-1')
    receitas = pd.read_csv(os.path.join(directory, "gdvReceitasExcel.csv"), encoding='ISO-8859-1')

    # Inserindo dados na tabela raw_despesas e dicionário
    for index, row in despesas.iterrows():
        id_fonte, nome_fonte = extract_id_and_name(row['Fonte de Recursos'])
        
        if id_fonte and nome_fonte:  # Verifica se a extração foi bem-sucedida
            # Corrigindo a formatação para conversão correta de string para float
            liquidado = float(row['Liquidado'].replace('.', '').replace(',', '.'))

            # Inserir no dicionário se não existir
            cur.execute("""
                INSERT INTO dic_fonte_recurso (id_fonte_recurso, nome_fonte_recurso)
                VALUES (%s, %s)
                ON CONFLICT (id_fonte_recurso) DO NOTHING
            """, (id_fonte, nome_fonte))

            # Inserir na tabela raw_despesas
            cur.execute("""
                INSERT INTO raw_despesas (fonte_recurso, despesa, liquidado) 
                VALUES (%s, %s, %s)
            """, (id_fonte, row['Despesa'], liquidado))

    # Inserindo dados na tabela raw_receitas e dicionário
    for index, row in receitas.iterrows():
        id_fonte, nome_fonte = extract_id_and_name(row['Fonte de Recursos'])

        if id_fonte and nome_fonte:  # Verifica se a extração foi bem-sucedida
            # Corrigindo a formatação para conversão correta de string para float
            arrecadado = float(row['Arrecadado'].replace('.', '').replace(',', '.'))

            # Inserir no dicionário se não existir
            cur.execute("""
                INSERT INTO dic_fonte_recurso (id_fonte_recurso, nome_fonte_recurso)
                VALUES (%s, %s)
                ON CONFLICT (id_fonte_recurso) DO NOTHING
            """, (id_fonte, nome_fonte))

            # Inserir na tabela raw_receitas
            cur.execute("""
                INSERT INTO raw_receitas (fonte_recurso, receita, arrecadado) 
                VALUES (%s, %s, %s)
            """, (id_fonte, row['Receita'], arrecadado))

    conn.commit()
    cur.close()
    conn.close()
