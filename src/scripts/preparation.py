import pandas as pd
import psycopg2
import requests
import os
from datetime import datetime
 

def download_files():
    # Definindo o diretório base do Data Lake
    datalake_dir = "datalake"
    transient_dir = os.path.join(datalake_dir, "transient")

    # Criar o diretório "transient" dentro do Data Lake, caso não exista
    if not os.path.exists(transient_dir):
        os.makedirs(transient_dir)

    # Obter o timestamp atual
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Criar a subpasta dentro de "transient" com o timestamp
    directory = os.path.join(transient_dir, timestamp)
    os.makedirs(directory, exist_ok=True)
    
    # Simulação das respostas HTTP
    response1 = requests.get('https://github.com/karhub-br/data_engineer_test_v2/raw/main/gdvDespesasExcel.csv')
    response2 = requests.get('https://github.com/karhub-br/data_engineer_test_v2/raw/main/gdvReceitasExcel.csv')

    # Download do primeiro arquivo
    if response1.status_code == 200:
        with open(os.path.join(directory, "gdvDespesasExcel.csv"), 'wb') as file:
            file.write(response1.content)
        print(f"Arquivo de despesas salvo em: {directory}/gdvDespesasExcel.csv")
    else:
        print(f"Falha ao baixar o arquivo de despesas. Status code: {response1.status_code}")

    # Download do segundo arquivo
    if response2.status_code == 200:
        with open(os.path.join(directory, "gdvReceitasExcel.csv"), 'wb') as file:
            file.write(response2.content)
        print(f"Arquivo de receitas salvo em: {directory}/gdvReceitasExcel.csv")
    else:
        print(f"Falha ao baixar o arquivo de receitas. Status code: {response2.status_code}")

    return directory

def get_postgres_connection():
    conn = psycopg2.connect(
        dbname=os.environ["POSTGRES_DB"], 
        user=os.environ["POSTGRES_USER"], 
        password=os.environ["POSTGRES_PASSWORD"], 
        host="localhost", 
        port="5432"
    )
    cur = conn.cursor()
    return conn, cur

def criar_tabelas():
    conn, cur = get_postgres_connection()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dic_fonte_recurso (
            id_fonte_recurso VARCHAR(10) PRIMARY KEY,
            nome_fonte_recurso VARCHAR(255)
        );
        """)
    # Criação da tabela raw_despesas
    cur.execute("""
    CREATE TABLE IF NOT EXISTS raw_despesas (
        id SERIAL PRIMARY KEY,
        fonte_recurso VARCHAR(255),
        despesa VARCHAR(255),
        liquidado NUMERIC(18, 2)
    );
    """)

    # Criação da tabela raw_receitas
    cur.execute("""
    CREATE TABLE IF NOT EXISTS raw_receitas (
        id SERIAL PRIMARY KEY,
        fonte_recurso VARCHAR(255),
        receita VARCHAR(255),
        arrecadado NUMERIC(18, 2)
    );
    """)

    # Criação da tabela trusted_despesas
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trusted_despesas (
        id SERIAL PRIMARY KEY,
        fonte_recurso VARCHAR(255),
        liquidado_brl NUMERIC(18, 2),
        dt_insert TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Criação da tabela trusted_receitas
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trusted_receitas (
        id SERIAL PRIMARY KEY,
        fonte_recurso VARCHAR(255),
        arrecadado_brl NUMERIC(18, 2),
        dt_insert TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Criação da tabela refined_orcamento
    cur.execute("""
    CREATE TABLE IF NOT EXISTS refined_orcamento (
        id_fonte_recurso VARCHAR(255),
        nome_fonte_recurso VARCHAR(255),
        total_arrecadado NUMERIC(18, 2),
        total_liquidado NUMERIC(18, 2),
        margem_bruta NUMERIC(18, 2) GENERATED ALWAYS AS (total_arrecadado - total_liquidado) STORED,
        dt_insert TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Criação da tabela de dicionário de fontes de recursos
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dic_fonte_recurso (
        id_fonte_recurso VARCHAR(10) PRIMARY KEY,
        nome_fonte_recurso VARCHAR(255)
    );
    """)

    conn.commit()
    cur.close()
    conn.close()