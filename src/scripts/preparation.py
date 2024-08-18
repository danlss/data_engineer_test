import psycopg2
import requests
import os
from datetime import datetime

POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", 5432)
 

def download_files():
    # Defining the base directory of the Data Lake
    datalake_dir = "datalake"
    transient_dir = os.path.join(datalake_dir, "transient")

    # Create the "transient" directory inside the Data Lake, if it doesn't exist
    if not os.path.exists(transient_dir):
        os.makedirs(transient_dir)

    # Get the current timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Create the subfolder inside "transient" with the timestamp
    directory = os.path.join(transient_dir, timestamp)
    os.makedirs(directory, exist_ok=True)
    
    # Simulating HTTP responses
    response1 = requests.get('https://github.com/karhub-br/data_engineer_test_v2/raw/main/gdvDespesasExcel.csv')
    response2 = requests.get('https://github.com/karhub-br/data_engineer_test_v2/raw/main/gdvReceitasExcel.csv')

    # Download the first file
    if response1.status_code == 200:
        with open(os.path.join(directory, "gdvDespesasExcel.csv"), 'wb') as file:
            file.write(response1.content)
        print(f"Expenses file saved at: {directory}/gdvDespesasExcel.csv")
    else:
        print(f"Failed to download the expenses file. Status code: {response1.status_code}")

    # Download the second file
    if response2.status_code == 200:
        with open(os.path.join(directory, "gdvReceitasExcel.csv"), 'wb') as file:
            file.write(response2.content)
        print(f"Revenues file saved at: {directory}/gdvReceitasExcel.csv")
    else:
        print(f"Failed to download the revenues file. Status code: {response2.status_code}")

    return directory

def get_postgres_connection():
    conn = psycopg2.connect(
        dbname=POSTGRES_DB, 
        user=POSTGRES_USER, 
        password=POSTGRES_PASSWORD, 
        host=POSTGRES_HOST, 
        port=POSTGRES_PORT
    )
    cur = conn.cursor()
    return conn, cur

def create_tables():
    conn, cur = get_postgres_connection()

    # Create the refined_budget table
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

    conn.commit()
    cur.close()
    conn.close()
