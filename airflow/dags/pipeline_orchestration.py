from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from src.scripts.preparation import create_tables
from src.scripts.ingestion_raw import preparation, ingest_raw
from src.scripts.transform_trusted import get_exchange_rate, transform_trusted
from src.scripts.persist_refined import consolidate_refined, save_csv, persist_db
from src.scripts.reports import save_markdown
from airflow.utils.dates import days_ago
import dotenv
import os
import pandas as pd
import logging

dotenv.load_dotenv("/home/danlss/Documentos/desafio karhub/data_engineer_test/.env")

# Basic logging configuration
logging.basicConfig(level=logging.INFO)

# Function to save logs in CSV
def save_log(log_content, task_id):
    log_dir = os.path.join("datalake", "logs")
    os.makedirs(log_dir, exist_ok=True)  # Cria o diretório se não existir
    log_path = os.path.join(log_dir, f"{task_id}_log.csv")
    
    # Criando um DataFrame para salvar o log
    log_df = pd.DataFrame(log_content, columns=["timestamp", "level", "message"])
    log_df.to_csv(log_path, index=False)

# Task 1: Consolidate data for the refined layer
def task_consolidate_refined(**kwargs):
    log_content = []
    try:
        logging.info("Starting data consolidation for the refined layer.")
        refined_df, refined_dir = consolidate_refined()
        logging.info(f"Data successfully consolidated. Saving to {refined_dir}")
        temp_csv_path = os.path.join(refined_dir, "temp_refined.csv")
        refined_df.to_csv(temp_csv_path, index=False)
        kwargs['ti'].xcom_push(key='temp_csv_path', value=temp_csv_path)
        logging.info("Temporary CSV path sent via XCom.")
    except Exception as e:
        logging.error(f"Error during consolidation: {str(e)}")
        log_content.append((datetime.now(), "ERROR", str(e)))
        raise
    finally:
        log_content.append((datetime.now(), "INFO", "Data consolidation completed."))
        save_log(log_content, "consolidate_refined")

# Task 2: Save the CSV in the refined layer
def task_save_csv(**kwargs):
    log_content = []
    try:
        logging.info("Starting to save the CSV in the refined layer.")
        temp_csv_path = kwargs['ti'].xcom_pull(key='temp_csv_path', task_ids='consolidate_refined')
        refined_df = pd.read_csv(temp_csv_path)
        refined_dir = os.path.dirname(temp_csv_path)
        save_csv(refined_df, refined_dir)
        logging.info("CSV successfully saved in the refined layer.")
    except Exception as e:
        logging.error(f"Error during CSV saving: {str(e)}")
        log_content.append((datetime.now(), "ERROR", str(e)))
        raise
    finally:
        log_content.append((datetime.now(), "INFO", "CSV saving completed."))
        save_log(log_content, "save_csv")

# Task 3: Persist data to the database
def task_persist_db(**kwargs):
    log_content = []
    try:
        logging.info("Starting data persistence to the database.")
        temp_csv_path = kwargs['ti'].xcom_pull(key='temp_csv_path', task_ids='consolidate_refined')
        refined_df = pd.read_csv(temp_csv_path)
        persist_db(refined_df)
        logging.info("Data successfully persisted to the database.")
    except Exception as e:
        logging.error(f"Error during database persistence: {str(e)}")
        log_content.append((datetime.now(), "ERROR", str(e)))
        raise
    finally:
        log_content.append((datetime.now(), "INFO", "Data persistence completed."))
        save_log(log_content, "persist_db")

with DAG(
    dag_id="etl_markdown_pipeline",
    start_date=days_ago(1),
    schedule_interval='@daily',
    max_active_runs=1,  # Garante que apenas uma execução da DAG ocorra ao mesmo tempo
    concurrency=5,  # Limita o número de tasks que podem ser executadas em paralelo
    default_args={
        'owner': 'airflow',
        'retries': 3,  # Tenta 3 vezes antes de marcar como falha
        'retry_delay': timedelta(minutes=5),  # Espera 5 minutos antes de tentar novamente
    }
) as dag:

    # Definição das tasks no Airflow
    task_preparation_raw = PythonOperator(
        task_id='preparation',
        python_callable=preparation,
        dag=dag,
        execution_timeout=timedelta(minutes=15),  # Tempo máximo de execução
    )

    task_ingest_raw = PythonOperator(
        task_id='ingest_raw',
        python_callable=ingest_raw,
        dag=dag,
        execution_timeout=timedelta(minutes=30),  # Tempo máximo de execução
    )

    task_get_exchange_rate = PythonOperator(
        task_id='dolar_exchange',
        python_callable=get_exchange_rate,
        dag=dag,
        execution_timeout=timedelta(minutes=10),  # Tempo máximo de execução
    )

    task_transform_trusted = PythonOperator(
        task_id='transform_trusted',
        python_callable=transform_trusted,
        dag=dag,
        execution_timeout=timedelta(minutes=20),  # Tempo máximo de execução
    )

    task_create_tables_refined = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables,
        dag=dag,
        trigger_rule='all_done',  # Executa mesmo que alguma task anterior falhe
    )

    task_consolidate = PythonOperator(
        task_id='consolidate_refined',
        python_callable=task_consolidate_refined,
        provide_context=True,
        dag=dag,
    )

    task_save_csv_ = PythonOperator(
        task_id='save_csv',
        python_callable=task_save_csv,
        provide_context=True,
        dag=dag,
    )

    task_persist_db_ = PythonOperator(
        task_id='persist_db',
        python_callable=task_persist_db,
        provide_context=True,
        dag=dag,
    )

    task_save_markdown = PythonOperator(
        task_id='save_markdown',
        python_callable=save_markdown,
        dag=dag,
    )

    # Definindo a ordem de execução das tasks com paralelização e monitoramento
    task_preparation_raw >> task_ingest_raw
    task_get_exchange_rate >> task_transform_trusted
    [task_ingest_raw, task_get_exchange_rate] >> task_transform_trusted
    task_transform_trusted >> [task_create_tables_refined, task_consolidate]
    task_consolidate >> task_save_csv_
    [task_create_tables_refined, task_save_csv_] >> task_persist_db_
    task_persist_db_ >> task_save_markdown
