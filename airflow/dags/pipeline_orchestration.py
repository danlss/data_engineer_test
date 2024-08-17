from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from src.scripts.preparation import criar_tabelas
from src.scripts.ingestion_raw import preparation, ingestao_raw
from src.scripts.transform_trusted import get_cotation, transformacao_trusted
from src.scripts.persist_refined import consolidacao_refined, save_csv, persists_db
from src.scripts.reports import salvar_markdown
from airflow.utils.dates import days_ago
import dotenv
import os
import pandas as pd
import logging

dotenv.load_dotenv("/home/danlss/Documentos/desafio karhub/data_engineer_test/.env")

# Configuração básica de logging
logging.basicConfig(level=logging.INFO)

# Função para salvar logs em CSV
def save_log(log_content, task_id):
    log_dir = os.path.join("datalake", "logs")
    os.makedirs(log_dir, exist_ok=True)  # Cria o diretório se não existir
    log_path = os.path.join(log_dir, f"{task_id}_log.csv")
    
    # Criando um DataFrame para salvar o log
    log_df = pd.DataFrame(log_content, columns=["timestamp", "level", "message"])
    log_df.to_csv(log_path, index=False)

# Task 1: Consolidação dos dados para a camada refined
def task_consolidacao_refined(**kwargs):
    log_content = []
    try:
        logging.info("Iniciando a consolidação dos dados para a camada refined.")
        refined_df, refined_dir = consolidacao_refined()
        logging.info(f"Dados consolidados com sucesso. Salvando em {refined_dir}")
        temp_csv_path = os.path.join(refined_dir, "temp_refined.csv")
        refined_df.to_csv(temp_csv_path, index=False)
        kwargs['ti'].xcom_push(key='temp_csv_path', value=temp_csv_path)
        logging.info("Caminho do CSV temporário enviado via XCom.")
    except Exception as e:
        logging.error(f"Erro durante a consolidação: {str(e)}")
        log_content.append((datetime.now(), "ERROR", str(e)))
        raise
    finally:
        log_content.append((datetime.now(), "INFO", "Consolidação de dados finalizada."))
        save_log(log_content, "consolidacao_refined")

# Task 2: Salvamento do CSV na camada refined
def task_save_csv(**kwargs):
    log_content = []
    try:
        logging.info("Iniciando o salvamento do CSV na camada refined.")
        temp_csv_path = kwargs['ti'].xcom_pull(key='temp_csv_path', task_ids='consolidacao_refined')
        refined_df = pd.read_csv(temp_csv_path)
        refined_dir = os.path.dirname(temp_csv_path)
        save_csv(refined_df, refined_dir)
        logging.info("CSV salvo com sucesso na camada refined.")
    except Exception as e:
        logging.error(f"Erro durante o salvamento do CSV: {str(e)}")
        log_content.append((datetime.now(), "ERROR", str(e)))
        raise
    finally:
        log_content.append((datetime.now(), "INFO", "Salvamento de CSV finalizado."))
        save_log(log_content, "save_csv")

# Task 3: Persistência dos dados no banco de dados
def task_persists_db(**kwargs):
    log_content = []
    try:
        logging.info("Iniciando a persistência dos dados no banco de dados.")
        temp_csv_path = kwargs['ti'].xcom_pull(key='temp_csv_path', task_ids='consolidacao_refined')
        refined_df = pd.read_csv(temp_csv_path)
        persists_db(refined_df)
        logging.info("Dados persistidos com sucesso no banco de dados.")
    except Exception as e:
        logging.error(f"Erro durante a persistência no banco de dados: {str(e)}")
        log_content.append((datetime.now(), "ERROR", str(e)))
        raise
    finally:
        log_content.append((datetime.now(), "INFO", "Persistência de dados finalizada."))
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

    task_ingestao_raw = PythonOperator(
        task_id='ingestao_raw',
        python_callable=ingestao_raw,
        dag=dag,
        execution_timeout=timedelta(minutes=30),  # Tempo máximo de execução
    )

    task_get_cotation_trusted = PythonOperator(
        task_id='dolar_cotation',
        python_callable=get_cotation,
        dag=dag,
        execution_timeout=timedelta(minutes=10),  # Tempo máximo de execução
    )

    task_transformacao_trusted = PythonOperator(
        task_id='transformacao_trusted',
        python_callable=transformacao_trusted,
        dag=dag,
        execution_timeout=timedelta(minutes=20),  # Tempo máximo de execução
    )

    task_tables_refined = PythonOperator(
        task_id='create_tables',
        python_callable=criar_tabelas,
        dag=dag,
        trigger_rule='all_done',  # Executa mesmo que alguma task anterior falhe
    )

    task_consolidacao = PythonOperator(
        task_id='consolidacao_refined',
        python_callable=task_consolidacao_refined,
        provide_context=True,
        dag=dag,
    )

    task_salvar_csv = PythonOperator(
        task_id='save_csv',
        python_callable=task_save_csv,
        provide_context=True,
        dag=dag,
    )

    task_persistir_db = PythonOperator(
        task_id='persist_db',
        python_callable=task_persists_db,
        provide_context=True,
        dag=dag,
    )

    task_salvar_markdown = PythonOperator(
        task_id='salvar_markdown',
        python_callable=salvar_markdown,
        dag=dag,
    )

    # Definindo a ordem de execução das tasks com paralelização e monitoramento
    task_preparation_raw >> task_ingestao_raw
    task_get_cotation_trusted >> task_transformacao_trusted
    [task_ingestao_raw, task_get_cotation_trusted] >> task_transformacao_trusted
    task_transformacao_trusted >> [task_tables_refined, task_consolidacao]
    task_consolidacao >> task_salvar_csv
    [task_tables_refined, task_salvar_csv] >> task_persistir_db
    task_persistir_db >> task_salvar_markdown
