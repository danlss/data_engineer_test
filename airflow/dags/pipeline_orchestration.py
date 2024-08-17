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

dotenv.load_dotenv("/home/danlss/Documentos/desafio karhub/data_engineer_test/.env")

# Task 1: Consolidação dos dados para a camada refined
def task_consolidacao_refined(**kwargs):
    refined_df, refined_dir = consolidacao_refined()
    # Salvar o DataFrame consolidado como CSV temporário para ser usado nas próximas tasks
    temp_csv_path = os.path.join(refined_dir, "temp_refined.csv")
    refined_df.to_csv(temp_csv_path, index=False)
    # Passando o caminho do CSV temporário para as próximas tasks via XCom
    kwargs['ti'].xcom_push(key='temp_csv_path', value=temp_csv_path)

# Task 2: Salvamento do CSV na camada refined
def task_save_csv(**kwargs):
    # Obtendo o caminho do CSV temporário via XCom
    temp_csv_path = kwargs['ti'].xcom_pull(key='temp_csv_path', task_ids='consolidacao_refined')
    refined_df = pd.read_csv(temp_csv_path)
    refined_dir = os.path.dirname(temp_csv_path)
    save_csv(refined_df, refined_dir)

# Task 3: Persistência dos dados no banco de dados
def task_persists_db(**kwargs):
    # Obtendo o caminho do CSV temporário via XCom
    temp_csv_path = kwargs['ti'].xcom_pull(key='temp_csv_path', task_ids='consolidacao_refined')
    refined_df = pd.read_csv(temp_csv_path)
    persists_db(refined_df)

# Configuração da DAG no Airflow com melhorias para monitoramento e robustez
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
