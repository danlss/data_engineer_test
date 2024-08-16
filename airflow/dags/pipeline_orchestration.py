from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from src.scripts.ingestion_raw import ingestao_raw
from src.scripts.transform_trusted import transformacao_trusted
from src.scripts.persist_refined import consolidacao_refined
from src.scripts.reports import salvar_markdown
from airflow.utils.dates import days_ago
import dotenv

dotenv.load_dotenv("/home/danlss/Documentos/desafio karhub/data_engineer_test/.env")
# Configuração da DAG no Airflow
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2024, 1, 1),
#     'retries': 1,
# }

# dag = DAG(
#     'etl_markdown_pipeline',
#     default_args=default_args,
#     description='Pipeline ETL com geração de markdown usando Airflow',
#     schedule_interval=None,  # Manual ou agendado conforme necessário
# )

with DAG(
    dag_id="etl_markdown_pipeline",
    start_date=days_ago(1),
    schedule_interval='@daily'
) as dag:

    # Definição das tasks no Airflow
    task_ingestao_raw = PythonOperator(
        task_id='ingestao_raw',
        python_callable=ingestao_raw,
        dag=dag,
    )

    task_transformacao_trusted = PythonOperator(
        task_id='transformacao_trusted',
        python_callable=transformacao_trusted,
        dag=dag,
    )

    task_consolidacao_refined = PythonOperator(
        task_id='consolidacao_refined',
        python_callable=consolidacao_refined,
        dag=dag,
    )

    task_salvar_markdown = PythonOperator(
        task_id='salvar_markdown',
        python_callable=salvar_markdown,
        dag=dag,
    )
    
    # Definindo a ordem de execução das tasks
    task_ingestao_raw >> task_transformacao_trusted >> task_consolidacao_refined >> task_salvar_markdown
