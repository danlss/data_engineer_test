import pandas as pd
import os
from datetime import datetime
from src.scripts.preparation import get_postgres_connection

def consolidacao_refined():
    # Define o diretório da camada trusted
    trusted_dir = os.path.join("datalake", "trusted")
    latest_trusted_dir = sorted(os.listdir(trusted_dir))[-1]
    trusted_dir = os.path.join(trusted_dir, latest_trusted_dir)

    despesas = pd.read_csv(os.path.join(trusted_dir, "trusted_despesas.csv"))
    receitas = pd.read_csv(os.path.join(trusted_dir, "trusted_receitas.csv"))

    conn, cur = get_postgres_connection()

    # Consolidação dos dados na camada refined
    despesas_agg = despesas.groupby(['id_fonte', 'nome_fonte']).agg({'liquidado_brl': 'sum'}).reset_index()
    receitas_agg = receitas.groupby(['id_fonte', 'nome_fonte']).agg({'arrecadado_brl': 'sum'}).reset_index()

    refined_df = pd.merge(despesas_agg, receitas_agg, on=['id_fonte', 'nome_fonte'], how='outer')
    refined_df.columns = ['id_fonte_recurso', 'nome_fonte_recurso', 'total_liquidado', 'total_arrecadado']
    refined_df['dt_insert'] = datetime.now()

    # Substituindo valores NaN por 0.0
    refined_df.fillna(0.0, inplace=True)  # Correção para evitar persistência de valores nulos

    # Define o diretório para salvar os arquivos refined
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    refined_dir = os.path.join("datalake", "refined", timestamp)
    os.makedirs(refined_dir, exist_ok=True)

    # Salva os arquivos CSV na camada refined
    refined_df.to_csv(os.path.join(refined_dir, "refined_orcamento.csv"), index=False)

    # Inserindo os dados refinados no banco de dados
    for _, row in refined_df.iterrows():
        cur.execute("""
            INSERT INTO refined_orcamento (id_fonte_recurso, nome_fonte_recurso, total_liquidado, total_arrecadado, dt_insert) 
            VALUES (%s, %s, %s, %s, %s)
        """, (row['id_fonte_recurso'], row['nome_fonte_recurso'], row['total_liquidado'], row['total_arrecadado'], row['dt_insert']))

    conn.commit()
    cur.close()
    conn.close()