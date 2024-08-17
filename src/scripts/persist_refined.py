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

    # Consolidação dos dados na camada refined
    despesas_agg = despesas.groupby(['id_fonte', 'nome_fonte']).agg({'liquidado_brl': 'sum'}).reset_index()
    receitas_agg = receitas.groupby(['id_fonte', 'nome_fonte']).agg({'arrecadado_brl': 'sum'}).reset_index()

    refined_df = pd.merge(despesas_agg, receitas_agg, on=['id_fonte', 'nome_fonte'], how='outer')
    refined_df.columns = ['ID Fonte Recurso', 'Nome Fonte Recurso', 'Total Liquidado', 'Total Arrecadado']
    refined_df['Dt_Insert'] = datetime.now()

    # Substituindo valores NaN por 0.0
    refined_df.fillna(0.0, inplace=True)

    # Define o diretório para salvar os arquivos refined
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    refined_dir = os.path.join("datalake", "refined", timestamp)
    os.makedirs(refined_dir, exist_ok=True)

    return refined_df, refined_dir

def save_csv(refined_df, refined_dir):
    # Salva os arquivos CSV na camada refined
    refined_df.to_csv(os.path.join(refined_dir, "refined_orcamento.csv"), index=False)

def persists_db(refined_df):
    conn, cur = get_postgres_connection()
    
    try:
        # Inserindo os dados refinados no banco de dados
        for _, row in refined_df.iterrows():
            cur.execute("""
                INSERT INTO refined_orcamento (id_fonte_recurso, nome_fonte_recurso, total_liquidado, total_arrecadado, dt_insert) 
                VALUES (%s, %s, %s, %s, %s)
            """, (row['ID Fonte Recurso'], row['Nome Fonte Recurso'], row['Total Liquidado'], row['Total Arrecadado'], row['Dt_Insert']))
        
        conn.commit()
    except Exception as e:
        print(f"Erro ao inserir dados no banco: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def main():
    # Consolida os dados e obtém o dataframe e o diretório para salvar
    refined_df, refined_dir = consolidacao_refined()

    # Salva o CSV no diretório especificado
    save_csv(refined_df, refined_dir)

    # Persiste os dados no banco de dados
    persists_db(refined_df)

if __name__ == "__main__":
    main()
