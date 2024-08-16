import pandas as pd
import os
from datetime import datetime
from src.scripts.preparation import get_postgres_connection

def consolidacao_refined():
    conn, cur = get_postgres_connection()

    # Agrupando e combinando os dados para a camada Refined
    cur.execute("""
        SELECT 
            sr.fonte_recurso, 
            fr.nome_fonte_recurso, 
            COALESCE(SUM(td.liquidado_brl), 0) AS total_liquidado, 
            COALESCE(SUM(sr.arrecadado_brl), 0) AS total_arrecadado
        FROM trusted_despesas td
        FULL OUTER JOIN trusted_receitas sr ON td.fonte_recurso = sr.fonte_recurso
        JOIN dic_fonte_recurso fr ON sr.fonte_recurso = fr.id_fonte_recurso
        GROUP BY sr.fonte_recurso, fr.nome_fonte_recurso
    """)
    resultados = cur.fetchall()

    if resultados:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        refined_dir = os.path.join("datalake", "refined", timestamp)
        os.makedirs(refined_dir, exist_ok=True)

        refined_df = pd.DataFrame(resultados, columns=['id_fonte_recurso', 'nome_fonte_recurso', 'total_liquidado', 'total_arrecadado'])
        refined_df.to_csv(os.path.join(refined_dir, "orcamento_refined.csv"), index=False)

        # Inserindo os dados na tabela refined_orcamento com dt_insert
        for row in resultados:
            cur.execute("""
                INSERT INTO refined_orcamento (id_fonte_recurso, nome_fonte_recurso, total_liquidado, total_arrecadado, dt_insert) 
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            """, (row[0], row[1], row[2], row[3]))

    conn.commit()
    cur.close()
    conn.close()