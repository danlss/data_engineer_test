import pandas as pd
import requests
from datetime import datetime
from src.scripts.preparation import get_postgres_connection


def transformacao_trusted():
    conn, cur = get_postgres_connection()

    # Obtendo a cotação do dólar para o dia 22/06/2022
    url = "https://economia.awesomeapi.com.br/json/daily/USD-BRL/1?start_date=20220622&end_date=20220622"
    response = requests.get(url)
    dolar_cotacao = float(response.json()[0]['high'])  # Garantir que seja float

    # Convertendo e inserindo dados na tabela trusted_despesas
    cur.execute("SELECT fonte_recurso, SUM(liquidado) FROM raw_despesas GROUP BY fonte_recurso")
    despesas_rows = cur.fetchall()

    for row in despesas_rows:
        liquidado_brl = float(row[1]) * dolar_cotacao  # Convertendo row[1] para float
        cur.execute("""
            INSERT INTO trusted_despesas (fonte_recurso, liquidado_brl) 
            VALUES (%s, %s)
        """, (row[0], liquidado_brl))

    # Convertendo e inserindo dados na tabela trusted_receitas
    cur.execute("SELECT fonte_recurso, SUM(arrecadado) FROM raw_receitas GROUP BY fonte_recurso")
    receitas_rows = cur.fetchall()

    for row in receitas_rows:
        arrecadado_brl = float(row[1]) * dolar_cotacao  # Convertendo row[1] para float
        cur.execute("""
            INSERT INTO trusted_receitas (fonte_recurso, arrecadado_brl) 
            VALUES (%s, %s)
        """, (row[0], arrecadado_brl))

    conn.commit()
    cur.close()
    conn.close()
