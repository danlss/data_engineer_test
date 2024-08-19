import pandas as pd
import os
from datetime import datetime
from scripts.preparation import get_postgres_connection

def consolidate_refined():
    # Define the directory for the trusted layer
    trusted_dir = os.path.join("datalake", "trusted")
    latest_trusted_dir = sorted(os.listdir(trusted_dir))[-1]
    trusted_dir = os.path.join(trusted_dir, latest_trusted_dir)

    expenses = pd.read_csv(os.path.join(trusted_dir, "trusted_expenses.csv"))
    revenues = pd.read_csv(os.path.join(trusted_dir, "trusted_revenues.csv"))

    # Data consolidation in the refined layer
    expenses_agg = expenses.groupby(['fund_id', 'fund_name']).agg({'liquidated_brl': 'sum'}).reset_index()
    revenues_agg = revenues.groupby(['fund_id', 'fund_name']).agg({'collected_brl': 'sum'}).reset_index()

    refined_df = pd.merge(expenses_agg, revenues_agg, on=['fund_id', 'fund_name'], how='outer')
    refined_df.columns = ['Fund ID', 'Fund Name', 'Total Liquidated', 'Total Collected']
    refined_df['Dt_Insert'] = datetime.now()

    refined_df.fillna(0.0, inplace=True)

    # Define the directory to save the refined files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    refined_dir = os.path.join("datalake", "refined", timestamp)
    os.makedirs(refined_dir, exist_ok=True)

    return refined_df, refined_dir

def save_csv(refined_df, refined_dir):
    refined_df.to_csv(os.path.join(refined_dir, "refined_budget.csv"), index=False)

def persist_db(refined_df):
    conn, cur = get_postgres_connection()
    
    try:
        for _, row in refined_df.iterrows():
            cur.execute("""
                INSERT INTO refined_orcamento (id_fonte_recurso, nome_fonte_recurso, total_arrecadado, total_liquidado, dt_insert) 
                VALUES (%s, %s, %s, %s, %s)
            """, (row['Fund ID'], row['Fund Name'], row['Total Collected'], row['Total Liquidated'], row['Dt_Insert']))

        
        conn.commit()
    except Exception as e:
        print(f"Error inserting data into the database: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()