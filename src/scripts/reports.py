from src.scripts.preparation import get_postgres_connection
import os
import locale
from datetime import datetime

# Set the locale to Brazilian Portuguese
locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

def execute_queries():
    conn, cur = get_postgres_connection()

    queries = {
        "top_5_collected": """
            SELECT id_fonte_recurso, nome_fonte_recurso, total_arrecadado
            FROM refined_orcamento
            ORDER BY total_arrecadado DESC
            LIMIT 5;
        """,
        "top_5_spent": """
            SELECT id_fonte_recurso, nome_fonte_recurso, total_liquidado
            FROM refined_orcamento
            ORDER BY total_liquidado DESC
            LIMIT 5;
        """,
        "best_gross_margin": """
            SELECT id_fonte_recurso, nome_fonte_recurso, (total_arrecadado - total_liquidado) AS margem_bruta
            FROM refined_orcamento
            ORDER BY margem_bruta DESC
            LIMIT 5;
        """,
        "least_collected": """
            SELECT id_fonte_recurso, nome_fonte_recurso, total_arrecadado
            FROM refined_orcamento
            ORDER BY total_arrecadado ASC
            LIMIT 5;
        """,
        "least_spent": """
            SELECT id_fonte_recurso, nome_fonte_recurso, total_liquidado
            FROM refined_orcamento
            ORDER BY total_liquidado ASC
            LIMIT 5;
        """,
        "worst_gross_margin": """
            SELECT id_fonte_recurso, nome_fonte_recurso, (total_arrecadado - total_liquidado) AS margem_bruta
            FROM refined_orcamento
            ORDER BY margem_bruta ASC
            LIMIT 5;
        """,
        "average_collection": """
            SELECT AVG(total_arrecadado) AS media_arrecadacao
            FROM refined_orcamento;
        """,
        "average_spending": """
            SELECT AVG(total_liquidado) AS media_gastos
            FROM refined_orcamento;
        """
    }

    results = {}
    
    for key, query in queries.items():
        cur.execute(query)
        results[key] = cur.fetchall()

    cur.close()
    conn.close()

    return results

# Convert values to Brazilian BR format
def generate_markdown(results):
    markdown = "# Expected Results Based on Data\n\n"

    markdown += "## What are the top 5 fund sources that collected the most?\n"
    markdown += "| Fund ID | Fund Name | Total Collected |\n"
    markdown += "|---------|-----------|-----------------|\n"
    for row in results['top_5_collected']:
        value = float(row[2]) if row[2] is not None else 0.0
        formatted_value = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        markdown += f"| {row[0]} | {row[1]} | {formatted_value} |\n"

    markdown += "\n## What are the top 5 fund sources that spent the most?\n"
    markdown += "| Fund ID | Fund Name | Total Spent |\n"
    markdown += "|---------|-----------|-------------|\n"
    for row in results['top_5_spent']:
        value = float(row[2]) if row[2] is not None else 0.0
        formatted_value = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        markdown += f"| {row[0]} | {row[1]} | {formatted_value} |\n"

    markdown += "\n## What are the top 5 fund sources with the best gross margin?\n"
    markdown += "| Fund ID | Fund Name | Gross Margin |\n"
    markdown += "|---------|-----------|--------------|\n"
    for row in results['best_gross_margin']:
        value = float(row[2]) if row[2] is not None else 0.0
        formatted_value = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        markdown += f"| {row[0]} | {row[1]} | {formatted_value} |\n"

    markdown += "\n## What are the top 5 fund sources that collected the least?\n"
    markdown += "| Fund ID | Fund Name | Total Collected |\n"
    markdown += "|---------|-----------|-----------------|\n"
    for row in results['least_collected']:
        value = float(row[2]) if row[2] is not None else 0.0
        formatted_value = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        markdown += f"| {row[0]} | {row[1]} | {formatted_value} |\n"

    markdown += "\n## What are the top 5 fund sources that spent the least?\n"
    markdown += "| Fund ID | Fund Name | Total Spent |\n"
    markdown += "|---------|-----------|-------------|\n"
    for row in results['least_spent']:
        value = float(row[2]) if row[2] is not None else 0.0
        formatted_value = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        markdown += f"| {row[0]} | {row[1]} | {formatted_value} |\n"

    markdown += "\n## What are the top 5 fund sources with the worst gross margin?\n"
    markdown += "| Fund ID | Fund Name | Gross Margin |\n"
    markdown += "|---------|-----------|--------------|\n"
    for row in results['worst_gross_margin']:
        value = float(row[2]) if row[2] is not None else 0.0
        formatted_value = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        markdown += f"| {row[0]} | {row[1]} | {formatted_value} |\n"

    average_collection = f"{float(results['average_collection'][0][0]):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    markdown += f"\n## What is the average collection per fund source?\n"
    markdown += f"- **Average collection:** {average_collection}\n"

    average_spending = f"{float(results['average_spending'][0][0]):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    markdown += f"\n## What is the average spending per fund source?\n"
    markdown += f"- **Average spending:** {average_spending}\n"

    return markdown

# Save markdown in a folder with a timestamp
def save_markdown():
    results = execute_queries()
    markdown = generate_markdown(results)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    markdown_dir = os.path.join("datalake", "reports", timestamp)
    os.makedirs(markdown_dir, exist_ok=True)

    with open(os.path.join(markdown_dir, "analyzed_results.md"), "w") as file:
        file.write(markdown)
