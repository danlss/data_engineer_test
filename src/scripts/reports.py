from src.scripts.preparation import get_postgres_connection
import os
import locale
from datetime import datetime

# Set the locale to Brazilian Portuguese
locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

def execute_queries():
    conn, cur = get_postgres_connection()

    queries = {
        "top_5_arrecadaram": """
            SELECT id_fonte_recurso, nome_fonte_recurso, total_arrecadado
            FROM refined_orcamento
            ORDER BY total_arrecadado DESC
            LIMIT 5;
        """,
        "top_5_gastaram": """
            SELECT id_fonte_recurso, nome_fonte_recurso, total_liquidado
            FROM refined_orcamento
            ORDER BY total_liquidado DESC
            LIMIT 5;
        """,
        "melhor_margem_bruta": """
            SELECT id_fonte_recurso, nome_fonte_recurso, (total_arrecadado - total_liquidado) AS margem_bruta
            FROM refined_orcamento
            ORDER BY margem_bruta DESC
            LIMIT 5;
        """,
        "menor_arrecadaram": """
            SELECT id_fonte_recurso, nome_fonte_recurso, total_arrecadado
            FROM refined_orcamento
            ORDER BY total_arrecadado ASC
            LIMIT 5;
        """,
        "menor_gastaram": """
            SELECT id_fonte_recurso, nome_fonte_recurso, total_liquidado
            FROM refined_orcamento
            ORDER BY total_liquidado ASC
            LIMIT 5;
        """,
        "pior_margem_bruta": """
            SELECT id_fonte_recurso, nome_fonte_recurso, (total_arrecadado - total_liquidado) AS margem_bruta
            FROM refined_orcamento
            ORDER BY margem_bruta ASC
            LIMIT 5;
        """,
        "media_arrecadacao": """
            SELECT AVG(total_arrecadado) AS media_arrecadacao
            FROM refined_orcamento;
        """,
        "media_gastos": """
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
    markdown = "# resultados Esperados Baseados nos Dados\n\n"

    markdown += "## Quais são as 5 fontes de recursos que mais arrecadaram?\n"
    markdown += "| ID Fonte Recurso | Nome Fonte Recurso | Total Arrecadado |\n"
    markdown += "|------------------|--------------------|------------------|\n"
    for row in results['top_5_arrecadaram']:
        valor = float(row[2]) if row[2] is not None else 0.0
        formatted_value = f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        markdown += f"| {row[0]} | {row[1]} | {formatted_value} |\n"

    markdown += "\n## Quais são as 5 fontes de recursos que mais gastaram?\n"
    markdown += "| ID Fonte Recurso | Nome Fonte Recurso | Total Liquidado |\n"
    markdown += "|------------------|--------------------|-----------------|\n"
    for row in results['top_5_gastaram']:
        valor = float(row[2]) if row[2] is not None else 0.0
        formatted_value = f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        markdown += f"| {row[0]} | {row[1]} | {formatted_value} |\n"

    markdown += "\n## Quais são as 5 fontes de recursos com a melhor margem bruta?\n"
    markdown += "| ID Fonte Recurso | Nome Fonte Recurso | Margem Bruta |\n"
    markdown += "|------------------|--------------------|--------------|\n"
    for row in results['melhor_margem_bruta']:
        valor = float(row[2]) if row[2] is not None else 0.0
        formatted_value = f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        markdown += f"| {row[0]} | {row[1]} | {formatted_value} |\n"

    markdown += "\n## Quais são as 5 fontes de recursos que menos arrecadaram?\n"
    markdown += "| ID Fonte Recurso | Nome Fonte Recurso | Total Arrecadado |\n"
    markdown += "|------------------|--------------------|------------------|\n"
    for row in results['menor_arrecadaram']:
        valor = float(row[2]) if row[2] is not None else 0.0
        formatted_value = f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        markdown += f"| {row[0]} | {row[1]} | {formatted_value} |\n"

    markdown += "\n## Quais são as 5 fontes de recursos que menos gastaram?\n"
    markdown += "| ID Fonte Recurso | Nome Fonte Recurso | Total Liquidado |\n"
    markdown += "|------------------|--------------------|-----------------|\n"
    for row in results['menor_gastaram']:
        valor = float(row[2]) if row[2] is not None else 0.0
        formatted_value = f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        markdown += f"| {row[0]} | {row[1]} | {formatted_value} |\n"

    markdown += "\n## Quais são as 5 fontes de recursos com a pior margem bruta?\n"
    markdown += "| ID Fonte Recurso | Nome Fonte Recurso | Margem Bruta |\n"
    markdown += "|------------------|--------------------|--------------|\n"
    for row in results['pior_margem_bruta']:
        valor = float(row[2]) if row[2] is not None else 0.0
        formatted_value = f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        markdown += f"| {row[0]} | {row[1]} | {formatted_value} |\n"

    media_arrecadacao = f"{float(results['media_arrecadacao'][0][0]):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    markdown += f"\n## Qual a média de arrecadação por fonte de recurso?\n"
    markdown += f"- **Média de arrecadação:** {media_arrecadacao}\n"

    media_gastos = f"{float(results['media_gastos'][0][0]):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    markdown += f"\n## Qual a média de gastos por fonte de recurso?\n"
    markdown += f"- **Média de gastos:** {media_gastos}\n"

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
