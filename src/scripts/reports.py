from src.scripts.preparation import get_postgres_connection


def executar_consultas():
    conn, cur = get_postgres_connection()

    consultas = {
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

    resultados = {}
    
    for key, query in consultas.items():
        cur.execute(query)
        resultados[key] = cur.fetchall()

    cur.close()
    conn.close()

    return resultados

#transformar valores de ponto e virgula BR
def gerar_markdown(resultados):
    markdown = "# Resultados Esperados Baseados nos Dados\n\n"

    # Seção: 5 fontes que mais arrecadaram
    markdown += "## Quais são as 5 fontes de recursos que mais arrecadaram?\n"
    markdown += "| ID Fonte Recurso | Nome Fonte Recurso | Total Arrecadado |\n"
    markdown += "|------------------|--------------------|------------------|\n"
    for row in resultados['top_5_arrecadaram']:
        markdown += f"| {row[0]} | {row[1]} | {row[2]:,.2f} |\n"

    # Seção: 5 fontes que mais gastaram
    markdown += "\n## Quais são as 5 fontes de recursos que mais gastaram?\n"
    markdown += "| ID Fonte Recurso | Nome Fonte Recurso | Total Liquidado |\n"
    markdown += "|------------------|--------------------|-----------------|\n"
    for row in resultados['top_5_gastaram']:
        markdown += f"| {row[0]} | {row[1]} | {row[2]:,.2f} |\n"

    # Seção: 5 fontes com a melhor margem bruta
    markdown += "\n## Quais são as 5 fontes de recursos com a melhor margem bruta?\n"
    markdown += "| ID Fonte Recurso | Nome Fonte Recurso | Margem Bruta |\n"
    markdown += "|------------------|--------------------|--------------|\n"
    for row in resultados['melhor_margem_bruta']:
        markdown += f"| {row[0]} | {row[1]} | {row[2]:,.2f} |\n"

    # Seção: 5 fontes que menos arrecadaram
    markdown += "\n## Quais são as 5 fontes de recursos que menos arrecadaram?\n"
    markdown += "| ID Fonte Recurso | Nome Fonte Recurso | Total Arrecadado |\n"
    markdown += "|------------------|--------------------|------------------|\n"
    for row in resultados['menor_arrecadaram']:
        markdown += f"| {row[0]} | {row[1]} | {row[2]:,.2f} |\n"

    # Seção: 5 fontes que menos gastaram
    markdown += "\n## Quais são as 5 fontes de recursos que menos gastaram?\n"
    markdown += "| ID Fonte Recurso | Nome Fonte Recurso | Total Liquidado |\n"
    markdown += "|------------------|--------------------|-----------------|\n"
    for row in resultados['menor_gastaram']:
        markdown += f"| {row[0]} | {row[1]} | {row[2]:,.2f} |\n"

    # Seção: 5 fontes com a pior margem bruta
    markdown += "\n## Quais são as 5 fontes de recursos com a pior margem bruta?\n"
    markdown += "| ID Fonte Recurso | Nome Fonte Recurso | Margem Bruta |\n"
    markdown += "|------------------|--------------------|--------------|\n"
    for row in resultados['pior_margem_bruta']:
        markdown += f"| {row[0]} | {row[1]} | {row[2]:,.2f} |\n"

    # Seção: Média de arrecadação por fonte de recurso
    media_arrecadacao = resultados['media_arrecadacao'][0][0]
    markdown += f"\n## Qual a média de arrecadação por fonte de recurso?\n"
    markdown += f"- **Média de arrecadação:** {media_arrecadacao:,.2f}\n"

    # Seção: Média de gastos por fonte de recurso
    media_gastos = resultados['media_gastos'][0][0]
    markdown += f"\n## Qual a média de gastos por fonte de recurso?\n"
    markdown += f"- **Média de gastos:** {media_gastos:,.2f}\n"

    return markdown

def salvar_markdown():
    resultados = executar_consultas()
    markdown = gerar_markdown(resultados)

    # Opcional: salvar em um arquivo .md
    with open("/home/danlss/Documentos/desafio karhub/data_engineer_test/resultados_esperados.md", "w") as file:
        file.write(markdown)

# # Executar as consultas e obter os resultados
# resultados = executar_consultas()

# # Gerar o markdown com base nos resultados
# markdown_dinamico = gerar_markdown(resultados)

# # Exibir ou salvar o markdown gerado
# print(markdown_dinamico)

# # Opcional: salvar em um arquivo .md
# with open("resultados_esperados.md", "w") as file:
#     file.write(markdown_dinamico)
