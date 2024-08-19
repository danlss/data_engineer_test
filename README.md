# Projeto de Engenharia de Dados: Orçamento do Estado de São Paulo 2022

## Descrição do Projeto

Este projeto foi desenvolvido para processar e analisar os dados de despesas e receitas do Orçamento do Estado de São Paulo de 2022. O principal objetivo é realizar o processo de ETL (Extração, Transformação e Carga) dos dados, consolidando-os em uma estrutura refinada que permita a análise das principais fontes de recursos do estado.

## Estrutura do Projeto

O projeto é composto por várias etapas, orquestradas pelo Apache Airflow. Abaixo estão as etapas e os processos realizados em cada uma delas:

1. **Download dos Dados**: 
    - Os arquivos de despesas e receitas são baixados de um repositório remoto e armazenados na camada `transient` do Data Lake.
  
2. **Ingestão de Dados na Camada Raw**:
    - Os arquivos são carregados e processados para extrair os campos relevantes, como `ID da Fonte de Recurso` e `Nome da Fonte de Recurso`. 
    - Os dados são salvos na camada `raw` do Data Lake.

3. **Transformação de Dados na Camada Trusted**:
    - Os valores de despesas e receitas são convertidos para Reais usando a cotação do dólar em 22/06/2022.
    - Os dados são consolidados e armazenados na camada `trusted` do Data Lake.

4. **Consolidação dos Dados na Camada Refined**:
    - Os dados são agrupados e consolidados para calcular o total arrecadado, total liquidado e a margem bruta para cada fonte de recurso.
    - Os dados consolidados são armazenados no banco de dados PostgreSQL e também salvos como um CSV na camada `refined` do Data Lake.

5. **Geração de Relatórios**:
    - Consultas SQL são executadas para responder às principais perguntas sobre as fontes de recursos.
    - Um relatório em Markdown é gerado com base nos resultados das consultas e salvo na camada `reports` do Data Lake.

## Instruções para Reproduzir as Análises

### Pré-requisitos

- **Python 3.10**
- **Apache Airflow 2.9.3** (Standalone)
- **Docker Compose** (para a orquestração dos containers)
- **Dependências Python** (definidas no arquivo `requirements.txt`)

### Setup do Ambiente

#### Clone o Repositório:

```bash
git clone https://github.com/danlss/data_engineer_test.git
```

#### Execução:

##### Execute com Docker:

1. **Crie o Arquivo .env**:
   - Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

```bash
POSTGRES_DB=nome_do_banco
POSTGRES_USER=usuario
POSTGRES_PASSWORD=senha
```

2. **Inicie os Containers Docker**:
   - Certifique-se de que o Docker e o Docker Compose estão instalados em sua máquina.
   - Use o comando abaixo para iniciar os containers Docker:

```bash
docker-compose up -d
```

Este comando iniciará o PostgreSQL, o Airflow (modo standalone) e executará o pipeline, gerando os resultados no diretório `datalake/reports`.

3. **Acesso ao Webserver do Airflow**:
   - O Airflow estará disponível em `http://localhost:8080`.
   - Use as credenciais de login `admin` e a senha disponível no diretório `airflow/secret` após a inicialização.

### Execução das Tarefas

1. **Preparação dos Dados**:
   - Acesse a interface do Airflow, onde a DAG `etl_markdown_pipeline` estará ativa com o fluxo de trabalho iniciado.
   - O processo implementado inclui a preparação dos dados e a criação das tabelas necessárias no PostgreSQL.

2. **Transformação e Consolidação**:
   - A DAG realiza a transformação dos dados na camada `trusted` e a consolidação na camada `refined`.

3. **Geração de Relatórios**:
   - Após a consolidação, a DAG executa as consultas no banco de dados e gera um relatório em Markdown, que será salvo na pasta `datalake/reports`.

## Estrutura do Data Lake

O Data Lake é organizado nas seguintes camadas:

- **transient**: Armazena os dados brutos baixados.
- **raw**: Contém os dados extraídos e organizados para processamento.
- **trusted**: Armazena os dados transformados e validados.
- **refined**: Contém os dados finais prontos para análise.
- **reports**: Armazena os relatórios gerados a partir dos dados refinados.

## Importância da Estrutura com Logs e Subpastas Timestamp

A estrutura adotada neste projeto, com logs detalhados e subpastas organizadas por timestamps, é fundamental para garantir um monitoramento eficaz. Cada etapa do processo é registrada em logs, permitindo rastrear qualquer evento ou erro ocorrido durante a execução das tarefas. As subpastas com timestamps garantem que todas as versões dos dados e logs sejam armazenadas de forma ordenada, facilitando o histórico de processamento e a recuperação de informações específicas em caso de necessidade. Esta abordagem melhora a transparência do fluxo de trabalho e reforça a confiabilidade do sistema, permitindo auditorias detalhadas e um gerenciamento de dados mais eficiente.

## Consultas e Relatórios

As consultas SQL realizadas para gerar os relatórios incluem:

- **Top 5 fontes que mais arrecadaram**.
- **Top 5 fontes que mais gastaram**.
- **Top 5 fontes de recursos com a melhor margem bruta**.
- **Top 5 fontes que menos arrecadaram**.
- **Top 5 fontes que menos gastaram**.
- **Top 5 fontes com a pior margem bruta**.
- **Médias de arrecadação e gastos**.

Os resultados dessas consultas são formatados e apresentados em um relatório Markdown armazenado na pasta `datalake/reports`.

## Considerações Finais

Este projeto demonstra a automação completa de um fluxo de dados utilizando orquestração com Apache Airflow e infraestrutura containerizada com Docker. A utilização de Docker para o PostgreSQL garante que o ambiente seja replicável e controlado, permitindo maior confiabilidade no processamento e análise dos dados. A estrutura com logs detalhados e subpastas organizadas por timestamps é essencial para o monitoramento contínuo e a auditoria de todo o processo, reforçando a transparência e a integridade dos dados. A abordagem empregada atende plenamente ao objetivo de avaliar conhecimentos em orquestração, infraestrutura e conteinerização do projeto, demonstrando a capacidade de integrar diferentes tecnologias de maneira eficiente e escalável.
