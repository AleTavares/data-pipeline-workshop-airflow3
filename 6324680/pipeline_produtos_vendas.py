from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import os

default_args = {
    'owner': 'aluno_6324680',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pipeline_produtos_vendas',
    default_args=default_args,
    description='Pipeline ETL completo para processar dados de produtos e vendas',
    schedule='0 6 * * *',
    catchup=False,
    tags=['produtos', 'vendas', 'exercicio'],
)

def extract_produtos(**context):
    file_path = '/opt/airflow/data/produtos_loja.csv'

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

    logging.info(f"Extraindo dados de produtos de: {file_path}")
    df = pd.read_csv(file_path)
    logging.info(f"Produtos extraídos: {len(df)} registros")

    df.to_csv('/tmp/produtos_extraidos.csv', index=False)
    return f"Extraídos {len(df)} produtos"

def extract_vendas(**context):
    file_path = '/opt/airflow/data/vendas_produtos.csv'

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

    logging.info(f"Extraindo dados de vendas de: {file_path}")
    df = pd.read_csv(file_path)
    logging.info(f"Vendas extraídas: {len(df)} registros")

    df.to_csv('/tmp/vendas_extraidas.csv', index=False)
    return f"Extraídas {len(df)} vendas"

def transform_data(**context):
    logging.info("Iniciando transformação dos dados")

    df_produtos = pd.read_csv('/tmp/produtos_extraidos.csv')
    df_vendas = pd.read_csv('/tmp/vendas_extraidas.csv')

    logging.info("Limpando dados de produtos")
    df_produtos['Preco_Custo'] = pd.to_numeric(df_produtos['Preco_Custo'], errors='coerce')

    media_acessorios = df_produtos[df_produtos['Categoria'] == 'Acessórios']['Preco_Custo'].mean()
    logging.info(f"Média de preço de custo em Acessórios: {media_acessorios}")

    df_produtos.loc[
        (df_produtos['Categoria'] == 'Acessórios') & (df_produtos['Preco_Custo'].isna()),
        'Preco_Custo'
    ] = media_acessorios

    df_produtos['Fornecedor'] = df_produtos['Fornecedor'].fillna('Não Informado')

    logging.info("Transformando dados de vendas")
    df_vendas['Quantidade_Vendida'] = pd.to_numeric(df_vendas['Quantidade_Vendida'], errors='coerce').fillna(0).astype(int)
    df_vendas['Preco_Venda'] = pd.to_numeric(df_vendas['Preco_Venda'], errors='coerce')
    df_vendas['Data_Venda'] = pd.to_datetime(df_vendas['Data_Venda'])

    df_vendas_merged = df_vendas.merge(
        df_produtos[['ID_Produto', 'Preco_Custo', 'Nome_Produto', 'Categoria']],
        on='ID_Produto',
        how='left'
    )

    df_vendas_merged.loc[df_vendas_merged['Preco_Venda'].isna(), 'Preco_Venda'] = \
        df_vendas_merged.loc[df_vendas_merged['Preco_Venda'].isna(), 'Preco_Custo'] * 1.3

    df_vendas_merged['Receita_Total'] = df_vendas_merged['Quantidade_Vendida'] * df_vendas_merged['Preco_Venda']
    df_vendas_merged['Margem_Lucro'] = df_vendas_merged['Preco_Venda'] - df_vendas_merged['Preco_Custo']
    df_vendas_merged['Mes_Venda'] = df_vendas_merged['Data_Venda'].dt.strftime('%Y-%m')

    logging.info(f"Produtos transformados: {len(df_produtos)} registros")
    logging.info(f"Vendas transformadas: {len(df_vendas_merged)} registros")

    df_produtos.to_csv('/tmp/produtos_transformados.csv', index=False)
    df_vendas_merged.to_csv('/tmp/vendas_transformadas.csv', index=False)

    return f"Transformados {len(df_produtos)} produtos e {len(df_vendas_merged)} vendas"

def load_data(**context):
    logging.info("Carregando dados transformados no PostgreSQL")

    df_produtos = pd.read_csv('/tmp/produtos_transformados.csv')
    df_vendas = pd.read_csv('/tmp/vendas_transformadas.csv')

    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()

    logging.info("Carregando produtos processados")
    df_produtos.to_sql('produtos_processados', engine, if_exists='replace', index=False, method='multi')
    logging.info(f"Produtos carregados: {len(df_produtos)} registros")

    logging.info("Carregando vendas processadas")
    df_vendas_load = df_vendas[[
        'ID_Venda', 'ID_Produto', 'Quantidade_Vendida', 'Preco_Venda',
        'Data_Venda', 'Canal_Venda', 'Receita_Total', 'Mes_Venda'
    ]].copy()
    df_vendas_load.to_sql('vendas_processadas', engine, if_exists='replace', index=False, method='multi')
    logging.info(f"Vendas carregadas: {len(df_vendas_load)} registros")

    logging.info("Gerando relatório consolidado")
    df_relatorio = df_vendas[[
        'ID_Venda', 'Nome_Produto', 'Categoria', 'Quantidade_Vendida',
        'Receita_Total', 'Margem_Lucro', 'Canal_Venda', 'Mes_Venda'
    ]].copy()
    df_relatorio.to_sql('relatorio_vendas', engine, if_exists='replace', index=False, method='multi')
    logging.info(f"Relatório carregado: {len(df_relatorio)} registros")

    return f"Carregados {len(df_produtos)} produtos, {len(df_vendas_load)} vendas e {len(df_relatorio)} registros no relatório"

def generate_report(**context):
    logging.info("Gerando relatórios analíticos")

    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()

    logging.info("=== RELATÓRIO DE VENDAS ===")

    query_categoria = """
        SELECT "Categoria",
               SUM("Receita_Total") as Total_Vendas,
               COUNT(*) as Numero_Vendas
        FROM relatorio_vendas
        GROUP BY "Categoria"
        ORDER BY Total_Vendas DESC
    """
    df_categoria = pd.read_sql(query_categoria, engine)
    logging.info("\nTotal de vendas por categoria:")
    for _, row in df_categoria.iterrows():
        logging.info(f"  {row['Categoria']}: R$ {row['total_vendas']:.2f} ({row['numero_vendas']} vendas)")

    query_produto = """
        SELECT "Nome_Produto",
               SUM("Quantidade_Vendida") as Total_Quantidade,
               SUM("Receita_Total") as Receita_Total
        FROM relatorio_vendas
        GROUP BY "Nome_Produto"
        ORDER BY Total_Quantidade DESC
        LIMIT 1
    """
    df_produto = pd.read_sql(query_produto, engine)
    if not df_produto.empty:
        produto_top = df_produto.iloc[0]
        logging.info(f"\nProduto mais vendido: {produto_top['Nome_Produto']}")
        logging.info(f"  Quantidade: {produto_top['total_quantidade']} unidades")
        logging.info(f"  Receita: R$ {produto_top['receita_total']:.2f}")

    query_canal = """
        SELECT "Canal_Venda",
               SUM("Receita_Total") as Receita_Total,
               COUNT(*) as Numero_Vendas
        FROM relatorio_vendas
        GROUP BY "Canal_Venda"
        ORDER BY Receita_Total DESC
        LIMIT 1
    """
    df_canal = pd.read_sql(query_canal, engine)
    if not df_canal.empty:
        canal_top = df_canal.iloc[0]
        logging.info(f"\nCanal com maior receita: {canal_top['Canal_Venda']}")
        logging.info(f"  Receita: R$ {canal_top['receita_total']:.2f}")
        logging.info(f"  Número de vendas: {canal_top['numero_vendas']}")

    query_margem = """
        SELECT "Categoria",
               AVG("Margem_Lucro") as Margem_Media
        FROM relatorio_vendas
        GROUP BY "Categoria"
        ORDER BY Margem_Media DESC
    """
    df_margem = pd.read_sql(query_margem, engine)
    logging.info("\nMargem de lucro média por categoria:")
    for _, row in df_margem.iterrows():
        logging.info(f"  {row['Categoria']}: R$ {row['margem_media']:.2f}")

    logging.info("\n=== FIM DO RELATÓRIO ===")

    return "Relatório analítico gerado com sucesso"

def detect_low_performance(**context):
    logging.info("Detectando produtos com baixa performance")

    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()

    query_performance = """
        SELECT p."ID_Produto",
               p."Nome_Produto",
               p."Categoria",
               p."Status",
               COALESCE(COUNT(v."ID_Venda"), 0) as numero_vendas
        FROM produtos_processados p
        LEFT JOIN vendas_processadas v ON p."ID_Produto" = v."ID_Produto"
        GROUP BY p."ID_Produto", p."Nome_Produto", p."Categoria", p."Status"
        HAVING COALESCE(COUNT(v."ID_Venda"), 0) < 2
        ORDER BY numero_vendas ASC
    """

    df_baixa_performance = pd.read_sql(query_performance, engine)

    if not df_baixa_performance.empty:
        logging.warning(f"ALERTA: {len(df_baixa_performance)} produtos com baixa performance detectados!")
        logging.warning("\nProdutos com menos de 2 vendas:")
        for _, row in df_baixa_performance.iterrows():
            logging.warning(f"  - {row['Nome_Produto']} ({row['ID_Produto']}): {row['numero_vendas']} vendas - Status: {row['Status']}")

        df_baixa_performance.to_sql('produtos_baixa_performance', engine, if_exists='replace', index=False, method='multi')
        logging.info(f"\nTabela produtos_baixa_performance criada com {len(df_baixa_performance)} registros")
    else:
        logging.info("Nenhum produto com baixa performance detectado")

        pd.DataFrame(columns=['ID_Produto', 'Nome_Produto', 'Categoria', 'Status', 'Numero_Vendas']).to_sql(
            'produtos_baixa_performance', engine, if_exists='replace', index=False
        )

    return f"Detectados {len(df_baixa_performance)} produtos com baixa performance"

create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS produtos_processados (
        ID_Produto VARCHAR(10),
        Nome_Produto VARCHAR(100),
        Categoria VARCHAR(50),
        Preco_Custo DECIMAL(10,2),
        Fornecedor VARCHAR(100),
        Status VARCHAR(20)
    );

    CREATE TABLE IF NOT EXISTS vendas_processadas (
        ID_Venda VARCHAR(10),
        ID_Produto VARCHAR(10),
        Quantidade_Vendida INTEGER,
        Preco_Venda DECIMAL(10,2),
        Data_Venda DATE,
        Canal_Venda VARCHAR(20),
        Receita_Total DECIMAL(10,2),
        Mes_Venda VARCHAR(7)
    );

    CREATE TABLE IF NOT EXISTS relatorio_vendas (
        ID_Venda VARCHAR(10),
        Nome_Produto VARCHAR(100),
        Categoria VARCHAR(50),
        Quantidade_Vendida INTEGER,
        Receita_Total DECIMAL(10,2),
        Margem_Lucro DECIMAL(10,2),
        Canal_Venda VARCHAR(20),
        Mes_Venda VARCHAR(7)
    );

    CREATE TABLE IF NOT EXISTS produtos_baixa_performance (
        ID_Produto VARCHAR(10),
        Nome_Produto VARCHAR(100),
        Categoria VARCHAR(50),
        Status VARCHAR(20),
        Numero_Vendas INTEGER
    );
    """,
    dag=dag,
)

extract_produtos_task = PythonOperator(
    task_id='extract_produtos',
    python_callable=extract_produtos,
    dag=dag,
)

extract_vendas_task = PythonOperator(
    task_id='extract_vendas',
    python_callable=extract_vendas,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

low_performance_task = PythonOperator(
    task_id='detect_low_performance',
    python_callable=detect_low_performance,
    dag=dag,
)

create_tables >> [extract_produtos_task, extract_vendas_task] >> transform_task >> load_task >> [report_task, low_performance_task]
