from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging

# Configuração padrão da DAG
default_args = {
    'owner': 'aula11',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
dag = DAG(
    'pipeline_etl_vendas',
    default_args=default_args,
    description='Pipeline ETL para dados de vendas',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['etl', 'vendas', 'aula11'],
)

def extract_produtos():
    """Extrai produtos do arquivo CSV"""
    file_path = '/opt/airflow/data/produtos_loja.csv'
    logging.info(f"Extraindo dados de: {file_path}")
    
    df = pd.read_csv(file_path)
    logging.info(f"Dados extraídos: {len(df)} registros")
    
    # Salva dados extraídos para próxima tarefa
    df.to_csv('/tmp/produtos_extraidos.csv', index=False)
    return f"Extraídos {len(df)} registros"

def extract_vendas():
    """Extrai vendas de produtos do arquivo CSV"""
    file_path = '/opt/airflow/data/vendas_produtos.csv'
    logging.info(f"Extraindo dados de: {file_path}")
    
    df = pd.read_csv(file_path)
    logging.info(f"Dados extraídos: {len(df)} registros")
    
    # Salva dados extraídos para próxima tarefa
    df.to_csv('/tmp/vendas_extraidos.csv', index=False)
    return f"Extraídos {len(df)} registros"

def transform_data():
    """Transforma os dados extraídos"""
    logging.info("Iniciando transformação dos dados")
    
    # Carrega dados extraídos
    df1 = pd.read_csv('/tmp/produtos_extraidos.csv')
    df2 = pd.read_csv('/tmp/vendas_extraidos.csv')
    df3 = pd.read_csv('/opt/airflow/data/dados_vendas.csv')
    
    # Limpeza: trata valores nulos
    df1['Preco_Custo'] = pd.to_numeric(df1['Preco_Custo'], errors='coerce').fillna(df1['Preco_Custo'].mean())
    df1['Fornecedor'] = pd.to_string(df1['Fornecedor'], errors='coerce').fillna("Não Informado")
    df2['Preco_Venda'] = pd.to_numeric(df2['Preco_Venda'], errors='coerce').fillna(df1['Preco_Custo'] * 1.3)

    # Transformação: calcula total de vendas
    df2['ReceitaTotal'] = df2['Quantidade_Vendida'] * df2['Preco_Venda']
    df2['Margem_Lucro'] = df2['Preco_Venda'] - df1['Preco_Custo']
    
    # Converte data para formato correto
    df2['Mes_Venda'] = pd.to_datetime(df3['Data'])
    
    logging.info(f"Dados transformados de Produtos: {len(df1)} registros")
    logging.info(f"Dados transformados de Vendas: {len(df2)} registros")

    # Salva dados transformados
    df1.to_csv('/tmp/produtos_processados.csv', index=False)
    df2.to_csv('/tmp/vendas_processadas.csv', index=False)
    return f"Transformados da tabela Produtos: {len(df1)} registros.\nTransformados da tabela Vendas: {len(df2)} registros."

def load_data():
    """Carrega dados transformados no PostgreSQL"""
    logging.info("Carregando dados no PostgreSQL")

    # Carrega dados transformados
    df_produtos = pd.read_csv('/tmp/produtos_processados.csv')
    df_vendas = pd.read_csv('/tmp/vendas_processadas.csv')

    # Realiza o JOIN (Transformação)
    # Usa a coluna 'ID_Produto' para o join
    # Faz um LEFT JOIN para garantir que todas as vendas tenham um produto correspondente
    df_relatorio = pd.merge(
        df_vendas,
        df_produtos[['ID_Produto', 'Nome_Produto', 'Categoria', 'Preco_Custo']],
        on='ID_Produto',
        how='left' # Mantém todas as vendas
    )
    
    # Conecta ao PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    # Carrega dados na tabela
    df_produtos.to_sql('produtos_processados', engine, if_exists='replace', index=False, method='multi')
    df_vendas.to_sql('vendas_processadas', engine, if_exists='replace', index=False, method='multi')
    
    logging.info(f"Dados carregados: {len(df_produtos)} registros na tabela produtos_processados")
    logging.info(f"Dados carregados: {len(df_vendas)} registros na tabela vendas_processadas")
    logging.info(f"Dados carregados: {len(df_relatorio)} registros na tabela relatorio_vendas")
    return f"Carregados Produtos: {len(df_produtos)} registros.\nCarregados Vendas: {len(df_vendas)} registros.\nCarregados Relatório: {len(df_relatorio)} registros."

# Tarefa para criar tabela
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE produtos_processados (
        ID_Produto VARCHAR(10),
        Nome_Produto VARCHAR(100),
        Categoria VARCHAR(50),
        Preco_Custo DECIMAL(10,2),
        Fornecedor VARCHAR(100),
        Status VARCHAR(20),
        Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE vendas_processadas (
        ID_Venda VARCHAR(10),
        ID_Produto VARCHAR(10),
        Quantidade_Vendida INTEGER,
        Preco_Venda DECIMAL(10,2),
        Data_Venda DATE,
        Canal_Venda VARCHAR(20),
        Receita_Total DECIMAL(10,2),
        Mes_Venda VARCHAR(7),
        Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE relatorio_vendas (
        ID_Venda VARCHAR(10),
        Nome_Produto VARCHAR(100),
        Categoria VARCHAR(50),
        Quantidade_Vendida INTEGER,
        Receita_Total DECIMAL(10,2),
        Margem_Lucro DECIMAL(10,2),
        Canal_Venda VARCHAR(20),
        Mes_Venda VARCHAR(7),
        Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)

# Definição das dependências
create_table >> extract_produtos >> extract_vendas >> transform_data >> load_data