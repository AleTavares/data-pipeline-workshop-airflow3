from datetime import datetime, timedelta
import pandas as pd
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Default arguments
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1)
}

# DAG definition
dag = DAG(
    'pipeline_produtos_vendas',
    default_args=default_args,
    description='Pipeline ETL para processamento de produtos e vendas',
    schedule='0 6 * * *',  # Diariamente às 6h
    catchup=False,
    tags=['produtos', 'vendas', 'exercicio']
)

# Funções de auxílio
def validate_file(filepath):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Arquivo não encontrado: {filepath}")
    return True

def extract_produtos():
    produtos_path = '/opt/airflow/data/produtos_loja.csv'
    validate_file(produtos_path)
    df = pd.read_csv(produtos_path)
    print(f"Extraídos {len(df)} registros de produtos")
    return df.to_json()

def extract_vendas():
    vendas_path = '/opt/airflow/data/vendas_produtos.csv'
    validate_file(vendas_path)
    df = pd.read_csv(vendas_path)
    print(f"Extraídos {len(df)} registros de vendas")
    return df.to_json()

def transform_data(**context):
    # Recuperar os dados extraídos
    produtos_json = context['task_instance'].xcom_pull(task_ids='extract_produtos')
    vendas_json = context['task_instance'].xcom_pull(task_ids='extract_vendas')
    
    produtos_df = pd.read_json(produtos_json)
    vendas_df = pd.read_json(vendas_json)
    
    # Limpeza e transformação dos produtos
    produtos_df['Preco_Custo'] = produtos_df.groupby('Categoria')['Preco_Custo'].transform(
        lambda x: x.fillna(x.mean())
    )
    produtos_df['Fornecedor'] = produtos_df['Fornecedor'].fillna('Não Informado')
    
    # Limpeza e transformação das vendas
    vendas_df['Preco_Venda'] = vendas_df.apply(
        lambda row: row['Preco_Venda'] if pd.notnull(row['Preco_Venda'])
        else produtos_df.loc[produtos_df['ID_Produto'] == row['ID_Produto'], 'Preco_Custo'].iloc[0] * 1.3,
        axis=1
    )
    
    # Calcular campos derivados
    vendas_df['Receita_Total'] = vendas_df['Quantidade_Vendida'] * vendas_df['Preco_Venda']
    vendas_df['Data_Venda'] = pd.to_datetime(vendas_df['Data_Venda'])
    vendas_df['Mes_Venda'] = vendas_df['Data_Venda'].dt.strftime('%Y-%m')
    
    # Criar relatório de vendas
    relatorio_df = pd.merge(
        vendas_df,
        produtos_df[['ID_Produto', 'Nome_Produto', 'Categoria', 'Preco_Custo']],
        on='ID_Produto',
        how='left'
    )
    relatorio_df['Margem_Lucro'] = relatorio_df['Preco_Venda'] - relatorio_df['Preco_Custo']
    
    return {
        'produtos': produtos_df.to_json(),
        'vendas': vendas_df.to_json(),
        'relatorio': relatorio_df.to_json()
    }

def load_data(**context):
    # Recuperar dados transformados
    transformed_data = context['task_instance'].xcom_pull(task_ids='transform_data')
    produtos_df = pd.read_json(transformed_data['produtos'])
    vendas_df = pd.read_json(transformed_data['vendas'])
    relatorio_df = pd.read_json(transformed_data['relatorio'])
    
    # Configurar conexão PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Carregar dados nas tabelas
    produtos_df.to_sql('produtos_processados', pg_hook.get_sqlalchemy_engine(),
                      if_exists='replace', index=False)
    vendas_df.to_sql('vendas_processadas', pg_hook.get_sqlalchemy_engine(),
                      if_exists='replace', index=False)
    relatorio_df.to_sql('relatorio_vendas', pg_hook.get_sqlalchemy_engine(),
                      if_exists='replace', index=False)
    
    print("Dados carregados com sucesso!")

def generate_report(**context):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Total de vendas por categoria
    vendas_categoria = pg_hook.get_pandas_df("""
        SELECT Categoria, SUM(Quantidade_Vendida) as Total_Vendas, SUM(Receita_Total) as Receita_Total
        FROM relatorio_vendas
        GROUP BY Categoria
        ORDER BY Receita_Total DESC
    """)
    print("\nTotal de vendas por categoria:")
    print(vendas_categoria)
    
    # Produto mais vendido
    produto_mais_vendido = pg_hook.get_pandas_df("""
        SELECT Nome_Produto, SUM(Quantidade_Vendida) as Total_Vendido
        FROM relatorio_vendas
        GROUP BY Nome_Produto
        ORDER BY Total_Vendido DESC
        LIMIT 1
    """)
    print("\nProduto mais vendido:")
    print(produto_mais_vendido)
    
    # Canal de venda com maior receita
    canal_maior_receita = pg_hook.get_pandas_df("""
        SELECT Canal_Venda, SUM(Receita_Total) as Receita_Total
        FROM relatorio_vendas
        GROUP BY Canal_Venda
        ORDER BY Receita_Total DESC
        LIMIT 1
    """)
    print("\nCanal de venda com maior receita:")
    print(canal_maior_receita)
    
    # Margem de lucro média por categoria
    margem_categoria = pg_hook.get_pandas_df("""
        SELECT Categoria, AVG(Margem_Lucro) as Margem_Media
        FROM relatorio_vendas
        GROUP BY Categoria
        ORDER BY Margem_Media DESC
    """)
    print("\nMargem de lucro média por categoria:")
    print(margem_categoria)

# Tasks
create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql=[
        """
        CREATE TABLE IF NOT EXISTS produtos_processados (
            ID_Produto VARCHAR(10),
            Nome_Produto VARCHAR(100),
            Categoria VARCHAR(50),
            Preco_Custo DECIMAL(10,2),
            Fornecedor VARCHAR(100),
            Status VARCHAR(20),
            Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS vendas_processadas (
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
        """
        CREATE TABLE IF NOT EXISTS relatorio_vendas (
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
        """
    ],
    dag=dag
)

# Task instances
t1_extract_produtos = PythonOperator(
    task_id='extract_produtos',
    python_callable=extract_produtos,
    dag=dag
)

t2_extract_vendas = PythonOperator(
    task_id='extract_vendas',
    python_callable=extract_vendas,
    dag=dag
)

t3_transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

t4_load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

t5_generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag
)

# Task dependencies
# Extract -> Transform -> Create Tables -> Load -> Report
[t1_extract_produtos, t2_extract_vendas] >> t3_transform_data >> create_tables >> t4_load_data >> t5_generate_report