from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 2
}

dag = DAG(
    'pipeline_produtos_vendas',
    default_args=default_args,
    schedule='0 6 * * *',
    catchup=False,
    tags=['produtos', 'vendas', 'exercicio']
)

def extract_produtos(**context):
    df = pd.read_csv('/opt/airflow/data/produtos_loja.csv')
    logging.info(f"Extraídos {len(df)} registros de produtos")
    df.to_csv('/opt/airflow/data/temp_produtos.csv', index=False)
    return len(df)

def extract_vendas(**context):
    df = pd.read_csv('/opt/airflow/data/vendas_produtos.csv')
    logging.info(f"Extraídos {len(df)} registros de vendas")
    df.to_csv('/opt/airflow/data/temp_vendas.csv', index=False)
    return len(df)

def transform_data(**context):
    produtos_df = pd.read_csv('/opt/airflow/data/temp_produtos.csv')
    vendas_df = pd.read_csv('/opt/airflow/data/temp_vendas.csv')
    
    # Limpeza produtos
    produtos_df['Preco_Custo'] = produtos_df['Preco_Custo'].fillna(
        produtos_df.groupby('Categoria')['Preco_Custo'].transform('mean')
    )
    produtos_df['Fornecedor'] = produtos_df['Fornecedor'].fillna('Não Informado')
    
    # Limpeza vendas
    vendas_merged = vendas_df.merge(produtos_df[['ID_Produto', 'Preco_Custo']], on='ID_Produto')
    vendas_merged['Preco_Venda'] = vendas_merged['Preco_Venda'].fillna(vendas_merged['Preco_Custo'] * 1.3)
    
    # Cálculos
    vendas_merged['Receita_Total'] = vendas_merged['Quantidade_Vendida'] * vendas_merged['Preco_Venda']
    vendas_merged['Margem_Lucro'] = vendas_merged['Preco_Venda'] - vendas_merged['Preco_Custo']
    vendas_merged['Mes_Venda'] = pd.to_datetime(vendas_merged['Data_Venda']).dt.strftime('%Y-%m')
    
    vendas_processed = vendas_merged.drop('Preco_Custo', axis=1)
    
    produtos_df.to_csv('/opt/airflow/data/produtos_processados.csv', index=False)
    vendas_processed.to_csv('/opt/airflow/data/vendas_processadas.csv', index=False)
    
    logging.info(f"Transformados: {len(produtos_df)} produtos, {len(vendas_processed)} vendas")

def load_data(**context):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    produtos_df = pd.read_csv('/opt/airflow/data/produtos_processados.csv')
    vendas_df = pd.read_csv('/opt/airflow/data/vendas_processadas.csv')
    
    for _, row in produtos_df.iterrows():
        hook.run("""
            INSERT INTO produtos_processados 
            (ID_Produto, Nome_Produto, Categoria, Preco_Custo, Fornecedor, Status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, parameters=(
            row['ID_Produto'], row['Nome_Produto'], row['Categoria'],
            row['Preco_Custo'], row['Fornecedor'], row['Status']
        ))
    
    for _, row in vendas_df.iterrows():
        hook.run("""
            INSERT INTO vendas_processadas 
            (ID_Venda, ID_Produto, Quantidade_Vendida, Preco_Venda, Data_Venda, 
             Canal_Venda, Receita_Total, Mes_Venda)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, parameters=(
            row['ID_Venda'], row['ID_Produto'], row['Quantidade_Vendida'],
            row['Preco_Venda'], row['Data_Venda'], row['Canal_Venda'],
            row['Receita_Total'], row['Mes_Venda']
        ))
    
    hook.run("""
        INSERT INTO relatorio_vendas 
        (ID_Venda, Nome_Produto, Categoria, Quantidade_Vendida, Receita_Total, 
         Margem_Lucro, Canal_Venda, Mes_Venda)
        SELECT v.ID_Venda, p.Nome_Produto, p.Categoria, v.Quantidade_Vendida,
               v.Receita_Total, (v.Preco_Venda - p.Preco_Custo), v.Canal_Venda, v.Mes_Venda
        FROM vendas_processadas v
        JOIN produtos_processados p ON v.ID_Produto = p.ID_Produto
    """)
    
    logging.info("Dados carregados no PostgreSQL")

def generate_report(**context):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Relatórios
    vendas_categoria = hook.get_records("SELECT Categoria, SUM(Receita_Total) FROM relatorio_vendas GROUP BY Categoria")
    produto_top = hook.get_records("SELECT Nome_Produto, SUM(Quantidade_Vendida) FROM relatorio_vendas GROUP BY Nome_Produto ORDER BY 2 DESC LIMIT 1")
    canal_top = hook.get_records("SELECT Canal_Venda, SUM(Receita_Total) FROM relatorio_vendas GROUP BY Canal_Venda ORDER BY 2 DESC LIMIT 1")
    margem_categoria = hook.get_records("SELECT Categoria, AVG(Margem_Lucro) FROM relatorio_vendas GROUP BY Categoria")
    
    logging.info(f"Vendas por categoria: {vendas_categoria}")
    logging.info(f"Produto mais vendido: {produto_top}")
    logging.info(f"Canal com maior receita: {canal_top}")
    logging.info(f"Margem por categoria: {margem_categoria}")

def detect_low_performance(**context):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    result = hook.get_records("""
        SELECT p.ID_Produto, p.Nome_Produto, COALESCE(SUM(v.Quantidade_Vendida), 0)
        FROM produtos_processados p
        LEFT JOIN vendas_processadas v ON p.ID_Produto = v.ID_Produto
        GROUP BY p.ID_Produto, p.Nome_Produto
        HAVING COALESCE(SUM(v.Quantidade_Vendida), 0) < 2
    """)
    
    if result:
        logging.warning(f"Produtos com baixa performance: {result}")
        for row in result:
            hook.run("INSERT INTO produtos_baixa_performance (ID_Produto, Nome_Produto, Total_Vendas) VALUES (%s, %s, %s)", parameters=row)
    else:
        logging.info("Nenhum produto com baixa performance")

# Definição das tarefas
extract_produtos_task = PythonOperator(
    task_id='extract_produtos',
    python_callable=extract_produtos,
    dag=dag
)

extract_vendas_task = PythonOperator(
    task_id='extract_vendas',
    python_callable=extract_vendas,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

create_tables_task = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql="""
        -- Tabela produtos_processados
        CREATE TABLE IF NOT EXISTS produtos_processados (
            ID_Produto VARCHAR(10),
            Nome_Produto VARCHAR(100),
            Categoria VARCHAR(50),
            Preco_Custo DECIMAL(10,2),
            Fornecedor VARCHAR(100),
            Status VARCHAR(20),
            Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Tabela vendas_processadas
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
        
        -- Tabela relatorio_vendas
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
        
        -- Tabela produtos_baixa_performance (Bônus)
        CREATE TABLE IF NOT EXISTS produtos_baixa_performance (
            ID_Produto VARCHAR(10),
            Nome_Produto VARCHAR(100),
            Total_Vendas INTEGER,
            Data_Deteccao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Limpar tabelas para nova execução
        TRUNCATE TABLE produtos_processados, vendas_processadas, relatorio_vendas, produtos_baixa_performance;
    """,
    dag=dag
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag
)

# Tarefa bônus
detect_low_performance_task = PythonOperator(
    task_id='detect_low_performance',
    python_callable=detect_low_performance,
    dag=dag
)

# Definição das dependências
[extract_produtos_task, extract_vendas_task] >> transform_data_task
transform_data_task >> create_tables_task >> load_data_task
load_data_task >> generate_report_task >> detect_low_performance_task