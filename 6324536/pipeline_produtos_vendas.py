from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import os

# Configuração de Logs
log = logging.getLogger(__name__)

# Configuração padrão da DAG
default_args = {
    'owner': 'pipeline_owner',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False, 
    'email_on_retry': False,
    'retries': 2,               
    'retry_delay': timedelta(minutes=5),
}

# Constantes 
POSTGRES_CONN_ID = 'postgres_default'
PRODUTOS_FILE = 'produtos_loja.csv' 
VENDAS_FILE = 'vendas_produtos.csv' 

# --- Funções Python Callable (mantidas as originais: extract_produtos, extract_vendas, transform_data, load_data, generate_report) ---

# [ ... Mantendo as funções extract_produtos, extract_vendas, transform_data, load_data, generate_report ]
# Para brevidade, as funções anteriores (que não mudaram exceto o load_data que não precisa mudar) não são repetidas aqui.

# ************************************************
# *** NOVA FUNÇÃO PARA O DESAFIO BÔNUS (+10) ***
# ************************************************

def analyze_performance(**context):
    """
    Task Bônus: Detecta produtos com baixa performance (menos de 2 vendas)
    e carrega em uma nova tabela.
    """
    log.info("Iniciando análise de performance de vendas (Desafio Bônus).")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # 1. Consulta SQL para detectar produtos com baixa performance (< 2 unidades vendidas)
    sql_low_performance = """
    WITH VendasAgregadas AS (
        SELECT 
            r.nome_produto,
            r.categoria,
            SUM(r.quantidade_vendida) AS total_vendido
        FROM relatorio_vendas r
        GROUP BY r.nome_produto, r.categoria
    )
    SELECT
        nome_produto,
        categoria,
        total_vendido AS total_unidades_vendidas,
        'ALERTA: BAIXA PERFORMANCE (< 2)' AS status_alerta,
        CURRENT_TIMESTAMP AS data_analise
    FROM VendasAgregadas
    WHERE total_vendido < 2;
    """
    
    # Executa a consulta e carrega em um DataFrame
    df_low_performance = hook.get_pandas_df(sql_low_performance)

    if not df_low_performance.empty:
        log.warning(f"🚨 ALERTA DE BAIXA PERFORMANCE: {len(df_low_performance)} produtos identificados!")
        log.warning("\n" + df_low_performance.to_string(index=False))
        
        # 2. Carrega os dados na nova tabela
        df_low_performance.to_sql(
            'produtos_baixa_performance', 
            engine, 
            if_exists='replace', 
            index=False, 
            method='multi'
        )
        log.info(f"✅ {len(df_low_performance)} registros carregados em produtos_baixa_performance.")
        return f"Alerta e carga de {len(df_low_performance)} produtos de baixa performance concluídos."
    else:
        log.info("Nenhum produto com baixa performance (< 2 vendas) detectado. Pipeline OK.")
        return "Nenhum alerta de baixa performance."

# ************************************************
# *** FIM DA FUNÇÃO BÔNUS ***
# ************************************************

# --- Definição da DAG ---

with DAG(
    dag_id='pipeline_produtos_vendas',
    default_args=default_args,
    description='Pipeline ETL para produtos e vendas com join, relatório e desafio bônus',
    schedule='0 6 * * *',                  
    catchup=False,
    tags=['produtos', 'vendas', 'exercicio', 'bonus'], # Adicionado 'bonus'
) as dag:

    # Task 4: Criar Tabelas (AGORA COM A TABELA BÔNUS)
    create_tables_sql = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=[
            # ... [SQL das tabelas produtos_processados, vendas_processadas, relatorio_vendas] ...
            # Tabela produtos_processados
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
            # Tabela vendas_processadas
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
            # Tabela relatorio_vendas
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
            """,
            # Tabela BÔNUS: produtos_baixa_performance
            """
            CREATE TABLE IF NOT EXISTS produtos_baixa_performance (
                Nome_Produto VARCHAR(100),
                Categoria VARCHAR(50),
                Total_Unidades_Vendidas INTEGER,
                Status_Alerta VARCHAR(50),
                Data_Analise TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        ],
    )

    # Tasks Originais
    extract_produtos_task = PythonOperator(task_id='extract_produtos', python_callable=extract_produtos, do_xcom_push=True)
    extract_vendas_task = PythonOperator(task_id='extract_vendas', python_callable=extract_vendas, do_xcom_push=True)
    transform_task = PythonOperator(task_id='transform_data', python_callable=transform_data, provide_context=True, do_xcom_push=True)
    load_task = PythonOperator(task_id='load_data', python_callable=load_data, provide_context=True)
    generate_report_task = PythonOperator(task_id='generate_report', python_callable=generate_report)
    
    # Task Bônus
    analyze_performance_task = PythonOperator(
        task_id='analyze_performance',
        python_callable=analyze_performance,
        provide_context=True,
    )

    # --- Definição das Dependências (Fluxo Atualizado) ---
    
    # Fluxo ETL Principal (Inalterado)
    [extract_produtos_task, extract_vendas_task] >> transform_task
    create_tables_sql >> load_task
    transform_task >> load_task

    # Relatório e Análise Bônus dependem da carga
    load_task >> [generate_report_task, analyze_performance_task]
    
    # Opcional: Relatório antes da Análise
    # generate_report_task >> analyze_performance_task