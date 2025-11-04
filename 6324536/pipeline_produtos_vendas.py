from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import os
import numpy as np

# Configuração de Logs
log = logging.getLogger(__name__)

# Configuração padrão da DAG
default_args = {
    'owner': 'pipeline_owner',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Constantes (ajuste os caminhos conforme seu ambiente)
POSTGRES_CONN_ID = 'postgres_default'
PRODUTOS_FILE = 'produtos_loja.csv' 
VENDAS_FILE = 'vendas_produtos.csv' 

# --- Funções Python Callable ---

def extract_produtos(**context):
    """Task 1: Extrair e validar o arquivo de Produtos."""
    ti = context['ti']
    file_path = f'/opt/airflow/data/{PRODUTOS_FILE}'

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ Erro: Arquivo de Produtos '{file_path}' não encontrado.")

    df_produtos = pd.read_csv(file_path)
    log.info(f"✅ Extração de Produtos concluída. Registros extraídos: {len(df_produtos)}")

    # Salva para uso em Transformação
    temp_path = '/tmp/df_produtos_raw.csv'
    df_produtos.to_csv(temp_path, index=False)
    ti.xcom_push(key='produtos_raw_path', value=temp_path)
    return f"Produtos extraídos: {len(df_produtos)} registros"

def extract_vendas(**context):
    """Task 2: Extrair e validar o arquivo de Vendas."""
    ti = context['ti']
    file_path = f'/opt/airflow/data/{VENDAS_FILE}'
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ Erro: Arquivo de Vendas '{file_path}' não encontrado.")

    df_vendas = pd.read_csv(file_path)
    log.info(f"✅ Extração de Vendas concluída. Registros extraídos: {len(df_vendas)}")

    # Salva para uso em Transformação
    temp_path = '/tmp/df_vendas_raw.csv'
    df_vendas.to_csv(temp_path, index=False)
    ti.xcom_push(key='vendas_raw_path', value=temp_path)
    return f"Vendas extraídas: {len(df_vendas)} registros"

def transform_data(**context):
    """Task 3: Limpeza, Transformações e Junção de Dados."""
    ti = context['ti']
    log.info("Iniciando transformação e limpeza dos dados.")

    # Recupera caminhos dos arquivos via XCom
    produtos_path = ti.xcom_pull(task_ids='extract_produtos', key='produtos_raw_path')
    vendas_path = ti.xcom_pull(task_ids='extract_vendas', key='vendas_raw_path')

    df_produtos = pd.read_csv(produtos_path)
    df_vendas = pd.read_csv(vendas_path)
    
    # --- Limpeza de Dados (Produtos) ---
    # 1. Preencher 'Fornecedor' nulo com "Não Informado"
    df_produtos['Fornecedor'].fillna("Não Informado", inplace=True)
    
    # 2. Preencher 'Preco_Custo' nulo com média da categoria
    df_produtos['Preco_Custo'] = pd.to_numeric(df_produtos['Preco_Custo'], errors='coerce')
    media_custo_categoria = df_produtos.groupby('Categoria')['Preco_Custo'].transform('mean')
    df_produtos['Preco_Custo'].fillna(media_custo_categoria, inplace=True)
    
    # 3. Preencher 'Preco_Venda' nulo com 'Preco_Custo * 1.3'
    df_produtos['Preco_Venda'] = pd.to_numeric(df_produtos['Preco_Venda'], errors='coerce')
    df_produtos['Preco_Venda'].fillna(
        df_produtos['Preco_Custo'] * 1.3, inplace=True
    )
    
    log.info("✅ Limpeza de Produtos concluída.")
    
    # --- Junção (Merge) e Transformação Final (Relatório) ---
    df_relatorio = pd.merge(
        df_vendas, 
        df_produtos.drop(columns=['Fornecedor']), # Evita colunas duplicadas no relatorio_vendas
        on='Produto_ID', 
        how='inner'
    )
    
    # Garantir colunas essenciais para cálculo
    df_relatorio['Quantidade_Vendida'] = pd.to_numeric(df_relatorio['Quantidade_Vendida'], errors='coerce').fillna(0).astype(int)
    
    # 4. Calcular 'Receita_Total'
    df_relatorio['Receita_Total'] = df_relatorio['Quantidade_Vendida'] * df_relatorio['Preco_Venda']
    
    # 5. Calcular 'Margem_Lucro'
    df_relatorio['Margem_Lucro'] = df_relatorio['Preco_Venda'] - df_relatorio['Preco_Custo']
    
    # 6. Criar campo 'Mes_Venda'
    df_relatorio['Data_Venda'] = pd.to_datetime(df_relatorio['Data_Venda'])
    df_relatorio['Mes_Venda'] = df_relatorio['Data_Venda'].dt.strftime('%Y-%m') # Formato YYYY-MM
    
    log.info(f"✅ Transformações e Junção de Dados concluídas. Total de linhas do Relatório: {len(df_relatorio)}")
    
    # Salva os DataFrames processados para a próxima tarefa
    temp_produtos_proc = '/tmp/df_produtos_processados.csv'
    temp_relatorio_final = '/tmp/df_relatorio_final.csv'
    
    df_produtos.to_csv(temp_produtos_proc, index=False)
    df_relatorio.to_csv(temp_relatorio_final, index=False)

    ti.xcom_push(key='produtos_proc_path', value=temp_produtos_proc)
    ti.xcom_push(key='relatorio_final_path', value=temp_relatorio_final)
    
    return f"Transformados {len(df_relatorio)} registros de relatório"

def load_data(**context):
    """Task 5: Carrega dados transformados nas tabelas PostgreSQL e valida."""
    ti = context['ti']
    log.info("Carregando dados no PostgreSQL")

    # Recupera caminhos dos arquivos via XCom
    produtos_proc_path = ti.xcom_pull(task_ids='transform_data', key='produtos_proc_path')
    relatorio_final_path = ti.xcom_pull(task_ids='transform_data', key='relatorio_final_path')
    
    df_produtos = pd.read_csv(produtos_proc_path)
    df_relatorio = pd.read_csv(relatorio_final_path)

    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = postgres_hook.get_sqlalchemy_engine()
    
    # 1. Carregar produtos_processados (substituir dados, assumindo que Produto_ID é a chave primária)
    df_produtos.to_sql('produtos_processados', engine, if_exists='replace', index=False, method='multi')
    log.info(f"✅ Produtos carregados: {len(df_produtos)} registros.")

    # 2. Carregar relatorio_vendas (substituir dados, assumindo que será gerado um novo relatório por DAG run)
    df_relatorio.to_sql('relatorio_vendas', engine, if_exists='replace', index=False, method='multi')
    log.info(f"✅ Relatório carregado: {len(df_relatorio)} registros.")

    # Validação da Carga
    total_produtos_db = postgres_hook.get_first("SELECT COUNT(*) FROM produtos_processados;")[0]
    total_relatorio_db = postgres_hook.get_first("SELECT COUNT(*) FROM relatorio_vendas;")[0]
    
    if total_produtos_db == len(df_produtos) and total_relatorio_db == len(df_relatorio):
        log.info("✅ Validação de carga concluída: as contagens de registros correspondem.")
    else:
        log.warning("⚠️ Alerta: Inconsistência na contagem. A carga pode ter falhado parcialmente.")

    return f"Carregados {total_relatorio_db} registros de relatório"

def generate_report(**context):
    """Task 6: Gera o relatório final a partir da tabela relatorio_vendas."""
    log.info("Iniciando geração do relatório final de vendas.")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # 1. Total de vendas por categoria (Quantidade)
    sql_vendas_categoria = """
    SELECT categoria, SUM(quantidade_vendida) as total_vendas
    FROM relatorio_vendas
    GROUP BY categoria
    ORDER BY total_vendas DESC;
    """
    # 2. Produto mais vendido (em quantidade)
    sql_produto_mais_vendido = """
    SELECT nome_produto, SUM(quantidade_vendida) as total_vendido
    FROM relatorio_vendas
    GROUP BY nome_produto
    ORDER BY total_vendido DESC
    LIMIT 1;
    """
    # 3. Canal de venda com maior receita
    sql_canal_maior_receita = """
    SELECT canal_venda, SUM(receita_total) as receita_total
    FROM relatorio_vendas
    GROUP BY canal_venda
    ORDER BY receita_total DESC
    LIMIT 1;
    """
    # 4. Margem de lucro média por categoria
    sql_margem_categoria = """
    SELECT categoria, AVG(margem_lucro) as margem_lucro_media
    FROM relatorio_vendas
    GROUP BY categoria
    ORDER BY margem_lucro_media DESC;
    """
    
    # Executa as consultas
    df_vendas_categoria = hook.get_pandas_df(sql_vendas_categoria)
    df_mais_vendido = hook.get_pandas_df(sql_produto_mais_vendido)
    df_canal_receita = hook.get_pandas_df(sql_canal_maior_receita)
    df_margem_categoria = hook.get_pandas_df(sql_margem_categoria)

    # Montar o Relatório Final (log)
    report_output = f"""
    \n--- 📊 RELATÓRIO DE VENDAS E PRODUTOS ---

    1. Total de Vendas por Categoria (Quantidade):
    {df_vendas_categoria.to_string(index=False)}

    2. Produto Mais Vendido:
    Produto: {df_mais_vendido['nome_produto'].iloc[0]} (Total Vendido: {df_mais_vendido['total_vendido'].iloc[0]})

    3. Canal de Venda com Maior Receita:
    Canal: {df_canal_receita['canal_venda'].iloc[0]} (Receita Total: R$ {df_canal_receita['receita_total'].iloc[0]:,.2f})

    4. Margem de Lucro Média por Categoria:
    {df_margem_categoria.to_string(index=False)}
    ------------------------------------------
    """
    log.info(report_output)
    
    return "Relatório gerado e salvo nos logs."

# --- Definição da DAG ---

with DAG(
    dag_id='pipeline_produtos_vendas', # Novo nome conforme o pedido inicial
    default_args=default_args,
    description='Pipeline ETL para produtos e vendas com join e relatório',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['etl', 'vendas', 'produtos'],
) as dag:

    # Task 4: Criar Tabelas
    create_tables_sql = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=[
            # Tabela produtos_processados
            """
            CREATE TABLE IF NOT EXISTS produtos_processados (
                produto_id INTEGER PRIMARY KEY,
                nome_produto VARCHAR(255),
                categoria VARCHAR(100),
                preco_custo NUMERIC(10, 2),
                preco_venda NUMERIC(10, 2),
                fornecedor VARCHAR(100)
            );
            """,
            # Tabela vendas_processadas (Opcional, mas mantida para ETL tradicional)
            """
            CREATE TABLE IF NOT EXISTS vendas_processadas (
                venda_id INTEGER,
                produto_id INTEGER,
                data_venda DATE,
                quantidade_vendida INTEGER,
                canal_venda VARCHAR(50)
            );
            """,
            # Tabela relatorio_vendas (Join dos dados e enriquecida)
            """
            CREATE TABLE IF NOT EXISTS relatorio_vendas (
                venda_id INTEGER,
                produto_id INTEGER,
                data_venda DATE,
                quantidade_vendida INTEGER,
                canal_venda VARCHAR(50),
                nome_produto VARCHAR(255),
                categoria VARCHAR(100),
                preco_custo NUMERIC(10, 2),
                preco_venda NUMERIC(10, 2),
                receita_total NUMERIC(10, 2),
                margem_lucro NUMERIC(10, 2),
                mes_venda VARCHAR(7) 
            );
            """
        ],
    )

    # Task 1: Extração de Produtos
    extract_produtos_task = PythonOperator(
        task_id='extract_produtos',
        python_callable=extract_produtos,
        do_xcom_push=True,
    )

    # Task 2: Extração de Vendas
    extract_vendas_task = PythonOperator(
        task_id='extract_vendas',
        python_callable=extract_vendas,
        do_xcom_push=True,
    )

    # Task 3: Transformação e Limpeza
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
        do_xcom_push=True,
    )

    # Task 5: Carregamento
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )

    # Task 6: Geração do Relatório
    generate_report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
    )

    # --- Definição das Dependências (Fluxo) ---
    
    # 1. Extrações e Criação de Tabelas podem ser paralelas
    extract_produtos_task >> transform_task
    extract_vendas_task >> transform_task
    
    # 2. Transformação depende das extrações
    [extract_produtos_task, extract_vendas_task] >> transform_task
    
    # 3. Carregamento depende da criação das tabelas e da transformação
    create_tables_sql >> load_task
    transform_task >> load_task

    # 4. Geração do Relatório depende do Carregamento
    load_task >> generate_report_task