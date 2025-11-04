"""
Pipeline ETL para Processamento de Produtos e Vendas
Exercício Final - Data Pipeline Workshop

Este pipeline implementa um ETL completo para processar dados de produtos e vendas,
aplicando limpeza de dados, transformações e geração de relatórios.

Aluno: Leonardo Frazão Sano
RA: 6324073
Data: Novembro de 2025
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import os

# Configuração padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
dag = DAG(
    'pipeline_produtos_vendas',
    default_args=default_args,
    description='Pipeline ETL completo para processar dados de produtos e vendas',
    schedule='0 6 * * *',  # Diário às 6h da manhã
    catchup=False,
    tags=['produtos', 'vendas', 'exercicio'],
)

# Caminhos dos arquivos
PRODUTOS_FILE = '/opt/airflow/data/produtos_loja.csv'
VENDAS_FILE = '/opt/airflow/data/vendas_produtos.csv'
TMP_PRODUTOS = '/tmp/produtos_extraidos.csv'
TMP_VENDAS = '/tmp/vendas_extraidas.csv'
TMP_PRODUTOS_TRANSFORM = '/tmp/produtos_transformados.csv'
TMP_VENDAS_TRANSFORM = '/tmp/vendas_transformadas.csv'


def extract_produtos(**context):
    """
    Task 1: Extrair dados de produtos
    - Valida existência do arquivo
    - Lê dados do CSV
    - Registra logs informativos
    """
    logging.info(f"=== INICIANDO EXTRAÇÃO DE PRODUTOS ===")
    
    # Validar se arquivo existe
    if not os.path.exists(PRODUTOS_FILE):
        raise FileNotFoundError(f"Arquivo não encontrado: {PRODUTOS_FILE}")
    
    logging.info(f"Arquivo encontrado: {PRODUTOS_FILE}")
    
    # Ler arquivo CSV
    df_produtos = pd.read_csv(PRODUTOS_FILE)
    
    # Registrar informações
    num_registros = len(df_produtos)
    logging.info(f"✓ Número de registros extraídos: {num_registros}")
    logging.info(f"✓ Colunas: {list(df_produtos.columns)}")
    logging.info(f"✓ Categorias encontradas: {df_produtos['Categoria'].unique().tolist()}")
    
    # Identificar problemas nos dados
    nulos_preco = df_produtos['Preco_Custo'].isna().sum()
    nulos_fornecedor = df_produtos['Fornecedor'].isna().sum()
    
    logging.info(f"⚠ Valores nulos encontrados:")
    logging.info(f"  - Preco_Custo: {nulos_preco}")
    logging.info(f"  - Fornecedor: {nulos_fornecedor}")
    
    # Salvar dados extraídos
    df_produtos.to_csv(TMP_PRODUTOS, index=False)
    logging.info(f"✓ Dados salvos em: {TMP_PRODUTOS}")
    
    return {
        'registros_extraidos': num_registros,
        'nulos_preco': nulos_preco,
        'nulos_fornecedor': nulos_fornecedor
    }


def extract_vendas(**context):
    """
    Task 2: Extrair dados de vendas
    - Valida existência do arquivo
    - Lê dados do CSV
    - Registra logs informativos
    """
    logging.info(f"=== INICIANDO EXTRAÇÃO DE VENDAS ===")
    
    # Validar se arquivo existe
    if not os.path.exists(VENDAS_FILE):
        raise FileNotFoundError(f"Arquivo não encontrado: {VENDAS_FILE}")
    
    logging.info(f"Arquivo encontrado: {VENDAS_FILE}")
    
    # Ler arquivo CSV
    df_vendas = pd.read_csv(VENDAS_FILE)
    
    # Registrar informações
    num_registros = len(df_vendas)
    logging.info(f"✓ Número de registros extraídos: {num_registros}")
    logging.info(f"✓ Colunas: {list(df_vendas.columns)}")
    logging.info(f"✓ Período de vendas: {df_vendas['Data_Venda'].min()} até {df_vendas['Data_Venda'].max()}")
    
    # Identificar problemas nos dados
    nulos_preco_venda = df_vendas['Preco_Venda'].isna().sum()
    
    logging.info(f"⚠ Valores nulos encontrados:")
    logging.info(f"  - Preco_Venda: {nulos_preco_venda}")
    
    # Salvar dados extraídos
    df_vendas.to_csv(TMP_VENDAS, index=False)
    logging.info(f"✓ Dados salvos em: {TMP_VENDAS}")
    
    return {
        'registros_extraidos': num_registros,
        'nulos_preco_venda': nulos_preco_venda
    }


def transform_data(**context):
    """
    Task 3: Transformar e limpar dados
    - Limpeza de dados nulos
    - Cálculos de receita e margem
    - Criação de campos derivados
    """
    logging.info(f"=== INICIANDO TRANSFORMAÇÃO DE DADOS ===")
    
    # Carregar dados extraídos
    df_produtos = pd.read_csv(TMP_PRODUTOS)
    df_vendas = pd.read_csv(TMP_VENDAS)
    
    logging.info(f"Dados carregados: {len(df_produtos)} produtos, {len(df_vendas)} vendas")
    
    # === LIMPEZA DE PRODUTOS ===
    logging.info("--- Limpeza de Produtos ---")
    
    # 1. Preencher Preco_Custo nulo com média da categoria
    df_produtos['Preco_Custo'] = pd.to_numeric(df_produtos['Preco_Custo'], errors='coerce')
    
    for categoria in df_produtos['Categoria'].unique():
        media_categoria = df_produtos[df_produtos['Categoria'] == categoria]['Preco_Custo'].mean()
        mask = (df_produtos['Categoria'] == categoria) & (df_produtos['Preco_Custo'].isna())
        df_produtos.loc[mask, 'Preco_Custo'] = media_categoria
        if mask.any():
            logging.info(f"✓ Preenchido Preco_Custo para categoria '{categoria}' com média: R$ {media_categoria:.2f}")
    
    # 2. Preencher Fornecedor nulo com "Não Informado"
    fornecedores_nulos = df_produtos['Fornecedor'].isna().sum()
    df_produtos['Fornecedor'].fillna('Não Informado', inplace=True)
    if fornecedores_nulos > 0:
        logging.info(f"✓ Preenchido {fornecedores_nulos} fornecedores com 'Não Informado'")
    
    # === LIMPEZA DE VENDAS ===
    logging.info("--- Limpeza de Vendas ---")
    
    # 3. Preencher Preco_Venda nulo com Preco_Custo * 1.3
    df_vendas['Preco_Venda'] = pd.to_numeric(df_vendas['Preco_Venda'], errors='coerce')
    
    # Fazer merge para obter Preco_Custo
    df_vendas_merge = df_vendas.merge(df_produtos[['ID_Produto', 'Preco_Custo']], 
                                       on='ID_Produto', how='left')
    
    mask_preco_nulo = df_vendas_merge['Preco_Venda'].isna()
    if mask_preco_nulo.any():
        df_vendas_merge.loc[mask_preco_nulo, 'Preco_Venda'] = df_vendas_merge.loc[mask_preco_nulo, 'Preco_Custo'] * 1.3
        logging.info(f"✓ Preenchido {mask_preco_nulo.sum()} preços de venda com Preco_Custo * 1.3")
    
    # === TRANSFORMAÇÕES ===
    logging.info("--- Aplicando Transformações ---")
    
    # 4. Calcular Receita_Total
    df_vendas_merge['Receita_Total'] = df_vendas_merge['Quantidade_Vendida'] * df_vendas_merge['Preco_Venda']
    logging.info(f"✓ Receita_Total calculada")
    
    # 5. Calcular Margem_Lucro
    df_vendas_merge['Margem_Lucro'] = df_vendas_merge['Preco_Venda'] - df_vendas_merge['Preco_Custo']
    logging.info(f"✓ Margem_Lucro calculada")
    
    # 6. Criar campo Mes_Venda
    df_vendas_merge['Data_Venda'] = pd.to_datetime(df_vendas_merge['Data_Venda'])
    df_vendas_merge['Mes_Venda'] = df_vendas_merge['Data_Venda'].dt.strftime('%Y-%m')
    logging.info(f"✓ Mes_Venda extraído")
    
    # Remover coluna temporária Preco_Custo do dataframe de vendas
    df_vendas_final = df_vendas_merge.drop(columns=['Preco_Custo'])
    
    # === RESUMO DAS TRANSFORMAÇÕES ===
    logging.info("--- Resumo das Transformações ---")
    logging.info(f"✓ Total de produtos processados: {len(df_produtos)}")
    logging.info(f"✓ Total de vendas processadas: {len(df_vendas_final)}")
    logging.info(f"✓ Receita total: R$ {df_vendas_final['Receita_Total'].sum():.2f}")
    logging.info(f"✓ Margem de lucro média: R$ {df_vendas_final['Margem_Lucro'].mean():.2f}")
    
    # Salvar dados transformados
    df_produtos.to_csv(TMP_PRODUTOS_TRANSFORM, index=False)
    df_vendas_final.to_csv(TMP_VENDAS_TRANSFORM, index=False)
    
    logging.info(f"✓ Dados transformados salvos")
    
    return {
        'produtos_processados': len(df_produtos),
        'vendas_processadas': len(df_vendas_final),
        'receita_total': float(df_vendas_final['Receita_Total'].sum())
    }


def load_data(**context):
    """
    Task 5: Carregar dados transformados no PostgreSQL
    - Insere dados em produtos_processados
    - Insere dados em vendas_processadas
    - Cria relatorio_vendas com join
    - Valida inserções
    """
    logging.info(f"=== INICIANDO CARGA DE DADOS ===")
    
    # Carregar dados transformados
    df_produtos = pd.read_csv(TMP_PRODUTOS_TRANSFORM)
    df_vendas = pd.read_csv(TMP_VENDAS_TRANSFORM)
    
    # Conectar ao PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='northwind_postgres')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    # === CARREGAR PRODUTOS ===
    logging.info("--- Carregando Produtos ---")
    df_produtos.to_sql('produtos_processados', engine, if_exists='append', index=False, method='multi')
    logging.info(f"✓ {len(df_produtos)} produtos inseridos em produtos_processados")
    
    # === CARREGAR VENDAS ===
    logging.info("--- Carregando Vendas ---")
    df_vendas.to_sql('vendas_processadas', engine, if_exists='append', index=False, method='multi')
    logging.info(f"✓ {len(df_vendas)} vendas inseridas em vendas_processadas")
    
    # === CRIAR RELATÓRIO ===
    logging.info("--- Criando Relatório Consolidado ---")
    
    # Join dos dados para criar relatório
    df_relatorio = df_vendas.merge(
        df_produtos[['ID_Produto', 'Nome_Produto', 'Categoria']], 
        on='ID_Produto', 
        how='left'
    )
    
    # Selecionar apenas colunas necessárias
    df_relatorio = df_relatorio[[
        'ID_Venda', 'Nome_Produto', 'Categoria', 'Quantidade_Vendida',
        'Receita_Total', 'Margem_Lucro', 'Canal_Venda', 'Mes_Venda'
    ]]
    
    # Carregar relatório
    df_relatorio.to_sql('relatorio_vendas', engine, if_exists='append', index=False, method='multi')
    logging.info(f"✓ {len(df_relatorio)} registros inseridos em relatorio_vendas")
    
    # === VALIDAÇÃO ===
    logging.info("--- Validando Dados Inseridos ---")
    
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    # Validar produtos
    cursor.execute("SELECT COUNT(*) FROM produtos_processados")
    count_produtos = cursor.fetchone()[0]
    logging.info(f"✓ Produtos na tabela: {count_produtos}")
    
    # Validar vendas
    cursor.execute("SELECT COUNT(*) FROM vendas_processadas")
    count_vendas = cursor.fetchone()[0]
    logging.info(f"✓ Vendas na tabela: {count_vendas}")
    
    # Validar relatório
    cursor.execute("SELECT COUNT(*) FROM relatorio_vendas")
    count_relatorio = cursor.fetchone()[0]
    logging.info(f"✓ Registros no relatório: {count_relatorio}")
    
    cursor.close()
    conn.close()
    
    if count_produtos == 0 or count_vendas == 0 or count_relatorio == 0:
        raise ValueError("Erro na validação: tabelas vazias detectadas!")
    
    logging.info("✓ Validação concluída com sucesso!")
    
    return {
        'produtos_inseridos': count_produtos,
        'vendas_inseridas': count_vendas,
        'relatorio_registros': count_relatorio
    }


def generate_report(**context):
    """
    Task 6: Gerar relatório analítico
    - Total de vendas por categoria
    - Produto mais vendido
    - Canal de venda com maior receita
    - Margem de lucro média por categoria
    """
    logging.info(f"=== GERANDO RELATÓRIO ANALÍTICO ===")
    
    # Conectar ao PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='northwind_postgres')
    
    logging.info("\n" + "="*60)
    logging.info("📊 RELATÓRIO DE ANÁLISE DE VENDAS")
    logging.info("="*60 + "\n")
    
    # === 1. TOTAL DE VENDAS POR CATEGORIA ===
    logging.info("--- 1. Total de Vendas por Categoria ---")
    query1 = """
    SELECT 
        Categoria,
        SUM(Quantidade_Vendida) as Total_Quantidade,
        SUM(Receita_Total) as Total_Receita,
        ROUND(AVG(Receita_Total), 2) as Ticket_Medio
    FROM relatorio_vendas
    GROUP BY Categoria
    ORDER BY Total_Receita DESC
    """
    df_categoria = postgres_hook.get_pandas_df(query1)
    logging.info(f"\n{df_categoria.to_string(index=False)}\n")
    
    # === 2. PRODUTO MAIS VENDIDO ===
    logging.info("--- 2. Produto Mais Vendido ---")
    query2 = """
    SELECT 
        Nome_Produto,
        SUM(Quantidade_Vendida) as Total_Vendido,
        SUM(Receita_Total) as Receita_Total,
        COUNT(DISTINCT ID_Venda) as Num_Vendas
    FROM relatorio_vendas
    GROUP BY Nome_Produto
    ORDER BY Total_Vendido DESC
    LIMIT 1
    """
    df_top_produto = postgres_hook.get_pandas_df(query2)
    logging.info(f"\n🏆 Produto Campeão: {df_top_produto['nome_produto'].values[0]}")
    logging.info(f"   Quantidade Total: {df_top_produto['total_vendido'].values[0]} unidades")
    logging.info(f"   Receita Gerada: R$ {df_top_produto['receita_total'].values[0]:.2f}")
    logging.info(f"   Número de Vendas: {df_top_produto['num_vendas'].values[0]}\n")
    
    # === 3. CANAL DE VENDA COM MAIOR RECEITA ===
    logging.info("--- 3. Desempenho por Canal de Venda ---")
    query3 = """
    SELECT 
        Canal_Venda,
        COUNT(*) as Num_Vendas,
        SUM(Quantidade_Vendida) as Total_Quantidade,
        SUM(Receita_Total) as Total_Receita,
        ROUND(AVG(Receita_Total), 2) as Ticket_Medio
    FROM relatorio_vendas
    GROUP BY Canal_Venda
    ORDER BY Total_Receita DESC
    """
    df_canal = postgres_hook.get_pandas_df(query3)
    logging.info(f"\n{df_canal.to_string(index=False)}")
    logging.info(f"\n🥇 Canal Líder: {df_canal['canal_venda'].values[0]}")
    logging.info(f"   Receita: R$ {df_canal['total_receita'].values[0]:.2f}\n")
    
    # === 4. MARGEM DE LUCRO MÉDIA POR CATEGORIA ===
    logging.info("--- 4. Margem de Lucro por Categoria ---")
    query4 = """
    SELECT 
        Categoria,
        ROUND(AVG(Margem_Lucro), 2) as Margem_Media,
        ROUND(MIN(Margem_Lucro), 2) as Margem_Minima,
        ROUND(MAX(Margem_Lucro), 2) as Margem_Maxima,
        COUNT(*) as Num_Vendas
    FROM relatorio_vendas
    GROUP BY Categoria
    ORDER BY Margem_Media DESC
    """
    df_margem = postgres_hook.get_pandas_df(query4)
    logging.info(f"\n{df_margem.to_string(index=False)}\n")
    
    # === RESUMO GERAL ===
    logging.info("--- Resumo Geral ---")
    query_resumo = """
    SELECT 
        COUNT(DISTINCT ID_Venda) as Total_Vendas,
        SUM(Quantidade_Vendida) as Total_Itens_Vendidos,
        SUM(Receita_Total) as Receita_Total_Geral,
        ROUND(AVG(Margem_Lucro), 2) as Margem_Lucro_Media_Geral
    FROM relatorio_vendas
    """
    df_resumo = postgres_hook.get_pandas_df(query_resumo)
    
    logging.info(f"\n💰 RESUMO EXECUTIVO")
    logging.info(f"   Total de Vendas: {df_resumo['total_vendas'].values[0]}")
    logging.info(f"   Total de Itens Vendidos: {df_resumo['total_itens_vendidos'].values[0]}")
    logging.info(f"   Receita Total: R$ {df_resumo['receita_total_geral'].values[0]:.2f}")
    logging.info(f"   Margem de Lucro Média: R$ {df_resumo['margem_lucro_media_geral'].values[0]:.2f}")
    
    logging.info("\n" + "="*60)
    logging.info("✓ Relatório gerado com sucesso!")
    logging.info("="*60 + "\n")
    
    return {
        'total_vendas': int(df_resumo['total_vendas'].values[0]),
        'receita_total': float(df_resumo['receita_total_geral'].values[0]),
        'produto_mais_vendido': df_top_produto['nome_produto'].values[0],
        'canal_lider': df_canal['canal_venda'].values[0]
    }


def detect_low_performance(**context):
    """
    BÔNUS: Detectar produtos com baixa performance
    - Identifica produtos com menos de 2 vendas
    - Registra alertas
    - Cria tabela produtos_baixa_performance
    """
    logging.info(f"=== ANALISANDO PERFORMANCE DE PRODUTOS ===")
    
    # Conectar ao PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='northwind_postgres')
    
    # Query para detectar produtos com baixa performance
    query = """
    SELECT 
        p.ID_Produto,
        p.Nome_Produto,
        p.Categoria,
        p.Preco_Custo,
        p.Status,
        COALESCE(COUNT(v.ID_Venda), 0) as Num_Vendas,
        COALESCE(SUM(v.Quantidade_Vendida), 0) as Total_Vendido
    FROM produtos_processados p
    LEFT JOIN vendas_processadas v ON p.ID_Produto = v.ID_Produto
    GROUP BY p.ID_Produto, p.Nome_Produto, p.Categoria, p.Preco_Custo, p.Status
    HAVING COALESCE(COUNT(v.ID_Venda), 0) < 2
    ORDER BY Num_Vendas ASC, p.Nome_Produto
    """
    
    df_baixa_performance = postgres_hook.get_pandas_df(query)
    
    if len(df_baixa_performance) > 0:
        logging.warning(f"\n⚠️  ALERTA: {len(df_baixa_performance)} produto(s) com baixa performance detectado(s)!")
        logging.warning(f"\n{df_baixa_performance.to_string(index=False)}\n")
        
        # Carregar na tabela de baixa performance
        engine = postgres_hook.get_sqlalchemy_engine()
        df_baixa_performance.to_sql('produtos_baixa_performance', engine, 
                                     if_exists='append', index=False, method='multi')
        
        logging.warning(f"✓ {len(df_baixa_performance)} produtos registrados em produtos_baixa_performance")
        
        # Detalhamento por produto
        for _, row in df_baixa_performance.iterrows():
            logging.warning(f"   • {row['nome_produto']} ({row['categoria']}): {row['num_vendas']} venda(s)")
    else:
        logging.info("✓ Todos os produtos têm performance satisfatória!")
    
    return {
        'produtos_baixa_performance': len(df_baixa_performance)
    }


# === DEFINIÇÃO DAS TASKS ===

# Task 4: Criar tabelas no PostgreSQL
create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='northwind_postgres',
    sql="""
    -- Tabela de produtos processados
    CREATE TABLE IF NOT EXISTS produtos_processados (
        ID_Produto VARCHAR(10),
        Nome_Produto VARCHAR(100),
        Categoria VARCHAR(50),
        Preco_Custo DECIMAL(10,2),
        Fornecedor VARCHAR(100),
        Status VARCHAR(20),
        Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Tabela de vendas processadas
    CREATE TABLE IF NOT EXISTS vendas_processadas (
        ID_Venda VARCHAR(10),
        ID_Produto VARCHAR(10),
        Quantidade_Vendida INTEGER,
        Preco_Venda DECIMAL(10,2),
        Data_Venda DATE,
        Canal_Venda VARCHAR(20),
        Receita_Total DECIMAL(10,2),
        Margem_Lucro DECIMAL(10,2),
        Mes_Venda VARCHAR(7),
        Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Tabela de relatório consolidado
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
    
    -- BÔNUS: Tabela de produtos com baixa performance
    CREATE TABLE IF NOT EXISTS produtos_baixa_performance (
        ID_Produto VARCHAR(10),
        Nome_Produto VARCHAR(100),
        Categoria VARCHAR(50),
        Preco_Custo DECIMAL(10,2),
        Status VARCHAR(20),
        Num_Vendas INTEGER,
        Total_Vendido INTEGER,
        Data_Analise TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Limpar tabelas antes de inserir novos dados (para evitar duplicatas em re-runs)
    TRUNCATE TABLE produtos_processados;
    TRUNCATE TABLE vendas_processadas;
    TRUNCATE TABLE relatorio_vendas;
    TRUNCATE TABLE produtos_baixa_performance;
    """,
    dag=dag,
)

# Task 1: Extrair produtos
extract_produtos_task = PythonOperator(
    task_id='extract_produtos',
    python_callable=extract_produtos,
    dag=dag,
)

# Task 2: Extrair vendas
extract_vendas_task = PythonOperator(
    task_id='extract_vendas',
    python_callable=extract_vendas,
    dag=dag,
)

# Task 3: Transformar dados
transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Task 5: Carregar dados
load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Task 6: Gerar relatório
generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

# BÔNUS: Detectar produtos com baixa performance
detect_low_performance_task = PythonOperator(
    task_id='detect_low_performance',
    python_callable=detect_low_performance,
    dag=dag,
)

# === DEFINIÇÃO DAS DEPENDÊNCIAS ===
# Estrutura do pipeline:
# create_tables → (extract_produtos, extract_vendas) → transform_data → load_data → (generate_report, detect_low_performance)

create_tables >> [extract_produtos_task, extract_vendas_task]
[extract_produtos_task, extract_vendas_task] >> transform_data_task
transform_data_task >> load_data_task
load_data_task >> [generate_report_task, detect_low_performance_task]
