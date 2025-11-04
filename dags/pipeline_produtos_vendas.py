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
    'owner': 'aula11',
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
    description='Pipeline ETL para produtos e vendas com relatórios',
    schedule='0 6 * * *',  # Diário às 6h da manhã
    catchup=False,
    tags=['produtos', 'vendas', 'exercicio'],
)

# ============================================
# Task 1: extract_produtos
# ============================================
def extract_produtos(**context):
    """Extrai dados do arquivo produtos_loja.csv"""
    file_path = '/opt/airflow/data/produtos_loja.csv'
    
    # Validar se o arquivo existe
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    
    logging.info(f"Extraindo dados de: {file_path}")
    
    df = pd.read_csv(file_path)
    logging.info(f"Dados extraídos: {len(df)} registros")
    
    # Salva dados extraídos para próxima tarefa
    df.to_csv('/tmp/produtos_extraidos.csv', index=False)
    
    logging.info(f"Produtos extraídos: {len(df)} registros")
    return f"Extraídos {len(df)} registros de produtos"

# ============================================
# Task 2: extract_vendas
# ============================================
def extract_vendas(**context):
    """Extrai dados do arquivo vendas_produtos.csv"""
    file_path = '/opt/airflow/data/vendas_produtos.csv'
    
    # Validar se o arquivo existe
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    
    logging.info(f"Extraindo dados de: {file_path}")
    
    df = pd.read_csv(file_path)
    logging.info(f"Dados extraídos: {len(df)} registros")
    
    # Salva dados extraídos para próxima tarefa
    df.to_csv('/tmp/vendas_extraidas.csv', index=False)
    
    logging.info(f"Vendas extraídas: {len(df)} registros")
    return f"Extraídos {len(df)} registros de vendas"

# ============================================
# Task 3: transform_data
# ============================================
def transform_data(**context):
    """Transforma e limpa os dados extraídos"""
    logging.info("Iniciando transformação dos dados")
    
    # Carrega dados extraídos
    df_produtos = pd.read_csv('/tmp/produtos_extraidos.csv')
    df_vendas = pd.read_csv('/tmp/vendas_extraidas.csv')
    
    logging.info(f"Produtos carregados: {len(df_produtos)} registros")
    logging.info(f"Vendas carregadas: {len(df_vendas)} registros")
    
    # ============================================
    # Limpeza de dados - Produtos
    # ============================================
    logging.info("Limpeza de dados - Produtos")
    
    # Preencher Preco_Custo nulo com média da categoria
    df_produtos['Preco_Custo'] = pd.to_numeric(df_produtos['Preco_Custo'], errors='coerce')
    
    # Calcula média por categoria
    media_por_categoria = df_produtos.groupby('Categoria')['Preco_Custo'].mean()
    
    # Preenche valores nulos com média da categoria
    for categoria in df_produtos['Categoria'].unique():
        mask = (df_produtos['Categoria'] == categoria) & (df_produtos['Preco_Custo'].isna())
        if mask.any():
            media_categoria = media_por_categoria.get(categoria, 0)
            df_produtos.loc[mask, 'Preco_Custo'] = media_categoria
            logging.info(f"Preenchidos {mask.sum()} Preco_Custo nulos na categoria {categoria} com média {media_categoria:.2f}")
    
    # Se ainda houver nulos (categoria sem valores), preenche com 0
    df_produtos['Preco_Custo'] = df_produtos['Preco_Custo'].fillna(0)
    
    # Preencher Fornecedor nulo com "Não Informado"
    df_produtos['Fornecedor'] = df_produtos['Fornecedor'].fillna('Não Informado')
    fornecedores_nulos = df_produtos['Fornecedor'].isna().sum()
    logging.info(f"Fornecedores nulos preenchidos: {df_produtos['Fornecedor'].isna().sum()}")
    
    # ============================================
    # Limpeza de dados - Vendas
    # ============================================
    logging.info("Limpeza de dados - Vendas")
    
    # Preencher Preco_Venda nulo com Preco_Custo * 1.3
    df_vendas['Preco_Venda'] = pd.to_numeric(df_vendas['Preco_Venda'], errors='coerce')
    
    # Merge com produtos para obter Preco_Custo
    df_vendas = df_vendas.merge(
        df_produtos[['ID_Produto', 'Preco_Custo']],
        on='ID_Produto',
        how='left'
    )
    
    # Preenche Preco_Venda nulo com Preco_Custo * 1.3
    mask_preco_venda_nulo = df_vendas['Preco_Venda'].isna()
    if mask_preco_venda_nulo.any():
        df_vendas.loc[mask_preco_venda_nulo, 'Preco_Venda'] = (
            df_vendas.loc[mask_preco_venda_nulo, 'Preco_Custo'] * 1.3
        )
        logging.info(f"Preenchidos {mask_preco_venda_nulo.sum()} Preco_Venda nulos com Preco_Custo * 1.3")
    
    # Se ainda houver nulos, preenche com 0
    df_vendas['Preco_Venda'] = df_vendas['Preco_Venda'].fillna(0)
    
    # ============================================
    # Transformações - Vendas
    # ============================================
    logging.info("Aplicando transformações")
    
    # Calcular Receita_Total = Quantidade_Vendida * Preco_Venda
    df_vendas['Quantidade_Vendida'] = pd.to_numeric(df_vendas['Quantidade_Vendida'], errors='coerce').fillna(0)
    df_vendas['Receita_Total'] = df_vendas['Quantidade_Vendida'] * df_vendas['Preco_Venda']
    
    # Criar campo Mes_Venda extraído de Data_Venda
    df_vendas['Data_Venda'] = pd.to_datetime(df_vendas['Data_Venda'], errors='coerce')
    df_vendas['Mes_Venda'] = df_vendas['Data_Venda'].dt.strftime('%Y-%m')
    df_vendas['Mes_Venda'] = df_vendas['Mes_Venda'].fillna('Desconhecido')
    
    # Guarda versão com Preco_Custo para calcular Margem_Lucro no relatório depois
    df_vendas_com_margem = df_vendas.copy()
    df_vendas_com_margem['Margem_Lucro'] = df_vendas_com_margem['Preco_Venda'] - df_vendas_com_margem['Preco_Custo']
    
    # Remove coluna Preco_Custo de vendas_processadas (não faz parte da estrutura)
    df_vendas = df_vendas.drop(columns=['Preco_Custo'], errors='ignore')
    
    # Converte Data_Venda para date (sem hora) para salvar como DATE no PostgreSQL
    # Trata valores nulos (NaT) mantendo como None
    df_vendas['Data_Venda'] = df_vendas['Data_Venda'].apply(
        lambda x: x.date() if pd.notna(x) else None
    )
    
    # ============================================
    # Salvar dados transformados
    # ============================================
    df_produtos.to_csv('/tmp/produtos_processados.csv', index=False)
    df_vendas.to_csv('/tmp/vendas_processadas.csv', index=False)
    # Salva também versão com margem para uso no relatório
    df_vendas_com_margem.to_csv('/tmp/vendas_com_margem.csv', index=False)
    
    logging.info(f"Produtos processados: {len(df_produtos)} registros")
    logging.info(f"Vendas processadas: {len(df_vendas)} registros")
    
    return f"Transformados {len(df_produtos)} produtos e {len(df_vendas)} vendas"

# ============================================
# Task 4: create_tables
# ============================================
create_tables_sql = """
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

-- Tabela relatorio_vendas (join dos dados)
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

-- Tabela produtos_baixa_performance
CREATE TABLE IF NOT EXISTS produtos_baixa_performance (
    ID_Produto VARCHAR(10),
    Nome_Produto VARCHAR(100),
    Categoria VARCHAR(50),
    Total_Vendas INTEGER,
    Receita_Total DECIMAL(10,2),
    Status VARCHAR(20),
    Data_Processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Limpar tabelas antes de inserir novos dados
TRUNCATE TABLE relatorio_vendas;
TRUNCATE TABLE produtos_processados;
TRUNCATE TABLE vendas_processadas;
TRUNCATE TABLE produtos_baixa_performance;
"""

# ============================================
# Task 5: load_data
# ============================================
def load_data(**context):
    """Carrega dados transformados nas tabelas PostgreSQL"""
    logging.info("Carregando dados no PostgreSQL")
    
    # Carrega dados transformados
    df_produtos = pd.read_csv('/tmp/produtos_processados.csv')
    df_vendas = pd.read_csv('/tmp/vendas_processadas.csv')
    
    # Conecta ao PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    # Carrega produtos_processados (sem Data_Processamento, será preenchido automaticamente)
    df_produtos.to_sql('produtos_processados', engine, if_exists='replace', index=False, method='multi')
    logging.info(f"Produtos carregados: {len(df_produtos)} registros na tabela produtos_processados")
    
    # Carrega vendas_processadas (sem Data_Processamento, será preenchido automaticamente)
    df_vendas.to_sql('vendas_processadas', engine, if_exists='replace', index=False, method='multi')
    logging.info(f"Vendas carregadas: {len(df_vendas)} registros na tabela vendas_processadas")
    
    # Criar relatorio_vendas com join e campos específicos
    df_vendas_com_margem = pd.read_csv('/tmp/vendas_com_margem.csv')
    df_produtos_load = pd.read_sql('SELECT * FROM produtos_processados', engine)
    
    # Faz join dos dados
    df_relatorio_full = df_vendas_com_margem.merge(
        df_produtos_load,
        on='ID_Produto',
        how='left'
    )
    
    # Seleciona apenas os campos necessários para relatorio_vendas
    df_relatorio = df_relatorio_full[[
        'ID_Venda',
        'Nome_Produto',
        'Categoria',
        'Quantidade_Vendida',
        'Receita_Total',
        'Margem_Lucro',
        'Canal_Venda',
        'Mes_Venda'
    ]].copy()
    
    # Carrega relatorio_vendas (sem Data_Processamento, será preenchido automaticamente)
    df_relatorio.to_sql('relatorio_vendas', engine, if_exists='replace', index=False, method='multi')
    logging.info(f"Relatório carregado: {len(df_relatorio)} registros na tabela relatorio_vendas")
    
    # ============================================
    # Validação dos dados inseridos
    # ============================================
    logging.info("Validando dados inseridos")
    
    # Verifica contagem de registros
    count_produtos = pd.read_sql('SELECT COUNT(*) as count FROM produtos_processados', engine).iloc[0]['count']
    count_vendas = pd.read_sql('SELECT COUNT(*) as count FROM vendas_processadas', engine).iloc[0]['count']
    count_relatorio = pd.read_sql('SELECT COUNT(*) as count FROM relatorio_vendas', engine).iloc[0]['count']
    
    logging.info(f"Validação - Produtos: {count_produtos} registros")
    logging.info(f"Validação - Vendas: {count_vendas} registros")
    logging.info(f"Validação - Relatório: {count_relatorio} registros")
    
    # Valida se os dados foram inseridos corretamente
    if count_produtos != len(df_produtos):
        raise ValueError(f"Erro na validação: Produtos esperados {len(df_produtos)}, encontrados {count_produtos}")
    
    if count_vendas != len(df_vendas):
        raise ValueError(f"Erro na validação: Vendas esperadas {len(df_vendas)}, encontradas {count_vendas}")
    
    logging.info("Validação concluída com sucesso!")
    
    return f"Carregados {count_produtos} produtos, {count_vendas} vendas e {count_relatorio} registros no relatório"

# ============================================
# Task 6: generate_report
# ============================================
def generate_report(**context):
    """Gera relatório com métricas de vendas"""
    logging.info("Gerando relatório de vendas")
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    # Carrega dados do relatório
    df_relatorio = pd.read_sql('SELECT * FROM relatorio_vendas', engine)
    
    if len(df_relatorio) == 0:
        logging.warning("Nenhum dado encontrado para gerar relatório")
        return "Relatório não gerado - sem dados"
    
    # ============================================
    # 1. Total de vendas por categoria
    # ============================================
    vendas_por_categoria = df_relatorio.groupby('Categoria')['Receita_Total'].sum().sort_values(ascending=False)
    logging.info("=" * 50)
    logging.info("TOTAL DE VENDAS POR CATEGORIA:")
    logging.info("=" * 50)
    for categoria, total in vendas_por_categoria.items():
        logging.info(f"{categoria}: R$ {total:,.2f}")
    
    # ============================================
    # 2. Produto mais vendido (por quantidade)
    # ============================================
    produto_mais_vendido_qtd = df_relatorio.groupby(['ID_Produto', 'Nome_Produto'])['Quantidade_Vendida'].sum().sort_values(ascending=False).head(1)
    logging.info("=" * 50)
    logging.info("PRODUTO MAIS VENDIDO (QUANTIDADE):")
    logging.info("=" * 50)
    for (produto_id, nome), quantidade in produto_mais_vendido_qtd.items():
        logging.info(f"{nome} ({produto_id}): {quantidade} unidades")
    
    # ============================================
    # 3. Canal de venda com maior receita
    # ============================================
    receita_por_canal = df_relatorio.groupby('Canal_Venda')['Receita_Total'].sum().sort_values(ascending=False).head(1)
    logging.info("=" * 50)
    logging.info("CANAL DE VENDA COM MAIOR RECEITA:")
    logging.info("=" * 50)
    for canal, receita in receita_por_canal.items():
        logging.info(f"{canal}: R$ {receita:,.2f}")
    
    # ============================================
    # 4. Margem de lucro média por categoria
    # ============================================
    margem_por_categoria = df_relatorio.groupby('Categoria')['Margem_Lucro'].mean().sort_values(ascending=False)
    logging.info("=" * 50)
    logging.info("MARGEM DE LUCRO MÉDIA POR CATEGORIA:")
    logging.info("=" * 50)
    for categoria, margem in margem_por_categoria.items():
        logging.info(f"{categoria}: R$ {margem:,.2f}")
    
    logging.info("=" * 50)
    logging.info("RELATÓRIO GERADO COM SUCESSO!")
    logging.info("=" * 50)
    
    # Resumo em formato de retorno
    return {
        'total_vendas_por_categoria': vendas_por_categoria.to_dict(),
        'produto_mais_vendido': produto_mais_vendido_qtd.to_dict(),
        'canal_maior_receita': receita_por_canal.to_dict(),
        'margem_media_por_categoria': margem_por_categoria.to_dict()
    }

# ============================================
# Task 7: detect_low_performance
# ============================================
def detect_low_performance(**context):
    """Detecta produtos com baixa performance (menos de 2 vendas)"""
    logging.info("=" * 50)
    logging.info("DETECTANDO PRODUTOS COM BAIXA PERFORMANCE")
    logging.info("=" * 50)
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    # Carrega dados de vendas processadas
    df_vendas = pd.read_sql('SELECT * FROM vendas_processadas', engine)
    
    # Carrega informações dos produtos
    df_produtos = pd.read_sql('SELECT * FROM produtos_processados', engine)
    
    if len(df_produtos) == 0:
        logging.warning("Nenhum produto encontrado para análise")
        return "Nenhum produto encontrado"
    
    # Conta número de vendas por produto (se houver vendas)
    if len(df_vendas) > 0:
        vendas_por_produto = df_vendas.groupby('ID_Produto').agg({
            'ID_Venda': 'count',  # Conta número de vendas
            'Receita_Total': 'sum'  # Soma receita total
        }).reset_index()
        
        vendas_por_produto.columns = ['ID_Produto', 'Total_Vendas', 'Receita_Total']
        
        # Faz join com todos os produtos para incluir produtos sem vendas
        produtos_com_vendas = df_produtos.merge(
            vendas_por_produto,
            on='ID_Produto',
            how='left'
        )
        
        # Preenche valores nulos (produtos sem vendas)
        produtos_com_vendas['Total_Vendas'] = produtos_com_vendas['Total_Vendas'].fillna(0).astype(int)
        produtos_com_vendas['Receita_Total'] = produtos_com_vendas['Receita_Total'].fillna(0)
    else:
        # Se não houver vendas, todos os produtos têm 0 vendas
        logging.warning("Nenhuma venda encontrada - todos os produtos serão marcados como baixa performance")
        produtos_com_vendas = df_produtos.copy()
        produtos_com_vendas['Total_Vendas'] = 0
        produtos_com_vendas['Receita_Total'] = 0
    
    # Identifica produtos com menos de 2 vendas (inclui produtos sem vendas)
    produtos_baixa_performance = produtos_com_vendas[produtos_com_vendas['Total_Vendas'] < 2].copy()
    
    # Seleciona apenas os campos necessários para a tabela
    df_resultado = produtos_baixa_performance[[
        'ID_Produto',
        'Nome_Produto',
        'Categoria',
        'Total_Vendas',
        'Receita_Total',
        'Status'
    ]].copy()
    
    # ============================================
    # Alerta por log
    # ============================================
    if len(df_resultado) > 0:
        logging.warning("=" * 50)
        logging.warning("⚠️  ALERTA: PRODUTOS COM BAIXA PERFORMANCE DETECTADOS!")
        logging.warning("=" * 50)
        logging.warning(f"Total de produtos com menos de 2 vendas: {len(df_resultado)}")
        logging.warning("")
        
        for idx, row in df_resultado.iterrows():
            logging.warning(
                f"⚠️  Produto: {row['Nome_Produto']} (ID: {row['ID_Produto']}) | "
                f"Categoria: {row['Categoria']} | "
                f"Vendas: {row['Total_Vendas']} | "
                f"Receita: R$ {row['Receita_Total']:,.2f} | "
                f"Status: {row['Status']}"
            )
        
        logging.warning("=" * 50)
        logging.warning("Ação recomendada: Revisar estratégia de vendas para estes produtos")
        logging.warning("=" * 50)
    else:
        logging.info("✅ Nenhum produto com baixa performance detectado. Todos os produtos têm 2 ou mais vendas.")
    
    # ============================================
    # Carrega dados na tabela produtos_baixa_performance
    # ============================================
    if len(df_resultado) > 0:
        df_resultado.to_sql(
            'produtos_baixa_performance',
            engine,
            if_exists='replace',
            index=False,
            method='multi'
        )
        logging.info(f"Produtos com baixa performance carregados: {len(df_resultado)} registros na tabela produtos_baixa_performance")
    else:
        # Limpa tabela se não houver produtos com baixa performance
        # Cria dataframe vazio com estrutura correta e carrega na tabela
        df_vazio = pd.DataFrame(columns=[
            'ID_Produto',
            'Nome_Produto',
            'Categoria',
            'Total_Vendas',
            'Receita_Total',
            'Status'
        ])
        df_vazio.to_sql(
            'produtos_baixa_performance',
            engine,
            if_exists='replace',
            index=False,
            method='multi'
        )
        logging.info("Tabela produtos_baixa_performance limpa (nenhum produto com baixa performance)")
    
    return f"Detectados {len(df_resultado)} produtos com baixa performance"

# ============================================
# Definição das Tarefas
# ============================================

# Task 1: extract_produtos
extract_produtos_task = PythonOperator(
    task_id='extract_produtos',
    python_callable=extract_produtos,
    dag=dag,
)

# Task 2: extract_vendas
extract_vendas_task = PythonOperator(
    task_id='extract_vendas',
    python_callable=extract_vendas,
    dag=dag,
)

# Task 3: transform_data
transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Task 4: create_tables
create_tables_task = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql=create_tables_sql,
    dag=dag,
)

# Task 5: load_data
load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Task 6: generate_report
generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

# Task 7: detect_low_performance
detect_low_performance_task = PythonOperator(
    task_id='detect_low_performance',
    python_callable=detect_low_performance,
    dag=dag,
)

# ============================================
# Definição das Dependências
# ============================================
# Extract tasks podem rodar em paralelo
# Transform depende de ambos extract
# Create tables pode rodar antes ou junto com transform
# Load depende de transform e create_tables
# Report e detect_low_performance dependem de load (podem rodar em paralelo)

[extract_produtos_task, extract_vendas_task] >> transform_data_task
create_tables_task >> load_data_task
transform_data_task >> load_data_task
load_data_task >> [generate_report_task, detect_low_performance_task]

