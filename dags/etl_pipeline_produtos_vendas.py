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
    'owner': '6324518',
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

def extract_produtos(**context):
    """Task 1: Extrai dados do arquivo produtos_loja.csv"""
    file_path = '/opt/airflow/data/produtos_loja.csv'
    logging.info(f"Extraindo dados de produtos de: {file_path}")
    
    # Validar se o arquivo existe
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    
    df = pd.read_csv(file_path)
    logging.info(f"Dados de produtos extraídos: {len(df)} registros")
    
    # Salva dados extraídos para próxima tarefa
    df.to_csv('/tmp/produtos_extraidos.csv', index=False)
    return f"Extraídos {len(df)} registros de produtos"

def extract_vendas(**context):
    """Task 2: Extrai dados do arquivo vendas_produtos.csv"""
    file_path = '/opt/airflow/data/vendas_produtos.csv'
    logging.info(f"Extraindo dados de vendas de: {file_path}")
    
    # Validar se o arquivo existe
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    
    df = pd.read_csv(file_path)
    logging.info(f"Dados de vendas extraídos: {len(df)} registros")
    
    # Salva dados extraídos para próxima tarefa
    df.to_csv('/tmp/vendas_extraidas.csv', index=False)
    return f"Extraídos {len(df)} registros de vendas"

def transform_data(**context):
    """Task 3: Transforma e limpa os dados extraídos"""
    logging.info("Iniciando transformação dos dados")
    
    # Carrega dados extraídos
    df_produtos = pd.read_csv('/tmp/produtos_extraidos.csv')
    df_vendas = pd.read_csv('/tmp/vendas_extraidas.csv')
    
    logging.info(f"Produtos carregados: {len(df_produtos)} registros")
    logging.info(f"Vendas carregadas: {len(df_vendas)} registros")
    
    # === LIMPEZA DE DADOS ===
    
    # 1. Preencher Preco_Custo nulo com média da categoria
    for categoria in df_produtos['Categoria'].unique():
        mask_categoria = df_produtos['Categoria'] == categoria
        media_categoria = df_produtos[mask_categoria]['Preco_Custo'].mean()
        df_produtos.loc[mask_categoria & df_produtos['Preco_Custo'].isna(), 'Preco_Custo'] = media_categoria
    
    # 2. Preencher Fornecedor nulo com "Não Informado"
    df_produtos['Fornecedor'] = df_produtos['Fornecedor'].fillna('Não Informado')
    
    # 3. Preencher Preco_Venda nulo com Preco_Custo * 1.3
    # Primeiro, fazer merge para ter acesso ao Preco_Custo
    df_vendas_merged = df_vendas.merge(df_produtos[['ID_Produto', 'Preco_Custo']], on='ID_Produto', how='left')
    df_vendas_merged['Preco_Venda'] = df_vendas_merged['Preco_Venda'].fillna(df_vendas_merged['Preco_Custo'] * 1.3)
    
    # === TRANSFORMAÇÕES ===
    
    # 4. Calcular Receita_Total = Quantidade_Vendida * Preco_Venda
    df_vendas_merged['Receita_Total'] = df_vendas_merged['Quantidade_Vendida'] * df_vendas_merged['Preco_Venda']
    
    # 5. Calcular Margem_Lucro = Preco_Venda - Preco_Custo
    df_vendas_merged['Margem_Lucro'] = df_vendas_merged['Preco_Venda'] - df_vendas_merged['Preco_Custo']
    
    # 6. Criar campo Mes_Venda extraído de Data_Venda
    df_vendas_merged['Data_Venda'] = pd.to_datetime(df_vendas_merged['Data_Venda'])
    df_vendas_merged['Mes_Venda'] = df_vendas_merged['Data_Venda'].dt.strftime('%Y-%m')
    
    # Remover coluna auxiliar Preco_Custo das vendas
    df_vendas_final = df_vendas_merged.drop('Preco_Custo', axis=1)
    
    logging.info(f"Dados transformados - Produtos: {len(df_produtos)} registros")
    logging.info(f"Dados transformados - Vendas: {len(df_vendas_final)} registros")
    
    # Salva dados transformados
    df_produtos.to_csv('/tmp/produtos_transformados.csv', index=False)
    df_vendas_final.to_csv('/tmp/vendas_transformadas.csv', index=False)
    
    return f"Transformados {len(df_produtos)} produtos e {len(df_vendas_final)} vendas"

def load_data(**context):
    """Task 5: Carrega dados transformados nas tabelas PostgreSQL"""
    logging.info("Carregando dados transformados no PostgreSQL")
    
    # Carrega dados transformados
    df_produtos = pd.read_csv('/tmp/produtos_transformados.csv')
    df_vendas = pd.read_csv('/tmp/vendas_transformadas.csv')
    
    # Conecta ao PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    # Carrega dados nas tabelas
    df_produtos.to_sql('produtos_processados', engine, if_exists='replace', index=False, method='multi')
    df_vendas.to_sql('vendas_processadas', engine, if_exists='replace', index=False, method='multi')
    
    # Criar tabela de relatório (join dos dados)
    df_relatorio = df_vendas.merge(
        df_produtos[['ID_Produto', 'Nome_Produto', 'Categoria']], 
        on='ID_Produto', 
        how='left'
    )
    
    # Selecionar apenas as colunas necessárias para o relatório
    df_relatorio_final = df_relatorio[[
        'ID_Venda', 'Nome_Produto', 'Categoria', 'Quantidade_Vendida',
        'Receita_Total', 'Margem_Lucro', 'Canal_Venda', 'Mes_Venda'
    ]]
    
    df_relatorio_final.to_sql('relatorio_vendas', engine, if_exists='replace', index=False, method='multi')
    
    logging.info(f"Dados carregados:")
    logging.info(f"- produtos_processados: {len(df_produtos)} registros")
    logging.info(f"- vendas_processadas: {len(df_vendas)} registros")
    logging.info(f"- relatorio_vendas: {len(df_relatorio_final)} registros")
    
    # Validar se os dados foram inseridos corretamente
    with engine.connect() as conn:
        count_produtos = conn.execute("SELECT COUNT(*) FROM produtos_processados").scalar()
        count_vendas = conn.execute("SELECT COUNT(*) FROM vendas_processadas").scalar()
        count_relatorio = conn.execute("SELECT COUNT(*) FROM relatorio_vendas").scalar()
        
        logging.info(f"Validação - Registros inseridos:")
        logging.info(f"- produtos_processados: {count_produtos}")
        logging.info(f"- vendas_processadas: {count_vendas}")
        logging.info(f"- relatorio_vendas: {count_relatorio}")
    
    return f"Carregados {len(df_produtos)} produtos, {len(df_vendas)} vendas e {len(df_relatorio_final)} relatórios"

def generate_report(**context):
    """Task 6: Gera relatório com métricas de negócio"""
    logging.info("Gerando relatório de vendas")
    
    # Conecta ao PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as conn:
        # Primeiro, vamos verificar a estrutura da tabela
        try:
            columns_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'relatorio_vendas'
            ORDER BY ordinal_position
            """
            columns_df = pd.read_sql(columns_query, conn)
            logging.info(f"Colunas disponíveis na tabela relatorio_vendas: {list(columns_df['column_name'])}")
        except Exception as e:
            logging.error(f"Erro ao verificar colunas: {e}")
        
        # 1. Total de vendas por categoria (usando aspas duplas para case-sensitive)
        query_vendas_categoria = '''
        SELECT "Categoria", 
               SUM("Receita_Total") as Total_Vendas,
               COUNT(*) as Num_Vendas
        FROM relatorio_vendas 
        GROUP BY "Categoria"
        ORDER BY Total_Vendas DESC
        '''
        try:
            df_vendas_categoria = pd.read_sql(query_vendas_categoria, conn)
            logging.info("=== TOTAL DE VENDAS POR CATEGORIA ===")
            for _, row in df_vendas_categoria.iterrows():
                logging.info(f"{row['Categoria']}: R$ {row['Total_Vendas']:.2f} ({row['Num_Vendas']} vendas)")
        except Exception as e:
            logging.error(f"Erro na consulta de vendas por categoria: {e}")
        
        # 2. Produto mais vendido
        query_produto_mais_vendido = '''
        SELECT "Nome_Produto", 
               SUM("Quantidade_Vendida") as Total_Quantidade,
               SUM("Receita_Total") as Total_Receita
        FROM relatorio_vendas 
        GROUP BY "Nome_Produto"
        ORDER BY Total_Quantidade DESC 
        LIMIT 1
        '''
        try:
            df_produto_top = pd.read_sql(query_produto_mais_vendido, conn)
            if not df_produto_top.empty:
                produto_top = df_produto_top.iloc[0]
                logging.info("=== PRODUTO MAIS VENDIDO ===")
                logging.info(f"Produto: {produto_top['Nome_Produto']}")
                logging.info(f"Quantidade vendida: {produto_top['Total_Quantidade']}")
                logging.info(f"Receita total: R$ {produto_top['Total_Receita']:.2f}")
        except Exception as e:
            logging.error(f"Erro na consulta de produto mais vendido: {e}")
        
        # 3. Canal de venda com maior receita
        query_canal_receita = '''
        SELECT "Canal_Venda", 
               SUM("Receita_Total") as Total_Receita,
               COUNT(*) as Num_Vendas
        FROM relatorio_vendas 
        GROUP BY "Canal_Venda"
        ORDER BY Total_Receita DESC
        '''
        try:
            df_canal_receita = pd.read_sql(query_canal_receita, conn)
            logging.info("=== RECEITA POR CANAL DE VENDA ===")
            for _, row in df_canal_receita.iterrows():
                logging.info(f"{row['Canal_Venda']}: R$ {row['Total_Receita']:.2f} ({row['Num_Vendas']} vendas)")
        except Exception as e:
            logging.error(f"Erro na consulta de canal de receita: {e}")
        
        # 4. Margem de lucro média por categoria
        query_margem_categoria = '''
        SELECT "Categoria", 
               AVG("Margem_Lucro") as Margem_Media,
               COUNT(*) as Num_Vendas
        FROM relatorio_vendas 
        GROUP BY "Categoria"
        ORDER BY Margem_Media DESC
        '''
        try:
            df_margem_categoria = pd.read_sql(query_margem_categoria, conn)
            logging.info("=== MARGEM DE LUCRO MÉDIA POR CATEGORIA ===")
            for _, row in df_margem_categoria.iterrows():
                logging.info(f"{row['Categoria']}: R$ {row['Margem_Media']:.2f} ({row['Num_Vendas']} vendas)")
        except Exception as e:
            logging.error(f"Erro na consulta de margem por categoria: {e}")
    
    return "Relatório gerado com sucesso"

def detect_low_performance_products(**context):
    """Desafio Bônus: Detecta produtos com baixa performance"""
    logging.info("Detectando produtos com baixa performance")
    
    # Conecta ao PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as conn:
        # Produtos com menos de 2 vendas
        query_baixa_performance = '''
        SELECT p."ID_Produto", p."Nome_Produto", p."Categoria", 
               COALESCE(SUM(v."Quantidade_Vendida"), 0) as Total_Vendido
        FROM produtos_processados p
        LEFT JOIN vendas_processadas v ON p."ID_Produto" = v."ID_Produto"
        GROUP BY p."ID_Produto", p."Nome_Produto", p."Categoria"
        HAVING COALESCE(SUM(v."Quantidade_Vendida"), 0) < 2
        ORDER BY Total_Vendido ASC
        '''
        try:
            df_baixa_performance = pd.read_sql(query_baixa_performance, conn)
            
            if not df_baixa_performance.empty:
                logging.warning("=== ALERTA: PRODUTOS COM BAIXA PERFORMANCE ===")
                logging.warning(f"Encontrados {len(df_baixa_performance)} produtos com menos de 2 vendas:")
                
                for _, row in df_baixa_performance.iterrows():
                    logging.warning(f"- {row['Nome_Produto']} ({row['Categoria']}): {row['Total_Vendido']} vendas")
                
                # Criar tabela de produtos com baixa performance
                df_baixa_performance.to_sql('produtos_baixa_performance', engine, if_exists='replace', index=False, method='multi')
                logging.info(f"Tabela 'produtos_baixa_performance' criada com {len(df_baixa_performance)} registros")
            else:
                logging.info("Nenhum produto com baixa performance encontrado")
                
            return f"Análise concluída - {len(df_baixa_performance) if not df_baixa_performance.empty else 0} produtos com baixa performance"
        except Exception as e:
            logging.error(f"Erro na análise de baixa performance: {e}")
            return "Erro na análise de baixa performance"

# Task 4: Criar tabelas
create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql="""
    -- Criar tabela produtos_processados
    DROP TABLE IF EXISTS produtos_processados CASCADE;
    CREATE TABLE produtos_processados (
        "ID_Produto" VARCHAR(10),
        "Nome_Produto" VARCHAR(100),
        "Categoria" VARCHAR(50),
        "Preco_Custo" DECIMAL(10,2),
        "Fornecedor" VARCHAR(100),
        "Status" VARCHAR(20),
        "Data_Processamento" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Criar tabela vendas_processadas
    DROP TABLE IF EXISTS vendas_processadas CASCADE;
    CREATE TABLE vendas_processadas (
        "ID_Venda" VARCHAR(10),
        "ID_Produto" VARCHAR(10),
        "Quantidade_Vendida" INTEGER,
        "Preco_Venda" DECIMAL(10,2),
        "Data_Venda" DATE,
        "Canal_Venda" VARCHAR(20),
        "Receita_Total" DECIMAL(10,2),
        "Margem_Lucro" DECIMAL(10,2),
        "Mes_Venda" VARCHAR(7),
        "Data_Processamento" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Criar tabela relatorio_vendas
    DROP TABLE IF EXISTS relatorio_vendas CASCADE;
    CREATE TABLE relatorio_vendas (
        "ID_Venda" VARCHAR(10),
        "Nome_Produto" VARCHAR(100),
        "Categoria" VARCHAR(50),
        "Quantidade_Vendida" INTEGER,
        "Receita_Total" DECIMAL(10,2),
        "Margem_Lucro" DECIMAL(10,2),
        "Canal_Venda" VARCHAR(20),
        "Mes_Venda" VARCHAR(7),
        "Data_Processamento" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Criar tabela produtos_baixa_performance (Desafio Bônus)
    DROP TABLE IF EXISTS produtos_baixa_performance CASCADE;
    CREATE TABLE produtos_baixa_performance (
        "ID_Produto" VARCHAR(10),
        "Nome_Produto" VARCHAR(100),
        "Categoria" VARCHAR(50),
        "Total_Vendido" INTEGER,
        "Data_Processamento" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)

# Definir as tarefas Python
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

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

# Desafio Bônus
detect_low_performance_task = PythonOperator(
    task_id='detect_low_performance_products',
    python_callable=detect_low_performance_products,
    dag=dag,
)

# Definição das dependências
create_tables >> [extract_produtos_task, extract_vendas_task] >> transform_data_task >> load_data_task >> generate_report_task >> detect_low_performance_task